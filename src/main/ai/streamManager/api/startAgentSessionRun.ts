import { application } from '@application'
import { agentSessionService } from '@data/services/AgentSessionService'
import { ErrorCode, isDataApiError } from '@shared/data/api/errors'
import type { CherryMessagePart } from '@shared/data/types/message'

import { buildAgentSessionTopicId } from '../../agentSession/topic'
import { agentChatContextProvider } from '../context/AgentChatContextProvider'
import { extractReplayBaselineText, resolveSessionChannelListeners } from '../listeners/resolveSessionChannelPushback'
import type { StreamListener } from '../types'

/**
 * Start (or inject into) an agent-session stream from a non-renderer caller.
 *
 * Encapsulates the user/assistant persistence + driver turn-begin done by
 * `AgentChatContextProvider`, so schedulers, channel inbound handlers, and
 * other backend triggers go through the same path as the renderer instead
 * of hand-rolling a `manager.send` call.
 *
 * The first listener is treated as the primary subscriber (gets the
 * `runtime.listeners` augmentation from the context provider); any
 * additional listeners are appended verbatim.
 *
 * Lives alongside `dispatch.ts` because stream-manager already owns the
 * downward dependency on agent-session (`AgentChatContextProvider` imports
 * ai/runtime + agent-session/topic). Putting this facade here
 * keeps the direction one-way; if it lived in agent-session/ the package
 * graph would loop back through stream-manager/context.
 */
export type StartAgentSessionRunResult =
  | { mode: 'started' }
  | { mode: 'not-started'; reason: 'busy' | 'session-invalid' }

export async function startAgentSessionRun(input: {
  sessionId: string
  userParts: CherryMessagePart[]
  listeners: StreamListener[]
  headless?: boolean
  requireIdle?: { expectedAgentId: string }
  /** 后台唤醒响应是否自动推回会话绑定的频道（默认 true；静默后台维护 turn 可显式传 false 关掉） */
  channelPushback?: boolean
}): Promise<StartAgentSessionRunResult> {
  if (input.listeners.length === 0) {
    throw new Error('startAgentSessionRun requires at least one listener')
  }
  const [primary, ...extras] = input.listeners

  const topicId = buildAgentSessionTopicId(input.sessionId)
  const manager = application.get('AiStreamManager')
  let result: StartAgentSessionRunResult = { mode: 'not-started', reason: 'session-invalid' }

  // Hold the per-topic dispatch lock around the whole `hasLiveStream → prepareDispatch
  // (writes a PENDING placeholder) → send` window, the same as the renderer's `dispatch()`.
  // Two backend triggers (scheduled tasks, channel inbound) can fire on one session topic
  // concurrently — or race a renderer open — and without this both could observe no live
  // stream and each write a placeholder, orphaning one as a permanently "thinking" row.
  await manager.withDispatchLock(topicId, async () => {
    // Write-quiesce admission gate (backup restore), re-checked under the lock and BEFORE
    // `prepareDispatch` writes the user/pending-assistant rows. Both callers handle the
    // rejection: an `agent.task` job settles failed-retryable; channel inbound notifies the
    // user — and per the restore orchestration order, channel batches are flushed and
    // admitted before the AI pause, so this throw only fires for out-of-order callers.
    if (manager.isWriteQuiesced) {
      throw new Error(
        'AiStreamManager is write-quiesced (backup restore in progress); refusing a new agent-session turn'
      )
    }

    if (input.requireIdle) {
      if (
        manager.hasLiveStream(topicId) ||
        application.get('AgentSessionRuntimeService').isSessionBusy(input.sessionId)
      ) {
        result = { mode: 'not-started', reason: 'busy' }
        return
      }
      try {
        const session = agentSessionService.getById(input.sessionId)
        if (session.agentId !== input.requireIdle.expectedAgentId) {
          result = { mode: 'not-started', reason: 'session-invalid' }
          return
        }
      } catch (error) {
        if (isDataApiError(error) && error.code === ErrorCode.NOT_FOUND) {
          result = { mode: 'not-started', reason: 'session-invalid' }
          return
        }
        throw error
      }
    }

    let prepared
    try {
      prepared = await agentChatContextProvider.prepareDispatch(
        primary,
        {
          trigger: 'submit-message',
          topicId,
          userMessageParts: input.userParts,
          headless: input.headless === true
        },
        {
          hasLiveStream: false,
          requireIdle: input.requireIdle !== undefined,
          expectedAgentId: input.requireIdle?.expectedAgentId
        }
      )
    } catch (error) {
      if (input.requireIdle && isDataApiError(error) && error.code === ErrorCode.RESOURCE_LOCKED) {
        result = { mode: 'not-started', reason: 'busy' }
        return
      }
      if (input.requireIdle && isDataApiError(error) && error.code === ErrorCode.NOT_FOUND) {
        result = { mode: 'not-started', reason: 'session-invalid' }
        return
      }
      throw error
    }

    // 组装最终 listener 列表（保持 requireIdle 下原有的顺序语义：primary 在最前）
    const allListeners = input.requireIdle
      ? [primary, ...extras, ...prepared.listeners.filter((listener) => listener.id !== primary.id)]
      : [...prepared.listeners, ...extras]

    // 后台唤醒响应自动推回频道（方案 A + 门控）：在 extras 之后追加会话绑定频道的
    // channel listener。频道入站自带 / runAgentTask 订阅的 `channel:…` listener 已在
    // existingIds 中 → 幂等跳过（不覆盖调用方自带 listener 的选项）；外部唤醒注入
    // （调用方不知道微信 chatId）→ 自动补挂，把回应流式推回会话所属频道。
    const channelPushbackListeners =
      input.channelPushback !== false
        ? resolveSessionChannelListeners({
            sessionId: input.sessionId,
            existingIds: new Set(allListeners.map((listener) => listener.id)),
            // 频道入站 / 定时任务由 prepareDispatch 构建请求，末条通常为 user（普通新回合，
            // 无历史重放）→ 基线为空串，行为不变；若未来出现延续语义，同样只推送基线之后的新增。
            baselineText: extractReplayBaselineText(prepared.models[0]?.request.messages)
          })
        : []

    manager.send({
      topicId: prepared.topicId,
      models: prepared.models,
      listeners: [...allListeners, ...channelPushbackListeners],
      siblingsGroupId: prepared.siblingsGroupId,
      lifecycle: prepared.lifecycle
    })
    result = { mode: 'started' }
  })
  return result
}
