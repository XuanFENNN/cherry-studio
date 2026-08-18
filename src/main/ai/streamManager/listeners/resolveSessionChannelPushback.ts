import { application } from '@application'
import { agentChannelService } from '@data/services/AgentChannelService'
import { channelMessageHandler } from '@main/ai/channels'

import { ChannelAdapterListener } from './ChannelAdapterListener'
import type { StreamListener } from '../types'

/**
 * 后台唤醒响应路由回频道（方案 A 的核心解析器）。
 *
 * 场景：微信消息发起 → agent 把长任务丢给后台 → 后台完成「唤醒 agent 继续」产生的新回应
 * 启动新 turn 时，把「会话 ↔ 频道」的绑定翻译成 `ChannelAdapterListener`，使回应流式
 * 推回微信/IM 频道，而不是只出现在桌面端 CherryStudio 界面。
 *
 * 绑定解析顺序：
 *   1) 内存反向索引（channelMessageHandler.getChatBindingsForSession，精确到 chat，
 *      `/new` 换会话后进程内仍精确，重启才丢失）
 *   2) 持久绑定（agentChannelService.findBySessionId，重启兜底；chat 集合取
 *      union(adapter.notifyChatIds, channel.activeChatIds ?? [])，精度降级为频道级广播）
 *   3) 无绑定 → 返回 []
 *
 * 幂等：existingIds 中已有的 `channel:${channelId}:${chatId}` 一律跳过，不覆盖调用方
 * 自带 listener（如 runAgentTask 的 suppressErrorMessage=true 订阅 listener 优先级不变）。
 * 每个 (channelId, chatId) 构建 `new ChannelAdapterListener(adapter, chatId, false, undefined,
 * baselineText)`，与频道入站一致：不 suppress 错误文案、无 replyToMessageId（唤醒无入站消息
 * 可 reply）；baselineText 由调用方按本 turn 请求的延续锚点解析（见 extractReplayBaselineText）。
 */

/**
 * 从请求消息列表提取「本 turn 会以 text-delta 重放的基线文本」。
 *
 * AI SDK 的延续（continue-conversation）语义下，请求最后一条为 assistant 消息（延续锚点）
 * 时，UIMessageStream 会先把该锚点消息已有的 text 部分以 text-delta 整段重放，随后才是本
 * turn 新生成的内容。拼接锚点消息全部 text 部分即为重放文本；请求末条为 user（普通新回合，
 * 无重放）或消息列表为空时返回空串。
 */
export function extractReplayBaselineText(
  messages: ReadonlyArray<{ role?: string; parts?: ReadonlyArray<{ type?: string; text?: unknown }> }> | undefined
): string {
  const anchor = messages?.at(-1)
  if (!anchor || anchor.role !== 'assistant' || !Array.isArray(anchor.parts)) return ''
  let baseline = ''
  for (const part of anchor.parts) {
    if (part && part.type === 'text' && typeof part.text === 'string') baseline += part.text
  }
  return baseline
}

export function resolveSessionChannelListeners(input: {
  sessionId: string
  existingIds: ReadonlySet<string>
  /**
   * 本 turn 会以 text-delta 重放的基线文本（延续锚点既有文本，见 `extractReplayBaselineText`）。
   * 传给 `ChannelAdapterListener` 后只推送基线之后的新增部分，避免把上一回合全文重复推回频道。
   */
  baselineText?: string
}): StreamListener[] {
  const bindings = resolveChannelBindings(input.sessionId)
  if (bindings.length === 0) return []

  const channelManager = application.get('ChannelManager')
  const listeners: StreamListener[] = []
  const seen = new Set<string>()
  for (const { channelId, chatId } of bindings) {
    const adapter = channelManager.getAdapter(channelId)
    if (!adapter) continue
    const id = `channel:${channelId}:${chatId}`
    // 幂等去重：跳过本次已挂的，以及调用方自带 listener 已覆盖的（id 相同即同一目标）
    if (seen.has(id) || input.existingIds.has(id)) continue
    seen.add(id)
    listeners.push(new ChannelAdapterListener(adapter, chatId, false, undefined, input.baselineText ?? ''))
  }
  return listeners
}

/** 解析会话 → (channelId, chatId) 绑定：内存反向索引优先，持久绑定兜底。 */
function resolveChannelBindings(sessionId: string): Array<{ channelId: string; chatId: string }> {
  // ① 内存反向索引（精确到 chat；进程内 /new 后仍指向最新会话）
  const memoryBindings = channelMessageHandler.getChatBindingsForSession(sessionId)
  if (memoryBindings.length > 0) return memoryBindings

  // ② 持久绑定兜底（重启后内存索引丢失，降级为频道级：union 该频道的可投递 chat 集合）
  const channel = agentChannelService.findBySessionId(sessionId)
  if (!channel) return [] // ③ 无绑定 → 不推送（后台任务新建独立会话即此情形）

  const adapter = application.get('ChannelManager').getAdapter(channel.id)
  if (!adapter) return []

  // chat 集合：union(adapter.notifyChatIds, channel.activeChatIds ?? [])，按 chatId 去重，
  // 两种配置模式（allowed_chat_ids / DB 累积 activeChatIds）都覆盖。
  const chatIds = new Set<string>(adapter.notifyChatIds)
  for (const chatId of channel.activeChatIds ?? []) chatIds.add(chatId)
  return [...chatIds].map((chatId) => ({ channelId: channel.id, chatId }))
}
