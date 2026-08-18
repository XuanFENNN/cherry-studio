import { loggerService } from '@logger'
import { type ChannelAdapter, sanitizeChannelOutput } from '@main/ai/channels'
import type { UniqueModelId } from '@shared/data/types/model'
import type { UIMessageChunk } from 'ai'

import type { StreamDoneResult, StreamErrorResult, StreamListener, StreamPausedResult } from '../types'

const logger = loggerService.withContext('ChannelAdapterListener')
const INCOMPLETE_CITATION_MARKER_PATTERN = /[ \t]?\[(?:c(?:i(?:t(?:e(?::[\w-]*)?)?)?)?)?$/

/** IM-channel sink (Discord / Slack / Feishu / Telegram / etc). */
export class ChannelAdapterListener implements StreamListener {
  readonly id: string
  private accumulatedText = ''

  constructor(
    private readonly adapter: ChannelAdapter,
    private readonly platformChatId: string,
    /**
     * Skip the generic `Error: …` channel message on failure. Scheduled-task runs
     * deliver a richer `[Task failed] …` summary themselves (see `runAgentTask`), so
     * leaving this on would double-notify every subscribed channel.
     */
    private readonly suppressErrorMessage = false,
    /** Inbound message id this run answers, so the reply targets it (e.g. QQ passive reply). */
    private readonly replyToMessageId?: string | number,
    /**
     * 本 turn 启动时会以 text-delta 重放的「基线文本」（会话已存在的 assistant 文本）。
     *
     * AI SDK 的延续（continue-conversation）语义下，请求最后一条 assistant 锚点消息的既有
     * 文本会以 text-delta 形式整段重放给所有监听器，随后才是本 turn 真正新生成的内容。后台
     * 唤醒 turn 正属此类：若从零累计，会把「上一回合全文 + 新输出」整段当作未发送文本推送。
     *
     * 传入后，onChunk / onDone / onPaused 只把「累计文本 - 基线」视为本 turn 新增部分推送，
     * 重放阶段静默跳过。普通新回合（请求末条为 user）无重放，保持默认空串、行为不变。
     */
    private readonly baselineText = ''
  ) {
    this.id = `channel:${adapter.channelId}:${this.platformChatId}`
  }

  /**
   * 累计文本中超出基线的新增部分。
   * - 累计以基线开头（重放与锚点一致）→ 裁剪出新增尾部；
   * - 累计尚未达到基线（重放中断，如 turn 被 abort）→ 无新增，返回空串，绝不推送旧文本残片；
   * - 长度超过基线但前缀不匹配（基线过期/锚点与重放不一致的防御降级）→ 原样返回，绝不误删新内容。
   */
  private newText(): string {
    if (!this.baselineText) return this.accumulatedText
    if (this.accumulatedText.startsWith(this.baselineText)) {
      return this.accumulatedText.slice(this.baselineText.length)
    }
    return this.accumulatedText.length <= this.baselineText.length ? '' : this.accumulatedText
  }

  /** Deliver a final message, threading the reply target only when this run has one. */
  private deliver(text: string): Promise<void> {
    return this.replyToMessageId !== undefined
      ? this.adapter.sendMessage(this.platformChatId, text, { replyToMessageId: this.replyToMessageId })
      : this.adapter.sendMessage(this.platformChatId, text)
  }

  // oxlint-disable-next-line no-unused-vars
  onChunk(chunk: UIMessageChunk, _sourceModelId?: UniqueModelId): void {
    if (chunk.type === 'text-delta' && chunk.delta) {
      this.accumulatedText += chunk.delta
      // 重放阶段（累计尚未超过基线）无新增内容，不推送；只把基线之后的新增部分交给适配器。
      // Best-effort streaming update; adapter chooses to throttle. Sanitize here — this is
      // the live delivery path that reaches the IM platform, so secrets (keys/tokens) must
      // be redacted before they leave.
      const { text } = sanitizeChannelOutput(this.newText())
      const effective = text.replace(INCOMPLETE_CITATION_MARKER_PATTERN, '')
      if (effective) {
        void this.adapter.onTextUpdate(this.platformChatId, effective).catch(() => {})
      }
    }
  }

  async onDone(result: StreamDoneResult): Promise<void> {
    const text = sanitizeChannelOutput(this.newText()).text.trim()
    if (!text) {
      logger.warn('ChannelAdapterListener.onDone with empty text', {
        channelId: this.adapter.channelId,
        chatId: this.platformChatId,
        status: result.status,
        hasBaseline: this.baselineText.length > 0
      })
      return
    }

    try {
      // Adapter finalizes its streaming UI first (e.g. close Feishu card).
      const handled = await this.adapter.onStreamComplete(this.platformChatId, text)
      if (!handled) {
        await this.deliver(text)
      }
    } catch (err) {
      logger.error('Failed to deliver message to channel', {
        channelId: this.adapter.channelId,
        chatId: this.platformChatId,
        err
      })
    }
  }

  // oxlint-disable-next-line no-unused-vars
  async onPaused(_result: StreamPausedResult): Promise<void> {
    const text = sanitizeChannelOutput(this.newText()).text.trim()
    if (!text) return

    try {
      const handled = await this.adapter.onStreamComplete(this.platformChatId, text)
      if (!handled) {
        await this.deliver(text + '\n\n_(stopped)_')
      }
    } catch (err) {
      logger.error('Failed to deliver paused message to channel', {
        channelId: this.adapter.channelId,
        chatId: this.platformChatId,
        err
      })
    }
  }

  async onError(result: StreamErrorResult): Promise<void> {
    if (this.suppressErrorMessage) return
    try {
      await this.deliver(`Error: ${result.error.message ?? 'Unknown error'}`)
    } catch (err) {
      logger.error('Failed to deliver error to channel', {
        channelId: this.adapter.channelId,
        chatId: this.platformChatId,
        err
      })
    }
  }

  isAlive(): boolean {
    return this.adapter.connected
  }
}
