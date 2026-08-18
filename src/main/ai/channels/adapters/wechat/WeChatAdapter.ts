import { application } from '@application'
import { agentChannelService } from '@data/services/AgentChannelService'
import { agentSessionService } from '@data/services/AgentSessionService'
import { WindowType } from '@main/core/window/types'
import type { FileAttachment, ImageAttachment } from '@main/utils/downloadAsBase64'
import { parseDataUrl } from '@shared/utils/dataUrl'

import { ChannelAdapter, type ChannelAdapterConfig, type SendMessageOptions } from '../../ChannelAdapter'
import { registerAdapterFactory } from '../../ChannelManager'
import { isSlashCommand } from '../../constants'
import { FlushController } from '../../FlushController'
import { resolveLocalFile } from '../../security/localFileResolver'
import { resolveWorkspaceFile } from '../../security/WorkspaceFileGuard'
import { FILE_EXTENSION_MIME_MAP, splitMessage } from '../../utils'
import { type IncomingMessage, WeixinBot } from './WeChatProtocol'

const WECHAT_MAX_LENGTH = 2000

/** Minimum interval between incremental stream bubbles (typewriter pacing). */
const STREAM_THROTTLE_MS = 300

/** Prefix prepended to each thinking (reasoning) bubble. */
const THINKING_PREFIX = '【思考中】'

/** Markdown image reference: `![alt](path)` (optional quoted title tolerated). */
const MARKDOWN_IMAGE_REF_RE = /!\[[^\]]*\]\(([^)]*)\)/g

/**
 * Remove complete markdown image references from text — their media is delivered
 * as real images instead (see `extractAndSendImages`).
 */
function stripMarkdownImageRefs(text: string): string {
  return text.replace(MARKDOWN_IMAGE_REF_RE, '')
}

/**
 * Hold back a trailing, still-incomplete markdown image reference so the user
 * never sees raw markdown while the model is typing it. Once the reference
 * closes it is stripped; on completion the held-back tail is flushed anyway.
 */
function trimIncompleteImageRef(text: string): string {
  const start = text.lastIndexOf('![')
  if (start < 0) return text
  return text.slice(0, start)
}

/**
 * Compute the not-yet-sent suffix of `target` relative to what was already
 * delivered (`sent`). Streaming deltas are prefix-based; when a markdown image
 * reference gets stripped from the cumulative text the lengths can diverge, so
 * fall back to the longest common prefix instead of a naive `slice(sent.length)`.
 */
function remainingAfterCommonPrefix(target: string, sent: string): string {
  if (!target) return ''
  if (sent && target.startsWith(sent)) return target.slice(sent.length)
  if (sent.startsWith(target)) return ''
  const max = Math.min(target.length, sent.length)
  let i = 0
  while (i < max && target[i] === sent[i]) i++
  return target.slice(i)
}

class WeChatAdapter extends ChannelAdapter {
  private bot: WeixinBot | null = null
  private readonly tokenPath: string
  private readonly allowedChatIds: string[]

  /** Per-chat throttled flush controller for incremental text streaming. */
  private readonly streamControllers = new Map<string, FlushController>()
  /** Per-chat latest cumulative full text handed to onTextUpdate. */
  private readonly streamFullTexts = new Map<string, string>()
  /** Per-chat text already delivered to the chat (used to compute deltas). */
  private readonly streamSentText = new Map<string, string>()

  /** Per-chat throttled flush controller for thinking (reasoning) bubbles. */
  private readonly thinkingControllers = new Map<string, FlushController>()
  /** Per-chat latest cumulative thinking text handed to onThinkingUpdate. */
  private readonly thinkingFullTexts = new Map<string, string>()
  /** Per-chat thinking text already delivered to the chat. */
  private readonly thinkingSentText = new Map<string, string>()

  constructor(config: ChannelAdapterConfig<'wechat'>) {
    super(config)
    const { token_path, allowed_chat_ids } = config.channelConfig
    this.tokenPath = token_path || application.getPath('feature.agents.channels', `weixin_bot_${config.channelId}.json`)
    this.allowedChatIds = allowed_chat_ids ?? []
    this.notifyChatIds = [...this.allowedChatIds]
  }

  protected override async checkReady(): Promise<boolean> {
    const bot = new WeixinBot({ tokenPath: this.tokenPath })
    const hasCreds = await bot.hasCredentials()
    return hasCreds
  }

  protected override async performConnect(signal: AbortSignal): Promise<void> {
    const bot = new WeixinBot({
      tokenPath: this.tokenPath,
      onError: (error) => {
        this.log.error('WeChat bot error', {
          error: error instanceof Error ? error.message : String(error)
        })
      },
      onQrUrl: (url) => {
        this.emit('qr', url)
        this.sendQrToRenderer(url, 'pending')
      }
    })
    this.bot = bot

    // Abort guard — if disconnect() was called before login completes
    if (signal.aborted) return

    const credentials = await bot.login({ signal }).catch((error) => {
      if (!signal.aborted) {
        const isExpired =
          error instanceof Error &&
          error.message === 'QR login failed after 3 expired QR codes. Use config tool to reconnect.'
        this.sendQrToRenderer('', isExpired ? 'expired' : 'error')
      }
      throw error
    })
    if (signal.aborted) return

    this.sendQrToRenderer('', 'confirmed', credentials.userId)
    this.registerMessageHandler(bot)
    this.markConnected()
    this.log.info('WeChat bot logged in and polling started', { userId: credentials.userId })

    // Start long-polling (fire-and-forget)
    bot.run().catch((err) => {
      if (!signal.aborted) {
        const msg = err instanceof Error ? err.message : String(err)
        this.markDisconnected(msg)
        this.log.error(`Polling stopped: ${msg}`)
      }
    })

    this.log.info('WeChat bot started')
  }

  protected override async performDisconnect(): Promise<void> {
    if (this.bot) {
      this.bot.stop()
      this.bot = null
      this.sendQrToRenderer('', 'disconnected')
      this.log.info('WeChat bot stopped')
    }
  }

  // oxlint-disable-next-line no-unused-vars -- abstract method signature
  async sendMessage(chatId: string, text: string, _opts?: SendMessageOptions): Promise<void> {
    if (!this.bot) {
      throw new Error('Bot is not connected')
    }

    const chunks = splitMessage(text, WECHAT_MAX_LENGTH)

    for (let i = 0; i < chunks.length; i++) {
      await this.bot.send(chatId, chunks[i])

      if (i < chunks.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
    }
  }

  override async sendFile(chatId: string, file: FileAttachment): Promise<void> {
    if (!this.bot) {
      throw new Error('Bot is not connected')
    }
    // The reverse-engineered WeChat protocol only supports outbound images today
    // (WeixinBot.sendImage). Document upload would need protocol-level CDN work.
    if (!file.media_type.startsWith('image/')) {
      throw new Error(`WeChat can only forward image files, not "${file.media_type}" (${file.filename})`)
    }
    await this.bot.sendImage(chatId, Buffer.from(file.data, 'base64'))
    this.log.info('Sent file', { chatId, filename: file.filename, size: file.size })
  }

  async sendTypingIndicator(chatId: string): Promise<void> {
    if (!this.bot) {
      throw new Error('Bot is not connected')
    }

    try {
      await this.bot.sendTyping(chatId)
    } catch {
      // sendTyping requires a cached context_token from a prior message;
      // silently ignore if not yet available
    }
  }

  // ── Incremental multi-bubble streaming ──────────────────────────
  // WeChat iLink does not support editing a sent message in place, so streaming
  // is emulated with incremental bubbles: a FlushController throttles ~300ms and
  // only the not-yet-delivered text delta is sent as a new short message.

  override async onTextUpdate(chatId: string, fullText: string): Promise<void> {
    if (!this.bot) return
    this.streamFullTexts.set(chatId, fullText)
    await this.getStreamController(chatId).throttledUpdate(STREAM_THROTTLE_MS)
  }

  /**
   * Flush any remaining delta, deliver images referenced by the final text, and
   * return true so the caller does not fall back to a duplicate sendMessage().
   */
  override async onStreamComplete(chatId: string, finalText: string): Promise<boolean> {
    if (!this.bot) return false
    this.streamFullTexts.set(chatId, finalText)

    const controller = this.streamControllers.get(chatId)
    if (controller) {
      controller.cancelPendingFlush()
      await controller.waitForFlush()
    }

    // Send images referenced by the final text, removing their markdown fragments.
    const cleanedText = await this.extractAndSendImages(chatId, finalText)
    await this.flushStreamDelta(chatId, cleanedText)

    this.streamControllers.delete(chatId)
    this.streamFullTexts.delete(chatId)
    this.streamSentText.delete(chatId)
    return true
  }

  override async onStreamError(chatId: string, error: string | Error): Promise<void> {
    if (!this.bot) return
    // The base signature passes a string; accepting Error as well keeps the
    // override compatible. Flush whatever text/thinking accumulated before the
    // failure so the partial response is not silently lost.
    this.log.warn('Stream error, flushing partial response', {
      chatId,
      error: error instanceof Error ? error.message : String(error)
    })

    const controller = this.streamControllers.get(chatId)
    if (controller) {
      controller.cancelPendingFlush()
      await controller.waitForFlush()
    }
    await this.flushStreamDelta(chatId)
    this.streamControllers.delete(chatId)
    this.streamFullTexts.delete(chatId)
    this.streamSentText.delete(chatId)

    const thinkingController = this.thinkingControllers.get(chatId)
    if (thinkingController) {
      thinkingController.cancelPendingFlush()
      await thinkingController.waitForFlush()
    }
    await this.flushThinkingDelta(chatId)
    this.thinkingControllers.delete(chatId)
    this.thinkingFullTexts.delete(chatId)
    this.thinkingSentText.delete(chatId)
  }

  /** Send thinking (reasoning) deltas as prefixed, separate bubbles. */
  override async onThinkingUpdate(chatId: string, thinking: string): Promise<void> {
    if (!this.bot) return
    this.thinkingFullTexts.set(chatId, thinking)
    await this.getThinkingController(chatId).throttledUpdate(STREAM_THROTTLE_MS)
  }

  override async onThinkingComplete(chatId: string, thinking: string): Promise<void> {
    if (!this.bot) return
    this.thinkingFullTexts.set(chatId, thinking)
    const controller = this.thinkingControllers.get(chatId)
    if (controller) {
      controller.cancelPendingFlush()
      await controller.waitForFlush()
    }
    await this.flushThinkingDelta(chatId)

    this.thinkingControllers.delete(chatId)
    this.thinkingFullTexts.delete(chatId)
    this.thinkingSentText.delete(chatId)
  }

  private getStreamController(chatId: string): FlushController {
    let controller = this.streamControllers.get(chatId)
    if (!controller) {
      controller = new FlushController(() => this.flushStreamDelta(chatId))
      this.streamControllers.set(chatId, controller)
    }
    return controller
  }

  private getThinkingController(chatId: string): FlushController {
    let controller = this.thinkingControllers.get(chatId)
    if (!controller) {
      controller = new FlushController(() => this.flushThinkingDelta(chatId))
      this.thinkingControllers.set(chatId, controller)
    }
    return controller
  }

  /** Send only the not-yet-delivered portion of the cumulative stream text. */
  private async flushStreamDelta(chatId: string, fullTextOverride?: string): Promise<void> {
    const bot = this.bot
    if (!bot) return
    const fullText = fullTextOverride ?? this.streamFullTexts.get(chatId) ?? ''
    // Image references are delivered as real images, never as raw markdown text.
    const cleanText = stripMarkdownImageRefs(fullText)
    // While streaming, hold back a trailing incomplete reference; at completion
    // the override path delivers everything (including a never-closed literal).
    const deliverable = fullTextOverride === undefined ? trimIncompleteImageRef(cleanText) : cleanText
    const sent = this.streamSentText.get(chatId) ?? ''
    const remaining = remainingAfterCommonPrefix(deliverable, sent)
    if (!remaining) return
    await this.sendTextChunks(bot, chatId, remaining)
    this.streamSentText.set(chatId, deliverable)
  }

  /** Send thinking (reasoning) deltas as prefixed, separate bubbles. */
  private async flushThinkingDelta(chatId: string): Promise<void> {
    const bot = this.bot
    if (!bot) return
    const fullThinking = this.thinkingFullTexts.get(chatId) ?? ''
    const sent = this.thinkingSentText.get(chatId) ?? ''
    if (fullThinking.length <= sent.length) return
    const delta = fullThinking.slice(sent.length)
    await this.sendThinkingChunks(bot, chatId, delta)
    this.thinkingSentText.set(chatId, fullThinking)
  }

  /** Split and send text, each bubble capped at WECHAT_MAX_LENGTH. */
  private async sendTextChunks(bot: WeixinBot, chatId: string, text: string): Promise<void> {
    if (!text) return
    const chunks = splitMessage(text, WECHAT_MAX_LENGTH)
    for (const chunk of chunks) {
      await bot.send(chatId, chunk)
    }
  }

  private async sendThinkingChunks(bot: WeixinBot, chatId: string, text: string): Promise<void> {
    if (!text.trim()) return
    const chunks = splitMessage(text, WECHAT_MAX_LENGTH)
    for (const chunk of chunks) {
      await bot.send(chatId, `${THINKING_PREFIX}${chunk}`)
    }
  }

  /**
   * Scan the final text for markdown image references, resolve each path to a
   * local file via the security file resolvers, send it as a real image, and
   * return the text with those fragments removed.
   */
  private async extractAndSendImages(chatId: string, text: string): Promise<string> {
    const bot = this.bot
    if (!bot) return text
    const matches = Array.from(text.matchAll(MARKDOWN_IMAGE_REF_RE))
    if (matches.length === 0) return text

    const workspaceRoot = await this.resolveWorkspaceRoot()
    let cleaned = text
    for (const match of matches) {
      const rawPath = (match[1] ?? '').trim().replace(/^['"]|['"]$/g, '')
      if (!rawPath) continue
      try {
        // Confine to the session workspace when we can resolve it; otherwise
        // resolve the path as-is (absolute paths still work).
        const attachment = workspaceRoot
          ? await resolveWorkspaceFile(workspaceRoot, rawPath)
          : await resolveLocalFile('', rawPath)
        if (!attachment.media_type.startsWith('image/')) {
          this.log.debug('Skipped non-image markdown reference', {
            chatId,
            path: rawPath,
            mediaType: attachment.media_type
          })
          continue
        }
        await bot.sendImage(chatId, Buffer.from(attachment.data, 'base64'))
        cleaned = cleaned.replace(match[0], '')
        this.log.info('Sent image from markdown reference', {
          chatId,
          path: rawPath,
          filename: attachment.filename
        })
      } catch (error) {
        this.log.warn('Failed to send markdown image reference', {
          chatId,
          path: rawPath,
          error: error instanceof Error ? error.message : String(error)
        })
      }
    }
    return cleaned
  }

  /** Best-effort lookup of the session workspace root for this channel. */
  private async resolveWorkspaceRoot(): Promise<string | undefined> {
    try {
      const channel = agentChannelService.getChannel(this.channelId)
      const sessionId = channel?.sessionId
      if (!sessionId) return undefined
      const session = agentSessionService.getById(sessionId)
      return session?.workspace?.path || undefined
    } catch (error) {
      this.log.debug('Failed to resolve session workspace', {
        error: error instanceof Error ? error.message : String(error)
      })
      return undefined
    }
  }

  private sendQrToRenderer(
    url: string,
    status: 'pending' | 'confirmed' | 'expired' | 'disconnected' | 'error',
    userId?: string
  ): void {
    application.get('IpcApiService').broadcastToType(WindowType.Main, 'channel.wechat.qr_login', {
      channelId: this.channelId,
      url,
      status,
      userId
    })
  }

  private registerMessageHandler(bot: WeixinBot): void {
    bot.onMessage(async (msg: IncomingMessage) => {
      if (this.allowedChatIds.length > 0 && !this.allowedChatIds.includes(msg.userId)) {
        this.log.debug('Dropping message from unauthorized user', { userId: msg.userId })
        return
      }

      // Download images from WeChat CDN (returns data URIs with base64)
      let images: ImageAttachment[] | undefined
      if (msg._imageItems && msg._imageItems.length > 0) {
        const dataUris = (await Promise.all(msg._imageItems.map((item) => bot.downloadImage(item)))).filter(
          (uri): uri is string => uri !== null
        )
        const parsed = dataUris
          .map((uri) => {
            const result = parseDataUrl(uri)
            if (!result || !result.isBase64 || !result.mediaType) return null
            return { media_type: result.mediaType, data: result.data } as ImageAttachment
          })
          .filter((img): img is ImageAttachment => img !== null)
        if (parsed.length > 0) images = parsed
      }

      // Download files from WeChat CDN
      let files: FileAttachment[] | undefined
      if (msg._fileItems && msg._fileItems.length > 0) {
        const results = await Promise.all(msg._fileItems.map((item) => bot.downloadFile(item)))
        const downloaded = results
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .map((r) => {
            const ext = r.filename.includes('.') ? r.filename.split('.').pop()!.toLowerCase() : ''
            return {
              filename: r.filename,
              data: r.data.toString('base64'),
              media_type: FILE_EXTENSION_MIME_MAP[ext] || 'application/octet-stream',
              size: r.data.length
            } satisfies FileAttachment
          })
        if (downloaded.length > 0) files = downloaded
      }

      const text = msg.text.trim()
      if (!text && !images && !files) return

      if (isSlashCommand(text)) {
        if (text.startsWith('/whoami')) {
          this.sendWhoami(msg).catch((err) => {
            this.log.error('Failed to send whoami response', {
              error: err instanceof Error ? err.message : String(err)
            })
          })
          return
        }

        // 'whoami' is handled above and returns early, so it won't reach here
        const cmd = text.split(/\s+/)[0].slice(1) as 'new' | 'compact' | 'help'
        this.emit('command', {
          chatId: msg.userId,
          userId: msg.userId,
          userName: msg.userId,
          command: cmd
        })
      } else {
        this.emit('message', {
          chatId: msg.userId,
          userId: msg.userId,
          userName: msg.userId,
          text,
          images,
          files
        })
      }
    })
  }

  private async sendWhoami(msg: IncomingMessage): Promise<void> {
    const message = [
      `Chat Info`,
      ``,
      `User ID: ${msg.userId}`,
      ``,
      `To enable notifications for this user:`,
      `1. Go to Agent Settings > Channels > WeChat`,
      `2. Add "${msg.userId}" to Allowed User IDs`,
      `3. Enable "Receive Notifications"`,
      ``,
      `Then use the notify tool or scheduled tasks will send messages here.`
    ].join('\n')

    await this.bot!.reply(msg, message)
  }
}

// Self-registration
registerAdapterFactory('wechat', (channel, agentId) => {
  return new WeChatAdapter({
    channelId: channel.id,
    channelType: channel.type,
    agentId,
    channelConfig: channel.config
  })
})
