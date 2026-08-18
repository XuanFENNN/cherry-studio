import { application } from '@application'
import { agentChannelService } from '@data/services/AgentChannelService'
import { agentSessionService } from '@data/services/AgentSessionService'
import { WindowType } from '@main/core/window/types'
import type { FileAttachment, ImageAttachment } from '@main/utils/downloadAsBase64'
import { parseDataUrl } from '@shared/utils/dataUrl'

import { ChannelAdapter, type ChannelAdapterConfig, type SendMessageOptions } from '../../ChannelAdapter'
import { registerAdapterFactory } from '../../ChannelManager'
import { isSlashCommand } from '../../constants'
import { resolveLocalFile } from '../../security/localFileResolver'
import { resolveWorkspaceFile } from '../../security/WorkspaceFileGuard'
import { FILE_EXTENSION_MIME_MAP, splitMessage } from '../../utils'
import { type IncomingMessage, WeixinBot } from './WeChatProtocol'

const WECHAT_MAX_LENGTH = 2000

/** Markdown image reference: `![alt](path)` (optional quoted title tolerated). */
const MARKDOWN_IMAGE_REF_RE = /!\[[^\]]*\]\(([^)]*)\)/g

/** Markdown media reference: `![alt](path)` embed form + `[text](path)` link form. */
const MARKDOWN_MEDIA_REF_RE = /(!?)\[([^\]]*)\]\(([^)]*)\)/g

/** 媒体类型桶：微信协议出站支持的四种消息类型。 */
type MediaKind = 'image' | 'video' | 'audio' | 'file'

/** 扩展名 → 媒体类型（MIME 前缀优先，扩展名兜底；未列出的扩展名一律归为 file）。 */
const MEDIA_EXTENSION_KIND: Record<string, Exclude<MediaKind, 'file'>> = {
  // 图片
  png: 'image',
  jpg: 'image',
  jpeg: 'image',
  gif: 'image',
  webp: 'image',
  bmp: 'image',
  svg: 'image',
  // 视频
  mp4: 'video',
  m4v: 'video',
  mov: 'video',
  avi: 'video',
  mkv: 'video',
  webm: 'video',
  flv: 'video',
  wmv: 'video',
  // 音频
  mp3: 'audio',
  wav: 'audio',
  m4a: 'audio',
  aac: 'audio',
  ogg: 'audio',
  oga: 'audio',
  flac: 'audio',
  wma: 'audio',
  amr: 'audio',
  opus: 'audio',
  silk: 'audio'
}

/** 远程 URL 引用（http/https/data/mailto 等）：保留原文为链接，不当作本地媒体发送。 */
function isRemoteMediaRef(target: string): boolean {
  return /^(https?:|data:|mailto:|ftp:)/i.test(target)
}

/**
 * 按 media_type（MIME 前缀）与扩展名把附件路由到微信协议支持的媒体类型。
 * media_type 可信时优先；仅当它是通用类型（如 application/octet-stream）时用扩展名兜底。
 */
function resolveMediaKind(mediaType: string, filename: string): MediaKind {
  if (mediaType.startsWith('image/')) return 'image'
  if (mediaType.startsWith('video/')) return 'video'
  if (mediaType.startsWith('audio/')) return 'audio'
  const ext = filename.includes('.') ? filename.split('.').pop()!.toLowerCase() : ''
  return MEDIA_EXTENSION_KIND[ext] ?? 'file'
}

/**
 * Remove complete markdown media references from text — their media is delivered
 * as real attachments instead (see `extractAndSendImages` / `extractAndSendMedia`).
 *
 * - `![alt](path)` embed form: always stripped (images keep the v1 behavior; a
 *   non-image embed target is sent as file/video/voice at stream complete).
 * - `[text](path)` link form: only stripped when the target is a local file path
 *   (has an extension); remote URLs and extension-less paths stay as text links.
 */
function stripMarkdownMediaRefs(text: string): string {
  return text.replace(MARKDOWN_MEDIA_REF_RE, (full, bang, _label, rawTarget) => {
    const isEmbed = bang === '!'
    const target = (rawTarget ?? '').trim().replace(/^['"]|['"]$/g, '')
    if (isEmbed) return ''
    // 链接形式：远程 URL 或没有扩展名的路径保留原文
    if (!target || isRemoteMediaRef(target)) return full
    const ext = target.split('.').pop()
    if (!ext || ext === target) return full
    return ''
  })
}

class WeChatAdapter extends ChannelAdapter {
  private bot: WeixinBot | null = null
  private readonly tokenPath: string
  private readonly allowedChatIds: string[]

  /** Per-chat latest cumulative full text handed to onTextUpdate (send source). */
  private readonly latestFullTexts = new Map<string, string>()
  /** Per-chat text already delivered to the chat (used to compute the unsent tail). */
  private readonly sentTexts = new Map<string, string>()
  /** Per-chat send serialization queue — see `flushUnsentText`. */
  private readonly sendQueues = new Map<string, Promise<void>>()

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
    const data = Buffer.from(file.data, 'base64')
    if (data.length === 0) {
      throw new Error(`WeChat cannot send an empty file: "${file.filename}"`)
    }
    // 按 media_type / 扩展名路由：图片→sendImage、视频→sendVideo、音频→sendVoice（降级 sendFile）、其余→sendFile
    const kind = resolveMediaKind(file.media_type, file.filename)
    await this.sendAttachment(chatId, file, kind)
    this.log.info('Sent file', { chatId, filename: file.filename, size: file.size, kind })
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

  // ── 阶段性发送（v2）─────────────────────────────────────────────
  // 微信 iLink 不支持原地编辑已发送消息，一期用 FlushController 增量气泡会把
  // 流式正文拆成几十条碎气泡，触发微信快速消息上限被截断。二期改为：只把模型在
  // 两次思考/工具调用之间产生的正文片段作为一条完整消息发出——onTextUpdate 仅
  // 记录最新全文，不逐字发送；onStageBoundary 一次性发出未发送尾部。

  override async onTextUpdate(chatId: string, fullText: string): Promise<void> {
    if (!this.bot) return
    // 只更新最新全文记录，不发送任何消息——发送由阶段边界/完成时触发
    this.latestFullTexts.set(chatId, fullText)
  }

  /**
   * 阶段边界：把 latestFullText 相对 sentText 的未发送尾部作为一条完整消息发出。
   * 思考/工具调用本身不产生气泡，仅作为「正文阶段结束」的信号。
   */
  override async onStageBoundary(chatId: string): Promise<void> {
    await this.flushUnsentText(chatId)
  }

  /**
   * 流式完成：发掉剩余尾部，处理正文中的图片引用（发送真实图片并剥离 markdown
   * 片段），清理状态。返回 true 让调用方不再回退到 sendMessage() 重复发送。
   */
  override async onStreamComplete(chatId: string, finalText: string): Promise<boolean> {
    if (!this.bot) return false
    this.latestFullTexts.set(chatId, finalText)
    await this.flushUnsentText(chatId)
    // 图片引用以真实图片发送（保留一期实现）；文件/视频/音频引用并行发送
    await this.extractAndSendImages(chatId, finalText)
    await this.extractAndSendMedia(chatId, finalText)
    this.latestFullTexts.delete(chatId)
    this.sentTexts.delete(chatId)
    this.sendQueues.delete(chatId)
    return true
  }

  override async onStreamError(chatId: string, error: string | Error): Promise<void> {
    // 基类签名传 string；兼容 Error 便于调用方直接透传。失败前已累积的正文
    // 也按阶段边界的方式发掉，避免部分回答静默丢失，随后清理状态。
    this.log.warn('Stream error, flushing partial response', {
      chatId,
      error: error instanceof Error ? error.message : String(error)
    })
    await this.flushUnsentText(chatId)
    this.latestFullTexts.delete(chatId)
    this.sentTexts.delete(chatId)
    this.sendQueues.delete(chatId)
  }

  /** 微信不展示思考过程：思考更新与完成均为 no-op。 */
  override async onThinkingUpdate(_chatId: string, _thinking: string): Promise<void> {}

  /** 微信不展示思考过程：思考更新与完成均为 no-op。 */
  override async onThinkingComplete(_chatId: string, _thinking: string): Promise<void> {}

  /**
   * 发送 latestFullText 相对 sentText 的未发送尾部，整条一次性发出
   * （超 2000 字符用 splitMessage 分块，块间 10ms）。
   *
   * 阶段边界可能密集触发（如思考分片连续到达），因此按 chat 串行化：后到的
   * 边界排在上一次发送完成之后，再重新计算未发送尾部，避免同一段正文被重复发送。
   */
  private flushUnsentText(chatId: string): Promise<void> {
    const prev = this.sendQueues.get(chatId) ?? Promise.resolve()
    const next = prev.then(() => this.doFlushUnsentText(chatId))
    // 队列尾部吞掉异常防止断链（异常由调用方 fire-and-forget 的 catch 记录）
    this.sendQueues.set(
      chatId,
      next.catch(() => {})
    )
    return next
  }

  private async doFlushUnsentText(chatId: string): Promise<void> {
    const bot = this.bot
    if (!bot) return
    const fullText = this.latestFullTexts.get(chatId) ?? ''
    // 图片/文件/视频/音频引用在 onStreamComplete 里以真实媒体发送，正文不出现原始 markdown
    const deliverable = stripMarkdownMediaRefs(fullText)
    const sent = this.sentTexts.get(chatId) ?? ''
    // 相对 sentText 的未发送尾部：常规为前缀差；被剥离的图片引用会让前后长度
    // 不一致，退化为最长公共前缀
    let remaining = ''
    if (deliverable) {
      if (sent && deliverable.startsWith(sent)) {
        remaining = deliverable.slice(sent.length)
      } else if (!sent.startsWith(deliverable)) {
        const max = Math.min(deliverable.length, sent.length)
        let i = 0
        while (i < max && deliverable[i] === sent[i]) i++
        remaining = deliverable.slice(i)
      }
    }
    if (!remaining) return
    const chunks = splitMessage(remaining, WECHAT_MAX_LENGTH)
    for (let i = 0; i < chunks.length; i++) {
      await bot.send(chatId, chunks[i])
      if (i < chunks.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
    }
    this.sentTexts.set(chatId, deliverable)
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

  /**
   * 按媒体类型发送附件（与 sendFile 共用路由；markdown 媒体引用也走这里）。
   */
  private async sendAttachment(chatId: string, attachment: FileAttachment, kind: MediaKind): Promise<void> {
    const bot = this.bot
    if (!bot) throw new Error('Bot is not connected')
    const data = Buffer.from(attachment.data, 'base64')
    switch (kind) {
      case 'image':
        await bot.sendImage(chatId, data)
        break
      case 'video':
        await bot.sendVideo(chatId, data)
        break
      case 'audio':
        await this.sendAudioWithVoiceFallback(chatId, data, attachment.filename)
        break
      default:
        await bot.sendFile(chatId, data, attachment.filename)
        break
    }
  }

  /**
   * 语音降级：先尝试 VOICE 语音条，随后无论成败都回退为 FILE 音频附件。
   *
   * 协议文档（T4a）实测提示：iLink 通道会静默丢弃 bot 方向的 VOICE 消息——HTTP 200
   * 但客户端不出现语音气泡，官方插件从不发 VOICE 而统一用文件附件代替。因此 sendVoice
   * 失败或返回后都必须再发一份文件，保证用户一定能收到可播放的音频文件卡片。
   */
  private async sendAudioWithVoiceFallback(chatId: string, data: Buffer, filename: string): Promise<void> {
    const bot = this.bot
    if (!bot) throw new Error('Bot is not connected')
    // 协议不暴露音频时长，按默认 24kHz/16bit 单声道 ≈ 48 字节/毫秒 粗略估算（仅供 VOICE 占位；
    // 实际送达依赖下方文件附件的回退路径）
    const playtimeMs = Math.max(1, Math.floor(data.length / 48))
    try {
      await bot.sendVoice(chatId, data, { playtimeMs })
    } catch (error) {
      this.log.warn('sendVoice failed, falling back to file attachment', {
        chatId,
        filename,
        error: error instanceof Error ? error.message : String(error)
      })
    }
    // VOICE 成功与否都无法确认送达（静默丢弃时同样返回 200），因此总是再发一份文件附件
    await bot.sendFile(chatId, data, filename)
  }

  /**
   * 扫描最终正文中的文件/视频/音频引用（`[text](path)` 链接形式 + `![alt](path)` 嵌入形式），
   * 按扩展名/URL 判断媒体类型并发送真实附件；嵌入形式的图片引用仍由 extractAndSendImages
   * 处理（本方法跳过，避免重复发送）。
   */
  private async extractAndSendMedia(chatId: string, text: string): Promise<string> {
    const bot = this.bot
    if (!bot) return text
    const matches = Array.from(text.matchAll(MARKDOWN_MEDIA_REF_RE))
    if (matches.length === 0) return text

    const workspaceRoot = await this.resolveWorkspaceRoot()
    let cleaned = text
    for (const match of matches) {
      const isEmbed = match[1] === '!'
      const rawPath = (match[3] ?? '').trim().replace(/^['"]|['"]$/g, '')
      if (!rawPath || isRemoteMediaRef(rawPath)) continue
      try {
        // 与 extractAndSendImages 相同的解析路径：会话工作区内优先，否则按原路径解析
        const attachment = workspaceRoot
          ? await resolveWorkspaceFile(workspaceRoot, rawPath)
          : await resolveLocalFile('', rawPath)
        const kind = resolveMediaKind(attachment.media_type, attachment.filename)
        // 嵌入形式的图片引用由 extractAndSendImages 发送，这里跳过
        if (kind === 'image' && isEmbed) continue
        await this.sendAttachment(chatId, attachment, kind)
        cleaned = cleaned.replace(match[0], '')
        this.log.info('Sent media from markdown reference', {
          chatId,
          path: rawPath,
          filename: attachment.filename,
          kind
        })
      } catch (error) {
        this.log.warn('Failed to send markdown media reference', {
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
