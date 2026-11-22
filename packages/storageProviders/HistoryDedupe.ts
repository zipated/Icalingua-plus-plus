import Message from '@icalingua/types/Message'
import MessagePageOptions, { MessageCursor } from '@icalingua/types/MessagePage'

// 各适配器的消息 _id 空间不通用，历史判重统一按 时间 + 发送者 + 文本/附件
export interface HistoryDedupeStorage {
    fetchMessages(roomId: number, options: MessagePageOptions, limit: number): Promise<Message[]>
}

export interface HistoryBatchDedupeResult {
    fresh: Message[]
    duplicates: number
    /** 边界消息是否已入库，是则无需再往更早拉 */
    boundaryStored: boolean
}

export interface HistoryBatchDedupeOptions {
    /** 默认取批次里最早的一条 */
    boundary?: Message
    pageSize?: number
    maxScan?: number
}

const defaultPageSize = 200
const defaultMaxScan = 2000
// 实时入库的消息把同一秒内的序号加在毫秒位上（history 是整秒），窗口两侧各留一秒
const msOffsetWindow = 999

export const normalizeHistoryContent = (content: unknown): string =>
    String(content ?? '')
        .trim()
        .replace(/\s+/g, ' ')

/** 图片/表情/语音只写 files、content 为空，取附件里跨适配器稳定的标识 */
const mediaKeyOf = (file: Message['files'][number]): string => {
    if (!file) return ''
    if (file.fid) return `fid:${file.fid}`
    if (file.name) return `name:${file.name}`
    const url = String(file.url || '')
    const fileid = /[?&]fileid=([^&]+)/.exec(url)
    if (fileid) return `fid:${fileid[1]}`
    const md5 = /([a-f\d]{32})/i.exec(url.split('?')[0])
    if (md5) return `md5:${md5[1].toLowerCase()}`
    return ''
}

/** 缺时间戳、或纯媒体且取不到附件标识时返回 null，退回 _id 判断（宁可重复也不丢消息） */
export const historyMessageIdentity = (message: Message): string | null => {
    const time = Number(message?.time) || 0
    if (!time) return null
    const content = normalizeHistoryContent(message?.content)
    const media = (message.files || []).map(mediaKeyOf).filter(Boolean).join(',')
    if (!content && !media) return null
    // 实时消息带同秒序号，按秒归并才能和历史消息对上
    return [Number(message?.senderId) || 0, Math.floor(time / 1000), content, media].join('|')
}

/** 只扫 [from, to] 窗口，最多 maxScan 条 */
const scanStoredMessages = async (
    storage: HistoryDedupeStorage,
    roomId: number,
    from: number,
    to: number,
    visit: (message: Message) => void,
    pageSize: number,
    maxScan: number,
) => {
    let cursor: MessageCursor = { time: from - 1, id: '' }
    let scanned = 0
    while (scanned < maxScan) {
        const page = await storage.fetchMessages(roomId, { after: cursor }, pageSize)
        if (!page?.length) return

        let reachedEnd = false
        for (const message of page) {
            if ((Number(message.time) || 0) > to) {
                reachedEnd = true
                break
            }
            visit(message)
        }
        scanned += page.length

        const last = page[page.length - 1]
        if (reachedEnd || page.length < pageSize) return
        cursor = { time: Number(last.time) || 0, id: last._id }
    }
}

/** 过滤掉库里已有的消息（可能是别的适配器写入的），并回报边界是否已存在 */
export const dedupeHistoryBatch = async (
    storage: HistoryDedupeStorage,
    roomId: number,
    messages: Message[],
    options: HistoryBatchDedupeOptions = {},
): Promise<HistoryBatchDedupeResult> => {
    const batch = Array.isArray(messages) ? messages.filter(Boolean) : []
    if (!batch.length) return { fresh: [], duplicates: 0, boundaryStored: false }

    const existingIds = new Set<string>()
    const existingIdentities = new Set<string>()
    const times = batch.map((message) => Number(message.time) || 0).filter((time) => time > 0)
    if (times.length) {
        await scanStoredMessages(
            storage,
            roomId,
            Math.min(...times) - msOffsetWindow,
            Math.max(...times) + msOffsetWindow,
            (message) => {
                existingIds.add(String(message._id))
                const identity = historyMessageIdentity(message)
                if (identity) existingIdentities.add(identity)
            },
            options.pageSize || defaultPageSize,
            options.maxScan || defaultMaxScan,
        )
    }

    const isStored = (message: Message) => {
        if (existingIds.has(String(message._id))) return true
        const identity = historyMessageIdentity(message)
        return !!identity && existingIdentities.has(identity)
    }

    const fresh = batch.filter((message) => !isStored(message))
    const boundary = options.boundary || batch[0]
    return {
        fresh,
        duplicates: batch.length - fresh.length,
        boundaryStored: isStored(boundary),
    }
}
