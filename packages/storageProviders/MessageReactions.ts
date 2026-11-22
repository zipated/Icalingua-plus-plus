import type Message from '@icalingua/types/Message'
import type { MessageReaction, MessageReactionRequest } from '@icalingua/types/Message'
import type StorageProvider from '@icalingua/types/StorageProvider'
import type { Client, GroupReactionEventData } from 'oicq-icalingua-plus-plus'
import { decodeMessageId } from './MessageId'

export async function sendGroupReaction(bot: Pick<Client, 'setEmojiReaction'>, request: MessageReactionRequest) {
    const { roomId, messageId, id, type, set } = request
    const message = decodeMessageId(messageId)
    if (message?.type !== 'group' || message.groupId !== -roomId || !message.messageSeq)
        throw new Error('无法定位群消息')
    if (typeof id !== 'string' || !id || (type !== 1 && type !== 2) || typeof set !== 'boolean')
        throw new Error('无效的表情回应')

    const result = await bot.setEmojiReaction(message.groupId, message.messageSeq, id, type, set)
    if (result.error) throw new Error(result.error.message || '表情回应发送失败')
}

const pendingUpdates = new WeakMap<StorageProvider, Map<string, Promise<void>>>()
const messageSaveWaiters = new WeakMap<StorageProvider, Map<string, () => void>>()
const reactionMessageWaitMs = 30000

// 与服务端 SetReaction 同规则：≤3 位是系统表情短 ID，更长是 unicode 码点
export function reactionTypeFromId(id: string): number {
    return id.length > 3 ? 2 : 1
}

export interface MessageReactionChange {
    id: string
    type: number
    set: boolean
    bySelf: boolean
}

function mergeReaction(
    reactions: MessageReaction[] | undefined,
    change: MessageReactionChange,
    authoritativeCount?: number,
): MessageReaction[] | null {
    const list = [...(reactions ?? [])]
    const index = list.findIndex((reaction) => reaction.type === change.type && reaction.id === change.id)
    const current = list[index]
    const is_clicked = change.bySelf ? change.set : (current?.is_clicked ?? false)
    const delta = change.bySelf && !!current?.is_clicked === change.set ? 0 : change.set ? 1 : -1
    const count = Number.isSafeInteger(authoritativeCount) ? Number(authoritativeCount) : (current?.count ?? 0) + delta
    if (count <= 0) {
        if (index < 0) return null
        list.splice(index, 1)
        return list
    }
    if (current?.count === count && current.is_clicked === is_clicked) return null
    list[index < 0 ? list.length : index] = { id: change.id, type: change.type, count, is_clicked }
    return list
}

export function readGroupReactions(data: object): Pick<Message, 'emoji_reactions'> {
    const group = data as { group_id?: number; emoji_reactions?: Message['emoji_reactions'] }
    if (!group?.group_id || !Array.isArray(group.emoji_reactions)) return {}
    return {
        emoji_reactions: group.emoji_reactions.filter(({ type, count }) => count > 0 && (type === 1 || type === 2)),
    }
}

const notifyMessageSaved = (storage: StorageProvider, key: string, write: Promise<unknown>) =>
    write.finally(() => messageSaveWaiters.get(storage)?.get(key)?.())

export function notifyGroupMessageSave(
    storage: StorageProvider,
    groupId: number,
    seq: number,
    write: Promise<unknown>,
) {
    return notifyMessageSaved(storage, `${groupId}:${seq}`, write)
}

export function notifyMessageSave(
    storage: StorageProvider,
    roomId: number,
    messageId: string,
    write: Promise<unknown>,
) {
    return notifyMessageSaved(storage, `${roomId}:${messageId}`, write)
}

async function getReactionMessage(
    storage: StorageProvider,
    roomId: number,
    key: string,
    messageId: string,
    deadline: number,
) {
    let waiters = messageSaveWaiters.get(storage)
    if (!waiters) {
        waiters = new Map()
        messageSaveWaiters.set(storage, waiters)
    }
    // Register before reading so a save that finishes during the lookup still wakes this reaction.
    const saved = new Promise<void>((resolve) => {
        waiters.set(key, resolve)
    })
    let timer: NodeJS.Timeout | undefined
    try {
        const message = await storage.getMessage(roomId, messageId)
        const remaining = deadline - Date.now()
        if (message || remaining <= 0) return message

        timer = setTimeout(() => waiters.get(key)?.(), remaining).unref()
        await saved
        return await storage.getMessage(roomId, messageId)
    } finally {
        clearTimeout(timer)
        waiters.delete(key)
    }
}

function enqueueReactionUpdate(storage: StorageProvider, key: string, update: () => Promise<void>): Promise<void> {
    let updates = pendingUpdates.get(storage)
    if (!updates) {
        updates = new Map()
        pendingUpdates.set(storage, updates)
    }

    const previous = updates.get(key) ?? Promise.resolve()
    const next = previous
        .catch(() => {})
        .then(update)
        .finally(() => {
            if (updates.get(key) === next) updates.delete(key)
        })
    updates.set(key, next)
    return next
}

export function applyGroupReaction(
    storage: StorageProvider,
    selfId: number,
    event: GroupReactionEventData,
    publish: (roomId: number, messageId: string, patch: Partial<Message>) => void,
    fetchMessage: (messageId: string) => Promise<{ data?: { group_id?: number; seqid?: number; message_id: string } }>,
): Promise<void> {
    if (event.type !== 1 && event.type !== 2) return Promise.resolve()

    const key = `${event.group_id}:${event.seq}`
    const deadline = Date.now() + reactionMessageWaitMs
    return enqueueReactionUpdate(storage, key, async () => {
        const roomId = -event.group_id
        let messageId = event.message_id
        if (!messageId) {
            // Older oicq events provide only the sequence. getMsg reads the group and sequence from the ID.
            const probe = Buffer.alloc(21)
            probe.writeUInt32BE(event.group_id, 0)
            probe.writeUInt32BE(event.seq, 8)
            probe[20] = 1
            const { data } = await fetchMessage(probe.toString('base64'))
            if (data?.group_id !== event.group_id || data.seqid !== event.seq) return
            messageId = data.message_id
        }
        const message = await getReactionMessage(storage, roomId, key, messageId, deadline)
        if (!message || message.deleted) return

        const reactions = mergeReaction(
            message.emoji_reactions,
            { id: event.id, type: event.type, set: event.set, bySelf: event.user_id === selfId },
            event.count,
        )
        if (!reactions) return

        const patch = { emoji_reactions: reactions }
        await storage.updateMessage(roomId, message._id, patch)
        publish(roomId, String(message._id), patch)
    })
}

export function applyMessageReaction(
    storage: StorageProvider,
    roomId: number,
    messageId: string,
    change: MessageReactionChange,
    publish: (roomId: number, messageId: string, patch: Partial<Message>) => void,
    waitKey?: string,
): Promise<void> {
    if (!change.id || (change.type !== 1 && change.type !== 2)) return Promise.resolve()

    const queueKey = `${roomId}:${messageId}`
    const deadline = Date.now() + reactionMessageWaitMs
    return enqueueReactionUpdate(storage, queueKey, async () => {
        const message = await getReactionMessage(storage, roomId, waitKey ?? queueKey, messageId, deadline)
        if (!message || message.deleted) return

        const reactions = mergeReaction(message.emoji_reactions, change)
        if (!reactions) return

        const patch = { emoji_reactions: reactions }
        await storage.updateMessage(roomId, message._id, patch)
        publish(roomId, String(message._id), patch)
    })
}
