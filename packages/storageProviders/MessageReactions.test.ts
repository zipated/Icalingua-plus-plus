import assert from 'node:assert/strict'
import test from 'node:test'
import Message from '@icalingua/types/Message'
import StorageProvider from '@icalingua/types/StorageProvider'
import { applyMessageReaction, notifyMessageSave, reactionTypeFromId } from './MessageReactions'

const createStorage = () => {
    const store = new Map<string, Message>()
    const publishes: Array<{ roomId: number; messageId: string; patch: Partial<Message> }> = []
    const storage = {
        async getMessage(roomId: number, messageId: string | number) {
            return store.get(`${roomId}:${messageId}`) ?? null
        },
        async updateMessage(roomId: number, messageId: string | number, patch: Partial<Message>) {
            const key = `${roomId}:${messageId}`
            const message = store.get(key)
            if (message) store.set(key, { ...message, ...patch })
        },
    } as unknown as StorageProvider
    return {
        storage,
        store,
        publishes,
        publish: (roomId: number, messageId: string, patch: Partial<Message>) =>
            publishes.push({ roomId, messageId, patch }),
    }
}

const groupMessage = (id: string): Message => ({
    _id: id,
    senderId: 42,
    username: '42',
    content: 'hi',
    files: [],
    emoji_reactions: [],
})

const reactionsOf = (store: Map<string, Message>, key: string) => store.get(key)?.emoji_reactions ?? []

test('他人回应累加计数，自己的回应标记 is_clicked', async () => {
    const { storage, store, publishes, publish } = createStorage()
    store.set('-100:m1', groupMessage('m1'))

    await applyMessageReaction(storage, -100, 'm1', { id: '128077', type: 2, set: true, bySelf: false }, publish)
    assert.deepEqual(reactionsOf(store, '-100:m1'), [{ id: '128077', type: 2, count: 1, is_clicked: false }])

    await applyMessageReaction(storage, -100, 'm1', { id: '128077', type: 2, set: true, bySelf: true }, publish)
    assert.deepEqual(reactionsOf(store, '-100:m1'), [{ id: '128077', type: 2, count: 2, is_clicked: true }])
    assert.equal(publishes.length, 2)
})

test('自己重复添加与服务端回声不会重复计数，取消后回落', async () => {
    const { storage, store, publishes, publish } = createStorage()
    store.set('-100:m1', groupMessage('m1'))

    await applyMessageReaction(storage, -100, 'm1', { id: '4', type: 1, set: true, bySelf: true }, publish)
    await applyMessageReaction(storage, -100, 'm1', { id: '4', type: 1, set: true, bySelf: true }, publish)
    assert.deepEqual(reactionsOf(store, '-100:m1'), [{ id: '4', type: 1, count: 1, is_clicked: true }])
    assert.equal(publishes.length, 1)

    await applyMessageReaction(storage, -100, 'm1', { id: '4', type: 1, set: false, bySelf: true }, publish)
    assert.deepEqual(reactionsOf(store, '-100:m1'), [])
})

test('他人取消回应会把计数减到零并删除条目', async () => {
    const { storage, store, publish } = createStorage()
    store.set('-100:m1', groupMessage('m1'))

    await applyMessageReaction(storage, -100, 'm1', { id: '5', type: 1, set: true, bySelf: false }, publish)
    await applyMessageReaction(storage, -100, 'm1', { id: '5', type: 1, set: false, bySelf: false }, publish)
    assert.deepEqual(reactionsOf(store, '-100:m1'), [])
})

test('回应早于消息入库时等待保存完成再更新', async () => {
    const { storage, store, publish } = createStorage()

    const pending = applyMessageReaction(
        storage,
        -100,
        'm1',
        { id: '128077', type: 2, set: true, bySelf: true },
        publish,
    )
    await new Promise((resolve) => setTimeout(resolve, 10))

    store.set('-100:m1', groupMessage('m1'))
    notifyMessageSave(storage, -100, 'm1', Promise.resolve())
    await pending

    assert.deepEqual(reactionsOf(store, '-100:m1'), [{ id: '128077', type: 2, count: 1, is_clicked: true }])
})

test('表情类型按系统表情短 ID / unicode 码点区分', () => {
    assert.equal(reactionTypeFromId('4'), 1)
    assert.equal(reactionTypeFromId('400'), 1)
    assert.equal(reactionTypeFromId('128077'), 2)
    assert.equal(reactionTypeFromId('marketface_abc'), 2)
})
