import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import Message from '@icalingua/types/Message'
import SQLStorageProvider, { MessageSearchIndexFactory } from './SQLStorageProvider'
import { dedupeHistoryBatch } from './HistoryDedupe'

const openProvider = (name: string) => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), `icalingua-history-dedupe-${name}-`))
    const searchIndex = {
        get isReady() {
            return false
        },
        async open() {},
        async close() {},
        async requestRebuild() {},
        async searchTimes() {
            return []
        },
    }
    const provider = new SQLStorageProvider(
        `history-dedupe-${name}`,
        'sqlite3',
        { dataPath: directory },
        (error) => {
            throw error
        },
        () => searchIndex as unknown as ReturnType<MessageSearchIndexFactory>,
    )
    return { provider, directory }
}

const message = (id: string | number, time: number, senderId: number, content: string, roomId = -100): Message => ({
    _id: id,
    time,
    senderId,
    content,
    username: String(senderId),
    files: [],
    roomId,
})

test('切换适配器后同一批历史消息不会被重复判为新消息', async () => {
    const { provider, directory } = openProvider('switch')
    try {
        await provider.connect()
        await provider.addMessages(-100, [
            message('bWlsa3ktMDAx', 1_700_000_001_000, 42, '你好'),
            message('bWlsa3ktMDAy', 1_700_000_002_000, 43, '在的'),
            message('bWlsa3ktMDAz', 1_700_000_003_000, 42, '看图'),
        ])

        const batch = [
            message(900001, 1_700_000_001_000, 42, '你好'),
            message(900002, 1_700_000_002_000, 43, '在的'),
            message(900003, 1_700_000_003_000, 42, '看图'),
        ]
        const result = await dedupeHistoryBatch(provider, -100, batch)
        assert.equal(result.fresh.length, 0)
        assert.equal(result.duplicates, 3)
        assert.equal(result.boundaryStored, true)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('部分重叠时只写入新的部分，并报告边界已存在', async () => {
    const { provider, directory } = openProvider('partial')
    try {
        await provider.connect()
        await provider.addMessages(-200, [
            message('old-1', 1_700_000_001_000, 42, 'a'),
            message('old-2', 1_700_000_002_000, 42, 'b'),
        ])
        const batch = [
            message('new-1', 1_700_000_001_000, 42, 'a'),
            message('new-2', 1_700_000_002_000, 42, 'b'),
            message('new-3', 1_700_000_003_000, 42, 'c'),
            message('new-4', 1_700_000_004_000, 42, 'd'),
        ]
        const result = await dedupeHistoryBatch(provider, -200, batch)
        assert.equal(result.duplicates, 2)
        assert.deepEqual(
            result.fresh.map((item) => item.content),
            ['c', 'd'],
        )
        assert.equal(result.boundaryStored, true)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('空库时全部视为新消息且边界不存在（要继续往更早拉）', async () => {
    const { provider, directory } = openProvider('empty')
    try {
        await provider.connect()
        const result = await dedupeHistoryBatch(provider, -300, [
            message('n-1', 1_700_000_001_000, 42, 'x'),
            message('n-2', 1_700_000_002_000, 42, 'y'),
        ])
        assert.equal(result.fresh.length, 2)
        assert.equal(result.boundaryStored, false)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('时间窗口内消息很多时会翻页扫描，不会只看第一页', async () => {
    const { provider, directory } = openProvider('paging')
    try {
        await provider.connect()
        const stored: Message[] = []
        for (let index = 0; index < 450; index++) {
            stored.push(message(`storage-${index}`, 1_700_000_000_000 + index, 42, `m${index}`))
        }
        await provider.addMessages(-400, stored)

        const batch: Message[] = []
        for (let index = 440; index < 450; index++) {
            batch.push(message(`other-${index}`, 1_700_000_000_000 + index, 42, `m${index}`))
        }
        const result = await dedupeHistoryBatch(provider, -400, batch)
        assert.equal(result.duplicates, 10)
        assert.equal(result.boundaryStored, true)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('同人同时间但内容不同不算重复', async () => {
    const { provider, directory } = openProvider('same-time')
    try {
        await provider.connect()
        await provider.addMessages(-500, [message('t-1', 1_700_000_005_000, 42, '1')])
        const result = await dedupeHistoryBatch(provider, -500, [message('t-2', 1_700_000_005_000, 42, '2')])
        assert.equal(result.fresh.length, 1)
        assert.equal(result.duplicates, 0)
        assert.equal(result.boundaryStored, false)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('同一秒两条不同图片不会被判成重复（content 为空，靠附件标识区分）', async () => {
    const { provider, directory } = openProvider('image-burst')
    const image = (id: string, url: string, time = 1_700_000_009_000): Message => ({
        ...message(id, time, 42, '', -600),
        files: [{ type: 'image/jpeg', url }],
    })
    try {
        await provider.connect()
        await provider.addMessages(-600, [
            image('old-img-1', 'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=AAA111'),
        ])
        const result = await dedupeHistoryBatch(provider, -600, [
            image('new-img-1', 'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=AAA111&rkey=rotated'),
            image('new-img-2', 'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=BBB222'),
        ])
        assert.deepEqual(
            result.fresh.map((item) => item._id),
            ['new-img-2'],
        )
        assert.equal(result.duplicates, 1)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('纯媒体且取不到附件标识时退回 _id 判断，不丢消息', async () => {
    const { provider, directory } = openProvider('media-no-key')
    try {
        await provider.connect()
        await provider.addMessages(-600, [message('old-anon', 1_700_000_010_000, 42, '', -600)])
        const result = await dedupeHistoryBatch(provider, -600, [message('new-anon', 1_700_000_010_000, 42, '', -600)])
        assert.equal(result.fresh.length, 1)
        assert.equal(result.duplicates, 0)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('库内是实时入库（同秒序号偏移）的消息，历史拉回来的整秒消息仍要判重', async () => {
    const { provider, directory } = openProvider('realtime-offset')
    const second = 1_700_000_020
    try {
        await provider.connect()
        // 实时路径：time = data.time * 1000 + 同秒序号
        await provider.addMessages(
            -700,
            ['a', 'b', 'c', 'd', 'e'].map((content, index) =>
                message(`realtime-${index}`, second * 1000 + index, 42, content, -700),
            ),
        )
        // 历史路径：time = data.time * 1000
        const batch = ['a', 'b', 'c', 'd', 'e'].map((content, index) =>
            message(`history-${index}`, second * 1000, 42, content, -700),
        )
        const result = await dedupeHistoryBatch(provider, -700, batch)
        assert.equal(result.duplicates, 5, '同秒 5 条全部应判重')
        assert.equal(result.fresh.length, 0)
        assert.equal(result.boundaryStored, true)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})

test('扫描窗口覆盖同秒序号偏移，边界上的 +2 记录也能判重', async () => {
    const { provider, directory } = openProvider('window-tail')
    const second = 1_700_000_030
    try {
        await provider.connect()
        await provider.addMessages(-800, [
            message('stored-a', second * 1000, 42, 'a', -800),
            message('stored-b', second * 1000 + 2, 42, 'b', -800),
        ])
        const result = await dedupeHistoryBatch(provider, -800, [
            message('history-a', second * 1000, 42, 'a', -800),
            message('history-b', second * 1000, 42, 'b', -800),
        ])
        assert.equal(result.duplicates, 2, 'b 的库内时间比批次上界大 2ms，不能被窗口截断')
        assert.equal(result.boundaryStored, true)
    } finally {
        await provider.close()
        fs.rmSync(directory, { recursive: true, force: true })
    }
})
