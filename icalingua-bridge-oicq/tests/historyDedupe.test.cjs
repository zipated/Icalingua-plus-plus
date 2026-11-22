const assert = require('node:assert/strict')

const { dedupeHistoryBatch, historyMessageIdentity } = require('../../packages/storageProviders/HistoryDedupe')

const compareMessages = (left, right) => {
    const timeDifference = Number(left.time || 0) - Number(right.time || 0)
    if (timeDifference) return timeDifference
    return String(left._id) < String(right._id) ? -1 : String(left._id) > String(right._id) ? 1 : 0
}

// 游标语义与 SQLStorageProvider 一致
function createStorage(messages) {
    return {
        messages,
        async fetchMessages(roomId, options, limit) {
            const sorted = this.messages.filter((message) => message.roomId === roomId).sort(compareMessages)
            const cursor = options?.after || options?.before
            let page = sorted
            if (cursor) {
                page = sorted.filter((message) => {
                    const time = Number(message.time || 0)
                    if (time !== Number(cursor.time)) return options.after ? time > cursor.time : time < cursor.time
                    return options.after
                        ? String(message._id) > String(cursor.id)
                        : String(message._id) < String(cursor.id)
                })
                if (options.before) page = page.reverse()
            }
            return page.slice(0, limit)
        },
    }
}

const stored = (id, time, senderId, content, roomId = -100) => ({
    _id: id,
    roomId,
    time,
    senderId,
    content,
    username: String(senderId),
    files: [],
})

const incoming = (id, time, senderId, content, roomId = -100) => ({ ...stored(id, time, senderId, content, roomId) })

async function run() {
    {
        const storage = createStorage([
            stored('AAAAmilkyid1', 1000, 42, '你好'),
            stored('AAAAmilkyid2', 2000, 43, '在的'),
            stored('AAAAmilkyid3', 3000, 42, '看图'),
        ])
        const batch = [
            incoming(900001, 1000, 42, '你好'),
            incoming(900002, 2000, 43, '在的'),
            incoming(900003, 3000, 42, '看图'),
        ]
        const result = await dedupeHistoryBatch(storage, -100, batch)
        assert.equal(result.fresh.length, 0, '换了 id 空间的同一批消息不应重复入库')
        assert.equal(result.duplicates, 3)
        assert.equal(result.boundaryStored, true, '最早一条已存在，应通知调用方停止继续往更早拉')
    }

    {
        const storage = createStorage([stored('old-1', 1000, 42, 'a'), stored('old-2', 2000, 42, 'b')])
        const batch = [1000, 2000, 3000, 4000].map((time, index) => incoming(`new-${index}`, time, 42, 'abcd'[index]))
        const result = await dedupeHistoryBatch(storage, -100, batch)
        assert.equal(result.fresh.length, 2)
        assert.deepEqual(
            result.fresh.map((message) => message.time),
            [3000, 4000],
        )
        assert.equal(result.boundaryStored, true)
    }

    {
        const storage = createStorage([])
        const batch = [incoming(1, 1000, 42, 'a'), incoming(2, 2000, 42, 'b')]
        const result = await dedupeHistoryBatch(storage, -100, batch)
        assert.equal(result.fresh.length, 2)
        assert.equal(result.boundaryStored, false)
    }

    {
        const storage = createStorage([stored('old-1', 5000, 42, '1')])
        const result = await dedupeHistoryBatch(storage, -100, [incoming('new-1', 5000, 42, '2')])
        assert.equal(result.fresh.length, 1)
        assert.equal(result.duplicates, 0)
    }

    {
        const messages = []
        for (let index = 0; index < 450; index++)
            messages.push(stored(`storage-${index}`, 100000 + index, 42, `m${index}`))
        const storage = createStorage(messages)
        const batch = []
        for (let index = 440; index < 450; index++)
            batch.push(incoming(`other-${index}`, 100000 + index, 42, `m${index}`))
        const result = await dedupeHistoryBatch(storage, -100, batch)
        assert.equal(result.fresh.length, 0)
        assert.equal(result.duplicates, 10)
    }

    {
        const storage = createStorage([stored('old-self', 3000, 7, 'self', 100)])
        const batch = [incoming(7001, 2000, 8, 'other', 100), incoming(7002, 3000, 7, 'self', 100)]
        const defaultResult = await dedupeHistoryBatch(storage, 100, batch)
        assert.equal(defaultResult.boundaryStored, false, '默认边界是这批最早的一条')
        const selfResult = await dedupeHistoryBatch(storage, 100, batch, { boundary: batch[1] })
        assert.equal(selfResult.boundaryStored, true)
    }

    {
        assert.equal(historyMessageIdentity({ _id: 'x', content: '  a  b ', senderId: 1 }), null)
        assert.equal(historyMessageIdentity({ _id: 'x', time: 1000, senderId: 1, content: ' a  b ' }), '1|1|a b|')
    }

    // 8. 同一秒两条不同的图片：content 都为空，必须靠附件标识区分，不能当成重复
    {
        const imageMsg = (id, time, senderId, url, roomId = -600) => ({
            ...stored(id, time, senderId, '', roomId),
            file: { type: 'image/jpeg', url },
            files: [{ type: 'image/jpeg', url }],
        })
        const storage = createStorage([
            imageMsg(
                'old-img-1',
                1_700_000_001_000,
                42,
                'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=AAA111',
            ),
        ])
        const batch = [
            imageMsg(
                'new-img-1',
                1_700_000_001_000,
                42,
                'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=AAA111&rkey=rotated',
            ),
            imageMsg(
                'new-img-2',
                1_700_000_001_000,
                42,
                'https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=BBB222',
            ),
        ]
        const result = await dedupeHistoryBatch(storage, -600, batch)
        assert.deepEqual(
            result.fresh.map((item) => item._id),
            ['new-img-2'],
            '同一秒另一张图片不能被误判为重复',
        )
        assert.equal(result.duplicates, 1, '只有 fileid 相同（rkey 轮换）那张才算重复')
    }

    // 9. 图片走 md5 链接时也能区分
    {
        const storage = createStorage([
            {
                ...stored('old-md5', 1_700_000_002_000, 42, '', -600),
                files: [
                    {
                        type: 'image/webp',
                        url: 'https://gchat.qpic.cn/gchatpic_new/0/0-0-0123456789abcdef0123456789abcdef/0',
                    },
                ],
            },
        ])
        const same = await dedupeHistoryBatch(storage, -600, [
            {
                ...incoming('new-md5', 1_700_000_002_000, 42, ''),
                files: [
                    {
                        type: 'image/webp',
                        url: 'https://gchat.qpic.cn/gchatpic_new/0/0-0-0123456789ABCDEF0123456789ABCDEF/0',
                    },
                ],
            },
        ])
        assert.equal(same.duplicates, 1, 'md5 大小写不同也应视为同一条')

        const different = await dedupeHistoryBatch(storage, -600, [
            {
                ...incoming('new-md5-b', 1_700_000_002_000, 42, ''),
                files: [
                    {
                        type: 'image/webp',
                        url: 'https://gchat.qpic.cn/gchatpic_new/0/0-0-ffffffffffffffffffffffffffffffff/0',
                    },
                ],
            },
        ])
        assert.equal(different.fresh.length, 1, 'md5 不同是新图片')
    }

    // 10. 纯媒体但取不到稳定标识：不生成身份，退回 _id（宁可重复也不丢消息）
    {
        const storage = createStorage([stored('old-anon', 1_700_000_003_000, 42, '', -600)])
        const result = await dedupeHistoryBatch(storage, -600, [incoming('new-anon', 1_700_000_003_000, 42, '')])
        assert.equal(result.fresh.length, 1, '无附件标识的纯媒体消息不能被判成重复')
        assert.equal(result.duplicates, 0)
        assert.equal(historyMessageIdentity(incoming('x', 1_700_000_003_000, 42, '')), null)
    }

    // 11. 同一秒两条相同的纯文本仍按内容判重
    {
        const storage = createStorage([stored('old-text', 1_700_000_004_000, 42, '收到', -600)])
        const result = await dedupeHistoryBatch(storage, -600, [incoming('new-text', 1_700_000_004_000, 42, '收到')])
        assert.equal(result.duplicates, 1)
    }

    // 12. 实时入库的消息带同秒序号偏移，历史拉回来的整秒消息仍要判重
    {
        const second = 1_700_000_020
        const contents = ['a', 'b', 'c', 'd', 'e']
        const storage = createStorage(
            contents.map((content, index) => stored(`realtime-${index}`, second * 1000 + index, 42, content, -700)),
        )
        const batch = contents.map((content, index) => incoming(`history-${index}`, second * 1000, 42, content, -700))
        const result = await dedupeHistoryBatch(storage, -700, batch)
        assert.equal(result.duplicates, 5)
        assert.equal(result.fresh.length, 0)
        assert.equal(result.boundaryStored, true)
    }

    // 13. 扫描窗口要覆盖序号偏移，否则边界上的记录既判不了重也停不下来
    {
        const second = 1_700_000_030
        const storage = createStorage([
            stored('stored-a', second * 1000, 42, 'a', -800),
            stored('stored-b', second * 1000 + 2, 42, 'b', -800),
        ])
        const result = await dedupeHistoryBatch(storage, -800, [
            incoming('history-a', second * 1000, 42, 'a', -800),
            incoming('history-b', second * 1000, 42, 'b', -800),
        ])
        assert.equal(result.duplicates, 2)
        assert.equal(result.boundaryStored, true)
    }

    console.log('history dedupe ok')
}

run().catch((error) => {
    console.error(error)
    process.exit(1)
})
