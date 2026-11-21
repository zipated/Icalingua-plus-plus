const assert = require('node:assert/strict')

const OnebotClient = require('../clients/OnebotClient').default

// 不连真实 WS：把 socket 换成一个记录 send 的假实现
function createClient() {
    const client = new OnebotClient('ws://127.0.0.1:3001')
    const sent = []
    client.socket = { send: (raw) => sent.push(JSON.parse(raw)) }
    return { client, sent }
}

function reply(client, frame) {
    return client.handleWebSocketMessage(JSON.stringify(frame))
}

async function run() {
    {
        const { client, sent } = createClient()
        const promise = client.markMessageAsRead(123)
        assert.equal(sent[0].action, 'mark_msg_as_read')
        reply(client, {
            status: 'failed',
            retcode: 100,
            data: null,
            wording: 'message not found',
            echo: sent[0].echo,
        })
        // 旧实现 reject 的是 "undefined\nmessage not found"，日志里只剩两行没法定位
        await assert.rejects(promise, (error) => {
            assert.ok(error instanceof Error, '必须以 Error 形式抛出')
            assert.equal(error.message, 'mark_msg_as_read 失败：message not found')
            return true
        })
    }

    // 某些实现把原因放在 msg 里
    {
        const { client, sent } = createClient()
        const promise = client.getForwardMessage('res-id')
        assert.deepEqual(
            { id: sent[0].params.id, message_id: sent[0].params.message_id },
            { id: 'res-id', message_id: 'res-id' },
            'resid 必须同时以 id / message_id 下发',
        )
        reply(client, { status: 'failed', retcode: 1404, msg: 'bad resid', echo: sent[0].echo })
        await assert.rejects(promise, { message: 'get_forward_msg 失败：bad resid' })
    }

    // 两者都没有时退回 retcode
    {
        const { client, sent } = createClient()
        const promise = client.getStatus()
        reply(client, { status: 'failed', retcode: 1400, echo: sent[0].echo })
        await assert.rejects(promise, { message: 'get_status 失败：1400' })
    }

    // 成功路径不受影响
    {
        const { client, sent } = createClient()
        const promise = client.getStatus()
        reply(client, { status: 'ok', retcode: 0, data: { online: true }, echo: sent[0].echo })
        assert.deepEqual(await promise, { online: true })
    }

    console.log('onebot api failure ok')
}

run().catch((error) => {
    console.error(error)
    process.exit(1)
})
