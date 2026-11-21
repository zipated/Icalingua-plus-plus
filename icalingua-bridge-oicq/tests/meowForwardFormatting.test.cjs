const assert = require('node:assert/strict')

const createProcessMessage = require('../utils/processMessage').default
const {
    createForwardOpenEvent,
    isStandaloneForwardText,
    parseForwardCard,
    parseForwardResource,
} = require('../../icalingua/src/renderer/components/vac-mod/utils/forwardMessage')
const { formatMessageParts } = require('../../icalingua/src/renderer/components/vac-mod/utils/messageFormatting')

// 客户端 FormatMessage 的 forwardMessage 计算属性：取正文里的第一条转发段
const pickForwardPart = (content) =>
    formatMessageParts(content).find((part) => part.type === 'forward' || part.type === 'nestedforward')

const RES_ID = 'N7vzi91424frrAsPA2lQS9taPTkLmD7D6EFXijqAXMkBHAZEBvrnryybf5gfvBWF'

async function run() {
    const processMessage = createProcessMessage({ getUin: () => 10000 })

    // 某些服务的 forward 段只带 resid，不内嵌 content
    const message = { content: '', files: [] }
    const lastMessage = { content: '' }
    await processMessage([{ type: 'forward', data: { id: RES_ID } }], message, lastMessage)

    assert.equal(lastMessage.content, '[Forward multiple messages]')
    assert.equal(message.content, `[Forward: ${RES_ID}]`)

    // code 必须让客户端渲染成卡片，并且卡片里能取到 resid 用于点击拉取
    assert.ok(message.code, 'forward 段必须写入 code，否则客户端只显示 [Forward: ...] 纯文本')
    const card = parseForwardCard(message.code)
    assert.equal(card.footer, '聊天记录')
    assert.equal(parseForwardResource(message.code).resId, RES_ID)

    // 后端若带上预览信息，卡片要能显示出来
    const rich = { content: '', files: [] }
    await processMessage(
        [
            {
                type: 'forward',
                data: {
                    id: RES_ID,
                    source: '张三和李四的聊天记录',
                    summary: '查看 3 条转发消息',
                    preview: ['张三: 你好', '李四: [图片]', null],
                },
            },
        ],
        rich,
        { content: '' },
    )
    assert.deepEqual(parseForwardCard(rich.code), {
        title: '张三和李四的聊天记录',
        messages: ['张三: 你好', '李四: [图片]'],
        footer: '查看 3 条转发消息',
    })

    // 只有 file_name 时按嵌套转发处理
    const nested = { content: '', files: [] }
    await processMessage(
        [{ type: 'forward', data: { file_name: 'MultiMsg_11111111-2222-3333-4444-555555555555' } }],
        nested,
        { content: '' },
    )
    assert.equal(nested.content, '[NestedForward: MultiMsg_11111111-2222-3333-4444-555555555555]')

    // NapCat 风格：forward 段内嵌 content，仍然直接铺成消息数组
    const inline = { content: '', files: [] }
    await processMessage(
        [
            {
                type: 'forward',
                data: {
                    id: RES_ID,
                    content: [
                        { sender: { user_id: 10086, nickname: '发送者A' }, time: 1595694374, message: [{ type: 'text', data: { text: 'hi' } }] },
                    ],
                },
            },
        ],
        inline,
        { content: '' },
    )
    assert.equal(inline.content, `[Forward: ${RES_ID}]`)
    const inlineMessages = JSON.parse(inline.code)
    assert.equal(inlineMessages.length, 1)
    assert.equal(inlineMessages[0].username, '发送者A')
    assert.equal(inlineMessages[0].content, 'hi')

    // 客户端侧：正文 + code 能否解析成「卡片 + 点击打开」所需的资源
    const part = pickForwardPart(message.content)
    assert.ok(part, '正文里的 [Forward: resid] 必须被识别成转发段')
    assert.equal(
        createForwardOpenEvent(part.type, part.value, parseForwardResource(message.code)).resId,
        RES_ID,
    )

    // 老消息：只存了 [Forward: resid] 没有 code，也仍然要能渲染卡片并打开
    const legacyPart = pickForwardPart(`[Forward: ${RES_ID}]`)
    assert.ok(legacyPart)
    assert.equal(createForwardOpenEvent(legacyPart.type, legacyPart.value, parseForwardResource(undefined)).resId, RES_ID)

    // 无 code 时只有「整条消息就是一个转发标记」才渲染卡片，夹带文字要保留文字
    assert.equal(isStandaloneForwardText(`[Forward: ${RES_ID}]`, legacyPart), true)
    assert.equal(isStandaloneForwardText(`  [Forward: ${RES_ID}]\n`, legacyPart), true)
    assert.equal(isStandaloneForwardText(`看看这个\n[Forward: ${RES_ID}]`, legacyPart), false)
    assert.equal(isStandaloneForwardText('[NestedForward: nested.json]', { type: 'nestedforward', value: 'nested.json' }), true)

    console.log('meow forward formatting ok')
}

run().catch((error) => {
    console.error(error)
    process.exit(1)
})
