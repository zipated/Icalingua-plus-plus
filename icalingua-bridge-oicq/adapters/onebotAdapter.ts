import type oicqAdapter from './oicqAdapter'
import { config, saveUserConfig, userConfig } from '../providers/configManager'
import LoginForm from '@icalingua/types/LoginForm'
import StorageProvider from '@icalingua/types/StorageProvider'
import MongoStorageProvider from '@icalingua/storage-providers/MongoStorageProvider'
import RedisStorageProvider from '@icalingua/storage-providers/RedisStorageProvider'
import SQLStorageProvider from '@icalingua/storage-providers/SQLStorageProvider'
import { broadcast, broadcastDatabaseUpgradeProgress } from '../providers/socketIoProvider'
import OnebotClient, { GroupMessage, PrivateMessage } from '../clients/OnebotClient'
import Room from '@icalingua/types/Room'
import ChatGroup from '@icalingua/types/ChatGroup'
import clients from '../utils/clients'
import IgnoreChatInfo from '@icalingua/types/IgnoreChatInfo'
import getSysInfo from '../utils/getSysInfo'
import {
    FakeMessage,
    FriendInfo,
    Gender,
    GfsDirStat,
    GfsFileStat,
    GroupInfo,
    GroupMessageEventData,
    GroupRole,
    MediaFile,
    MemberBaseInfo,
    MemberInfo,
    MessageElem,
    PrivateMessageEventData,
    Ret,
} from 'oicq-icalingua-plus-plus'
import Message from '@icalingua/types/Message'
import MessagePageOptions from '@icalingua/types/MessagePage'
import createProcessMessage, { registerSilkDecodeCompleter } from '../utils/processMessage'
import { convertLegacyIcalinguaAt, decodeIcalinguaAtName, findIcalinguaAtMarkup } from '../utils/icalinguaAt'
import {
    getMediaPartIndex,
    shiftMediaOrdersAfterTextReplacement,
    splitContentByMediaOrder,
} from '../utils/messageMediaOrder'
import formatDate from '../utils/formatDate'
import sleep from '../utils/sleep'
import { Socket } from 'socket.io'
import SendMessageParams from '@icalingua/types/SendMessageParams'
import crypto from 'crypto'
import SearchableFriend from '@icalingua/types/SearchableFriend'
import { isArrayLike } from 'lodash'
import createRoom from '../utils/createRoom'
import md5 from 'md5'
import path from 'path'
import fsP from 'fs/promises'
import { deleteUploadedFile, getUploadedFilePath, getUploadedFileName } from '../utils/uploadFileManager'

let bot: OnebotClient
let loginForm: LoginForm
let storage: StorageProvider
let uin: number
let bkn: number = 0
let nickname: string
let isAutoFetching = false
let stopFetching = false
// 同一会话同时只允许一轮拉取，客户端重复触发时直接跳过
const fetchingHistoryRooms = new Set<number>()

// NapCat 的 message_id 是 MessageUnique 生成的短 id，会因映射表淘汰、peer 变化、重建等原因改变，
// 不能作为“是否同一条消息 / 是否拉到旧消息”的唯一依据。
// real_seq（msgSeq）对同一条消息是稳定的，优先用它做身份标识。
const getHistoryRealSeq = (data?: { real_seq?: string | number }) => {
    const seq = Number(data?.real_seq)
    return Number.isSafeInteger(seq) && seq > 0 ? seq : undefined
}

// real_seq 缺失时退化为 时间 + 发送者 + 消息段摘要 的指纹
const getHistoryMessageIdentity = (data: GroupMessage | PrivateMessage) => {
    // 直接用字符串形式，避免大整数转 Number 后丢精度
    const seq = data.real_seq
    if (seq !== undefined && seq !== null && String(seq) !== '' && String(seq) !== '0') return `seq:${seq}`
    const segments = Array.isArray(data.message)
        ? data.message
              .map((elem) => {
                  const elemData = (elem as any).data ?? {}
                  return `${elem.type}:${elemData.file ?? ''}:${elemData.text ?? ''}`
              })
              .join('|')
        : ''
    return `fp:${data.time}:${data.sender?.user_id ?? ''}:${segments}`
}

// 已入库消息的指纹：只用不随 message_id 变化的字段（时间、发送者、内容、附件摘要）
const getStoredMessageIdentity = (message: Message) => {
    const files = Array.isArray(message.files)
        ? message.files.map((file) => `${file.type}:${file.name ?? ''}:${file.fid ?? ''}`).join(',')
        : ''
    return `${message.time}|${message.senderId}|${message.content}|${files}`
}

// 每个会话已入库消息的指纹缓存，跨多次拉取有效，避免 message_id 变化后同一消息被反复入库
const roomStoredIdentities = new Map<number, Set<string>>()
const ROOM_STORED_IDENTITIES_LIMIT = 30
const ROOM_STORED_IDENTITIES_SEED = 2000

const getRoomStoredIdentities = async (roomId: number): Promise<Set<string>> => {
    const cached = roomStoredIdentities.get(roomId)
    if (cached) {
        // 重新放到 Map 末尾，保持简单的 LRU 顺序
        roomStoredIdentities.delete(roomId)
        roomStoredIdentities.set(roomId, cached)
        return cached
    }
    const identities = new Set<string>()
    try {
        const messages = await storage.fetchMessages(roomId, {}, ROOM_STORED_IDENTITIES_SEED)
        for (const message of messages || []) identities.add(getStoredMessageIdentity(message))
    } catch (e) {
        console.log('读取已入库消息指纹失败', e)
    }
    roomStoredIdentities.set(roomId, identities)
    while (roomStoredIdentities.size > ROOM_STORED_IDENTITIES_LIMIT) {
        const oldestRoomId = roomStoredIdentities.keys().next().value
        if (oldestRoomId === undefined) break
        roomStoredIdentities.delete(oldestRoomId)
    }
    return identities
}

// 群成员信息缓存
const MEMBER_CACHE_TTL = 5 * 60 * 1000 // 5分钟
const memberInfoCache = new Map<string, { info: MemberInfo; timestamp: number }>()
let lastReceivedMessageInfo = {
    timestamp: 0,
    id: 0,
}
let rkey: {
    type: 'private' | 'group'
    rkey: string
    created_at: number
    ttl: string
}[] = []

const initStorage = async () => {
    try {
        switch (loginForm.storageType) {
            case 'mdb':
                storage = new MongoStorageProvider(loginForm.mdbConnStr, loginForm.username)
                break
            case 'redis':
                storage = new RedisStorageProvider(loginForm.rdsHost, `${loginForm.username}`)
                break
            case 'sqlite':
                storage = new SQLStorageProvider(`${loginForm.username}`, 'sqlite3', {
                    dataPath: 'data',
                })
                break
            case 'mysql':
                storage = new SQLStorageProvider(`${loginForm.username}`, 'mysql', {
                    host: loginForm.sqlHost,
                    user: loginForm.sqlUsername,
                    password: loginForm.sqlPassword,
                    database: loginForm.sqlDatabase,
                })
                break
            case 'pg':
                storage = new SQLStorageProvider(`${loginForm.username}`, 'pg', {
                    host: loginForm.sqlHost,
                    user: loginForm.sqlUsername,
                    password: loginForm.sqlPassword,
                    database: loginForm.sqlDatabase,
                })
                break
            default:
                break
        }
        storage.onUpgradeProgress = (progress) => broadcastDatabaseUpgradeProgress(progress)
        await storage.connect()
        registerSilkDecodeCompleter({
            replaceMessage: (roomId, messageId, message) => storage.replaceMessage(roomId, messageId, message),
            renewMessage: (roomId, messageId, message) => clients.renewMessage(roomId, messageId, message),
            getMessage: (roomId, messageId) => storage.getMessage(roomId, messageId),
        })
        storage.getAllRooms().then((e) => {
            e.forEach(async (e) => {
                //更新群的名称
                if (e.roomId > -1) return
                try {
                    const group = await bot.getGroupInfo(-e.roomId)
                    if (group && group.group_name !== e.roomName) {
                        await storage.updateRoom(e.roomId, { roomName: group.group_name })
                    }
                } catch (e) {
                    console.error(e)
                }
            })
        })
    } catch (err) {
        console.log(err)
        console.log('无法连接数据库')
        broadcast('fatal', '无法连接数据库')
        process.exit(2)
    }
}
const attachEventHandler = () => {
    bot.on('message', async (data) => {
        if (data.time !== lastReceivedMessageInfo.timestamp) {
            lastReceivedMessageInfo.timestamp = data.time
            lastReceivedMessageInfo.id = 0
        }
        const now = new Date(data.time * 1000)
        const groupId = data.message_type === 'group' ? data.group_id : undefined
        const nonSelfId = data.user_id === uin ? data.target_id || data.user_id : data.user_id
        const senderId = data.sender.user_id
        let roomId = groupId ? -groupId : nonSelfId
        if (await storage.isChatIgnored(roomId)) return
        const isSelfMsg = uin === senderId
        let senderName: string
        if (groupId && (<GroupMessage>data).anonymous) senderName = (<GroupMessage>data).anonymous.name
        else if (groupId && isSelfMsg) senderName = 'You'
        else if (groupId) senderName = (data.sender as MemberBaseInfo).card || data.sender.nickname
        else {
            let info: Awaited<ReturnType<typeof bot.getStrangerInfo>>
            try {
                info = await bot.getStrangerInfo(senderId)
            } catch (e) {
                console.log('无法 getStrangerInfo', e)
            }
            senderName = info?.remark || (data.sender as FriendInfo).remark || info?.nickname || data.sender.nickname
        }
        const group = groupId ? await bot.getGroupInfo(groupId) : null
        let roomName = groupId ? group.group_name : senderName

        const message: Message = {
            senderId: senderId,
            username: senderName,
            content: '',
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            _id: data.message_id,
            role: (data.sender as MemberBaseInfo).role,
            title: groupId && (<GroupMessage>data).anonymous ? '匿名' : (data.sender as MemberBaseInfo).title,
            files: [],
            anonymousId: groupId && (<GroupMessage>data).anonymous ? (<GroupMessage>data).anonymous.id : null,
            anonymousflag: groupId && (<GroupMessage>data).anonymous ? (<GroupMessage>data).anonymous.flag : null,
        }

        let room = await storage.getRoom(roomId)
        if (!room) {
            if (data.post_type === 'message_sent') {
                const info = await adapter.getFriendInfo(data.user_id)
                roomName = info.remark || info.nickname
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        } else {
            if (!room.roomName.startsWith(roomName) && data.post_type === 'message') {
                room.roomName = roomName
            }
        }

        //begin process msg
        const lastMessage = {
            content: '',
            timestamp: formatDate('hh:mm', now),
            username: senderName,
            userId: senderId,
        }
        ////process message////
        try {
            await processMessage(data.message, message, lastMessage, roomId)
        } catch (e) {
            // 单个消息段解析失败时保留已解析的内容继续入库，避免整条消息丢失
            console.error('处理消息失败', e)
        }

        // 鬼知道服务器改了什么，收到的语言消息有时候没有 url，尝试重新获取
        if (message.content === '[无法处理的语音]undefined') {
            const regetMsg = await adapter.getMsg(data.message_id.toString())
            if (!regetMsg.error && regetMsg.data) {
                message.content = ''
                await processMessage(regetMsg.data.message, message, lastMessage, roomId)
            }
        }

        // 自动回复消息作为小通知短暂显示
        // if ('auto_reply' in data && data.auto_reply) {
        //     clients.message(message.content)
        //     return
        // }

        const at = message.at
        if (at) {
            room.at = at
            room.atMessageId = String(message._id)
        }

        if (!room.priority) {
            room.priority = groupId ? 2 : 4
        }

        //可能要发通知，所以由客户端来决定
        let image
        if (message.file && message.file.type.startsWith('image/')) image = message.file.url
        broadcast('notify', {
            priority: room.priority,
            roomId,
            at,
            isSelfMsg,
            image,
            data: {
                title: room.roomName,
                body: (groupId ? senderName + ': ' : '') + lastMessage.content,
                hasReply: true,
                replyPlaceholder: 'Reply to ' + room.roomName,
            },
        })

        if (isSelfMsg) {
            room.unreadCount = 0
            room.at = false
        } else room.unreadCount++
        if (!room.at) room.atMessageId = null
        // 加上同一秒收到消息的id，防止消息乱序
        room.utime = data.time * 1000 + lastReceivedMessageInfo.id
        room.lastMessage = lastMessage
        message.time = data.time * 1000 + lastReceivedMessageInfo.id
        lastReceivedMessageInfo.id++
        if (await storage.isChatIgnored(senderId)) message.hide = true
        clients.addMessage(room.roomId, message)
        await storage.updateRoom(roomId, room)
        clients.updateRoom(room)
        storage.addMessage(roomId, message)
        if (config.custom && data.post_type === 'message') {
            try {
                require('../custom').onMessage(data, bot)
            } catch (e) {
                clients.messageError('自定义插件出错')
                console.error(e)
            }
        }
    })
    bot.on('friendRecall', async (data) => {
        clients.deleteMessage(data.message_id.toString())
        storage.updateMessage(data.user_id, data.message_id, { deleted: true, reveal: false })
    })
    bot.on('groupRecall', async (data) => {
        clients.deleteMessage(data.message_id.toString())
        storage.updateMessage(-data.group_id, data.message_id, { deleted: true, reveal: false })
    })
    bot.on('friendPoke', async (data) => {
        const roomId = data.user_id == uin ? data.target_id : data.user_id
        if (await storage.isChatIgnored(roomId)) return
        const room = await storage.getRoom(roomId)
        const operator = data.sender_id || data.user_id
        const nors: any[] = data.raw_info || []
        if (room) {
            room.utime = data.time * 1000
            let msg = ''
            let qqCount = 0
            for (const nor of nors) {
                switch (nor.type) {
                    case 'qq':
                        qqCount++
                        if (qqCount === 1) {
                            if (operator != uin) msg += room.roomName
                            else msg += '你'
                            break
                        }
                        if (qqCount === 2) {
                            if (operator == data.target_id) msg += '自己'
                            else if (data.target_id != uin) msg += room.roomName
                            else msg += '你'
                            break
                        }
                        break
                    case 'img':
                        msg += '<ica:img>'
                        break
                    case 'nor':
                        msg += nor.txt
                        break
                }
            }
            room.lastMessage = {
                content: msg.replace(/<ica:img>/g, ''),
                username: null,
                timestamp: formatDate('hh:mm'),
                userId: operator,
            }
            const message: Message = {
                username: '',
                content: msg,
                senderId: data.sender_id,
                timestamp: formatDate('hh:mm:ss'),
                date: formatDate('yyyy/MM/dd'),
                _id: data.time,
                system: true,
                time: data.time * 1000,
                files: nors
                    .filter((it) => it.type === 'img')
                    .map((it) => ({
                        url: it.src,
                        type: 'image/gif',
                    })),
            }
            clients.addMessage(roomId, message)
            clients.updateRoom(room)
            storage.updateRoom(room.roomId, room)
            storage.addMessage(roomId, message)
        }
    })
    bot.on('groupPoke', async (data) => {
        if (await storage.isChatIgnored(-data.group_id)) return
        const room = await storage.getRoom(-data.group_id)
        if (room) {
            room.utime = data.time * 1000
            const operatorId = 'sender_id' in data ? (data.sender_id as number) : data.user_id
            const operatorObj = await bot.getGroupMemberInfo(data.group_id, operatorId, false)
            const operator = operatorObj.card ? operatorObj.card : operatorObj.nickname
            const userObj = await bot.getGroupMemberInfo(data.group_id, data.target_id, false)
            const user = userObj.card ? userObj.card : userObj.nickname
            const nors: any[] = data.raw_info?.filter((it) => (it.type as any) === 'nor') || []
            let msg = ''
            if (operatorId !== uin) msg += operator
            else msg += '你'
            msg += nors[0]?.txt || '戳了戳'
            if (data.target_id === operatorId) msg += '自己'
            else if (data.target_id === uin) msg += '你'
            else msg += user
            if (nors[1]?.txt) msg += nors[1]?.txt
            room.lastMessage = {
                content: msg,
                username: null,
                timestamp: formatDate('hh:mm'),
                userId: operatorId,
            }
            const message: Message = {
                username: '',
                content: msg,
                senderId: operatorId,
                timestamp: formatDate('hh:mm:ss'),
                date: formatDate('yyyy/MM/dd'),
                _id: data.time,
                system: true,
                time: data.time * 1000,
                files: [],
            }
            clients.addMessage(room.roomId, message)
            clients.updateRoom(room)
            storage.updateRoom(room.roomId, room)
            storage.addMessage(room.roomId, message)
        }
    })
    bot.on('groupIncrease', async (data) => {
        const now = new Date(data.time * 1000)
        const groupId = data.group_id
        const senderId = data.user_id
        const roomId = -groupId
        if (await storage.isChatIgnored(roomId)) return
        const userInfo = await bot.getGroupMemberInfo(data.group_id, data.user_id)
        const message: Message = {
            _id: `${now.getTime()}-${groupId}-${senderId}`,
            content: `${userInfo.nickname} 加入了本群`,
            username: userInfo.nickname,
            senderId,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            const group = await bot.getGroupInfo(groupId)
            let roomName = groupId.toString()
            if (group && group.group_name) {
                roomName = group.group_name
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', now),
            userId: senderId,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
    bot.on('groupDecrease', async (data) => {
        const now = new Date(data.time * 1000)
        const groupId = data.group_id
        const senderId = data.user_id
        let operator: Awaited<ReturnType<OnebotClient['getGroupMemberInfo']>>
        try {
            operator = await bot.getGroupMemberInfo(groupId, data.operator_id)
        } catch {}
        let roomId = -groupId
        if (await storage.isChatIgnored(roomId)) return
        let userDisplay = senderId.toString()
        try {
            const userInfo = await bot.getGroupMemberInfo(groupId, senderId)
            userDisplay = `${userInfo.card || userInfo.nickname}(${senderId})`
        } catch (e) {
            try {
                const userInfo = await bot.getStrangerInfo(senderId)
                userDisplay = `${userInfo.remark || userInfo.nickname}(${senderId})`
            } catch (e2) {
                console.log(e, e2)
            }
        }
        const message: Message = {
            _id: `${now.getTime()}-${groupId}-${senderId}`,
            content:
                userDisplay +
                (data.sub_type === 'leave'
                    ? ' 离开了本群'
                    : ` 被 ${operator?.card ? operator?.card : operator?.nickname} 踢了`),
            username: data.user_id.toString(),
            senderId: data.operator_id,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            const group = await bot.getGroupInfo(groupId)
            let roomName = groupId.toString()
            if (group && group.group_name) {
                roomName = group.group_name
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', now),
            userId: senderId,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
    bot.on('groupBan', async (data) => {
        const roomId = -data.group_id
        if (await storage.isChatIgnored(roomId)) return
        const now = new Date(data.time * 1000)
        const operator = await bot.getGroupMemberInfo(data.group_id, data.operator_id)
        let mutedUserName: string
        let muteAll = false
        if (data.user_id === 0) muteAll = true
        else if (data.user_id === 80000000) mutedUserName = '匿名用户'
        else {
            const mutedUser = await bot.getGroupMemberInfo(data.group_id, data.user_id)
            mutedUserName = mutedUser ? mutedUser.card || mutedUser.nickname : data.user_id.toString()
        }
        let content = `${operator.card || operator.nickname} `
        if (muteAll && data.duration > 0) content += '开启了全员禁言'
        else if (muteAll) content += '关闭了全员禁言'
        else if (data.duration === 0) content += `将 ${mutedUserName} 解除禁言`
        else content += `禁言 ${mutedUserName} ${data.duration / 60} 分钟`
        const message: Message = {
            _id: `mute-${now.getTime()}-${data.user_id}-${data.operator_id}`,
            content,
            username: operator.card || operator.nickname,
            senderId: data.operator_id,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            const group = await bot.getGroupInfo(data.group_id)
            let roomName = data.group_id.toString()
            if (group && group.group_name) {
                roomName = group.group_name
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', new Date(data.time)),
            userId: data.operator_id,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
    bot.on('groupAdmin', async (data) => {
        console.log(data)
        const roomId = -data.group_id
        if (await storage.isChatIgnored(roomId)) return
        const now = new Date(data.time * 1000)
        const newAdmin = await bot.getGroupMemberInfo(data.group_id, data.user_id)
        let content =
            data.sub_type === 'set'
                ? `群主设置 ${newAdmin.card || newAdmin.nickname}(${data.user_id}) 为管理员`
                : `群主取消了 ${newAdmin.card || newAdmin.nickname}(${data.user_id}) 的管理员资格`
        const message: Message = {
            _id: `admin-${now.getTime()}-${data.group_id}-${data.user_id}`,
            content,
            username: '群系统信息',
            senderId: 10000,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            const group = await bot.getGroupInfo(data.group_id)
            let roomName = data.group_id.toString()
            if (group && group.group_name) {
                roomName = group.group_name
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', new Date(data.time)),
            userId: data.user_id,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
    bot.on('groupTitle', async (data) => {
        console.log(data)
        const roomId = -data.group_id
        if (await storage.isChatIgnored(roomId)) return
        const now = new Date(data.time * 1000)
        const member = await bot.getGroupMemberInfo(data.group_id, data.user_id)
        let content = `恭喜 ${member.nickname}(${data.user_id}) 获得群主授予的 ${data.title} 头衔`
        const message: Message = {
            _id: `title-${now.getTime()}-${data.group_id}-${data.user_id}`,
            content,
            username: '群系统信息',
            senderId: 10000,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            const group = await bot.getGroupInfo(data.group_id)
            let roomName = data.group_id.toString()
            if (group && group.group_name) {
                roomName = group.group_name
            }
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', now),
            userId: data.user_id,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
    bot.on('friendAdd', async (data) => {
        const now = new Date(data.time * 1000)
        const senderId = data.user_id
        const roomId = senderId
        const friend = await bot.getStrangerInfo(data.user_id)
        const roomName = friend.nickname
        const message: Message = {
            _id: `${now.getTime()}-${senderId}-friendIncrease`,
            content: '你们成为了好友',
            username: friend.nickname,
            senderId,
            time: data.time * 1000,
            timestamp: formatDate('hh:mm:ss', now),
            date: formatDate('yyyy/MM/dd', now),
            system: true,
            files: [],
        }
        let room = await storage.getRoom(roomId)
        if (!room) {
            // create room
            room = createRoom(roomId, roomName)
            await storage.addRoom(room)
        }
        room.utime = data.time * 1000
        room.lastMessage = {
            content: message.content,
            username: '',
            timestamp: formatDate('hh:mm', now),
            userId: data.user_id,
        }
        clients.addMessage(roomId, message)
        clients.updateRoom(room)
        storage.updateRoom(roomId, room)
        storage.addMessage(roomId, message)
    })
}

const refreshRkey = async () => {
    const rkeyy = await bot.getRkey()
    if (isArrayLike(rkeyy)) {
        rkey = rkeyy
    }
    console.log('Rkey 已刷新', rkeyy)
}

const replaceRkey = (url: string) => {
    if (!rkey.length) return url
    if (!url) return url
    if (
        !url.startsWith('https://multimedia.nt.qq.com.cn/download') &&
        !url.startsWith('https://gchat.qpic.cn/download')
    ) {
        return url
    }

    const u = new URL(url)
    let r = ''
    switch (u.searchParams.get('appid')) {
        case '1406':
            r = rkey.find((it) => it.type === 'private')?.rkey
            break
        case '1407':
            r = rkey.find((it) => it.type === 'group')?.rkey
            break
        default:
            return url
    }
    if (!r) return url
    if (r.startsWith('&rkey=')) r = r.slice('&rkey='.length)
    u.searchParams.set('rkey', r)
    return u.toString()
}

const adapter: typeof oicqAdapter = {
    isMessageSearchIndexReady: () => storage?.isMessageSearchIndexReady?.() === true,
    validateMessageSearchIndex: () => storage?.validateMessageSearchIndex?.() || Promise.resolve(),
    migrateLegacyAtMessages: () => storage?.migrateLegacyAtMessages?.() || Promise.resolve(),
    loggedIn: false,
    async createBot(form: LoginForm) {
        bot = new OnebotClient(config.onebot)
        await bot.connect()
        adapter.loggedIn = true
        const loginInfo = await bot.getLoginInfo()
        uin = loginInfo.user_id
        nickname = loginInfo.nickname
        loginForm = form
        loginForm.username = loginInfo.user_id
        await initStorage()
        attachEventHandler()
        setInterval(adapter.sendOnlineData, 1000 * 60)
        userConfig.account = loginForm
        saveUserConfig()
        adapter.sendOnlineData()
        refreshRkey()
        setInterval(refreshRkey, 1000 * 60 * 10)
    },
    async sendOnlineData() {
        const versionInfo = await bot.getVersionInfo()
        clients.sendOnlineData({
            online: (await bot.getStatus()).online,
            nick: nickname,
            uin,
            sysInfo:
                getSysInfo() +
                '\n\n' +
                `OneBot Backend: ${versionInfo.app_name} ${versionInfo.app_version}` +
                (versionInfo.runtime_os && versionInfo.runtime_version
                    ? `\n${versionInfo.runtime_os} ${versionInfo.runtime_version}`
                    : ''),
            bkn: 0,
        })
        clients.setAllRooms(await storage.getAllRooms())
        clients.setAllChatGroups(await storage.getAllChatGroups())
    },
    async getMsg(id: string) {
        const message = await bot.getMessage(Number(id))
        return {
            data:
                message.message_type === 'group'
                    ? ({
                          message: message.message,
                          message_id: message.message_id.toString(),
                          sub_type: 'normal',
                          message_type: 'group',
                          atme: false,
                          block: false,
                          group_id: message.group_id,
                          group_name: '',
                          font: '',
                          anonymous: null,
                          sender: {
                              user_id: message.sender.user_id,
                              age: 0,
                              nickname: message.sender.nickname,
                              card: '',
                              sex: 'unknown',
                              role: 'member',
                              title: '',
                              area: '',
                              level: 0,
                          },
                          user_id: message.sender.user_id,
                          bubble_id: 0,
                          time: message.time,
                          post_type: 'message',
                          reply(...args: any) {},
                          seqid: 0,
                          self_id: uin,
                          raw_message: message.raw_message,
                      } as GroupMessageEventData)
                    : ({
                          user_id: message.sender.user_id,
                          message: message.message,
                          message_id: message.message_id.toString(),
                          self_id: uin,
                          raw_message: message.raw_message,
                          time: message.time,
                          post_type: 'message',
                          reply(...args: any) {},
                          sub_type: 'friend',
                          font: '',
                          bubble_id: 0,
                          sender: {
                              user_id: message.sender.user_id,
                              age: 0,
                              sex: 'unknown',
                              nickname: message.sender.nickname,
                              remark: message.sender.nickname,
                          },
                          auto_reply: false,
                          message_type: 'private',
                      } as PrivateMessageEventData),
            error: null,
            status: 'ok',
            retcode: 0,
        }
    },
    async handleRequest(type: 'friend' | 'group', flag: string, accept: boolean = true) {
        switch (type) {
            case 'friend':
                await bot.setFriendAddRequest(flag, accept)
                break
            case 'group':
                await bot.setGroupAddRequest(flag, accept)
                break
        }
        return null
    },
    async getGroupFileMeta(gin: number, fid: string, resolve) {
        const data = await bot.getGroupFileUrl(gin, fid, 0)
        resolve({ url: data.url })
    },
    async getForwardMsg(resId: string, fileName: string, resolve) {
        try {
            const history = await bot.getForwardMessage(fileName || resId)
            const messages = []
            for (let i = 0; i < history.messages.length; i++) {
                const data = history.messages[i]
                const message: Message = {
                    senderId: data.sender.user_id,
                    username: data.sender.nickname,
                    content: '',
                    timestamp: formatDate('hh:mm:ss', new Date(data.time * 1000)),
                    date: formatDate('yyyy/MM/dd', new Date(data.time * 1000)),
                    _id: i.toString(),
                    time: data.time * 1000,
                    files: [],
                    bubble_id: 0,
                }
                await processMessage(data.message, message, {})
                messages.push(message)
            }
            resolve(messages)
        } catch (e) {
            console.log(e)
            const res: [Message] = [
                {
                    senderId: 0,
                    username: '错误',
                    // callApi 失败时 reject 的是字符串，e.message 会是 undefined
                    content: e instanceof Error ? e.message : typeof e === 'string' ? e : '获取转发消息失败',
                    timestamp: formatDate('hh:mm:ss'),
                    date: formatDate('yyyy/MM/dd'),
                    _id: 0,
                    time: 0,
                    files: [],
                },
            ]
            resolve(res)
        }
    },
    async sendGroupSign(gin: number) {
        await bot.sendGroupSign(gin)
    },
    async sendButtonCallback(groupId: number, msgSeq: number, appid: number, id: string, data: string): Promise<void> {
        try {
            await bot.clickInlineKeyboardButton(groupId, appid, id, data, msgSeq)
        } catch (e) {
            // NapCat 的 click_inline_keyboard_button 仅支持群聊（硬编码 chatType:2），
            // 私聊及失败情况统一提示不支持，避免暴露底层 API 错误。
            clients.messageError('暂不支持此功能')
        }
    },
    async getGroups(resolve) {
        const groups = await bot.getGroupList()
        const groupsAll: Array<GroupInfo & { sc: string }> = groups.map((it) => ({
            group_id: it.group_id,
            group_name: it.group_name,
            group_remark: it.group_remark || it.group_memo || '',
            shutup_time_me: 0,
            grade: it.group_level || 0,
            create_time: it.group_create_time || 0,
            active_member_count: 0,
            last_join_time: 0,
            last_sent_time: 0,
            max_admin_count: 0,
            owner_id: 0,
            max_member_count: it.max_member_count || 0,
            member_count: it.member_count || 0,
            shutup_time_whole: 0,
            update_time: 0,
            sc: (it.group_name + it.group_id).toUpperCase(),
        }))
        resolve(groupsAll)
    },
    //roomId 和 room 必有一个
    async sendMessage({
        content,
        roomId,
        file,
        replyMessage,
        room,
        media,
        at,
        sticker,
        messageType,
    }: SendMessageParams) {
        if (!messageType) {
            messageType = 'text'
        }
        if (!room) room = await storage.getRoom(roomId)
        if (!roomId) roomId = room.roomId
        if (file && ((file.type && !file.type.includes('image')) || !file.type)) {
            const filePath = getUploadedFilePath(file.path)
            const fileName = getUploadedFileName(file.path)
            if (!filePath || !fileName) {
                clients.messageError('文件上传失败：找不到已上传的文件')
                clients.closeLoading()
                return
            }
            if (roomId > 0) {
                bot.sendPrivateFile(roomId, filePath, fileName)
                    .then(() => {
                        clients.messageSuccess('文件上传成功')
                    })
                    .catch((e) => {
                        clients.messageError('文件上传失败 ' + e.message)
                        console.error(e)
                    })
                    .finally(() => {
                        clients.closeLoading()
                        deleteUploadedFile(file.path)
                    })
            } else {
                bot.gfsUpload(-roomId, filePath, fileName, '/')
                    .then(() => {
                        clients.messageSuccess('文件上传成功')
                    })
                    .catch((e) => {
                        clients.messageError('文件上传失败 ' + e.message)
                        console.error(e)
                    })
                    .finally(() => {
                        clients.closeLoading()
                        deleteUploadedFile(file.path)
                    })
            }
            clients.message('文件上传中')
            return
        }

        const chain: MessageElem[] = []
        const consumedMedia = new Set<number>()
        const appendMedia = (index: number) => {
            const img = media?.[index]
            if (!img || consumedMedia.has(index)) return
            const rawB64 = img.b64 ? img.b64.replace(/^data:.+;base64,/, '') : null
            if (img.b64) {
                chain.push({
                    type: 'image',
                    data: {
                        file: 'base64://' + rawB64,
                        type: sticker ? 'face' : 'image',
                        // @ts-ignore
                        sub_type: sticker ? 1 : 0,
                    },
                })
            } else if (img.url) {
                chain.push({
                    type: 'image',
                    data: {
                        file: img.url,
                        type: sticker ? 'face' : 'image',
                        // @ts-ignore
                        sub_type: sticker ? 1 : 0,
                        url: img.url.replace(/\\/g, '/'),
                    },
                })
            }
            consumedMedia.add(index)
        }

        if (messageType === 'anonymous') {
            if (roomId < 0)
                chain.push({
                    type: 'anonymous',
                    data: {
                        ignore: false, //匿名失败时不继续发送
                    },
                })
            messageType = 'text'
        }

        if (replyMessage) {
            chain.push({
                type: 'reply',
                data: {
                    id: replyMessage._id,
                    text: replyMessage.content,
                },
            })
        }
        if (content) {
            content = convertLegacyIcalinguaAt(content, (index, replacedLength, replacementLength) => {
                shiftMediaOrdersAfterTextReplacement(media, index, replacedLength, replacementLength)
            })
            // 转换 @ 标记
            let icalinguaAt = findIcalinguaAtMarkup(content)
            while (icalinguaAt) {
                const name = decodeIcalinguaAtName(icalinguaAt.encodedName)
                at.push({
                    id: icalinguaAt.qq === 1 ? 'all' : icalinguaAt.qq,
                    text: name,
                })
                shiftMediaOrdersAfterTextReplacement(media, icalinguaAt.index, icalinguaAt.raw.length, name.length)
                content = content.replace(icalinguaAt.raw, name)
                icalinguaAt = findIcalinguaAtMarkup(content)
            }
            //这里是处理@人和表情 markup 的逻辑
            const FACE_REGEX = /\[Face: (\d+)]/
            let splitContent = messageType === 'text' ? splitContentByMediaOrder(content, media || []) : [content]
            // 把 @xxx 的部分单独分割开
            // '喵@小A @小B呜' -> ['喵', '@小A', ' ', '@小B', '呜']
            for (const { text } of at) {
                if (!text) continue
                const newParts: string[] = []
                for (let part of splitContent) {
                    while (part.includes(text)) {
                        const index = part.indexOf(text)
                        const before = part.substr(0, index)
                        part = part.substr(index + text.length)
                        before && newParts.push(before)
                        newParts.push(text)
                    }
                    part && newParts.push(part)
                }
                splitContent = newParts
            }
            // 分离类似 [Face: 265] 的表情
            const newParts: string[] = []
            for (let part of splitContent) {
                if (at.find((e) => e.text === part)) {
                    // @的成分不做处理
                    newParts.push(part)
                    continue
                }
                while (FACE_REGEX.test(part)) {
                    const exec = FACE_REGEX.exec(part)
                    const index = exec.index
                    const before = part.substr(0, index)
                    const text = exec[0]
                    part = part.substr(index + text.length)
                    before && newParts.push(before)
                    newParts.push(text)
                }
                part && newParts.push(part)
            }
            splitContent = newParts
            // 最后根据每个 string 元素判断类型并且换成对应的 MessageElem
            for (const part of splitContent) {
                const mediaIndex = getMediaPartIndex(part)
                if (mediaIndex !== null) {
                    appendMedia(mediaIndex)
                    continue
                }
                const atInfo = at.find((e) => e.text === part)
                const isFace = FACE_REGEX.test(part)
                let element: MessageElem
                if (atInfo)
                    element = {
                        type: 'at',
                        data: {
                            qq: atInfo.id,
                            text: atInfo.text,
                        },
                    }
                else if (isFace) {
                    var temp: string = FACE_REGEX.exec(part)[1]
                    element = {
                        type: 'face',
                        data: {
                            id: Number.parseInt(temp, 10),
                        },
                    }
                } else if (messageType === 'text') {
                    element = {
                        type: 'text',
                        data: {
                            text: part,
                        },
                    }
                } else if (messageType === 'json') {
                    chain.length = 0
                    chain.push({
                        type: 'json',
                        data: {
                            data: content,
                        },
                    })
                    break
                } else if (messageType === 'xml') {
                    chain.length = 0
                    chain.push({
                        type: 'xml',
                        data: {
                            data: content,
                        },
                    })
                    break
                } else if (messageType === 'rps') {
                    chain.length = 0
                    chain.push({
                        type: 'rps',
                        data: {
                            id: parseInt(content),
                        },
                    })
                    break
                } else if (messageType === 'dice') {
                    chain.length = 0
                    chain.push({
                        type: 'dice',
                        data: {
                            id: parseInt(content),
                        },
                    })
                    break
                } else if (messageType === 'shake') {
                    chain.length = 0
                    chain.push({
                        type: 'shake',
                    })
                    break
                } else if (messageType === 'raw') {
                    // Only for debug
                    chain.length = 0
                    const rawMessage = JSON.parse(content)
                    chain.push(...rawMessage)
                    break
                }
                chain.push(element)
            }
        }
        if (media && media.length) {
            media.forEach((_, index) => appendMedia(index))
        } else if (file) {
            chain.push({
                type: 'image',
                data: {
                    file: file.path,
                    type: sticker ? 'face' : 'image',
                    // @ts-ignore
                    sub_type: sticker ? 1 : 0,
                    url: file.path,
                },
            })
        }
        if (messageType === 'text') {
            const idReg = content.match(/\[QLottie: (\d+)\,(\d+)\]/)
            if (idReg && idReg.length >= 3 && content === idReg[0]) {
                const qlottie = idReg[1]
                const faceId = idReg[2]
                chain.length = 0
                chain.push({
                    type: 'face',
                    data: {
                        id: Number.parseInt(faceId, 10),
                        qlottie: qlottie,
                    },
                })
            }
        }
        //发送消息链
        try {
            let data: { message_id: number }
            if (roomId > 0) data = await bot.sendPrivateMessage(roomId, chain)
            else data = await bot.sendGroupMessage(-roomId, chain)
        } catch (e) {
            clients.notifyError({
                title: 'Failed to send',
                message: e.message,
            })
            clients.addMessageText(content)
        }

        clients.closeLoading()
    },
    async renewMessage(roomId: number, messageId: string, message: Message) {
        const res = await adapter.getMsg(messageId)
        if (!res.error) {
            const data = res.data
            const newMessage: Message = {
                senderId: message.senderId,
                username: message.username,
                content: '',
                timestamp: message.timestamp,
                date: message.date,
                _id: messageId,
                time: message.time,
                role: message.role,
                title: message.title,
                files: [],
                anonymousId: message.anonymousId,
                anonymousflag: message.anonymousflag,
                bubble_id: message.bubble_id,
            }
            try {
                await processMessage(data.message, newMessage, {}, roomId)
                await storage.replaceMessage(roomId, messageId, newMessage)
                clients.renewMessage(roomId, messageId, newMessage)
            } catch (e) {
                console.error(e)
            }
        } else {
            if (res.error.message !== 'msg not exists') clients.messageError('错误：' + res.error.message)
            else clients.messageError('错误：该消息不存在。')
        }
    },
    async deleteMessage(roomId: number, messageId: string) {
        const res = await bot.deleteMessage(Number(messageId))
        clients.deleteMessage(messageId)
        await storage.updateMessage(roomId, messageId, { deleted: true, reveal: false })
    },
    async getFriendInfo(user_id: number): Promise<FriendInfo> {
        const data = await bot.getStrangerInfo(user_id)
        return {
            remark: data.remark || data.nickname,
            sex: data.sex as Gender,
            user_id: data.user_id,
            age: data.age,
            nickname: data.nickname,
        }
    },
    async getFriendsFallback(cb) {
        const friends = await bot.getFriendList()
        const list: SearchableFriend[] = friends.map((them) => ({
            user_id: them.user_id,
            age: 0,
            sex: 'unknown',
            nickname: them.nickname,
            remark: them.remark,
            uin: them.user_id,
            nick: them.nickname,
            sc: (them.nickname + them.remark + them.user_id).toUpperCase(),
        }))
        cb(list)
    },
    async _getGroupMemberInfo(group: number, member: number, noCache: boolean) {
        const cacheKey = `${group}:${member}`
        if (!noCache) {
            const cached = memberInfoCache.get(cacheKey)
            if (cached && Date.now() - cached.timestamp < MEMBER_CACHE_TTL) {
                return cached.info
            }
        }
        const data = await bot.getGroupMemberInfo(group, member, noCache)
        const info = {
            ...data,
            rank: '',
            shutup_time: 0,
            update_time: 0,
            sex: data.sex as Gender,
            level: Number(data.level),
            role: data.role as GroupRole,
        }
        memberInfoCache.set(cacheKey, { info, timestamp: Date.now() })
        return info
    },
    async getGroupMemberInfo(group: number, member: number, noCache: boolean, resolve) {
        resolve(await adapter._getGroupMemberInfo(group, member, noCache))
    },
    setGroupNick(group: number, nick: string) {
        bot.setGroupCard(group, uin, nick)
    },
    async getGroupMembers(group: number, resolve) {
        try {
            const data = await bot.getGroupMemberList(group, true)
            const all: MemberInfo[] = data.map((them) => ({
                ...them,
                rank: '',
                shutup_time: 0,
                update_time: 0,
                sex: them.sex as Gender,
                level: Number(them.level),
                role: them.role as GroupRole,
            }))
            resolve(all)
        } catch (e) {
            clients.messageError('获取群成员列表失败')
            resolve([])
            return
        }
    },
    reportRead(messageId: string): any {
        bot.markMessageAsRead(Number(messageId))
    },
    async makeForward(
        fakes: FakeMessage | Iterable<FakeMessage>,
        dm?: boolean,
        origin?: number,
        target?: number,
    ): Promise<any> {
        if (!target) {
            clients.notify({
                title: '未实现',
                message: '未实现',
            })
        }
        if (!isArrayLike(fakes)) {
            fakes = [fakes as FakeMessage]
        }
        if (!Array.isArray(fakes)) {
            fakes = Array.from(fakes as Iterable<FakeMessage>)
        }
        const nodes = (fakes as Array<FakeMessage>).map((data) => ({
            type: 'node',
            data,
        })) as { type: 'node'; data: any }[]
        if (dm) {
            await bot.sendPrivateForwardMessage(target, nodes)
        } else {
            await bot.sendGroupForwardMessage(-target, nodes)
        }
    },
    setGroupBan(gin: number, uin: number, duration?: number): any {
        if (uin === 0) bot.setGroupWholeBan(gin, duration > 0)
        else bot.setGroupBan(gin, uin, duration)
    },
    setGroupAnonymousBan(gin: number, flag: string, duration?: number): any {
        bot.setGroupAnonymousBan(gin, flag, duration)
    },
    setGroupRemark(gin: number, remark: string): any {
        bot.setGroupRemark(gin, remark)
    },
    setFriendRemark(uin: number, remark: string): any {
        bot.setFriendRemark(uin, remark)
    },
    setGroupKick(gin: number, uin: number): any {
        bot.setGroupKick(gin, uin)
    },
    setGroupLeave(gin: number): any {
        bot.setGroupLeave(gin)
    },
    async getFriend(uin: number, resolve: (friend: FriendInfo) => any) {
        const list = await bot.getFriendList()
        const friend = list.find((f) => f.user_id === uin)
        resolve(friend && { ...friend, age: 0, sex: 'unknown' })
    },
    async getGroup(gin: number, resolve: (group: GroupInfo) => any) {
        const it = await bot.getGroupInfo(gin)
        await resolve({
            group_id: it.group_id,
            group_name: it.group_name,
            group_remark: it.group_remark || it.group_memo || '',
            shutup_time_me: 0,
            grade: it.group_level || 0,
            create_time: it.group_create_time || 0,
            active_member_count: 0,
            last_join_time: 0,
            last_sent_time: 0,
            max_admin_count: 0,
            owner_id: 0,
            max_member_count: it.max_member_count || 0,
            member_count: it.member_count || 0,
            shutup_time_whole: 0,
            update_time: 0,
        })
    },
    async getMsgNewURL(id: string, resolve): Promise<string> {
        const history = await adapter.getMsg(id)
        if (history.error) {
            console.log(history.error)
            if (history.error.message !== 'msg not exists') clients.messageError('错误：' + history.error.message)
            resolve('error')
            return
        }
        const data = history.data
        console.log(data)
        if (data) {
            const message: Message = {
                senderId: data.sender.user_id,
                username: data.sender.nickname,
                content: '',
                timestamp: formatDate('hh:mm:ss', new Date(data.time * 1000)),
                date: formatDate('yyyy/MM/dd', new Date(data.time * 1000)),
                _id: id,
                time: data.time * 1000,
                files: [],
                bubble_id: data.bubble_id,
            }
            await processMessage(data.message, message, {})
            if (message.file) {
                resolve(message.file.url || 'error')
                return
            }
        }
        resolve('error')
        return
    },
    getNTPicURLbyFileid(fileId: string, appid: string, resolve): Promise<string> {
        resolve(replaceRkey(`https://multimedia.nt.qq.com.cn/download?appid=${appid}&fileid=${fileId}`))
        return
    },
    async fetchHistory(messageId: string, roomId: number, currentLoadedMessagesCount: number) {
        if (fetchingHistoryRooms.has(roomId)) {
            // 客户端可能重复触发同一会话的拉取，直接跳过，避免两条循环同时插入重复消息
            console.log(`${roomId} 正在拉取消息，忽略重复请求`)
            return
        }
        fetchingHistoryRooms.add(roomId)
        try {
            console.log(`${roomId} 开始拉取消息`)
            clients.messageSuccess('开始拉取消息')
            let totalCount = 0
            const minDate = config.fetchHistoryMinDate ? new Date(config.fetchHistoryMinDate).getTime() : null
            let reachedMinDate = false
            // 客户端传入的是已加载的最早一条消息 id；NapCat 的 message_id 与 message_seq 同为短 id，
            // 直接作为起始分页点，避免每次都从最新消息重新拉取。
            const initialSeq = Number(messageId)
            let messageSeq: number | undefined =
                Number.isSafeInteger(initialSeq) && initialSeq > 0 ? initialSeq : undefined
            let anchorFallbackTried = false
            // message_id 不稳定，改用 real_seq 记录本轮已拉到的消息，防止同一消息换 id 后被反复入库
            const fetchedIdentities = new Set<string>()
            // 库里已存在的消息指纹，用来跳过重复入库并判断是否已经接上已有历史
            const knownStoredIdentities = await getRoomStoredIdentities(roomId)
            // 从某条历史消息往前翻时，一次只补一屏（20 条）新消息，避免把整个历史都拉一遍
            const boundedByAnchor = messageSeq !== undefined
            let newMessagesInCall = 0
            // 分页必须严格向更早的消息推进，否则说明锚点失效或服务端忽略了 message_seq
            let previousOldestRealSeq: number | undefined
            let previousOldestTime: number | undefined
            const maxPages = 2000
            let pageCount = 0
            while (true) {
                if (stopFetching) {
                    stopFetching = false
                    break
                }
                if (pageCount++ >= maxPages) {
                    console.log(`${roomId} 达到单次拉取页数上限，停止拉取`)
                    break
                }
                const startedFromLatest = messageSeq === undefined
                try {
                    const history = await (roomId > 0
                        ? bot.getPrivateMessageHistory(roomId, messageSeq)
                        : bot.getGroupMessageHistory(-roomId, messageSeq))
                    const batchMessages: Message[] = []
                    // 记录本批时间最早的一条，作为下一页的分页锚点
                    // （NapCat 的短 id 无大小顺序，不能像序号一样取最小值或减 1）
                    let oldestData: GroupMessage | PrivateMessage | undefined
                    for (let i = 0; i < history.messages.length; i++) {
                        const data = history.messages[i]
                        // 同一秒内的消息时间相同，再用 real_seq 精确比较一次，避免锚点选到较新的一条
                        const dataRealSeq = getHistoryRealSeq(data)
                        const oldestRealSeq = getHistoryRealSeq(oldestData)
                        if (
                            !oldestData ||
                            data.time < oldestData.time ||
                            (data.time === oldestData.time &&
                                dataRealSeq !== undefined &&
                                oldestRealSeq !== undefined &&
                                dataRealSeq < oldestRealSeq)
                        ) {
                            oldestData = data
                        }
                        // 检查日期限制
                        if (minDate && data.time * 1000 < minDate) {
                            reachedMinDate = true
                            break
                        }
                        // 同一条消息可能因为各种原因换了 message_id，按稳定身份去重，避免重复入库
                        const identity = getHistoryMessageIdentity(data)
                        if (fetchedIdentities.has(identity)) continue
                        fetchedIdentities.add(identity)
                        const message: Message = {
                            senderId: data.sender.user_id,
                            username:
                                'group_id' in data
                                    ? data.group_id
                                        ? data.anonymous
                                            ? data.anonymous.name
                                            : data.sender.nickname
                                        : data.sender.nickname
                                    : data.sender.nickname,
                            content: '',
                            timestamp: formatDate('hh:mm:ss', new Date(data.time * 1000)),
                            date: formatDate('yyyy/MM/dd', new Date(data.time * 1000)),
                            _id: data.message_id,
                            time: data.time * 1000,
                            role: (data.sender as MemberBaseInfo).role,
                            title:
                                (data as GroupMessage).group_id && (data as GroupMessage).anonymous
                                    ? '匿名'
                                    : (data.sender as MemberBaseInfo).title,
                            files: [],
                            anonymousId:
                                (data as GroupMessage).group_id && (data as GroupMessage).anonymous
                                    ? (data as GroupMessage).anonymous.id
                                    : null,
                            anonymousflag:
                                (data as GroupMessage).group_id && (data as GroupMessage).anonymous
                                    ? (data as GroupMessage).anonymous.flag
                                    : null,
                            bubble_id: 0,
                        }
                        try {
                            await processMessage(data.message, message, {}, roomId, true)
                            if (await storage.isChatIgnored(message.senderId)) message.hide = true
                            // 库里已有同一条消息（message_id 可能已经变了），直接跳过，不再重复入库
                            if (knownStoredIdentities.has(getStoredMessageIdentity(message))) continue
                            batchMessages.push(message)
                        } catch (e) {
                            console.error(e)
                        }
                    }
                    // 检查第一条消息是否已存在（在存储之前检查）
                    const firstMsg = batchMessages[0]
                    const firstMsgExists = firstMsg && (await storage.getMessage(roomId, firstMsg._id as string))
                    // 边拉边存：每批消息立即存入数据库，并按实际写入条数判断是否已接上库里已有的历史
                    let addedCount = 0
                    if (batchMessages.length > 0) {
                        const added = await storage.addMessages(roomId, batchMessages)
                        addedCount = Array.isArray(added) ? added.length : batchMessages.length
                        totalCount += addedCount
                        newMessagesInCall += addedCount
                        for (const message of batchMessages)
                            knownStoredIdentities.add(getStoredMessageIdentity(message))
                    }
                    if (reachedMinDate) break
                    if (history.messages.length === 0) break
                    // 整页都没有写入新消息（message_id 可能已变化），说明已接上已有的历史，正常结束
                    if (batchMessages.length === 0 || addedCount === 0) break
                    // 从指定消息往前翻时，补够一屏就停，返回的条数由调用方按已加载数量决定
                    if (boundedByAnchor && newMessagesInCall >= 20) break
                    // NapCat 的 message_seq 是 MessageUnique 生成的短 id（MD5 前半段），
                    // 并非连续序号，不能减 1 后再用，否则下次查询会命中不存在的位置并抛「消息X不存在」。
                    // 使用本批时间最早一条的短 id 作为下一页锚点。
                    const nextSeq = Number(oldestData?.message_seq ?? oldestData?.message_id)
                    if (!Number.isSafeInteger(nextSeq) || nextSeq <= 0) break
                    // 锚点是短 id，无法用大小判断方向；改用稳定的 real_seq 确认分页确实在向更早推进，
                    // 否则服务端忽略了 message_seq 时会一直返回同一批消息，造成无限拉取。
                    const oldestSeq = getHistoryRealSeq(oldestData)
                    if (oldestSeq !== undefined) {
                        if (previousOldestRealSeq !== undefined && oldestSeq >= previousOldestRealSeq) {
                            console.log(`${roomId} 分页未向更早推进，停止拉取`)
                            break
                        }
                        previousOldestRealSeq = oldestSeq
                    } else if (previousOldestTime !== undefined && (oldestData?.time ?? 0) >= previousOldestTime) {
                        console.log(`${roomId} 分页未向更早推进，停止拉取`)
                        break
                    }
                    previousOldestTime = oldestData?.time
                    messageSeq = nextSeq
                    //todo 所有消息都过一遍，数据库里面都有才能结束
                    // 仅从最新消息开始拉时，用它判断是否已接上库中已有历史；
                    // 显式从某条消息往前拉时，该消息本身就在库里，不能据此提前结束。
                    if (startedFromLatest && firstMsgExists) break
                } catch (e) {
                    const errorMessage = e instanceof Error ? e.message : String(e)
                    // NapCat 在历史到底时返回空列表并抛「消息X不存在」，按正常结束处理，不向客户端弹错误。
                    // 若首次传入的起始消息已失效（NapCat 重启后短 id 表重建），回退到从最新消息重新拉一次。
                    if (/消息.*不存在/.test(errorMessage)) {
                        if (messageSeq !== undefined && !anchorFallbackTried && totalCount === 0) {
                            anchorFallbackTried = true
                            messageSeq = undefined
                            previousOldestRealSeq = undefined
                            previousOldestTime = undefined
                            continue
                        }
                        break
                    }
                    console.log(e)
                    clients.messageError('错误：' + errorMessage)
                    break
                }
            }
            console.log(`${roomId} 已拉取 ${totalCount} 条消息`)
            clients.messageSuccess(`已拉取 ${totalCount} 条消息`)
            storage
                .fetchMessages(roomId, {}, currentLoadedMessagesCount + 20)
                .then((messages) => clients.setMessages(roomId, messages))
        } finally {
            fetchingHistoryRooms.delete(roomId)
        }
    },

    stopFetchingHistory() {
        stopFetching = true
    },
    async fetch7DaysHistory() {
        if (isAutoFetching) return
        console.log('正在获取历史消息')
        const rooms = await storage.getAllRooms()
        // 先私聊后群聊，与 oicq 适配器保持一致
        // NapCat 没有批量查询群 seq/未读的接口（OidbSvc.0x88d 不可用），直接从每个会话最新一页开始拉取
        const now = Date.now() - 3000
        const dmRoomIds = rooms
            .filter((e) => e.roomId > 0 && now - e.utime <= 1000 * 60 * 60 * 24 * 7)
            .map((e) => e.roomId)
        const groupRoomIds = rooms
            .filter((e) => e.roomId < 0 && now - e.utime <= 1000 * 60 * 60 * 24 * 7)
            .map((e) => e.roomId)
        isAutoFetching = true
        for (const roomId of [...dmRoomIds, ...groupRoomIds]) {
            if (stopFetching) {
                stopFetching = false
                break
            }
            await adapter.fetchHistory('', roomId, 0)
            await sleep(100)
        }
        isAutoFetching = false
        clients.messageSuccess('历史消息获取完成')
        console.log('历史消息获取完成')
    },
    async getCookies(domain: any, resolve) {
        const res = await bot.getCookies(domain)
        bkn = Number(res.bkn)
        resolve(res.cookies)
    },
    async getRoamingStamp(no_cache: boolean | undefined, cb) {
        cb((await bot.getCustomStickers()).map((url, id) => ({ url, id })))
    },

    // 存储动作
    async fetchMessages(
        roomId: number,
        options: MessagePageOptions,
        client: Socket,
        callback: (arg0: Message[]) => void,
    ) {
        const initialPage = !options?.before && !options?.after
        if (initialPage) {
            storage.updateRoom(roomId, {
                unreadCount: 0,
                at: false,
                atMessageId: null,
            })
            if (roomId < 0) {
                const gid = -roomId
                const group = await bot.getGroupInfo(gid)
                if (group) client.emit('setShutUp', false)
                else {
                    client.emit('setShutUp', true)
                    client.emit('message', '你已经不是群成员了')
                }
            } else {
                client.emit('setShutUp', false)
            }
        }
        const messages = (await storage.fetchMessages(roomId, options || {}, 20)) || []
        if (messages.length && initialPage && typeof messages[messages.length - 1]._id === 'string')
            adapter.reportRead(<string>messages[messages.length - 1]._id)
        for (const message of messages) {
            if (message.file?.url) {
                message.file.url = replaceRkey(message.file?.url)
            }
            if (Array.isArray(message.files)) {
                for (const file of message.files) {
                    if (file.url) {
                        file.url = replaceRkey(file.url)
                    }
                }
            }
            if (message.replyMessage?.file?.url) {
                message.replyMessage.file.url = replaceRkey(message.replyMessage.file?.url)
            }
        }
        callback(messages)
    },
    async fetchImageMessages(
        roomId: number,
        offset: number,
        endTime: number | undefined,
        client: Socket,
        callback: (arg0: Message[]) => void,
    ) {
        const messages = (await storage.fetchImageMessages(roomId, offset, 30, endTime)) || []
        for (const message of messages) {
            if (message.file?.url) {
                message.file.url = replaceRkey(message.file?.url)
            }
            if (Array.isArray(message.files)) {
                for (const file of message.files) {
                    if (file.url) {
                        file.url = replaceRkey(file.url)
                    }
                }
            }
        }
        callback(messages)
    },
    async fetchMessagesAround(
        roomId: number,
        messageId: string,
        before: number,
        after: number,
        client: Socket,
        callback: (arg0: Message[]) => void,
    ) {
        const messages = (await storage.fetchMessagesAround(roomId, messageId, before, after)) || []
        for (const message of messages) {
            if (message.file?.url) {
                message.file.url = replaceRkey(message.file?.url)
            }
            if (Array.isArray(message.files)) {
                for (const file of message.files) {
                    if (file.url) {
                        file.url = replaceRkey(file.url)
                    }
                }
            }
        }
        callback(messages)
    },
    async markMessageUnread(roomId: number, messageId: string, callback: (unreadCount: number) => void) {
        try {
            const room = await storage.getRoom(roomId)
            if (!room) return callback(0)
            const unreadCount = await storage.countUnreadMessagesFrom(roomId, messageId)
            if (!unreadCount) return callback(0)
            await storage.updateRoom(roomId, { unreadCount, at: false, atMessageId: null })
            callback(unreadCount)
        } catch (error) {
            console.error('Failed to mark message unread', error)
            callback(0)
        }
    },
    async resolveUnreadTargetMessageId(
        roomId: number,
        unreadCount: number,
        callback: (messageId: string | null) => void,
    ) {
        callback(await storage.resolveUnreadTargetMessageId(roomId, unreadCount))
    },
    async fetchMessagesBySender(
        roomId: number,
        senderId: number,
        offset: number,
        snapshotTime: number | undefined,
        client: Socket,
        callback: (arg0: Message[]) => void,
    ) {
        const messages = (await storage.fetchMessagesBySender(roomId, String(senderId), offset, 20, snapshotTime)) || []
        for (const message of messages) {
            if (message.file?.url) {
                message.file.url = replaceRkey(message.file?.url)
            }
            if (Array.isArray(message.files)) {
                for (const file of message.files) {
                    if (file.url) {
                        file.url = replaceRkey(file.url)
                    }
                }
            }
        }
        callback(messages)
    },
    async searchMessages(
        roomId: number,
        keyword: string,
        offset: number,
        senderId: number | undefined,
        startTime: number | undefined,
        endTime: number | undefined,
        client: Socket,
        callback: (arg0: Message[]) => void,
    ) {
        const messages =
            (await storage.searchMessages(
                roomId,
                keyword,
                offset,
                20,
                senderId === undefined ? undefined : String(senderId),
                startTime,
                endTime,
            )) || []
        for (const message of messages) {
            if (message.file?.url) {
                message.file.url = replaceRkey(message.file?.url)
            }
            if (Array.isArray(message.files)) {
                for (const file of message.files) {
                    if (file.url) {
                        file.url = replaceRkey(file.url)
                    }
                }
            }
        }
        callback(messages)
    },
    addRoom(room: Room) {
        return storage.addRoom(room)
    },
    addChatGroup(chatGroup: ChatGroup) {
        return storage.addChatGroup(chatGroup)
    },
    async setRoomPriority(roomId: number, priority: 1 | 2 | 3 | 4 | 5) {
        await storage.updateRoom(roomId, { priority })
        clients.setAllRooms(await storage.getAllRooms())
    },
    async setRoomAutoDownload(roomId: number, autoDownload: boolean) {
        await storage.updateRoom(roomId, { autoDownload })
    },
    async setRoomAutoDownloadPath(roomId: number, downloadPath: string) {
        await storage.updateRoom(roomId, { downloadPath })
    },
    async pinRoom(roomId: number, pin: boolean) {
        await storage.updateRoom(roomId, { index: pin ? 1 : 0 })
        clients.setAllRooms(await storage.getAllRooms())
    },
    async ignoreChat(data: IgnoreChatInfo) {
        await storage.addIgnoredChat(data)
        await adapter.removeChat(data.id)
    },
    async removeChat(roomId: number) {
        await storage.removeRoom(roomId)
        clients.setAllRooms(await storage.getAllRooms())
    },
    async removeChatGroup(name: string) {
        await storage.removeChatGroup(name)
        clients.setAllChatGroups(await storage.getAllChatGroups())
    },
    async hideMessage(roomId: number, messageId: string) {
        await storage.updateMessage(roomId, messageId, { hide: true, reveal: false })
    },
    async revealMessage(roomId: number, messageId: string | number) {
        await storage.updateMessage(roomId, messageId, { hide: false, reveal: true })
    },
    getUnreadCount: async (priority: 1 | 2 | 3 | 4 | 5, resolve) => resolve(await storage.getUnreadCount(priority)),
    getFirstUnreadRoom: async (priority: 1 | 2 | 3 | 4 | 5, resolve) =>
        resolve(await storage.getFirstUnreadRoom(priority)),
    getRoom: async (roomId: number, resolve) => resolve(await storage.getRoom(roomId)),
    getMessageFromStorage: (roomId: number, msgId: string) => storage.getMessage(roomId, msgId),
    async getIgnoredChats(resolve) {
        resolve(await storage.getIgnoredChats())
    },
    removeIgnoredChat(roomId: number): any {
        return storage.removeIgnoredChat(roomId)
    },
    updateRoom(roomId: number, room: object) {
        if (!storage) return
        return storage.updateRoom(roomId, room)
    },
    updateChatGroup(name: string, chatGroup: ChatGroup) {
        return storage.updateChatGroup(name, chatGroup)
    },
    updateMessage(roomId: number, messageId: string, message: object) {
        return storage.updateMessage(roomId, messageId, message)
    },
    async sendGroupPoke(gin: number, uin: number) {
        if (gin == uin) {
            await bot.sendFriendPoke(uin)
            return
        }
        await bot.sendGroupPoke(gin, uin)
    },

    // 本地动作
    getUin: () => uin,
    async renewMessageURL(roomId: number, messageId: string | number, URL) {
        clients.renewMessageURL(messageId, URL)
    },
    getBkn: () => bkn,

    acquireGfs(gid: number) {
        const ls = async (fid?: string) => {
            let res: Awaited<ReturnType<typeof bot.gfsListDir>>
            if (!fid || fid === '/') {
                res = await bot.gfsListRoot(gid)
            } else {
                res = await bot.gfsListDir(gid, fid)
            }
            return [
                ...res.folders.map(
                    (it) =>
                        ({
                            fid: it.folder_id,
                            pid: fid || '/',
                            name: it.folder_name,
                            user_id: Number(it.creator),
                            create_time: Number(it.create_time),
                            file_count: Number(it.total_file_count),
                            is_dir: true,
                        }) satisfies GfsDirStat,
                ),
                ...res.files.map(
                    (it) =>
                        ({
                            fid: it.file_id,
                            pid: fid || '/',
                            name: it.file_name,
                            user_id: Number(it.uploader),
                            create_time: Number(it.modify_time),
                            size: Number(it.size),
                            duration: it.dead_time,
                            busid: it.busid,
                            md5: '',
                            sha1: '',
                            download_times: it.download_times,
                        }) satisfies GfsFileStat,
                ),
            ]
        }
        const download = (fid: string, name?: string) => {
            return bot.gfsDownloadUrl(gid, fid).then((res) => ({ url: res.url, name }))
        }
        return {
            gid,
            ls,
            dir: ls,
            stat: download,
            mkdir: (name) => {
                return bot.gfsMkdir(gid, name)
            },
            upload: async (file: MediaFile, pid?: string, name?: string) => {
                if (typeof file === 'object') {
                    const p = path.join('/app/.config/QQ/NapCat/temp', md5(file as Uint8Array))
                    await fsP.writeFile(p, file as any)
                    file = p
                }
                await bot.gfsUpload(gid, file, name, pid || '/')
                if (file.startsWith('/app/.config/QQ/NapCat/temp')) {
                    await fsP.unlink(file)
                }
            },
            mv: (fid: string, dirId: string) => bot.gfsMove(gid, fid, '/', dirId),
            rm: async (fid: string) => {
                try {
                    await bot.gfsDeleteFile(gid, fid)
                } catch {
                    await bot.gfsDeleteFolder(gid, fid)
                }
            },
            rename: (fid: string, name: string) => bot.gfsRename(gid, fid, '/', name),
            download,
        } as any
    },
    getPrivateFileUrl: async (fileId: string, cb) => {
        const { url } = await bot.getPrivateFileUrl(fileId)
        cb(url)
    },

    // 未支持动作
    disabledFeatures: ['IdLogin', 'OnlineStatus'],
    async sendPacket(type: string, cmd: string, body: any, cb) {
        cb()
    },
    async preloadImages(urls: string[]) {
        return false
    },
    async getSystemMsg(cb) {
        cb({})
    },
    setOnlineStatus(status: number) {
        return null
    },
    async getLoginDevices(cb) {
        cb([])
    },
    async deleteLoginDevice(flag: string) {
        return null
    },

    // 没必要实现的动作
    logOut() {
        return storage?.close()
    },
    randomDevice(username: number) {},
    reLogin() {},
    sliderLogin(ticket: string) {},
    submitSmsCode(smsCode: string) {},
}

const processMessage = createProcessMessage(adapter)

export default adapter
