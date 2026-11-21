import { random } from 'lodash'
import { EventEmitter } from 'eventemitter3'
import { Gender, MessageElem, Sendable } from 'oicq-icalingua-plus-plus'
import WebSocket from 'ws'
import ReconnectingWebSocket from 'reconnecting-websocket'

const ONEBOT_API_TIMEOUT = 60 * 1000
const HISTORY_PAGE_SIZE = 20

/** 连接参数 */
export type OneBotClientOptions = {
    /** OneBot 服务端 access token，会拼接到 WebSocket URL 的查询串上 */
    accessToken?: string
}

/** 探测到的后端信息 */
export type OneBotBackendInfo = {
    appName: string
    appVersion: string
    runtimeOs?: string
    runtimeVersion?: string
    isMeow: boolean
}

/** 下载 rkey（多个 OneBot 实现的字段名不同，这里归一化成同一种形状） */
export type DownloadRkey = {
    type: 'private' | 'group'
    rkey: string
    created_at: number
    ttl: string
}

/** 群文件列表条目（Meow 用 file_size 而不是 size） */
type GroupFileEntry = {
    group_id: number
    file_id: string
    file_name: string
    busid: number
    size: number
    upload_time: number
    dead_time: number
    modify_time: number
    download_times: number
    uploader: number
    uploader_name: string
    file_size?: number
}

type GroupFolderEntry = {
    group_id: number
    folder_id: string
    folder: string
    folder_name: string
    create_time: string
    creator: string
    creator_name: string
    total_file_count: string
}

type GroupFileList = {
    files: GroupFileEntry[]
    folders: GroupFolderEntry[]
}

const normalizeGroupFileList = (list: GroupFileList): GroupFileList => ({
    files: (list?.files ?? []).map((file) => {
        const size = Number(file.size)
        return Number.isFinite(size) ? file : { ...file, size: Number(file.file_size) || 0 }
    }),
    folders: list?.folders ?? [],
})

type BaseEvent = {
    self_id: number
    time: number
}

type BaseMessage = BaseEvent & {
    message_id: number
    user_id: number
    target_id: number
    message: MessageElem[]
    raw_message: string
    font: number
    sender: {
        user_id: number
        nickname: string
        sex: Gender
        age: number
    }
    post_type: 'message' | 'message_sent'
}

export type PrivateMessage = BaseMessage & {
    message_type: 'private'
    sub_type: 'friend' | 'group' | 'group_self' | 'other'
    temp_source?: number
}

export type GroupMessage = BaseMessage & {
    message_type: 'group'
    sub_type: 'normal' | 'anonymous' | 'notice'
    group_id: number
    anonymous?: {
        id: number
        name: string
        flag: string
    }
}

export default class extends EventEmitter<{
    message: [GroupMessage | PrivateMessage]
    friendRecall: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'friend_recall'
            user_id: number
            message_id: number
        },
    ]
    groupRecall: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'group_recall'
            group_id: number
            user_id: number
            operator_id: number
            message_id: number
        },
    ]
    friendPoke: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'notify'
            sub_type: 'poke'
            sender_id: number
            user_id: number
            target_id: number
            raw_info: any
        },
    ]
    groupPoke: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'notify'
            sub_type: 'poke'
            group_id: number
            user_id: number
            target_id: number
            raw_info: any
        },
    ]
    groupIncrease: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'group_increase'
            sub_type: 'approve' | 'invite'
            group_id: number
            operator_id: number
            user_id: number
        },
    ]
    groupDecrease: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'group_decrease'
            sub_type: 'leave' | 'kick' | 'kick_me'
            group_id: number
            operator_id: number
            user_id: number
        },
    ]
    groupBan: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'group_ban'
            sub_type: 'ban' | 'lift_ban'
            group_id: number
            operator_id: number
            user_id: number
            duration: number
        },
    ]
    groupAdmin: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'group_admin'
            sub_type: 'set' | 'unset'
            group_id: number
            user_id: number
        },
    ]
    groupTitle: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'notify'
            sub_type: 'title'
            group_id: number
            user_id: number
            title: string
        },
    ]
    friendAdd: [
        BaseEvent & {
            post_type: 'notice'
            notice_type: 'friend_add'
            user_id: number
        },
    ]
}> {
    private socket: ReconnectingWebSocket
    private readonly echoMap = new Map<
        string,
        {
            action: string
            resolve: (result: any) => void
            reject: (result: any) => void
            timeout: NodeJS.Timeout
        }
    >()

    private backendInfo: OneBotBackendInfo | undefined
    private backendInfoPromise: Promise<OneBotBackendInfo> | undefined

    public constructor(private readonly url: string, private readonly options: OneBotClientOptions = {}) {
        super()
    }

    /** 已探测到的后端信息；未探测时为 undefined（此时按标准 OneBot v11 行为处理） */
    public get backend() {
        return this.backendInfo
    }

    public get isMeow() {
        return this.backendInfo?.isMeow === true
    }

    /**
     * 探测后端实现
     */
    public detectBackend() {
        if (!this.backendInfoPromise) {
            this.backendInfoPromise = this.getVersionInfo()
                .then((info) => {
                    const appName = String(info?.app_name ?? '')
                    const backend: OneBotBackendInfo = {
                        appName,
                        appVersion: String(info?.app_version ?? ''),
                        runtimeOs: info?.runtime_os,
                        runtimeVersion: info?.runtime_version,
                        isMeow: /meow/i.test(appName),
                    }
                    this.backendInfo = backend
                    return backend
                })
                .catch((error) => {
                    // 探测失败不缓存，允许下次重试
                    this.backendInfoPromise = undefined
                    throw error
                })
        }
        return this.backendInfoPromise
    }

    /** 把 accessToken 拼进查询 */
    private resolveUrl() {
        const token = this.options.accessToken
        if (!token) return this.url
        if (/[?&]access_token=/.test(this.url)) return this.url
        const separator = this.url.includes('?') ? '&' : '?'
        return `${this.url}${separator}access_token=${encodeURIComponent(token)}`
    }

    public connect() {
        return new Promise((resolve) => {
            this.socket = new ReconnectingWebSocket(this.resolveUrl(), [], {
                WebSocket,
            })
            this.socket.onopen = resolve
            this.socket.onmessage = (event) => this.handleWebSocketMessage(event.data.toString())
            this.socket.onerror = (event) => {
                this.rejectPendingRequests(new Error(`OneBot WebSocket error: ${event.message}`))
            }
            this.socket.onclose = (event) => {
                const reason = event.reason ? `: ${event.reason}` : ''
                this.rejectPendingRequests(new Error(`OneBot WebSocket closed${reason}`))
            }
        })
    }

    private takePendingRequest(echo: string) {
        const pending = this.echoMap.get(echo)
        if (!pending) return undefined
        this.echoMap.delete(echo)
        clearTimeout(pending.timeout)
        return pending
    }

    private rejectPendingRequests(error: Error) {
        const pendingRequests = Array.from(this.echoMap.values())
        this.echoMap.clear()
        for (const pending of pendingRequests) {
            clearTimeout(pending.timeout)
            pending.reject(error)
        }
    }

    private async handleWebSocketMessage(message: string) {
        // console.log('receive', message)
        const data = JSON.parse(message)
        if (data.echo) {
            const pending = this.takePendingRequest(String(data.echo))
            if (!pending) return
            if (data.status === 'ok') {
                pending.resolve(data.data)
            } else {
                // 看日志用
                const reason = data.wording || data.msg || data.retcode || 'unknown error'
                pending.reject(new Error(`${pending.action} 失败：${reason}`))
            }
            return
        } else if (data.post_type === 'message' || data.post_type === 'message_sent') {
            this.emit('message', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'friend_recall') {
            this.emit('friendRecall', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'group_recall') {
            this.emit('groupRecall', data)
        } else if (data.post_type === 'notice' && data.sub_type === 'poke') {
            if ('group_id' in data) this.emit('groupPoke', data)
            else this.emit('friendPoke', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'group_increase') {
            this.emit('groupIncrease', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'group_decrease') {
            this.emit('groupDecrease', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'group_ban') {
            this.emit('groupBan', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'group_admin') {
            this.emit('groupAdmin', data)
        } else if (data.post_type === 'notice' && data.sub_type === 'title') {
            this.emit('groupTitle', data)
        } else if (data.post_type === 'notice' && data.notice_type === 'friend_add') {
            this.emit('friendAdd', data)
        }
    }

    private async callApi<T>(action: string, params: { [key: string]: any } = {}) {
        return new Promise<T>((resolve, reject) => {
            const echo = `${new Date().getTime()}${random(100000, 999999)}`
            const timeout = setTimeout(() => {
                const pending = this.takePendingRequest(echo)
                pending?.reject(new Error(`OneBot API request timed out: ${action}`))
            }, ONEBOT_API_TIMEOUT)
            this.echoMap.set(echo, { action, resolve, reject, timeout })
            try {
                this.socket.send(JSON.stringify({ action, params, echo }))
            } catch (error) {
                const pending = this.takePendingRequest(echo)
                pending?.reject(error)
            }
            // console.log('send', JSON.stringify({ action, params, echo }))
        })
    }

    public getGroupInfo = (group_id: number, no_cache = false) =>
        this.callApi<{
            //群号
            group_id: number
            //群名称
            group_name: string
            //群备注
            group_memo: string
            //群创建时间
            group_create_time: number
            //群等级
            group_level: number
            //成员数
            member_count: number
            //最大成员数（群容量）
            max_member_count: number
        }>('get_group_info', { group_id, no_cache })

    public sendGroupMessage = (group_id: number, message: Sendable) =>
        this.callApi<{
            message_id: number
        }>('send_group_msg', { group_id, message })
    public sendPrivateMessage = (user_id: number, message: Sendable) =>
        this.callApi<{
            message_id: number
        }>('send_private_msg', { user_id, message })
    public getLoginInfo = () =>
        this.callApi<{
            user_id: number
            nickname: string
        }>('get_login_info')
    public getVersionInfo = () =>
        this.callApi<{
            app_name: string
            app_version: string
            app_full_name?: string
            protocol_version?: string
            coolq_edition?: 'pro'
            coolq_directory?: string
            'go-cqhttp'?: true
            plugin_version?: string
            plugin_build_number?: number
            plugin_build_configuration?: string
            runtime_version?: string
            runtime_os?: string
            version?: string
            //当前登陆使用协议类型
            protocol?: number
            // 其他实现可能带更多字段
            [key: string]: any
        }>('get_version_info')
    public getStatus = () =>
        this.callApi<{
            online: boolean
        }>('get_status')
    public getMessage = (message_id: number) =>
        this.callApi<{
            group: boolean
            group_id: number
            message_id: number
            real_id: number
            message_type: string
            sender: { nickname: string; user_id: number }
            time: number
            message: MessageElem[]
            raw_message: string
        }>('get_msg', { message_id })
    public setFriendAddRequest = (flag: string, approve = true, remark = '') =>
        this.callApi('set_friend_add_request', { flag, approve, remark })
    public setGroupAddRequest = (flag: string, approve = true, sub_type: 'add' | 'invite' = 'add', reason = '') =>
        this.callApi('set_group_add_request', { flag, approve, sub_type, reason })
    public deleteMessage = (message_id: number) => this.callApi('delete_msg', { message_id })
    public getGroupFileUrl = (group_id: number, file_id: string, busid: number) =>
        this.callApi<{
            url: string
        }>('get_group_file_url', { group_id, file_id, busid })
    public getPrivateFileUrl = (file_id: string) =>
        this.callApi<{
            url: string
        }>('get_private_file_url', { file_id })
    public getForwardMessage = (message_id: string) =>
        this.callApi<{
            messages: [
                {
                    content: MessageElem[]
                    sender: {
                        nickname: '发送者A'
                        user_id: 10086
                    }
                    time: 1595694374
                },
                {
                    content: MessageElem[]
                    sender: {
                        nickname: '发送者B'
                        user_id: 10087
                    }
                    time: 1595694393
                },
            ]
        }>('get_forward_msg', { message_id, id: message_id })
    public sendGroupSign = (group_id: number) => this.callApi('send_group_sign', { group_id })
    public getGroupList = (no_cache = false) =>
        this.callApi<Awaited<ReturnType<typeof this.getGroupInfo>>[]>('get_group_list', { no_cache })
    public getStrangerInfo = (user_id: number, no_cache = false) =>
        this.callApi<{
            user_id: number
            nickname: string
            sex: string
            age: number
            qid: string
            level: number
            login_days: number
            remark?: string
        }>('get_stranger_info', { no_cache, user_id })
    public getFriendList = () =>
        this.callApi<
            {
                user_id: number
                nickname: string
                remark: string
            }[]
        >('get_friend_list')
    public getGroupMemberInfo = (group_id: number, user_id: number, no_cache = false) =>
        this.callApi<{
            group_id: number
            user_id: number
            nickname: string
            card: string
            sex: string
            age: number
            area: string
            join_time: number
            last_sent_time: number
            level: string
            role: string
            unfriendly: boolean
            title: string
            title_expire_time: number
            card_changeable: boolean
            shut_up_timestamp: number
            subid: number
            uid: string
        }>('get_group_member_info', { group_id, user_id, no_cache })
    public setGroupCard = (group_id: number, user_id: number, card: string) =>
        this.callApi('set_group_card', { group_id, user_id, card })
    public getGroupMemberList = (group_id: number, no_cache = false) =>
        this.callApi<Awaited<ReturnType<typeof this.getGroupMemberInfo>>[]>('get_group_member_list', {
            group_id,
            no_cache,
        })
    public markMessageAsRead = (message_id: number) => this.callApi('mark_msg_as_read', { message_id })
    public sendGroupForwardMessage = (group_id: number, messages: { type: 'node'; data: any }[]) =>
        this.callApi<{
            message_id: number
            forward_id: string
        }>('send_group_forward_msg', { group_id, messages })
    public sendPrivateForwardMessage = (user_id: number, messages: { type: 'node'; data: any }[]) =>
        this.callApi<{
            message_id: number
            forward_id: string
        }>('send_private_forward_msg', { user_id, messages })
    public setGroupBan = (group_id: number, user_id: number, duration: number) =>
        this.callApi('set_group_ban', { group_id, user_id, duration })
    public setGroupWholeBan = (group_id: number, enable: boolean) =>
        this.callApi('set_group_whole_ban', { group_id, enable })
    public setGroupAnonymousBan = (group_id: number, anonymous_flag: string, duration: number) =>
        this.callApi('set_group_anonymous_ban', { group_id, anonymous_flag, duration })
    public setGroupKick = (group_id: number, user_id: number, reject_add_request = false) =>
        this.callApi('set_group_kick', { group_id, user_id, reject_add_request })
    public setGroupLeave = (group_id: number, is_dismiss = true) =>
        this.callApi('set_group_leave', { group_id, is_dismiss })
    public getGroupMessageHistory = (group_id: number, message_seq?: number) =>
        this.callApi<{
            messages: GroupMessage[]
        }>('get_group_msg_history', {
            group_id,
            message_seq,
            message_id: Number(message_seq) || 0,
            count: HISTORY_PAGE_SIZE,
            reverseOrder: true,
        })
    public getPrivateMessageHistory = (user_id: number, message_seq?: number) =>
        this.callApi<{
            messages: PrivateMessage[]
        }>('get_friend_msg_history', {
            user_id,
            message_seq,
            message_id: Number(message_seq) || 0,
            count: HISTORY_PAGE_SIZE,
            reverseOrder: true,
        })
    public setGroupRemark = (group_id: number, remark: string) => {
        // not implemented
    }
    public setFriendRemark = (user_id: number, remark: string) => {
        // not implemented
    }
    public sendFriendPoke = (user_id: number) => this.callApi('send_poke', { user_id })
    public sendGroupPoke = (group_id: number, user_id: number) => this.callApi('send_poke', { user_id, group_id })
    public getCookies = async (domain: string) => {
        const res = await this.callApi<{
            cookies: string
            bkn?: string
            token?: string | number
            csrf_token?: string | number
        }>('get_cookies', { domain })
        let bkn = res?.bkn ?? res?.token
        if (bkn === undefined || bkn === null || bkn === '') {
            try {
                const credentials = await this.callApi<{ token?: string | number; csrf_token?: string | number }>(
                    'get_credentials',
                    { domain },
                )
                bkn = credentials?.token ?? credentials?.csrf_token
            } catch (e) {
                try {
                    const csrf = await this.callApi<{ token?: string | number }>('get_csrf_token')
                    bkn = csrf?.token
                } catch (e2) {
                    // 后端不支持时保持缺失，由适配器兜底成 0
                }
            }
        }
        return { ...res, cookies: res?.cookies ?? '', bkn }
    }
    public getCustomStickers = () => this.callApi<string[]>('fetch_custom_face', { count: 1145141919 })
    public sendPrivateFile = (user_id: number, file: string, name: string) =>
        this.callApi('upload_private_file', { user_id, file, name })
    /**
     * 获取下载 rkey。
     * 统一归一化成 { type: 'private' | 'group', rkey, created_at, ttl }。
     */
    public getRkey = async (): Promise<DownloadRkey[]> => {
        try {
            const list = await this.callApi<any>('get_rkey')
            const rkeys = this.normalizeRkeyList(list)
            if (rkeys.length) return rkeys
        } catch (e) {
            // 部分后端没有 get_rkey，继续尝试 get_rkey_server
        }
        const serverRkey = await this.callApi<{
            expired_time?: number
            private_rkey?: string
            group_rkey?: string
        }>('get_rkey_server')
        return this.normalizeRkeyServer(serverRkey)
    }

    private normalizeRkeyType(type: any): 'private' | 'group' | undefined {
        if (type === 'private' || type === 'group') return type
        const numeric = Number(type)
        // Meow：10 = 私聊，20 = 群聊
        if (numeric === 10) return 'private'
        if (numeric === 20) return 'group'
        return undefined
    }

    private normalizeRkeyList(list: any): DownloadRkey[] {
        const rkeys: DownloadRkey[] = []
        for (const item of Array.isArray(list) ? list : []) {
            if (!item || typeof item.rkey !== 'string' || !item.rkey) continue
            const type = this.normalizeRkeyType(item.type)
            if (!type) continue
            rkeys.push({
                type,
                rkey: item.rkey,
                created_at: Number(item.created_at ?? item.create_time ?? 0),
                ttl: String(item.ttl ?? ''),
            })
        }
        return rkeys
    }

    private normalizeRkeyServer(res: { expired_time?: number; private_rkey?: string; group_rkey?: string }) {
        const rkeys: DownloadRkey[] = []
        const createdAt = Math.floor(Date.now() / 1000)
        const ttl = res?.expired_time ? String(Math.max(Math.floor(res.expired_time - createdAt), 0)) : ''
        if (res?.private_rkey) rkeys.push({ type: 'private', rkey: res.private_rkey, created_at: createdAt, ttl })
        if (res?.group_rkey) rkeys.push({ type: 'group', rkey: res.group_rkey, created_at: createdAt, ttl })
        return rkeys
    }
    public gfsMove = (
        group_id: number,
        file_id: string,
        current_parent_directory: string,
        target_parent_directory: string,
    ) =>
        this.callApi('move_group_file', {
            group_id,
            file_id,
            current_parent_directory,
            target_parent_directory,
            // Meow
            parent_directory: current_parent_directory,
            target_directory: target_parent_directory,
        })
    public gfsRename = (group_id: number, file_id: string, current_parent_directory: string, new_name: string) =>
        this.callApi('rename_group_file', { group_id, file_id, current_parent_directory, new_name })
    public gfsUpload = (group_id: number, file: string, name: string, folder_id: string) =>
        this.callApi('upload_group_file', { group_id, file, name, folder_id })
    public gfsMkdir = (group_id: number, folder_name: string) =>
        this.callApi('create_group_file_folder', {
            group_id,
            folder_name,
            // Meow
            name: folder_name,
            parent_id: '/',
        })
    public gfsDeleteFile = (group_id: number, file_id: string) =>
        this.callApi('delete_group_file', { group_id, file_id })
    public gfsDeleteFolder = (group_id: number, folder_id: string) =>
        this.callApi('delete_group_folder', { group_id, folder_id })
    public gfsDf = (group_id: number) =>
        this.callApi<{
            file_count: 0
            limit_count: 0
            used_space: 0
            total_space: 0
        }>('get_group_file_system_info', { group_id })
    public gfsListRoot = async (group_id: number) =>
        normalizeGroupFileList(await this.callApi<{
            files: [
                {
                    group_id: 0
                    file_id: 'string'
                    file_name: 'string'
                    busid: 0
                    size: 0
                    upload_time: 0
                    dead_time: 0
                    modify_time: 0
                    download_times: 0
                    uploader: 0
                    uploader_name: 'string'
                },
            ]
            folders: [
                {
                    group_id: 0
                    folder_id: 'string'
                    folder: 'string'
                    folder_name: 'string'
                    create_time: 'string'
                    creator: 'string'
                    creator_name: 'string'
                    total_file_count: 'string'
                },
            ]
        }>('get_group_root_files', { group_id }))
    public gfsListDir = async (group_id: number, folder_id: string) =>
        normalizeGroupFileList(await this.callApi<{
            files: [
                {
                    group_id: 0
                    file_id: 'string'
                    file_name: 'string'
                    busid: 0
                    size: 0
                    upload_time: 0
                    dead_time: 0
                    modify_time: 0
                    download_times: 0
                    uploader: 0
                    uploader_name: 'string'
                },
            ]
            folders: [
                {
                    group_id: 0
                    folder_id: 'string'
                    folder: 'string'
                    folder_name: 'string'
                    create_time: 'string'
                    creator: 'string'
                    creator_name: 'string'
                    total_file_count: 'string'
                },
            ]
        }>('get_group_files_by_folder', {
            group_id,
            folder_id,
            file_count: 11451419,
            count: 11451419,
        }))
    public gfsDownloadUrl = (group_id: number, file_id: string) =>
        this.callApi<{
            url: 'string'
        }>('get_group_file_url', { group_id, file_id })
}
