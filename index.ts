/**
 * chat-service — Socket.io 在线客服实时通信服务
 *
 * 端口: 3004 (固定)
 * 路径: / (Caddy 网关转发要求, 前端连接: io("/?XTransformPort=3004"))
 *
 * 职责:
 *  1. 维护访客 / 客服的 socket 连接
 *  2. 通过 room 机制广播会话消息 (room = `session:{sessionId}`)
 *  3. 桥接 Socket ↔ Go HTTP API (访客消息 → API → AI 回复 → 内部 HTTP 回调 → 推送)
 *  4. 暴露内部 HTTP 接口供 Go API 主动推送消息/事件
 *
 * ⚠️ 技术细节: engine.io 的 attach 会替换 httpServer 的所有 'request' 监听器,
 *    当 path='/' 时 engine.io 会拦截所有 HTTP 请求 (check(req) 恒为 true).
 *    为此我们在 attach 之后, 把 engine.io 的监听器替换为包装器, 优先处理 /internal/ 路径.
 *
 * 前端 (访客) 事件:
 *   client → server:  chat:join | chat:message | chat:typing | chat:leave
 *   server → client:  chat:history | chat:message | chat:typing | chat:agent_joined | chat:agent_left | chat:closed | server:shutdown | error
 *
 * 前端 (客服) 事件:
 *   client → server:  agent:online | agent:takeover | agent:message | agent:typing | agent:close
 *   server → client:  session:list | session:new | session:update | session:message | error
 *
 * 内部 HTTP 接口 (供 Go API 调用, 仅 localhost):
 *   GET  /internal/health
 *   GET  /internal/sessions
 *   POST /internal/push              { sessionId, message }
 *   POST /internal/typing            { sessionId, isTyping, from: 'ai'|'agent' }
 *   POST /internal/agent_event       { sessionId, type: 'agent_joined'|'agent_left'|'closed', agentName? }
 *   POST /internal/session_new       { session: ChatSession }
 *   POST /internal/session_update    { session: ChatSession }
 */

import { createServer, type IncomingMessage, type ServerResponse } from 'http'
import { Server, type Socket } from 'socket.io'

// ============ Types ============

interface ChatMessage {
  id: string
  sessionId: string
  role: 'user' | 'assistant' | 'agent' | 'system'
  content: string
  meta?: string | null // JSON string (与 Prisma 模型一致)
  createdAt: string
}

interface ChatSession {
  id: string
  sessionId: string
  visitorName?: string | null
  visitorIp?: string | null
  status: 'active' | 'waiting' | 'closed'
  assignedTo?: string | null
  source?: string
  createdAt?: string
  updatedAt?: string
}

interface SessionSummary {
  sessionId: string
  visitorName?: string | null
  status: string
  assignedTo?: string | null
  lastMessageAt: string
  messageCount: number
}

interface SocketBinding {
  type: 'visitor' | 'agent'
  id: string // sessionId 或 agentId
}

interface SessionMeta {
  lastMessageAt: string
  messageCount: number
  assignedTo?: string | null
  visitorName?: string | null
  status: string
}

// ============ Constants ============

const PORT = 3004
const API_BASE = process.env.API_BASE || 'http://localhost:8080'

// ============ In-Memory State ============

/** sessionId → Set<socketId>  (一个访客可能多端登录) */
const visitorSockets = new Map<string, Set<string>>()
/** agentId → Set<socketId>  (一个客服可能多端登录) */
const agentSockets = new Map<string, Set<string>>()
/** socketId → binding */
const socketToSession = new Map<string, SocketBinding>()
/** sessionId → metadata (用于 session list) */
const sessionMetadata = new Map<string, SessionMeta>()
/** 等待客服接管的会话 */
const waitingSessions = new Set<string>()

// ============ Utils ============

function log(level: 'info' | 'warn' | 'error' | 'debug', msg: string, data?: unknown): void {
  const ts = new Date().toISOString()
  const prefix = `[${ts}] [${level.toUpperCase()}]`
  if (data !== undefined) {
    const payload = typeof data === 'string' ? data : safeJson(data)
    console.log(`${prefix} ${msg} ${payload}`)
  } else {
    console.log(`${prefix} ${msg}`)
  }
}

function safeJson(data: unknown): string {
  try {
    return JSON.stringify(data)
  } catch {
    return String(data)
  }
}

function roomFor(sessionId: string): string {
  return `session:${sessionId}`
}

function getVisitorSockets(sessionId: string): Socket[] {
  const ids = visitorSockets.get(sessionId)
  if (!ids || ids.size === 0) return []
  const out: Socket[] = []
  for (const id of ids) {
    const s = io.sockets.sockets.get(id)
    if (s) out.push(s)
  }
  return out
}

function getAllAgentSockets(): Socket[] {
  const out: Socket[] = []
  for (const ids of agentSockets.values()) {
    for (const id of ids) {
      const s = io.sockets.sockets.get(id)
      if (s) out.push(s)
    }
  }
  return out
}

function countVisitors(): number {
  let n = 0
  for (const set of visitorSockets.values()) n += set.size
  return n
}

function countAgents(): number {
  return agentSockets.size
}

function summarizeSessions(): SessionSummary[] {
  const out: SessionSummary[] = []
  for (const [sid, meta] of sessionMetadata.entries()) {
    out.push({
      sessionId: sid,
      visitorName: meta.visitorName ?? null,
      status: meta.status,
      assignedTo: meta.assignedTo ?? null,
      lastMessageAt: meta.lastMessageAt,
      messageCount: meta.messageCount,
    })
  }
  out.sort((a, b) => new Date(b.lastMessageAt).getTime() - new Date(a.lastMessageAt).getTime())
  return out
}

function broadcastSessionListToAgents(): void {
  const sessions = summarizeSessions()
  for (const s of getAllAgentSockets()) {
    s.emit('session:list', { sessions })
  }
}

/** 调用后端 API, 失败返回 null */
async function fetchJson<T = unknown>(url: string, init?: RequestInit): Promise<T | null> {
  try {
    const res = await fetch(url, init)
    if (!res.ok) {
      log('warn', `fetch ${url} returned ${res.status} ${res.statusText}`)
      return null
    }
    return (await res.json()) as T
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e)
    log('warn', `fetch ${url} failed: ${msg}`)
    return null
  }
}

function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 1_000_000) {
        req.destroy()
        resolve(null)
      }
    })
    req.on('end', () => {
      if (!data) return resolve(null)
      try {
        resolve(JSON.parse(data))
      } catch {
        resolve(null)
      }
    })
    req.on('error', () => resolve(null))
  })
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body)
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  })
  res.end(payload)
}

function cleanupVisitorSocket(socket: Socket, sessionId: string): void {
  const set = visitorSockets.get(sessionId)
  if (set) {
    set.delete(socket.id)
    if (set.size === 0) {
      visitorSockets.delete(sessionId)
      // 注意: sessionMetadata 保留, 客服端依然可见历史会话
    }
  }
  socket.leave(roomFor(sessionId))
}

// ============ HTTP Server (internal API) ============
//
// 注意: engine.io 的 attach() 会移除 httpServer 上已有的 'request' 监听器并替换为自己的.
// 当 path='/' 时, engine.io 的 check(req) 恒为 true, 会拦截所有 HTTP 请求.
// 因此我们:
//   1. 先创建空的 httpServer (不带 request handler)
//   2. 让 socket.io attach (engine.io 会接管 request 事件)
//   3. 再把 engine.io 的监听器替换为包装器: 优先处理 /internal/, 其余透传给 engine.io

const httpServer = createServer()

/** 内部 HTTP 请求处理 (仅处理 /internal/* 路径) */
async function handleInternalRequest(req: IncomingMessage, res: ServerResponse, url: string): Promise<void> {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    sendJson(res, 204, {})
    return
  }

  const method = req.method ?? 'GET'

  // GET /internal/health
  if (method === 'GET' && url === '/internal/health') {
    sendJson(res, 200, {
      ok: true,
      service: 'chat-service',
      port: PORT,
      uptime: process.uptime(),
      visitors: countVisitors(),
      agents: countAgents(),
      waiting: waitingSessions.size,
      sessions: sessionMetadata.size,
    })
    return
  }

  // GET /internal/sessions
  if (method === 'GET' && url === '/internal/sessions') {
    sendJson(res, 200, {
      sessions: summarizeSessions(),
      waiting: Array.from(waitingSessions),
    })
    return
  }

  // 以下均为 POST, 需读取 body
  const body = method === 'POST' ? await readBody(req) : null

  // POST /internal/push — 推送消息到会话 room (访客 + 接管客服均收到)
  if (method === 'POST' && url === '/internal/push') {
    const b = body as { sessionId?: string; message?: ChatMessage } | null
    if (!b?.sessionId || !b?.message) {
      sendJson(res, 400, { ok: false, error: 'missing sessionId or message' })
      return
    }
    const { sessionId, message } = b
    io.to(roomFor(sessionId)).emit('chat:message', { message })
    const meta = sessionMetadata.get(sessionId)
    if (meta) {
      meta.lastMessageAt = new Date().toISOString()
      meta.messageCount += 1
    }
    broadcastSessionListToAgents()
    log('info', `push → ${sessionId}`, { role: message.role, len: message.content?.length ?? 0 })
    sendJson(res, 200, { ok: true })
    return
  }

  // POST /internal/typing — 通知访客 AI/客服 正在输入
  if (method === 'POST' && url === '/internal/typing') {
    const b = body as { sessionId?: string; isTyping?: boolean; from?: string } | null
    if (!b?.sessionId) {
      sendJson(res, 400, { ok: false, error: 'missing sessionId' })
      return
    }
    const from: 'ai' | 'agent' = b.from === 'agent' ? 'agent' : 'ai'
    for (const s of getVisitorSockets(b.sessionId)) {
      s.emit('chat:typing', { isTyping: !!b.isTyping, from })
    }
    sendJson(res, 200, { ok: true })
    return
  }

  // POST /internal/agent_event — agent_joined | agent_left | closed
  if (method === 'POST' && url === '/internal/agent_event') {
    const b = body as { sessionId?: string; type?: string; agentName?: string } | null
    if (!b?.sessionId || !b?.type) {
      sendJson(res, 400, { ok: false, error: 'missing sessionId or type' })
      return
    }
    const visitors = getVisitorSockets(b.sessionId)
    const meta = sessionMetadata.get(b.sessionId)
    if (b.type === 'agent_joined') {
      for (const s of visitors) {
        s.emit('chat:agent_joined', { agentName: b.agentName ?? '客服' })
      }
      if (meta) {
        meta.status = 'active'
        waitingSessions.delete(b.sessionId)
      }
    } else if (b.type === 'agent_left') {
      for (const s of visitors) s.emit('chat:agent_left', {})
      if (meta) meta.assignedTo = null
    } else if (b.type === 'closed') {
      for (const s of visitors) s.emit('chat:closed', {})
      if (meta) {
        meta.status = 'closed'
        waitingSessions.delete(b.sessionId)
      }
    } else {
      sendJson(res, 400, { ok: false, error: `unknown event type: ${b.type}` })
      return
    }
    broadcastSessionListToAgents()
    log('info', `agent_event ${b.type} → ${b.sessionId}`)
    sendJson(res, 200, { ok: true })
    return
  }

  // POST /internal/session_new — 通知客服有新会话
  if (method === 'POST' && url === '/internal/session_new') {
    const b = body as { session?: ChatSession } | null
    if (!b?.session) {
      sendJson(res, 400, { ok: false, error: 'missing session' })
      return
    }
    const s = b.session
    sessionMetadata.set(s.sessionId, {
      lastMessageAt: s.updatedAt ?? new Date().toISOString(),
      messageCount: 0,
      assignedTo: s.assignedTo ?? null,
      visitorName: s.visitorName ?? null,
      status: s.status ?? 'active',
    })
    if (s.status === 'waiting' || !s.assignedTo) {
      waitingSessions.add(s.sessionId)
    } else {
      waitingSessions.delete(s.sessionId)
    }
    for (const sock of getAllAgentSockets()) {
      sock.emit('session:new', { session: s })
    }
    broadcastSessionListToAgents()
    sendJson(res, 200, { ok: true })
    return
  }

  // POST /internal/session_update — 通知客服会话状态变更
  if (method === 'POST' && url === '/internal/session_update') {
    const b = body as { session?: ChatSession } | null
    if (!b?.session) {
      sendJson(res, 400, { ok: false, error: 'missing session' })
      return
    }
    const s = b.session
    const meta: SessionMeta = sessionMetadata.get(s.sessionId) ?? {
      lastMessageAt: new Date().toISOString(),
      messageCount: 0,
    }
    meta.assignedTo = s.assignedTo ?? null
    meta.visitorName = s.visitorName ?? meta.visitorName
    meta.status = s.status ?? meta.status
    meta.lastMessageAt = s.updatedAt ?? meta.lastMessageAt
    sessionMetadata.set(s.sessionId, meta)
    if (meta.status === 'closed') {
      waitingSessions.delete(s.sessionId)
    } else if (meta.assignedTo) {
      waitingSessions.delete(s.sessionId)
    } else if (meta.status === 'waiting') {
      waitingSessions.add(s.sessionId)
    }
    for (const sock of getAllAgentSockets()) {
      sock.emit('session:update', { session: s })
    }
    sendJson(res, 200, { ok: true })
    return
  }

  // 404
  sendJson(res, 404, { ok: false, error: 'not found', url, method })
}

// ============ Socket.io Server ============

const io = new Server(httpServer, {
  // DO NOT change path — Caddy uses it for routing
  path: '/',
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  connectTimeout: 10000,
})

// ⚠️ 关键: 替换 engine.io 的 request 监听器为包装器, 优先处理 /internal/* 路径
// engine.io 在 attach 时会移除所有 'request' 监听器并替换为自己的 (见 engine.io/build/server.js attach()).
// 当 path='/' 时, 它的 check(req) 恒为 true, 会拦截所有 HTTP 请求.
// 我们把它的监听器替换为: /internal/* 走我们的 handler, 其余透传给 engine.io.
const engineRequestListeners = httpServer.listeners('request').slice(0) as Array<
  (req: IncomingMessage, res: ServerResponse) => void
>
httpServer.removeAllListeners('request')
httpServer.on('request', (req, res) => {
  const url = (req.url ?? '').split('?')[0]
  if (url.startsWith('/internal/')) {
    handleInternalRequest(req, res, url).catch((e: unknown) => {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `internal handler error: ${msg}`)
      if (!res.headersSent) {
        sendJson(res, 500, { ok: false, error: 'internal error' })
      } else if (!res.writableEnded) {
        res.end()
      }
    })
    return
  }
  // 其余请求透传给 engine.io (socket.io 握手 / polling / websocket upgrade)
  for (const listener of engineRequestListeners) {
    listener.call(httpServer, req, res)
  }
})

io.on('connection', (socket: Socket) => {
  log('info', `socket connected: ${socket.id}`)

  // ==================== Visitor events ====================

  socket.on('chat:join', async (data: { sessionId: string; visitorName?: string }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId || typeof sessionId !== 'string') {
        socket.emit('error', { event: 'chat:join', message: 'invalid sessionId' })
        return
      }
      const visitorName = data.visitorName?.trim() || null

      // Register visitor socket (multi-tab support)
      if (!visitorSockets.has(sessionId)) visitorSockets.set(sessionId, new Set())
      visitorSockets.get(sessionId)!.add(socket.id)
      socketToSession.set(socket.id, { type: 'visitor', id: sessionId })

      // Join the session room
      socket.join(roomFor(sessionId))

      // Initialize or update metadata
      const existed = sessionMetadata.has(sessionId)
      const meta: SessionMeta = sessionMetadata.get(sessionId) ?? {
        lastMessageAt: new Date().toISOString(),
        messageCount: 0,
        assignedTo: null,
        visitorName,
        status: 'waiting',
      }
      if (visitorName) meta.visitorName = visitorName
      sessionMetadata.set(sessionId, meta)

      if (!existed) {
        waitingSessions.add(sessionId)
        const sessionPayload: ChatSession = {
          id: sessionId,
          sessionId,
          visitorName,
          status: 'waiting',
          assignedTo: null,
          source: 'website',
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        }
        for (const s of getAllAgentSockets()) {
          s.emit('session:new', { session: sessionPayload })
        }
        broadcastSessionListToAgents()
      } else {
        // Already known — refresh list (visitor count etc. not in summary, but okay)
        broadcastSessionListToAgents()
      }

      // Fetch history from backend API (best-effort)
      const history = await fetchJson<{ messages?: ChatMessage[] }>(
        `${API_BASE}/api/chat/history?sessionId=${encodeURIComponent(sessionId)}`,
      )
      socket.emit('chat:history', { messages: history?.messages ?? [] })

      // If session already assigned to an agent, notify visitor
      if (meta.assignedTo) {
        socket.emit('chat:agent_joined', { agentName: '客服' })
      }

      log('info', `visitor joined ${sessionId}`, {
        visitorName,
        sockets: visitorSockets.get(sessionId)!.size,
        isNew: !existed,
      })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `chat:join failed: ${msg}`)
      socket.emit('error', { event: 'chat:join', message: 'internal error' })
    }
  })

  socket.on('chat:message', async (data: { sessionId: string; content: string }) => {
    try {
      const sessionId = data?.sessionId
      const content = data?.content
      if (!sessionId || !content || typeof content !== 'string') {
        socket.emit('error', { event: 'chat:message', message: 'invalid payload' })
        return
      }
      if (content.length > 5000) {
        socket.emit('error', { event: 'chat:message', message: 'message too long' })
        return
      }
      // Forward to backend API for DB save + AI reply.
      // The API will call /internal/push to broadcast back to the room.
      await fetchJson(`${API_BASE}/api/chat/message`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, content, source: 'socket' }),
      })
      log('info', `forwarded visitor message → API`, { sessionId, len: content.length })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `chat:message forward failed: ${msg}`)
      socket.emit('error', { event: 'chat:message', message: 'failed to send' })
    }
  })

  socket.on('chat:typing', (data: { sessionId: string; isTyping: boolean }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId) return
      // Notify agents (and any other room participants except sender)
      socket.to(roomFor(sessionId)).emit('chat:typing', {
        isTyping: !!data.isTyping,
        from: 'visitor',
      })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('warn', `chat:typing failed: ${msg}`)
    }
  })

  socket.on('chat:leave', (data: { sessionId: string }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId) return
      cleanupVisitorSocket(socket, sessionId)
      log('info', `visitor left ${sessionId}`)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('warn', `chat:leave failed: ${msg}`)
    }
  })

  // ==================== Agent events ====================

  socket.on('agent:online', (data: { agentId: string; agentName?: string }) => {
    try {
      const agentId = data?.agentId
      if (!agentId || typeof agentId !== 'string') {
        socket.emit('error', { event: 'agent:online', message: 'invalid agentId' })
        return
      }
      if (!agentSockets.has(agentId)) agentSockets.set(agentId, new Set())
      agentSockets.get(agentId)!.add(socket.id)
      socketToSession.set(socket.id, { type: 'agent', id: agentId })
      socket.join('agents')

      // Send current session list immediately
      socket.emit('session:list', { sessions: summarizeSessions() })
      log('info', `agent online: ${agentId}`, { sockets: agentSockets.get(agentId)!.size })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `agent:online failed: ${msg}`)
    }
  })

  socket.on('agent:takeover', (data: { sessionId: string }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId) return
      const info = socketToSession.get(socket.id)
      if (!info || info.type !== 'agent') {
        socket.emit('error', { event: 'agent:takeover', message: 'not authorized as agent' })
        return
      }
      // Forward to backend API to persist assignment in DB
      fetchJson(`${API_BASE}/api/chat/agent/takeover`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, agentId: info.id }),
      }).catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e)
        log('warn', `takeover API failed: ${msg}`)
      })

      // Optimistic local update
      const meta = sessionMetadata.get(sessionId)
      if (meta) {
        meta.assignedTo = info.id
        meta.status = 'active'
        waitingSessions.delete(sessionId)
      }
      // Join the session room so agent receives subsequent messages
      socket.join(roomFor(sessionId))
      // Notify visitor
      for (const s of getVisitorSockets(sessionId)) {
        s.emit('chat:agent_joined', { agentName: '客服' })
      }
      broadcastSessionListToAgents()
      log('info', `agent ${info.id} took over session ${sessionId}`)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `agent:takeover failed: ${msg}`)
    }
  })

  socket.on('agent:message', (data: { sessionId: string; content: string }) => {
    try {
      const sessionId = data?.sessionId
      const content = data?.content
      if (!sessionId || !content || typeof content !== 'string') {
        socket.emit('error', { event: 'agent:message', message: 'invalid payload' })
        return
      }
      const info = socketToSession.get(socket.id)
      if (!info || info.type !== 'agent') {
        socket.emit('error', { event: 'agent:message', message: 'not authorized as agent' })
        return
      }
      // Forward to backend API to persist + broadcast via /internal/push
      fetchJson(`${API_BASE}/api/chat/agent/message`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, content, agentId: info.id }),
      }).catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e)
        log('warn', `agent:message API failed: ${msg}`)
      })
      log('info', `agent ${info.id} → session ${sessionId}`, { len: content.length })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `agent:message failed: ${msg}`)
    }
  })

  socket.on('agent:typing', (data: { sessionId: string; isTyping: boolean }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId) return
      for (const s of getVisitorSockets(sessionId)) {
        s.emit('chat:typing', { isTyping: !!data.isTyping, from: 'agent' })
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('warn', `agent:typing failed: ${msg}`)
    }
  })

  socket.on('agent:close', (data: { sessionId: string }) => {
    try {
      const sessionId = data?.sessionId
      if (!sessionId) return
      const info = socketToSession.get(socket.id)
      if (!info || info.type !== 'agent') {
        socket.emit('error', { event: 'agent:close', message: 'not authorized as agent' })
        return
      }
      fetchJson(`${API_BASE}/api/chat/agent/close`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, agentId: info.id }),
      }).catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e)
        log('warn', `agent:close API failed: ${msg}`)
      })
      const meta = sessionMetadata.get(sessionId)
      if (meta) {
        meta.status = 'closed'
        waitingSessions.delete(sessionId)
      }
      for (const s of getVisitorSockets(sessionId)) {
        s.emit('chat:closed', {})
      }
      broadcastSessionListToAgents()
      log('info', `agent ${info.id} closed session ${sessionId}`)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      log('error', `agent:close failed: ${msg}`)
    }
  })

  // ==================== Generic ====================

  // Latency probe (lightweight, no auth)
  socket.on('ping', (cb: unknown) => {
    if (typeof cb === 'function') cb()
  })

  socket.on('error', (err: unknown) => {
    const msg = err instanceof Error ? err.message : String(err)
    log('error', `socket error ${socket.id}: ${msg}`)
  })

  socket.on('disconnect', (reason: string) => {
    const info = socketToSession.get(socket.id)
    if (info) {
      if (info.type === 'visitor') {
        cleanupVisitorSocket(socket, info.id)
        log('info', `visitor socket disconnected: ${socket.id}`, { sessionId: info.id, reason })
      } else if (info.type === 'agent') {
        const set = agentSockets.get(info.id)
        if (set) {
          set.delete(socket.id)
          if (set.size === 0) {
            agentSockets.delete(info.id)
            log('info', `agent offline: ${info.id}`, { reason })
          } else {
            log('info', `agent socket disconnected: ${socket.id}`, { agentId: info.id, reason, remaining: set.size })
          }
        }
      }
      socketToSession.delete(socket.id)
    } else {
      log('info', `socket disconnected: ${socket.id}`, { reason })
    }
  })
})

// ============ Start ============

httpServer.listen(PORT, () => {
  log('info', `chat-service listening on port ${PORT}`)
  log('info', `socket.io path: /  (frontend: io("/?XTransformPort=${PORT}"))`)
  log('info', `API base: ${API_BASE}`)
  log('info', `internal HTTP endpoints:`)
  log('info', `  GET  /internal/health`)
  log('info', `  GET  /internal/sessions`)
  log('info', `  POST /internal/push              { sessionId, message }`)
  log('info', `  POST /internal/typing            { sessionId, isTyping, from }`)
  log('info', `  POST /internal/agent_event       { sessionId, type, agentName? }`)
  log('info', `  POST /internal/session_new       { session }`)
  log('info', `  POST /internal/session_update    { session }`)
})

// ============ Graceful Shutdown ============

let shuttingDown = false
function shutdown(signal: string): void {
  if (shuttingDown) return
  shuttingDown = true
  log('info', `received ${signal}, shutting down...`)

  // Notify all connected clients
  try {
    io.emit('server:shutdown', { message: 'chat-service restarting', at: new Date().toISOString() })
  } catch {
    // ignore
  }

  // Close socket.io (closes all sockets)
  io.close(() => {
    log('info', 'socket.io closed')
  })

  // Close HTTP server
  httpServer.close(() => {
    log('info', 'http server closed')
    process.exit(0)
  })

  // Force exit after 5s
  setTimeout(() => {
    log('warn', 'force exit after 5s timeout')
    process.exit(1)
  }, 5000)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

process.on('uncaughtException', (e: Error) => {
  log('error', `uncaughtException: ${e?.message ?? e}`)
})
process.on('unhandledRejection', (e: unknown) => {
  const msg = e instanceof Error ? e.message : String(e)
  log('error', `unhandledRejection: ${msg}`)
})