# chat-service

Socket.io 在线客服实时通信 mini-service (端口 **3004**)

## 启动

```bash
cd mini-services/chat-service
bun install
bun run dev        # 热重载开发模式
# 或
bun run start      # 生产模式
```

## 前端连接

```ts
import { io } from 'socket.io-client'

// ⚠️ path 必须为 "/" 且必须带 XTransformPort=3004 (Caddy 网关要求)
const socket = io('/?XTransformPort=3004', {
  transports: ['websocket', 'polling'],
  reconnection: true,
})
```

## 事件契约

### 访客 (website user)

| 方向 | 事件 | Payload | 说明 |
|------|------|---------|------|
| C → S | `chat:join` | `{ sessionId, visitorName? }` | 访客加入, 创建/恢复会话 |
| C → S | `chat:message` | `{ sessionId, content }` | 访客发送消息 (转发到 Next.js API 处理 AI 回复) |
| C → S | `chat:typing` | `{ sessionId, isTyping }` | 访客正在输入 |
| C → S | `chat:leave` | `{ sessionId }` | 访客离开 |
| S → C | `chat:history` | `{ messages: ChatMessage[] }` | 加入后返回历史 |
| S → C | `chat:message` | `{ message: ChatMessage }` | 新消息 (AI/客服/自己) |
| S → C | `chat:typing` | `{ isTyping, from: 'ai'\|'agent' }` | AI/客服 输入状态 |
| S → C | `chat:agent_joined` | `{ agentName }` | 客服接管通知 |
| S → C | `chat:agent_left` | `{}` | 客服离开 |
| S → C | `chat:closed` | `{}` | 会话被关闭 |
| S → C | `server:shutdown` | `{ message, at }` | 服务关闭 (重启) |
| S → C | `error` | `{ event, message }` | 错误 |

### 客服 (admin agent)

| 方向 | 事件 | Payload | 说明 |
|------|------|---------|------|
| C → S | `agent:online` | `{ agentId, agentName? }` | 客服上线 |
| C → S | `agent:takeover` | `{ sessionId }` | 接管会话 |
| C → S | `agent:message` | `{ sessionId, content }` | 客服发消息 |
| C → S | `agent:typing` | `{ sessionId, isTyping }` | 客服输入状态 |
| C → S | `agent:close` | `{ sessionId }` | 关闭会话 |
| S → C | `session:list` | `{ sessions: SessionSummary[] }` | 上线时全量推送 |
| S → C | `session:new` | `{ session: ChatSession }` | 新会话创建 |
| S → C | `session:update` | `{ session: ChatSession }` | 会话状态变更 |
| S → C | `error` | `{ event, message }` | 错误 |

### 类型

```ts
interface ChatMessage {
  id: string
  sessionId: string
  role: 'user' | 'assistant' | 'agent' | 'system'
  content: string
  meta?: string | null   // JSON string (与 Prisma 模型一致)
  createdAt: string      // ISO
}

interface SessionSummary {
  sessionId: string
  visitorName?: string | null
  status: 'active' | 'waiting' | 'closed'
  assignedTo?: string | null
  lastMessageAt: string
  messageCount: number
}
```

## 内部 HTTP 接口 (供 Next.js API 调用, 仅 localhost)

Base URL: `http://localhost:3004`

| 方法 | 路径 | Body | 说明 |
|------|------|------|------|
| GET | `/internal/health` | — | 健康检查 |
| GET | `/internal/sessions` | — | 当前在线会话列表 |
| POST | `/internal/push` | `{ sessionId, message }` | 推送消息到 room (访客+客服) |
| POST | `/internal/typing` | `{ sessionId, isTyping, from: 'ai'\|'agent' }` | 通知访客输入状态 |
| POST | `/internal/agent_event` | `{ sessionId, type: 'agent_joined'\|'agent_left'\|'closed', agentName? }` | 会话事件 |
| POST | `/internal/session_new` | `{ session }` | 通知客服新会话 |
| POST | `/internal/session_update` | `{ session }` | 通知客服会话更新 |

## 数据流

### 访客发消息 (走 Socket)

```
访客 → chat:message → chat-service → POST /api/chat/message (Next.js)
                                         ↓
                                       保存 DB + 调用 AI (z-ai-web-dev-sdk)
                                         ↓
                                       POST /internal/push (回 chat-service)
                                         ↓
                                       io.to(room).emit('chat:message')
                                         ↓
                                    访客 + 接管客服 均收到
```

### 访客发消息 (走 HTTP, 推荐方式)

前端直接 `POST /api/chat/message` → Next.js 处理 → `POST /internal/push` → chat-service 广播

### 客服接管

```
客服 → agent:takeover → chat-service → POST /api/chat/agent/takeover (持久化)
                              ↓
                          加入 room, 通知访客 chat:agent_joined
                          广播 session:update 给所有客服
```

## Caddy 配置

前端所有连接/请求都通过 Caddy 网关转发。Socket.io path 必须为 `/`, 通过 `XTransformPort=3004` 路由到本服务。

```
# Caddyfile (示意)
handle_path /socket.io/* {
    reverse_proxy localhost:3004
}
```

## 开发说明

- `bun --hot index.ts` 会在文件变更时自动重启
- 日志输出到 stdout (JSON 行格式)
- 优雅关闭: SIGTERM/SIGINT 会先发 `server:shutdown` 通知客户端, 然后关闭 io + http, 5s 后强制退出
- 心跳: pingTimeout=60s, pingInterval=25s (与示例一致)
- 内存状态: 重启后会丢失访客/客服的 socket 绑定 (会话历史由 Next.js API 持久化在 SQLite)