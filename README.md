# webhook-delivery-go

事件通知与 Webhook 投递服务。上游业务事件按订阅关系扇出到多个下游 HTTP
端点，提供**每端点严格有序**、**稳定投递 ID 去重**、**失败退避重试**、
**端点级熔断**、**死信队列**、**HMAC 签名鉴权**和**落盘重启恢复**能力。

包路径：`github.com/guoyyit/webhook-delivery-go`（包名 `webhook`），无第三方
依赖，仅使用标准库。

## 快速开始

```go
d, err := webhook.NewDispatcher(webhook.DefaultConfig()) // StorePath 为空时仅内存
if err != nil { return err }
defer d.Shutdown(context.Background())

if err := d.Start(context.Background()); err != nil { return err }

d.AddEndpoint(webhook.Endpoint{
    ID: "billing", URL: "https://billing.example/hooks",
    Secret: []byte("hmac-secret"), AuthToken: "bearer-token",
})
d.PutSubscription(webhook.Subscription{
    ID: "billing-orders", EndpointID: "billing",
    EventTypes: []string{"order.created", "order.paid"}, // 或 "*" 全量
})

n, err := d.Publish(webhook.Event{
    ID: "evt-123", Type: "order.created", Payload: []byte(`{"id":42}`),
})
// n = 命中的订阅数；Publish 永不因慢端点而阻塞。
```

## 投递语义

| 主题 | 保证 |
|---|---|
| 扇出 | 一次事件可同时命中多个订阅；匹配基于快照，不受注册顺序/并发注册影响 |
| 顺序 | **同一端点内**按全局单调 `Seq` 严格串行投递；一条失败重试期间其后的事件在该端点排队等待，不会越过它 |
| 端点隔离 | 每个端点一个独立 goroutine；慢端点只阻塞自己，不影响其他端点，也不阻塞事件产生方 |
| 投递保证 | **至少一次**（at-least-once）。崩溃/超时导致结果未知时会重投，下游必须用稳定的投递 ID 幂等去重 |
| 去重标识 | 投递 ID = `dlv_` + `SHA256(eventID + NUL + endpointID)` 前 16 字节 hex。重试、重启、死信重放都保持不变 |
| 退订 | `RemoveSubscription` 后该订阅不再匹配；已在队列中的事件到达队首时进入死信（`subscription_removed`），不静默丢弃 |
| 重试 | 408/409/425/429/5xx 与网络错误可重试，指数退避 + 可配置抖动；其他 4xx 为终态直接死信 |
| 时限 | 每条投递有独立的总时限 `TotalDeadline` 与每次尝试超时 `HTTPTimeout`；到期进入死信 |
| 熔断 | 连续可重试失败达到阈值即断开（open）；隔离期间新事件进入死信（`endpoint_circuit_open`）；TTL 后放行限量探测（half-open），探测成功自动恢复（closed），失败则重新隔离 |
| 背压 | 每端点待投队列有上限；满时新事件立即落盘并进死信（`endpoint_queue_overflow`），`Publish` 不阻塞 |
| 死信 | 所有终态失败持久化在死信集合中，带原因码；可用显式 `Replay` 在保留窗口内重放 |
| 持久化 | 投递进度、Seq、死信均写盘；写盘采用临时文件 + fsync + rename 原子替换，落盘中断不会损坏已有状态 |
| 重启恢复 | pending 继续推进；崩溃时 inflight 回滚为 pending（结果未知按未确认处理）；已确认（succeeded/dead）不重复投递 |
| 关闭 | `Shutdown` 取消进行中的 HTTP 请求，inflight 回滚 pending 并落盘后 worker 才退出，不留半完成状态 |

### 死信原因码（用于定位“卡在哪一步”）

- `non_retryable_status` — 不可重试的 4xx
- `max_attempts_exceeded` — 重试上限
- `delivery_deadline_exceeded` — 总投递时限
- `endpoint_circuit_open` — 熔断隔离期间
- `endpoint_queue_overflow` — 端点队列满
- `subscription_removed` — 退订

## HTTP 线路协议

`POST` 到端点 URL，`Content-Type: application/json`，body 为事件原始 JSON。

| Header | 含义 |
|---|---|
| `X-Webhook-Event-Id` | 上游事件 ID |
| `X-Webhook-Delivery-Id` | **稳定投递 ID**，下游幂等键 |
| `X-Webhook-Sequence` | 全局单调序号（同端点内即投递顺序） |
| `X-Webhook-Event-Type` | 事件类型 |
| `X-Webhook-Timestamp` | Unix 秒签名时间戳 |
| `X-Webhook-Signature` | `hex(HMAC-SHA256(secret, timestamp + "\n" + body))` |
| `Authorization` | 配置了 `AuthToken` 时携带 `Bearer <token>` |

接收方校验示例（推荐容忍窗口 5 分钟，拒绝伪造与过期请求）：

```go
err := signer.Verify(secret, body, r.Header.Get("X-Webhook-Timestamp"),
    r.Header.Get("X-Webhook-Signature"),
    func() int64 { return time.Now().Unix() }, 300)
```

## 配置项（`Config`）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Clock` | 系统时钟 | 注入 `FakeClock` 可做确定性测试 |
| `StorePath` | `""`（内存） | 持久化 JSON 文件路径 |
| `HTTPTimeout` | `10s` | 单次尝试超时 |
| `MaxAttempts` | `8` | 最大尝试次数（含首次） |
| `TotalDeadline` | `24h` | 单条投递总时限 |
| `Backoff.Base` | `500ms` | 首次重试退避基准 |
| `Backoff.Max` | `60s` | 退避上限 |
| `Backoff.Multiplier` | `2` | 指数倍率 |
| `Backoff.Jitter` | `0.2` | 对称抖动比例（0 关闭） |
| `Circuit.FailureThreshold` | `5` | 连续可重试失败多少次断开 |
| `Circuit.OpenTTL` | `30s` | 隔离时长，过后进入 half-open 探测 |
| `Circuit.HalfOpenMax` | `1` | half-open 期间放行的探测数 |
| `QueueSize` | `1024` | 每端点待投队列上限，溢出进死信 |
| `HistoryRetention` | `10000` | 保留的终态（成功/死信）投递数，同时界定重放与去重历史边界 |
| `Sender` | 默认 HTTP client | 可替换的 `Sender`（测试用） |
| `TimestampTolerance` | `5m` | 文档化的接收方校验窗口（发送端不强制） |
| `Log` | stderr | `Logger`；**密钥与 Token 永不入日志**，日志只含 ID/状态码/时长，密钥可用 `Redact` 脱敏 |

## 故障定位

```go
d.Delivery(id)                      // 单条投递状态 + 每次尝试的状态码/错误/耗时
d.DeliveryByEvent(eventID)          // 一个事件在所有端点上的投递
d.DeadLetters()                     // 全部死信（含原因码）
webhook.Replay(d, id)               // 在保留窗口内重放某条死信（ID 不变）
d.Circuit(endpointID).State()       // 查看端点熔断状态
```

## 测试

全部测试使用本地回环（`httptest`）、可控假发送器与 `FakeClock`，无需外部服务，
结果可重复；`go test -race ./...` 全绿。覆盖：

- 订阅匹配 / 通配符 / 乱序注册 / 并发注册与匹配 / 退订
- 扇出、每端点严格有序、慢端点不拖累快端点、背压不阻塞生产方
- 去重（并发同事件折叠为一条、跨重启稳定 ID）
- 指数退避重试、网络错误重试、4xx 终态、重试上限死信
- 熔断打开/隔离死信/half-open 探测恢复
- HMAC 签名：正文篡改、错误密钥、过期时间戳、真实回环端到端校验
- 落盘恢复：pending 续投、inflight 回滚、已确认不重投、损坏文件拒绝启动
- 死信重放边界（历史截断后 `ErrNotFound`）、关闭不留半完成状态、可观测轨迹

```bash
go test ./...         # 功能
go test -race ./...   # 含竞态检测
```
