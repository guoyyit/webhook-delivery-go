# webhook-delivery-go

事件通知与 Webhook 投递服务：上游业务事件按订阅关系扇出到多个下游端点，支持
失败重试、端点级熔断降级、死信重放、签名鉴权、持久化与重启恢复。零第三方依赖，
仅使用 Go 标准库。

## 目录

- [架构与投递语义](#架构与投递语义)
- [幂等与去重](#幂等与去重)
- [重试与退避](#重试与退避)
- [熔断、降级与死信](#熔断降级与死信)
- [重放](#重放)
- [签名与鉴权](#签名与鉴权)
- [持久化与重启恢复](#持久化与重启恢复)
- [HTTP API](#http-api)
- [配置项](#配置项)
- [测试](#测试)

## 架构与投递语义

```
上游 ──POST /v1/events──▶ Engine ──匹配订阅──▶ 每端点队列(内存信号 + 磁盘待办)
                                                  │  每端点一个 worker goroutine
                                                  ▼
                                          签名 HTTP 投递 ──▶ 下游端点
```

- **扇出**：一个事件命中多个订阅，每个订阅生成一条独立的投递（Delivery），
  投递之间互不影响。
- **按类型匹配**：订阅声明 `event_types`；为空表示接收所有类型。匹配结果按
  订阅 ID 排序，与注册顺序、并发注册无关；退订后不再产生新投递。
- **每端点严格有序**：同一端点的事件按产生顺序（事件单调序号）逐条送达，前一条
  未终态前不投递后一条。不同端点使用各自独立的 worker，慢端点只阻塞自己，不
  拖慢快端点，也不阻塞事件产生方（`Ingest` 只写盘 + 非阻塞信号，立即返回）。
- **投递状态机**：

  | 状态 | 含义 |
  | --- | --- |
  | `pending` | 待投递或等待重试窗口 |
  | `in_progress` | 正在投递中（崩溃恢复时回退为 `pending`） |
  | `succeeded` | 投递成功（2xx），终态 |
  | `failed` | 不可重试失败（如 4xx）或已退订，终态，不重放 |
  | `dead_lettered` | 熔断隔离 / 重试耗尽 / 超总时限，终态，可重放 |

## 幂等与去重

- 事件由上游携带稳定 `id`（或由服务生成）。重复提交同一 `id` 的事件直接返回
  已存事件，**不会**产生重复投递。
- 每条投递的 ID 是 `del_<sha256(event_id + "|" + subscription_id)>`，由事件与
  订阅确定性计算，重试、重启后都不变。请求同时携带：
  - `X-Event-Id`：事件 ID；
  - `X-Delivery-Id` / `X-Idempotency-Key`：投递 ID，下游可用作幂等键。
- 重启后只继续推进未完成的投递；`succeeded` 的投递不会重复发送。下游据此可
  自行判重：同一 `X-Idempotency-Key` 的请求只处理一次。

## 重试与退避

- 可重试失败：网络错误、超时、HTTP `408/429/5xx`；不可重试失败：其余 4xx
  （直接进入 `failed` 终态）。
- 退避 = `InitialInterval * Multiplier^(attempt-1)`，受 `MaxInterval` 封顶，
  并叠加 `Jitter` 比例的随机抖动（默认 ±20%），避免重试风暴。
- 两道上限：
  - `MaxRetries`：最大重试次数，耗尽后进入死信（`retries_exhausted`）；
  - `MaxElapsedTime`：自首次尝试起的总投递时限，超时后进入死信
    （`max_elapsed_time_exceeded`），即使次数未用完。
- 退避等待可被关闭/重启安全打断；`NextRetryAt` 落盘，重启后仍按窗口等待，
  不留下半完成状态。

## 熔断、降级与死信

每个端点一个独立熔断器（状态：`closed` → `open` → `half_open`）：

- 连续失败达到 `FailureThreshold` 后熔断打开；
- 打开期间该端点的新投递**不再发起 HTTP 请求**，直接进入死信
  （`circuit_open`），不会无声丢弃；
- 经过 `OpenInterval` 后进入半开，放行**一个**探测请求：成功则关闭熔断、
  恢复投递；失败则重新打开并重新计时。

死信原因（`dead_letter_reason`）：

| 原因 | 触发条件 | 可重放 |
| --- | --- | --- |
| `circuit_open` | 熔断隔离期间的投递 | ✅ |
| `retries_exhausted` | 重试次数耗尽 | ✅ |
| `max_elapsed_time_exceeded` | 超过总投递时限 | ✅ |
| `unsubscribed` / `subscription_gone` | 退订/订阅不存在 | ❌（终态 `failed`） |

## 重放

`POST /v1/replay` 重放死信，时间范围为**左闭右开**区间 `[from, to)`
（按事件产生时间）：只有事件时间落在区间内的死信投递会被重置为 `pending`
并重新投递，区间外的不受影响。重放前会清空失败计数与错误原因。

## 签名与鉴权

每个请求携带：

| Header | 内容 |
| --- | --- |
| `X-Webhook-Timestamp` | 签名时刻的 Unix 秒 |
| `X-Webhook-Signature` | `v1=<hmac_sha256(secret, "<timestamp>.<body>")>` |
| `X-Event-Id` / `X-Delivery-Id` / `X-Idempotency-Key` | 身份与幂等标识 |

下游校验方式（与 Stripe 风格一致）：

1. 用相同密钥对 `"<timestamp>.<原始 body>"` 计算 HMAC-SHA256，常量时间比较；
2. 检查时间戳与当前时间偏差不超过容差（默认 5 分钟），拒绝重放/伪造请求。

密钥在创建订阅时由服务生成（也可上游指定），仅通过 API 响应返回一次；
日志只记录订阅 ID 与端点 host，**不记录密钥**。

## 持久化与重启恢复

数据目录下三个文件：

| 文件 | 格式 | 写入方式 |
| --- | --- | --- |
| `events.log` | JSONL 追加日志 | 每条 append + fsync |
| `subscriptions.json` | JSON 快照 | 临时文件 + fsync + 原子 rename |
| `deliveries.json` | JSON 快照 | 同上 |

崩溃安全性：

- 追加日志遇到崩溃可能在文件尾留下半行，加载时忽略最后一条不完整记录；
- 快照通过原子 rename 切换，任何时刻要么是旧完整状态、要么是新完整状态；
- 加载时：`in_progress` 投递回退为 `pending`；补建崩溃在"事件已落盘、投递
  快照未写"窗口内缺失的投递（仅对创建时间不晚于事件时间的订阅生效）；
- 未完成投递按 `NextRetryAt` 继续推进，已成功投递不重复发送。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| POST | `/v1/subscriptions` | 创建订阅（`url`、`event_types`、可选 `secret`） |
| GET | `/v1/subscriptions` | 列出订阅 |
| DELETE | `/v1/subscriptions/{id}` | 退订（未完成投递置为终态） |
| POST | `/v1/events` | 摄入事件（`id`、`type`、`payload`），返回生成的投递 |
| GET | `/v1/deliveries` | 投递列表，支持 `status`/`endpoint`/`subscription_id`/`event_id` 过滤 |
| POST | `/v1/replay` | 重放死信，body `{"from":"...","to":"..."}`（RFC3339） |

调用示例：

```bash
# 创建订阅（event_types 为空表示接收所有类型）
curl -X POST http://localhost:8080/v1/subscriptions \
  -d '{"url":"https://example.com/hook","event_types":["order.created"]}'

# 摄入事件（id 用于幂等，重复提交不会产生重复投递）
curl -X POST http://localhost:8080/v1/events \
  -d '{"id":"evt_001","type":"order.created","payload":{"order_id":42}}'

# 查询投递状态
curl 'http://localhost:8080/v1/deliveries?event_id=evt_001'

# 重放某时间范围内的死信（[from, to)）
curl -X POST http://localhost:8080/v1/replay \
  -d '{"from":"2026-10-01T00:00:00Z","to":"2026-10-02T00:00:00Z"}'
```

## 配置项

命令行参数（`cmd/webhookd`）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-addr` | `:8080` | HTTP 监听地址（也可用环境变量 `WEBHOOKD_ADDR`） |
| `-store-dir` | `./data` | 持久化目录 |
| `-request-timeout` | `10s` | 单次投递尝试超时 |

引擎配置（`engine.Config`）：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `RequestTimeout` | `10s` | 单次 HTTP 尝试超时 |
| `Retry.InitialInterval` | `1s` | 首次重试退避 |
| `Retry.MaxInterval` | `1m` | 退避上限 |
| `Retry.Multiplier` | `2` | 退避乘数 |
| `Retry.MaxRetries` | `5` | 最大重试次数 |
| `Retry.MaxElapsedTime` | `5m` | 自首次尝试起的总投递时限 |
| `Retry.Jitter` | `0.2` | 抖动比例 |
| `Breaker.FailureThreshold` | `5` | 熔断连续失败阈值 |
| `Breaker.OpenInterval` | `30s` | 熔断打开时长，后半开探测 |

## 测试

```bash
go test ./... -race
```

测试使用本地回环 HTTP 服务器模拟下游端点，用可控时钟（`clock.FakeClock`，
可手动推进时间）驱动退避与熔断，保证可重复。覆盖：

- 订阅匹配：类型过滤、空类型通配、顺序无关、并发注册/退订（race）；
- 签名验签：正确/错误密钥、篡改 body、过期与未来时间戳；
- 重试退避：增长与封顶、抖动范围、次数上限、状态码分类；
- 熔断：打开、半开探测、探测失败重开、成功重置；
- 持久化：追加与快照、重开恢复、文件尾半行崩溃恢复、损坏快照报错；
- 引擎集成：扇出过滤、每端点有序、慢端点背压不阻塞生产方、重试成功、
  不可重试终态、总时限死信、熔断隔离与探测恢复、重放边界、摄入幂等、
  重启恢复不重复投递、退订即停、签名头端到端校验。
