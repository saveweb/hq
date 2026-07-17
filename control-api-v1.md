# SavewebHQ Control API v1

> 状态：实现前冻结稿。机器可读契约以
> [`api/openapi-v1.yaml`](./api/openapi-v1.yaml) 为准。

本文只补充 agent 上线、heartbeat 和 shard access token 的语义。Queue
请求、状态机和错误处理见 [`api-v1.md`](./api-v1.md)。

## 1. Agent 注册

Shard 和 worker 首次启动时生成并持久化稳定的 `agent_id`。两者都调用：

```http
PUT /api/v1/agents/{agent_id}
Authorization: Bearer <machine-token>
```

该操作是幂等 upsert。`kind` 创建后不可修改；machine token 所属用户必须
具有对应 role。Worker 的 endpoint、endpoint version 和 pin 必须都是
`null`。Shard 必须登记一个公网 HTTP(S) endpoint 和正整数 endpoint
version；只有 HTTPS 才允许携带 SPKI pin。

Endpoint 或 pin 改变时，owner 必须先递增 endpoint version。相同 version
对应不同 endpoint identity 返回 `409 invalid_request`，旧 version 返回
`409 stale_endpoint_version`。`details` 会包含当前 endpoint version。

Tracker 对新 shard endpoint 同步执行 challenge：

```http
POST /api/v1/shard/endpoint-challenge
```

语法或 SSRF 策略不合法时拒绝注册；公网暂时不可达时保存 agent，但将
`endpoint_status` 设为 `unreachable`，不用于 assignment。后续注册或
heartbeat 会重新检查。Challenge 通过后，HTTPS endpoint 为 `healthy`，
显式允许的 HTTP endpoint 为 `insecure`。

## 2. Agent Heartbeat

```http
POST /api/v1/agents/{agent_id}/heartbeat
Authorization: Bearer <machine-token>
```

Heartbeat 更新版本、能力和 `last_heartbeat_at`。对 shard agent，它还以
PostgreSQL 条件更新续租仍属于该 agent 的 owner lease，并返回：

- tracker server time 和下一次 heartbeat 间隔；
- 当前 owner assignments；
- tracker 当前和仍可能验证有效 token 的 Ed25519 public keys。

Shard 必须使用 server time 判断与 tracker 的时钟偏差，并在本地记录每个
assignment 的 `owner_lease_expires_at`。本地时间达到 owner lease 后，shard
立即停止 claim、complete、fail 和 extend-lease；它不能因为 tracker
暂时不可达而自行延长 lease。

Worker heartbeat 不返回 owner assignment。Worker 的 job session 使用独立
的 session heartbeat，不由 agent heartbeat 隐式续租。

第一版默认值：

| 参数 | 默认值 |
| --- | ---: |
| agent heartbeat interval | 30 秒 |
| owner lease | 120 秒 |
| session heartbeat interval | 30 秒 |
| session lease | 120 秒 |
| shard access token TTL | 600 秒 |
| clock skew allowance | 30 秒 |

这些值是 tracker 配置，不由 client request 覆盖。Job lease 仍由 Queue API
显式请求，默认 300 秒、范围 30–3600 秒。

## 3. Shard Access Token

Tracker 的 assignment 返回 compact JWS/JWT。Header 固定为：

```json
{"alg":"EdDSA","typ":"JWT","kid":"tracker-key-1"}
```

Payload 必须包含且只解释以下安全相关 claims：

| claim | 含义 |
| --- | --- |
| `iss` | tracker 配置的稳定 issuer |
| `aud` | 固定为 `saveweb-shard` |
| `sub` | 与 `worker_agent_id` 相同 |
| `jti` | 随机 token ID，只用于审计 |
| `iat`、`nbf`、`exp` | int64 UNIX 秒 |
| `worker_agent_id` | worker agent |
| `session_id` | vnode session |
| `session_expires_at` | 签发时已知的 session lease |
| `project_id`、`shard_id` | 唯一 queue route |
| `generation` | owner epoch |
| `owner_agent_id` | token 预期到达的 shard agent |

`exp` 不超过 `iat + 600`。Token 可以比某次 owner lease 活得更久，因为
shard 对每个请求独立检查本地 owner lease；lease 到期后 token 本身不能让
旧 owner 继续服务。Session heartbeat 延长后，worker 必须刷新 assignment
才能获得带有新 `session_expires_at` 的 token。

Shard 的验证顺序固定为：

1. 限制 Authorization header 和 compact token 总长度；
2. 只接受两个点分隔符、`alg=EdDSA`、`typ=JWT` 和已知 `kid`；
3. 验证 Ed25519 signature，再解析并限制 JSON claims；
4. 验证 issuer、audience、`nbf`、`exp` 和最多 30 秒时钟偏差；
5. 验证 worker/session/project/shard/generation/owner scope；
6. 验证 `session_expires_at`；
7. 验证 shard 本地 owner lease。

任何一步失败都不得读取大 request body。普通 token 错误返回
`invalid_access_token`；session lease 过期返回 `session_expired`；本地 owner
lease 过期返回 `owner_lease_expired`；generation 或 owner 不匹配返回
`stale_generation`。日志只能记录 `jti`、`kid` 和脱敏后的 agent/route，不能
记录 compact token。

## 4. Key Rotation

Tracker 至少保留一个 active signing key 和所有仍可能对应未过期 token 的
retiring public keys。Heartbeat 返回 `kid`、raw Ed25519 public key 的
base64url 编码、`not_before` 和 `not_after`。Shard 按 `kid` 原子替换本地
key set，并保留当前 assignment 所需的最近一次有效集合；不得在请求热路径
回调 tracker 获取未知 key。

轮换流程为：先通过 heartbeat 分发新 public key，再开始用新 `kid` 签发，
最后等待旧 token 最大 TTL 和 clock skew 都结束后删除旧 key。
