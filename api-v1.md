# SavewebHQ Queue API v1

> 状态：实现前冻结稿
>
> 本文只定义 tracker、shard queue RPC 和 worker SDK 共同依赖的最小协议。管理页面、Project 管理、checkpoint 上传和 artifact 上传使用独立 API。

## 1. 性能目标与传输方式

### 1.1 目标口径

第一版的性能目标是：在足够多的公网 shard endpoint 分摊 SQLite、CPU 和网络压力时，系统整体尝试承载 **100,000 completed jobs/s** 的 queue metadata 流量。每个 job 至少经过一次 claim 和一次 complete/fail，但这些操作必须批量发送；不把目标定义为单台机器处理 100,000 个独立 HTTP request/s。

默认 batch 为 64，达到 100,000 completed jobs/s 时，大约产生：

```text
100000 / 64 × 2 ≈ 3125 HTTP requests/s
```

这里的两次请求是一次 claim batch 和一次 complete/fail batch，并且分散到多个 shard endpoint。Lease extension 会产生额外流量，但只用于执行时间较长的 job。

### 1.2 1 Gbps Tracker 的边界

Worker 直接访问 shard，claim/complete/fail 不经过 tracker。因此 tracker 的 1 Gbps 上下行不再承载 100,000 completed jobs/s 的 queue body，只承载：

- user/session/agent heartbeat；
- shard assignment 和 endpoint 查询；
- 短期 shard access token 签发；
- Project/shard 控制面与管理页面；
- receiver、artifact gateway 和 checkpoint 控制流量中的 HQ 部分。

Assignment 和 access token 可以缓存到过期前，不需要每个 batch 都访问 tracker。100,000 completed jobs/s 是否成立，主要由 shard 总数、每个 SQLite shard 的批量事务吞吐、志愿者上下行和平均 JobSpec 大小决定。

WARC、页面正文、截图、receiver 大 batch 和 checkpoint 不得放进 queue RPC JSON。若这些内容仍通过与 tracker 同机的 HQ gateway，它们会单独竞争 1 Gbps 带宽，必须独立做容量规划，不能算入 queue QPS 结论。

### 1.3 网络拓扑

- shard owner 必须提供一个可从公网访问的 HTTP(S) endpoint URI；它背后是 daemon 直接监听、四层端口转发，还是 Caddy/cloudflared 反向代理，对 Queue API 都不可见；
- worker SDK 使用 keep-alive 连接该 shard endpoint；worker 自身不监听公网端口，也不提供入站服务；
- shard daemon 仍主动访问 tracker 做上线、heartbeat、owner lease 和 checkpoint 控制，不需要 tracker 主动连接内网地址；
- worker 先从 tracker 获取 assignment；tracker 返回 endpoint、generation、可选 TLS pin 和短期 shard access token；
- queue 热路径不访问 tracker 或 PostgreSQL；generation、attempt、job lease 和 access token 都由 shard 本地验证；
- shard 对 batch body 设置上限和有界队列，过载时尽早返回 `backpressure`，不得无限缓存；
- 四层端口转发保持 TCP 透传，使 TLS 在 worker 与 shard daemon 之间端到端终止；七层反向代理可以终止 TLS，但必须遵守本文的认证、body、redirect 和 cache 规则。

公开 API 使用 JSON，是为了 Go/Python SDK 和调试简单。只有压测证明 JSON CPU 是瓶颈时，才增加 binary encoding，不在第一版同时维护两种语义。

## 2. Authentication

Worker 和 shard 访问 tracker 时都使用所属 user 的共享 machine token：

```http
Authorization: Bearer <machine-token>
X-Saveweb-Agent-ID: <stable-agent-id>
```

`X-Saveweb-Agent-ID` 由 daemon 首次启动时随机生成并持久化，不是 secret。Tracker 由 machine token 找到 user，再验证 agent ID 是否属于该 user、agent kind、user status、roles 和 Project 权限。

Worker 直连 shard 时不得发送 machine token。Tracker 为 assignment 签发短期、可重复使用的 shard access token：

```http
Authorization: Bearer <shard-access-token>
```

Token 使用 compact JWS/JWT，header 固定 `alg=EdDSA` 并携带 `kid`，不接受 client 指定其他算法。Payload 至少包含 `worker_agent_id`、`session_id`、`project_id`、`shard_id`、`generation`、`owner_agent_id`、`iat` 和 `exp`。Shard 从 tracker heartbeat 获得按 `kid` 索引的 Ed25519 public keys，并离线验证 signature、scope、generation 和有效期。第一版 token 默认有效 10 分钟，SDK 在过期前自动刷新；它只授权 queue metadata API，与 checkpoint/R2 上传时长无关。

Tracker API 只允许 TLS。Shard queue API 推荐 HTTPS；仅当 owner 和 worker 都显式允许 insecure endpoint 时才接受 HTTP，因为它会暴露 access token、JobSpec 和 outcome，不可作为生产默认值。Machine token、shard access token、local admin token 和 GitHub OAuth token 不能互换。

每个请求可以携带 `X-Request-ID`。它只用于追踪，不使 claim 自动具备 exactly-once 语义。

### 2.1 Shard Endpoint 与 TLS

Shard 不区分“直连 endpoint”和“反代 endpoint”，两者都是同一个 HTTP(S) URI、提供同一套 Queue API。Worker 根据 URI scheme 和可选 `tls_spki_sha256` 推导校验方式：

| URI 与 pin | 验证方式 | 常见部署 |
| --- | --- | --- |
| `https://...` + pin | 校验 SPKI SHA-256；不依赖 system CA 或 hostname | daemon 直出、自签 certificate、四层端口转发 |
| `https://...` + `null` | 正常校验 public CA、hostname 和 certificate 有效期 | Caddy、cloudflared 或其他 HTTPS 反向代理 |
| `http://...` + `null` | 无传输安全；双方必须显式允许 | 仅开发或兼容，不建议生产使用 |
| `http://...` + pin | 非法配置 | 不允许 |

HTTPS 带 pin 时：

1. shard daemon 首次启动时生成 ECDSA P-256 TLS private key，写入仅当前 OS user 可读的 `0600` secret file；后续启动复用同一 key；
2. daemon 用该 key 生成自签名 certificate，并计算 certificate `SubjectPublicKeyInfo` 的 SHA-256；
3. owner 向 tracker 登记 endpoint、`tls_spki_sha256` 和单调递增的 `endpoint_version`；
4. worker 从 tracker HTTPS response 取得 endpoint 和 pin，必须验证 peer SPKI SHA-256 完全匹配，并检查 certificate 有效期和 TLS handshake；
5. certificate 到期可以用相同 private key 自动重签，pin 不变。Private key 丢失或主动轮换时必须更新 pin 并递增 `endpoint_version`。

HTTPS 不带 pin 时，SDK 使用系统信任库做标准 HTTPS 验证；不 pin Cloudflare 等 CDN 的边缘 certificate，因为其 certificate 和 key 可以正常轮换。若 cloudflared 的公网入口为 HTTPS、到本机 shard 的最后一跳为 HTTP，tracker 中仍登记公网 HTTPS URL。

TLS 最低版本为 1.2，优先使用 TLS 1.3。SDK 的 custom verification 只能在成功匹配 pin 后接受 self-signed certificate，绝不能变成“关闭证书校验”。HTTP 没有机密性或完整性，会暴露 bearer token 和 queue 数据，必须在 shard 配置和 worker SDK 中分别显式启用。

Endpoint URI 必须是 `http` 或 `https`、不得包含 userinfo、query 或 fragment；允许包含固定 base path。Tracker 的 validator 只接受解析到 global-unicast 的公网地址，拒绝 loopback、private、link-local、multicast 和云 metadata 地址。Health checker 每次连接都重新验证解析结果，避免把 endpoint 注册功能变成访问 tracker 内网的 SSRF 工具。

Owner 使用四层端口转发时通常登记 shard daemon 的 pin。HTTPS reverse proxy 终止 TLS 时通常不登记 pin，使用 public CA；代理必须原样传递 `Authorization`、`Content-Type`、`X-Request-ID` 和 request/response body，不得把 queue path 重定向到其他 origin。

Tracker 对所有 endpoint 执行 challenge：HTTPS 带 pin 时校验 pin，HTTPS 不带 pin 时执行正常 CA/hostname 校验，HTTP endpoint 标记为 insecure。Health check 固定为：

```http
POST /api/v1/shard/endpoint-challenge
```

```json
{"agent_id":"agent-shard-01","challenge":"tracker-random-value"}
```

Response 必须原样返回 `agent_id` 和 `challenge`。Challenge 高熵、短期、单次使用；该 endpoint 不接受其他管理操作。

### 2.2 Reverse Proxy 与 Cache

所有 queue 和 endpoint challenge API 都使用 `POST`。Worker 的 queue request 必须发送：

```http
Cache-Control: no-store, no-cache, max-age=0
Pragma: no-cache
```

Shard 的 response 必须发送：

```http
Cache-Control: no-store, no-cache, max-age=0
Cloudflare-CDN-Cache-Control: no-store
CDN-Cache-Control: no-store
Expires: 0
```

使用 Cloudflare 的 owner 还必须为 endpoint base path 下的 `/api/v1/queue/*` 和 `/api/v1/shard/endpoint-challenge` 配置 **Bypass Cache**；不能假设 origin header 一定优先于账号 Cache Rules。Worker SDK 对 `CF-Cache-Status` 采用 allowlist：只接受 `DYNAMIC`、`BYPASS` 或 header 缺失，其他值均作为 `cache_misconfigured` 停止本次 route 并刷新 assignment。

Queue client 禁止跟随 redirect，以免把 `Authorization` 发送到未登记的 origin。代理也不得缓存、合并、重试或变换 queue request/response body。Caddy、cloudflared 和其他代理的 request size、timeout、buffering、rate limit 必须覆盖 Queue API 的 batch 上限，并纳入单 shard 压测。

参考：[Cloudflare default cache behavior](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/)、[Cloudflare Cache-Control](https://developers.cloudflare.com/cache/concepts/cache-control/)、[Cloudflare CDN-Cache-Control](https://developers.cloudflare.com/cache/concepts/cdn-cache-control/)。

## 3. JobSpec 与 Source 格式

### 3.1 `JobSpecV1`

```json
{
  "id": "j1_4e2f...",
  "url": "https://example.org/",
  "type": "seed",
  "via": null,
  "hops": 0,
  "attr": {}
}
```

字段规则：

| 字段 | 必需 | 规则 |
| --- | --- | --- |
| `id` | 是 | 1–128 byte ASCII，匹配 `[A-Za-z0-9._:-]+`；由 producer 固定，queue 不修改 |
| `url` | 是 | 1–8192 byte UTF-8 string；第一版不做 URL canonicalization |
| `type` | 否 | `seed` 或 `asset`，默认 `seed` |
| `via` | 否 | 发现该 job 的来源 URL；默认 `null`，不参与默认 ID |
| `hops` | 否 | 非负整数，默认 `0`，不参与默认 ID |
| `attr` | 否 | JSON object，默认 `{}`；规范化后最大 8 KiB |

单条规范化 JobSpec 编码不得超过 32 KiB。未知的顶层字段返回 `invalid_job`；扩展字段放进 `attr`。

`url` 是第一版明确的目标字段。Queue 不猜测 URL 等价关系，fragment、host 大小写、默认端口和 percent encoding 都按 producer 提供的原字符串处理。

### 3.2 默认 Job ID

Go/Python SDK 和 source 工具提供同名 helper，默认算法固定为：

```text
id = "j1_" + lowercase_hex(
  SHA256(UTF8(type) || 0x00 || UTF8(url))
)
```

`type` 使用填充默认值后的值。`via`、`hops` 和 `attr` 不参与默认 ID，因此同一类型和 URL 默认被视为同一 job。若相同 URL 因请求 header、cookie 或其他 `attr` 必须执行多次，producer 必须显式提供不同 ID；queue 不自行推断。

同一 shard 再次写入相同 ID 时：

- `type`、`url` 和规范化后的 `attr` 都相同：视为幂等重复，保留第一次写入的 `via` 和 `hops`；
- 任一身份输入不同：返回 `identity_conflict`，不能静默覆盖旧 job；
- source loader 遇到 conflict 时进入 `load_failed` 并记录行号；同 shard derived job conflict 只拒绝对应 completion item，父 job 保持 `wip`。

数据库中的完整身份仍是 `(project_id, shard_id, id)`，但保留同一个 `id` 可以支持 receiver 合并后跨 shard 去重。

### 3.3 S3 Source

第一版 source format 固定为 `jobs-jsonl-zstd-v1`：

- S3 object 是 Zstandard 压缩的 UTF-8 JSON Lines；
- 每个非空行是一个完整 `JobSpecV1` object，禁止 JSON array 和 comment；
- 每行必须显式包含 `id`。Source 打包工具可以使用 3.2 节算法从 URL 生成；
- loader 验证 `source_etag`、逐行限制展开大小并批量 enqueue；
- 第一版不实现 zstd seek index。恢复载入时从对象开头重新解压，依靠 job ID 幂等跳过已经导入的记录；
- receiver 输出使用同一 record encoding，使 merge/dedup/split 工具可以直接生成下一阶段 source。

为保留简单的 `jobs.txt` 工作流，工具提供：

```text
saveweb-source pack jobs.txt jobs.jobs.jsonl.zst
```

输入的每个非空文本行作为一个 `seed` URL，工具生成默认 ID。手动 split 可以发生在 pack 前，也可以由同一工具按目标记录数执行。

## 4. Session API

### 4.1 创建 Session

```http
POST /api/v1/worker/sessions
```

```json
{
  "project_id": "project-a",
  "attrs": {"sdk": "go", "version": "0.1.0"}
}
```

```json
{
  "session_id": "vs_...",
  "lease_expires_at": 1780000000,
  "heartbeat_after_seconds": 30
}
```

一个 session 只属于一个 worker agent 和一个 Project。

### 4.2 Session Heartbeat

```http
POST /api/v1/worker/sessions/{session_id}/heartbeat
```

返回新的 `lease_expires_at`。Session heartbeat 不是每个 job 的 lease extension。

### 4.3 获取 Shard Assignment

该请求发送给 tracker：

```http
POST /api/v1/worker/assignments
```

```json
{
  "session_id": "vs_...",
  "accept_types": ["seed", "asset"]
}
```

Tracker 选择 active shard，并返回 worker 直连所需的 route bundle：

```json
{
  "assignment": {
    "project_id": "project-a",
    "shard_id": "shard-001",
    "generation": 7,
    "owner_agent_id": "agent-shard-01",
    "endpoint": "https://203.0.113.10:18443",
    "endpoint_version": 3,
    "tls_spki_sha256": "base64url-sha256-spki",
    "access_token": "signed-token",
    "access_token_expires_at": 1780000600
  },
  "retry_after_ms": 0
}
```

没有可用 shard 时返回 HTTP 200、`assignment: null` 和非零 `retry_after_ms`。Assignment 可以在 access token、owner lease、generation 和 endpoint version 都未变化时复用；SDK 不得为每个 queue batch 都重新请求 tracker。

`tls_spki_sha256` 是可选字段：HTTPS 自签/固定 key endpoint 返回 pin；public CA HTTPS 和 HTTP endpoint 返回 `null`。部署方式不定义不同的 Queue API。

## 5. Queue API

所有 queue batch 的最大 body 为 1 MiB，默认 item 数为 64，最大 256。一个 batch 只能指向一个 session 和一个 shard generation；SDK 必须按 shard 分组。

本节 endpoint 位于 assignment 返回的 shard `endpoint`，不是 tracker。所有请求使用对应 shard access token。

所有时间戳为 int64 UNIX 秒，所有 TTL 为整数秒。服务端限制 TTL 的最小值和最大值，不信任 client 本地时间。

### 5.1 Claim

```http
POST /api/v1/queue/claim
```

```json
{
  "project_id": "project-a",
  "shard_id": "shard-001",
  "generation": 7,
  "session_id": "vs_...",
  "max_jobs": 64,
  "lease_seconds": 300,
  "accept_types": ["seed", "asset"]
}
```

Worker 先使用 4.3 节取得 shard assignment，再直接向该 shard claim。Shard 必须确认 request route 与 access token scope、自身 owner lease 和当前 generation 全部匹配。一次 response 只来自该 shard：

```json
{
  "project_id": "project-a",
  "shard_id": "shard-001",
  "generation": 7,
  "jobs": [
    {
      "id": "j1_...",
      "attempt_id": "at_...",
      "lease_expires_at": 1780000300,
      "url": "https://example.org/",
      "type": "seed",
      "via": null,
      "hops": 0,
      "attr": {}
    }
  ],
  "retry_after_ms": 0
}
```

没有可用 job 时仍返回 HTTP 200，`jobs` 为空并设置 `retry_after_ms`。

Claim 在 shard 内使用一个 SQLite transaction 原子认领整个返回 batch。若 shard 已提交但 response 丢失，这批 job 会保持 `wip` 直到 lease 到期；SDK 不得把网络重试伪装成同一次 exactly-once claim。

### 5.2 Complete

```http
POST /api/v1/queue/complete
```

```json
{
  "project_id": "project-a",
  "shard_id": "shard-001",
  "generation": 7,
  "session_id": "vs_...",
  "items": [
    {
      "job_id": "j1_...",
      "attempt_id": "at_...",
      "outcome": {
        "kind": "success",
        "code": 200,
        "uri": "artifact://...",
        "meta": {}
      },
      "discovered_jobs": []
    }
  ]
}
```

`outcome.kind` 第一版允许：

- `success`：成功抓取；
- `http_error`：HTTP 4xx/5xx 等已经得到确定响应的业务结果；
- `skipped`：按 Project 规则明确跳过，但仍视为已处理。

`code`、`uri` 可以为空，`meta` 最大 8 KiB。WARC 或页面内容本身不能内嵌，只能填写 artifact URI。网络超时、DNS 失败等需要重试的执行错误使用 fail API。

整个 batch 使用一个外层 SQLite transaction；每个 item 先预校验或使用 savepoint 隔离，该 item 的 outcome、父 job `done` 和 `discovered_jobs` 必须保持原子。合法 item 可以与被拒绝 item 同批处理，response 按请求顺序逐项返回；若外层 commit 失败，则整个 batch 返回 batch-level error，不能报告任何 item 已应用：

```json
{
  "results": [
    {
      "job_id": "j1_...",
      "attempt_id": "at_...",
      "status": "applied",
      "job_status": "done"
    }
  ]
}
```

`status` 为 `applied`、`already_applied` 或 `rejected`。若相同 attempt 的 complete 已经成功但 response 丢失，重试必须返回 `already_applied`，SDK 将其视为成功。

### 5.3 Fail

```http
POST /api/v1/queue/fail
```

```json
{
  "project_id": "project-a",
  "shard_id": "shard-001",
  "generation": 7,
  "session_id": "vs_...",
  "items": [
    {
      "job_id": "j1_...",
      "attempt_id": "at_...",
      "retryable": true,
      "error": {
        "code": "network_timeout",
        "message": "connect timeout",
        "details": {}
      }
    }
  ]
}
```

- `retryable=true`：原子进入 reset 流程并递增 `reset_count`；未超限回到 `todo`，超限进入 `reset_exhausted`；
- `retryable=false`：进入 `failed`；
- 基础设施导致的 shard recovery 不通过该 API，不增加 `reset_count`。

Response 使用 complete 相同的逐项结构，并在 `job_status` 返回 `todo`、`failed` 或 `reset_exhausted`。

### 5.4 Extend Lease

```http
POST /api/v1/queue/extend-lease
```

```json
{
  "project_id": "project-a",
  "shard_id": "shard-001",
  "generation": 7,
  "session_id": "vs_...",
  "extend_seconds": 300,
  "items": [
    {"job_id": "j1_...", "attempt_id": "at_..."}
  ]
}
```

服务端以自己的当前时间计算新 lease，不接受 client 提供绝对时间。

## 6. Error Model

Batch-level error 使用以下统一结构：

```json
{
  "error": {
    "code": "stale_generation",
    "message": "shard generation changed",
    "retryable": false,
    "retry_after_ms": 0,
    "details": {"current_generation": 8}
  }
}
```

Item-level rejection 把同一个 `error` object 放入对应 result。`message` 只用于诊断，SDK 只能根据稳定的 `code` 分支。

| HTTP | code | 含义 | SDK 行为 |
| ---: | --- | --- | --- |
| 400 | `invalid_request` / `invalid_job` | schema、大小或字段非法 | 不重试，报告调用错误 |
| 401 | `invalid_machine_token` | tracker machine token 缺失、错误或已重置 | 停止 tracker 请求，要求重新配置 |
| 401 | `invalid_access_token` | shard access token 错误、过期或 scope 不匹配 | 从 tracker 刷新 assignment |
| 403 | `permission_denied` / `agent_disabled` | user/role/Project/agent 不允许 | 不重试 |
| 404 | `not_found` | Project、shard 或 session 不存在 | 刷新控制面；配置错误则停止 |
| 409 | `stale_generation` | shard generation 已改变 | outcome 直接丢弃；claim 刷新后重试 |
| 409 | `shard_not_active` | shard 不可消费 | claim 选择其他 shard |
| 409 | `identity_conflict` | 相同 job ID 对应不同身份输入 | 不重试相同 payload |
| 429 | `rate_limited` / `backpressure` | 配额或 shard 有界队列已满 | 按 `retry_after_ms` + jitter 重试 |
| 503 | `shard_unavailable` / `owner_lease_expired` | shard 正在停止或其 owner lease 已失效 | 刷新 assignment；outcome 不转交新 owner |

稳定的 item-level code：

| code | 含义 | SDK 行为 |
| --- | --- | --- |
| `lease_expired` | job lease 已过期 | 丢弃本 attempt 的 outcome |
| `stale_attempt` | attempt 已被 reset 或重新 claim | 丢弃本 attempt 的 outcome |
| `session_expired` | vnode session 已失效 | 丢弃旧 attempt，创建新 session |
| `attempt_already_finalized` | attempt 已以不同终态完成 | 不覆盖第一次终态 |
| `unsupported_operation` | receiver 或 shard 不支持该操作 | 调用错误，不重试 |

`reset_exhausted` 不是 RPC error，而是成功应用 retryable fail 后返回的 `job_status`。

SDK 还暴露不来自 HTTP response 的稳定 transport code：`tls_verification_failed`、`cache_misconfigured`、`redirect_rejected` 和 `endpoint_unreachable`。它们不得伪装成某个 queue item 的处理结果。

### 6.1 Timeout 与重试

- claim timeout：结果不确定，SDK 不自动把它重试成“同一次 claim”；未知的 WIP 等 lease 到期回收。Worker 有空余 capacity 时可以发起新的 claim；
- complete timeout：在本地认为 lease 仍有效时，以完全相同的 generation、session、attempt 和 payload 重试；`already_applied` 视为成功；
- fail timeout：可以重试同一 attempt；若返回 `stale_attempt`，说明该 attempt 已被消费或失效，SDK 不再提交 outcome；
- `stale_generation`、`stale_attempt`、`lease_expired` 和 `session_expired` 永不把旧 outcome 转发到新 owner；
- SPKI pin mismatch 或 public CA/hostname 验证失败：立即关闭连接并从 tracker 刷新 assignment；刷新后仍失败则作为安全错误停止，绝不能回退到不验证 certificate 或 HTTP；
- `cache_misconfigured`：停止使用本次 route 并刷新 assignment；配置未变化时不继续发送 queue request；
- endpoint connection timeout/refused：刷新 assignment；若 endpoint、generation 和可选 pin 未变化，按有上限的 backoff 重试同一 shard；
- 429/503 使用有上限的 exponential backoff 和 full jitter；不得无限无界重试。

## 7. SDK 最小接口

Go 与 Python SDK 暴露相同语义：

```text
CreateSession(project_id, attrs)
HeartbeatSession(session_id)
GetAssignment(session_id, accept_types)
Claim(assignment, session_id, max_jobs, lease_seconds)
CompleteBatch(route, session_id, items)
FailBatch(route, session_id, items)
ExtendLeaseBatch(route, session_id, attempts, extend_seconds)
DefaultJobID(type, url)
```

SDK 必须：

- 复用连接，并默认使用 batch 64；
- 缓存 assignment 到 access token 过期前，并保存每个 claim 返回的 project、shard、generation、session 和 attempt；
- complete/fail 前按 route 分组，禁止把不同 shard/generation 混进一个 batch；
- 返回 typed error code，不要求用户解析字符串 message；
- 对 batch partial success 暴露逐项结果；
- 提供最大 in-flight job 数，避免 claim timeout 或重试导致本地无界堆积；
- 根据 assignment 的 endpoint scheme 与可选 pin 执行 SPKI、public CA 或显式 insecure HTTP 策略，并拒绝 redirect 和可疑 cache response；
- 不在日志中输出 machine token、shard access token、完整敏感 header 或未脱敏 `attr`。

OpenAPI 文件应作为 HTTP types 和 error envelope 的权威来源；Go/Python 可以生成低层 client，再各自提供一层手写的 batch、retry 和 lease 管理封装。

## 8. 第一阶段验收链路

实现顺序只要求先跑通：

```text
GitHub login
  → user machine token
  → shard 注册公网 HTTP(S) endpoint 与可选 TLS pin
  → worker session
  → tracker assignment + shard access token
  → worker direct TLS connection to shard
  → batch claim
  → batch complete/fail
  → SQLite state verified
```

第一阶段不包含 R2 checkpoint、receiver、artifact body 上传和完整 admin 页面。完成这条链路后，用真实平均 JobSpec 对多个 shard endpoint 做压测，分别记录 tracker 控制面 QPS、单 shard SQLite 吞吐和系统 aggregate completed jobs/s。
