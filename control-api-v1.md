# SavewebHQ Control API v1

> 状态：v1 冻结并由当前实现覆盖。机器可读契约以
> [`api/openapi-v1.yaml`](./api/openapi-v1.yaml) 为准。

本文只补充 agent 上线、heartbeat 和 shard access token 的语义。Queue
请求、状态机和错误处理见 [`api-v1.md`](./api-v1.md)。

## Tracker Web 身份与 Machine Token

Tracker contributor portal 使用服务端渲染页面，不是另一套 JSON credential
API：

- `GET /auth/github/start` 创建 256-bit `state` 与 PKCE verifier，并用
  `S256` challenge 跳转 GitHub；
- `GET /auth/github/callback` 校验带 HMAC 的短期 OAuth transaction cookie，
  由 tracker 后端换 code 并调用 GitHub `GET /user`；只用数值 GitHub ID
  识别用户，access token 随后丢弃；
- tracker Web session 是随机 opaque cookie，数据库只保存 hash；生产 cookie
  为 `HttpOnly`、`Secure`，所有 POST 同时校验 `Origin` 和 session 派生的
  CSRF token；
- `POST /portal/machine-token/reset` 生成或重置该用户唯一、长期、可复用的
  machine token。旧值立即失效，原 agent 记录不删除；
- `GET /admin/users` 和对应 access POST 只允许 active admin，负责审核
  `pending/active/suspended` 与三个显式 role。暂停用户同时 revoke 其 agent，
  access/token 变更写 audit log。

OAuth 请求 `read:org`，每次登录都查询显式配置的 GitHub organization/team。
active team member 同步为 active `admin + shard_owner + worker`，其他用户同步为
active `worker`；已经 suspended 的用户不会被登录自动解封。organization 与
team 通过 `--oauth-admin-org` / `--oauth-admin-team`（或对应环境变量）配置，
不写死在核心逻辑中。`bootstrap-user --github-user-id` 仅保留作紧急管理入口。

## 1. Agent 注册

Shard 和 worker 首次启动时生成并持久化稳定的 `agent_id`。两者都调用：

```http
PUT /api/v1/agents/{agent_id}
Authorization: Bearer <machine-token>
X-Saveweb-Agent-ID: <agent_id>
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
X-Saveweb-Agent-ID: <agent_id>
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

### 2.1 Receiver-only ingress

一个有效 worker session 可以把下一阶段的 jobs 写到同一 Project 下由管理员
显式登记的 receiver：

```http
POST /api/v1/worker/receivers/{receiver_id}/batches
Authorization: Bearer <machine-token>
X-Saveweb-Agent-ID: <worker-agent-id>
```

Request 包含 `session_id` 和 1–1000 个 `JobSpecV1`；部署可以把该上限调低，但
不能提高 v1 的硬上限。Tracker 校验 user、worker
agent、session lease、Project 和 receiver 状态，规范化全部 jobs，再压缩为一个
不可变的 `jobs-jsonl-zstd-v1` 对象并直接 PUT 到受信配置中的 R2 prefix。Client
不能指定 bucket、prefix 或 object key，也不会得到 R2 credential。

只有 PUT 和 `HeadObject` 大小/ETag 校验成功后才返回 `201`，response 包含 object
URI、记录数、压缩大小、SHA-256 和 int64 UNIX `created_at`。Receiver 没有 owner、
generation、SQLite 或 queue endpoint。网络结果不确定时可能留下重复对象；worker
不得把该情况当成成功，Stage 2 工具按稳定 job ID 去重。

### 2.2 Source assignment 与载入回报

状态为 `loading` 且登记了 source 的 owner assignment 同时
包含不可变的 `source_uri`、`source_format=jobs-jsonl-zstd-v1`、
`source_etag`，以及 tracker 刚签发的 `source_download_url` 和
`source_url_expires_at`。后两项是敏感的短期 exact-object GET 能力：shard
不得记录或持久化 URL，也不持有 S3/R2 credential。

Shard 以 `(source_uri, source_etag, generation)` 作为装载身份。Heartbeat
导致 URL 轮换时不得重复启动装载；进程重启或新 generation 从头流式读取，
依靠稳定 job ID 幂等写入 SQLite。`loading` 不接受 claim。

装载结束后，当前 owner 调用：

```http
POST /api/v1/shards/{project_id}/{shard_id}/load-result
Authorization: Bearer <machine-token>
X-Saveweb-Agent-ID: <agent_id>
```

Request 带 `generation`、`success` 与稳定的 `error_code`。Tracker 只在
agent、owner、generation、owner lease 和原状态都匹配时更新同一 shard row：
成功进入 `active`；失败进入独立的 `load_failed` 并释放 owner lease。重复、
迟到或 takeover 后的回报返回 `409 stale_generation`。若 source 已写入但回报
暂时失败，shard 只重试回报，不重新导入 source。

### 2.3 Checkpoint multipart 与发布点

当前 `active` 或 `draining` owner 可以发布 `sqlite-zstd-v1` checkpoint：

1. `POST /api/v1/shards/{project_id}/{shard_id}/checkpoints` 声明 generation、
   压缩后 size 和 SHA-256；tracker 验证 owner lease，分配不可变 object key，
   并代表 shard 创建 multipart upload；
2. 对每个 part 调用 `.../{upload_id}/parts`，携带 part number、精确 size 和
   Base64 `Content-MD5`。Tracker 只签发该 object/upload/part 的 URL 与必要
   header；part number 和 size 必须与最初声明的完整对象严格对应；
3. Shard 直接 PUT 到 R2，并保存响应 ETag。某个 URL 到期或请求结果不确定时，
   可以为同一 part 重新申请 URL 并覆盖上传；已确认 part 不重传；
4. `.../{upload_id}/complete` 提交从 1 开始、连续有序的 ETag 清单。Tracker
   完成 multipart 并 `HeadObject` 检查 size，最后才以 PostgreSQL 条件更新
   发布 pointer；
5. `.../{upload_id}/abort` 显式放弃当前 upload。新 generation 会清空旧 upload
   控制状态，遗留 multipart 由 R2 lifecycle 清理。

所有步骤都重新验证 machine token、role、agent、owner、generation 和 tracker
时间的 lease。Presigned URL TTL 不是总上传时限；只要 lease 仍有效，可以逐
part 续签。R2 complete 不是提交点，`checkpoint_uri` 的 generation-CAS 才是。
旧 owner 即使上传成功，CAS 失败后对象也不能用于恢复。

### 2.4 Checkpoint recovery 与回报

管理员把已有 checkpoint 的 shard 指派给新 owner 时，必须递增 generation 并
显式设置 `recovering`。这种 assignment 不再复用 source loader；它包含最新已
发布 checkpoint 的 generation、sequence、format、压缩 size、SHA-256、创建
时间，以及 tracker 刚签发的 exact-object GET URL。内部 `s3://` URI 和 R2
credential 都不发送给 shard。

新 owner 在后台下载并依次校验压缩 size、SHA-256、Zstd 展开上限、SQLite
`quick_check`、schema、Project、shard 和 checkpoint generation。目标目录为空时，
文件在目标 SQLite 的同一目录原子安装；若本地已有同一 checkpoint/new generation
的完整 SQLite，可以直接复用。未知或过旧的本地数据库不覆盖，恢复明确失败。
随后 SQLite fence 推进到 assignment generation，把快照中的 `wip` 清回 `todo`。

只有上述步骤完成后，owner 才调用：

```http
POST /api/v1/shards/{project_id}/{shard_id}/recovery-result
```

Tracker 再次 CAS 校验 user role、agent、owner、generation、lease、`recovering`
状态和 checkpoint pointer。成功进入 `active`；失败进入独立的
`recovery_failed` 并释放 lease。恢复和回报期间 heartbeat 继续运行，大对象 GET
没有固定的客户端总超时；GET URL 的当前默认 TTL 为 3600 秒。
`recovering` shard 不会被分配给 worker，也不开放 claim/update。

第一版默认值：

| 参数 | 默认值 |
| --- | ---: |
| agent heartbeat interval | 30 秒 |
| owner lease | 120 秒 |
| session heartbeat interval | 30 秒 |
| session lease | 120 秒 |
| shard access token TTL | 600 秒 |
| source GET URL TTL | 900 秒 |
| checkpoint part URL TTL | 3600 秒 |
| checkpoint recovery GET URL TTL | 3600 秒 |
| checkpoint recommended part size | 8 MiB |
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

## 5. Shard 本机管理面

Shard 管理 server 与公网 Queue API 是两套 listener。管理 server 只允许绑定
`127.0.0.1`，默认地址为 `127.0.0.1:9081`：

- 未指定 `SAVEWEB_LOCAL_ADMIN_TOKEN` 时，每次进程启动生成新的 256-bit
  token，原子写入 `<data-dir>/runtime/local-admin.token`，权限 `0600`；日志
  只记录路径；
- 指定环境变量时 token 只在内存中使用，拒绝过短值；不提供可能泄漏到进程
  列表的 command-line token 参数；
- 登录表单换取 30 分钟 `HttpOnly`、`SameSite=Strict` session；状态变更同时
  校验 loopback `Origin` 和 CSRF token；
- 页面/API 显示 agent、assignment、generation、owner lease 与 queue stats；
  页面可以暂停/恢复新 claim，但不阻止已有 attempt 的 complete/fail；
- 本机 token 不能访问 tracker 或 Queue API，页面也不能自行修改 tracker 的
  owner、generation 或 Project/shard 状态。
