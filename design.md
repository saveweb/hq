# SavewebHQ 分布式任务队列设计

> 状态：草案
>
> 本文描述一个面向 Web Archive 场景、可水平扩展的任务队列。重点是把分片的逻辑位置与物理存储解耦，并通过 S3 checkpoint 和 generation fencing 恢复失效分片。

## 1. 背景与问题

SavewebHQ 需要管理大量抓取任务。任务数据可能远大于单机容量，并且不同项目可能使用不同的任务分片方式。

系统需要解决以下问题：

- 如何把预先切分并存放在 S3 的任务文件，按 shard 逐批载入和执行；
- 如何让志愿者把本阶段发现的 jobs 回传 S3，供下一阶段重新分片；
- 如何先用 SQLite 实现 shard，同时为未来可能的其他 backend 保留清晰接口；
- 如何在 shard 机器宕机后从 S3 checkpoint 恢复，并安全拒绝旧 owner 和旧 client；
- 如何从 S3-compatible storage 载入预切分的待抓取种子。

“无限吞吐、无限容量”在这里指架构上没有固定的单机上限。预切分模式可以持续登记新的冷 shard，只把当前需要处理的少量 shard 载入在线后端；它不是绝对的性能承诺。

## 2. 设计目标

### 2.1 目标

- **逻辑与物理解耦**：任务只依赖逻辑分片 ID，不直接依赖后端地址。
- **只支持预切分 Project**：Project 由一组可动态增减的独立 queue 组成，不在系统内计算 job 到 shard 的映射。
- **SQLite-first**：第一版只实现 SQLite；主逻辑通过最小队列接口访问 shard，便于未来按需增加其他 backend。
- **S3-first**：第一版即要求 S3-compatible storage；AWS S3 只是实现之一，不依赖 AWS 专有能力。
- **冷热分离**：所有长期数据保存在 S3，志愿者机器只保存 active shard 的 SQLite 和 checkpoint 临时文件。
- **多阶段收集**：receiver-only virtual shard 只接收任务并写入 S3，下一阶段再人工去重和分片。
- **Checkpoint 恢复**：shard 宕机后由 tracker 递增 generation、指派新机器并从 S3 checkpoint 恢复。
- **支持租约**：worker 异常退出后，未完成任务可以重新入队。
- **支持队列归档**：冷分片可以转移到对象存储，释放在线存储空间；WARC
  内容归档由独立的 WARC Receiver 负责，不进入 tracker 或 shard 数据面。

### 2.2 暂不解决

- 跨分片事务；
- 全局严格 FIFO；
- exactly-once 执行；
- PostgreSQL、Redis 等其他 shard backend 的实现与兼容性；
- `computed` Project、系统内分片算法和自动重分片；
- 在线迁移 shard 的逻辑任务归属；
- worker 公网 endpoint，以及 tracker/shard 主动向 worker push JobSpec 或等待 worker 执行完成。

### 2.3 信任与故障模型

第一版只处理宕机、进程崩溃、网络分区和请求重试，不实现 Byzantine fault tolerance：

- HQ、tracker、PostgreSQL 和 storage profile 是受信组件；
- shard owner 必须是经过审核和认证的节点，假设其会掉线或出错，但不会主动篡改 SQLite、伪造 checkpoint 或滥用已授予的单对象存储权限；
- 普通志愿者作为 job worker，不能直接访问 SQLite，也不持有 S3/R2 credential；
- 第一版不能证明 worker 返回的网页内容一定真实。明显异常可以记录、限流和封禁，但结果共识、冗余执行和恶意 worker 检测不属于队列一致性范围。

存储权限限制用于防止凭据泄漏、程序 bug 和误操作扩大到整个 bucket，不承诺抵抗一个已经被信任的 shard owner 主动作恶。如果未来需要允许任意不可信节点成为 owner，必须重新设计权威状态存储，不能继续直接信任其 SQLite checkpoint。

系统只对以下输入承诺 **at-least-once**：不可变 S3 source 中的 job，以及和父任务状态在同一 SQLite 事务中提交、因而可以重新生成的同 shard 派生 job。第一版不开放向运行中 queue 任意直接 enqueue 的 API；新增独立任务应上传为新的 source 并 attach 新 shard。Checkpoint 之后完成的工作可能在恢复时回滚并重复执行，消费者和外部结果写入必须按 job ID 与 attempt ID 幂等处理。

## 3. 核心概念

### Project

一组共享配置和任务字段定义的长期任务集合。第一版 Project 全部采用 `explicit` 语义：任务在系统外已经分片，例如手动把 `jobs.txt` 切成多份并上传 S3。每个 source 文件显式对应一个独立 queue，不需要分片算法；Project 运行期间可以动态 attach 或 detach shard。

### Logical Shard

Project 内可独立登记和移除的 queue。逻辑 shard ID 保持稳定且不能复用；增减
一个 shard 不会改变其他 shard 的任务归属。Receiver 是 Project 下的另一类独立
入口，不塞进 queue shard 的 owner/generation 状态机。

### Shard Backend

Queue shard 当前使用的物理存储。第一版固定为 SQLite，一个在线 queue shard 对应一个 SQLite 文件。队列核心通过最小接口访问它，不直接依赖具体 SQL；未来确有需要时再增加其他实现。

### Job Receiver（Receiver-only Virtual Shard）

一种只接收 job batch 并写入 S3 的逻辑入口，用于多阶段抓取流程。它不是可消费
queue，不分配 SQLite、owner 或 generation，只暴露 Tracker batch ingress；不存在
可以误调用的 receiver claim、complete、fail、续租或状态更新 endpoint。

Receiver 在 S3 对象写入成功后才返回成功。调用方必须先收到 receiver 成功响应，再完成当前执行 job；失败或结果不确定时不完成当前 job，允许其稍后重新执行。该模型允许重复，但不会因为写入顺序丢失下一阶段任务。

### WARC Receiver 与 Receipt

WARC Receiver 是 Saveweb 自行维护、独立部署的大文件接收和校验服务，不是
receiver-only virtual shard，也不属于 tracker。Worker 把 WARC 直接传给 Receiver；
WARC 字节不经过 tracker、shard 或 Job Receiver。

Receiver 只有在完整接收文件、验证 WARC 合法、把文件原子落入持久目录并保存接收
记录后，才签发不可伪造的 receipt。Worker 持有当前 job attempt，只有 Worker 可以
携 receipt 调用原有 `complete`；Receiver 不回调 shard，也不能直接修改 job。

WARC job 在取得 receipt 前一直保持 `wip`，必要时由 Worker 使用原有
`extend-lease`。第一版不增加 `uploading` 或 `upload_pending` job 状态。若 Receiver
已经接收 WARC、但 Worker 在 `complete` 前宕机，job 会按 lease 规则重试，已接收
WARC 可能成为重复或孤立对象；这是 at-least-once 语义明确接受的代价。

Receipt 至少稳定标识 Receiver、接收记录、WARC 文件名、大小、内容摘要和接收时间。
具体签名编码由独立 WARC Receiver 协议定义；Queue API 只承载有界 receipt 和
outcome metadata，不承载 WARC body。Receipt 证明可信 Receiver 已接收一份合法
WARC，不试图证明不可信 Worker 的每个业务判断都正确。

Receiver ACK 后，job 可以进入 `done`。MegaWARC 合并、Internet Archive 或其他
最终存储上传、重试和人工修复是独立的文件归档链路；tracker 可以追踪 receipt/file
的归档状态，但这些状态不能重新打开、阻止或改变 job。未来若支持按 WARC record
流式接收，Receiver 可以在某个 job 的完整 record group 被验证并持久化后签发粒度
更小的 receipt，而不改变 job 状态机。

### Checkpoint 与 Generation

在线 queue shard 定期把一致的 SQLite 快照上传到 S3，并由 tracker 记录最新有效 checkpoint。每次 owner 失效、接管或物理位置切换时，tracker 都会递增 shard `generation`。Owner 只有在其 tracker lease 有效时才允许接受写入。

Client 领取任务时必须记录 generation 和任务 lease。只有 generation 与 lease 都仍然有效时，shard 才接受 outcome。旧 generation 或已失效 lease 的结果直接丢弃，不尝试转发或 replay；新 owner 从 checkpoint 恢复后会重新分发其中未完成的任务。

### VNode Session

worker 与系统之间的租约会话。会话记录 worker 身份、主机信息和租约截止时间，用于认领任务以及回收失联 worker 的 WIP 任务。

### User、Role 与 Agent

`user` 表示通过 tracker 登录的自然人。Saveweb 管理员、shard owner 志愿者和 worker 志愿者使用同一套 GitHub OAuth 登录；登录只证明 GitHub 身份，实际权限由 tracker 中的 `admin`、`shard_owner`、`worker` role 决定，一个人可以具有多个 role。

`agent` 表示绑定到某个 user 的一台 shard 或 worker 机器。人的网页登录、机器到 tracker、worker 到 shard 和本机管理认证使用互不通用的 credential：

- GitHub access token 只由 tracker 短暂用于查询 GitHub 身份，不下发给 agent；
- 每个 user 在 tracker 后台拥有一个长期、可重置的 machine token，其所有 shard/worker 机器共用该 token 上线；
- worker 直连 shard 时使用 tracker 签发的短期 shard access token，不能把自己的 machine token 发送给 shard owner；
- shard/worker 启动时生成的 local admin token 只允许访问该进程监听在 `127.0.0.1` 的管理页面，tracker 不接受它。

GitHub OAuth 认证与 tracker 授权必须分离。首次登录的用户可以先处于 `pending`；只有 `active` 且具有相应 role 的用户，其 agent 才能成为 shard owner 或创建 worker session。GitHub OAuth token 本身不赋予 Saveweb 管理权限。

### Job

最小任务单元。当前包含 seed 和 asset 两种任务类型。

## 4. 总体架构

系统分为控制面和数据面：

```text
 ┌────────────────────────────┐   assignment / pin / token   ┌────────────┐
 │ Tracker + PostgreSQL       │ ────────────────────────────►│ Worker SDK │
 │ users / agents / projects  │                              └─────┬──────┘
 │ shards / vnode_sessions    │◄── session / heartbeat ────────────┘
 └─────────────┬──────────────┘
               │ owner lease / recovery control
               ▼
       ┌────────────────┐       direct pinned HTTPS
       │ Shard Owner    │◄════════════════════════════════ Worker
       │ public IP:port │
       │ SQLite         │
       └───────┬────────┘
               │ load / restore / periodic checkpoint
               ▼
           ┌──────────┐
           │ S3 / R2  │
           └──────────┘
```

- **控制面**：使用 PostgreSQL 保存项目、逻辑分片、物理位置和 worker 租约等少量元数据。
- **冷数据面**：S3-compatible storage 保存尚未激活的 shard source、运行中的 checkpoint、Job Receiver 输出，以及 queue shard 归档。
- **在线数据面**：第一版使用 SQLite 保存正在执行的 shard job。
- **loader**：激活 shard 时，从 `source_uri` 流式载入在线 backend；重试时从头读取，依靠稳定 job ID 幂等去重。
- **调度器**：只向 worker 分配 `active queue` shard，并遵守 Project 的 active queue 数量限制；receiver 永不进入 claim 候选。
- **Job Receiver gateway**：无状态地接收 job batch，压缩并写入 receiver 的 S3 prefix，持久化成功后才响应。
- **WARC Receiver**：独立接收、验证并持久化 Worker 上传的 WARC，签发 receipt；后续归档链路与 job 状态解耦。
- **tracker / recovery controller**：监控 shard owner，递增 generation，指派接管机器并协调 checkpoint 恢复。
- **shard queue endpoint**：shard owner 登记一个公网 HTTP(S) URI；它可以由 daemon 直出、四层端口转发，或由 Caddy、cloudflared 等反向代理暴露。Worker 从 tracker 取得 endpoint、generation、可选 TLS pin 和短期 access token 后直接访问 shard，queue 热路径不经过 tracker。
- **tracker web**：提供 GitHub OAuth 登录、machine token 管理、agent 登记、贡献者自助页面和基于 role 的管理员页面。
- **local admin web**：每个 shard/worker 进程只在 `127.0.0.1` 提供本机状态、诊断和安全停止操作，由独立的 local admin token 保护。
- **maintenance worker**：回收过期租约、生成 checkpoint 和执行归档。

### 4.1 S3-compatible Storage 约定

本文中的 “S3” 指 S3-compatible API，不特指 AWS。第一版不提供本地文件系统作为长期存储 fallback。

最小依赖接口：

```text
PutObject
GetObject
HeadObject
ListObjectsV2
DeleteObject
```

为支持低带宽节点可恢复地上传大 checkpoint，storage adapter 还需要封装：

```text
CreateMultipartUpload
UploadPart
ListParts
CompleteMultipartUpload
AbortMultipartUpload
```

存储 endpoint、bucket、region、path-style 选项和凭据由 HQ 的受信配置管理。数据库只保存 `s3://bucket/key` 等对象 URI 或 storage profile 引用，不保存 access key。实现不得依赖 AWS IAM、ACL、KMS 或特定 storage class，确保未来可以切换到自建的 S3-compatible 分布式存储。

志愿者机器的本地空间只用于：

- 当前 active shard 的 SQLite 文件；
- `VACUUM INTO` 和 Zstd 过程中的临时 checkpoint 文件；
- 有上限、可随时删除的网络传输临时文件。

Source、已发布 checkpoint、Job Receiver 输出和 queue archive 都必须写入 S3。
Checkpoint 上传并验证成功后立即删除本地临时副本。WARC 使用独立 Receiver 的
持久 spool 和最终 sink 策略，不使用 tracker 的 R2 credential，也不经过 tracker
带宽。

### 4.2 Cloudflare R2 第一版配置

第一版预计使用 **Cloudflare R2**，默认 storage profile 可命名为 `r2-default`。R2 只是当前部署选择，不进入 queue、shard 或 checkpoint 的领域模型；这些模块仍只使用 4.1 节的 S3-compatible 接口和 `s3://bucket/key`。

R2 profile 的固定约定：

- S3 API endpoint 为 `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`，region 使用 `auto`；
- bucket 保持 private，R2 API token 只配置在 HQ 的受信服务中；
- 普通 worker 不直接访问 R2。Receiver batch 由 HQ receiver gateway 限长、有界压缩并写入 R2，gateway 收到 R2 成功响应并完成对象校验后才确认；
- 经过审核的 shard owner 不持有 R2 credential。Tracker 决定不可复用的完整 object key，并代表 owner 创建 multipart upload；owner 只能取得当前 upload 中某个确切 part number 的短期 presigned `UploadPart` URL，不能 list、读取、删除或写入其他对象；
- presigned URL 的 TTL 在签发后不能修改，但上传能力没有固定的总时间窗口。Owner 逐 part 申请 URL；只要 owner lease、generation 和 upload ID 仍有效，就可以为尚未完成的 part 取得新 URL，已经成功的 part 不需要重传；
- 同一 shard 同时只允许一个 checkpoint upload，避免多个 checkpoint 乱序完成。Tracker 负责创建、完成和放弃 multipart；数据从 shard 直接流向 R2，不经过 tracker；失联后留下的未完成 multipart 由 R2 lifecycle 清理；
- Multipart 的每个 part 携带 `Content-MD5`，R2 用它发现传输损坏；完整对象的 SHA-256 由 owner 计算并由恢复节点下载后复核；
- 上传前由 HQ 按 Project 配额限制声明的 object size，上传后再用 `HeadObject` 确认实际大小；
- 不依赖 R2 custom domain、ACL、KMS、Object Lock、bucket versioning 或其他非最小 S3 能力。

Presigned part URL 只是上传能力，不是 checkpoint 的提交凭证。即使旧 owner 在失联后上传了 part，只有 tracker 中 owner、generation 和 lease 都仍有效时，tracker 才会完成 multipart 并以 CAS 发布 checkpoint pointer；否则 upload 会被放弃或由 lifecycle 清理。

参考：[R2 S3 API](https://developers.cloudflare.com/r2/api/s3/api/)、[R2 presigned URL](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)、[R2 multipart upload](https://developers.cloudflare.com/r2/objects/upload-objects/)。

## 5. 控制面数据模型（PostgreSQL）

以下为逻辑模型；落地时应使用 PostgreSQL 原生类型和约束。

### 5.0 时间约定

所有时间戳统一保存为 **int64 UNIX time（秒）**：

- PostgreSQL 使用 `BIGINT`；
- SQLite 使用 `INTEGER`；
- 所有服务按 UTC epoch 读写，不在数据库中保存时区；
- 尚未发生的可选时间使用 `NULL`，不使用 `0` 作为哨兵值；
- `created_at`、`updated_at`、所有 `*_at` 以及 `*_expires_at` 字段都遵守此约定。

### 5.1 `projects`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `id` | `text` | 主键 |
| `status` | `text` | `active`、`draining`、`archived`；替代原 `active` 布尔值 |
| `config` | `jsonb` | 项目扩展配置；当前包含 `max_resets`、`max_active_shards` |
| `job_additional_fields` | `text[]` | 该项目启用的额外 job 字段，例如 `type`、`via`、`hops`、`attr` |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

`config` 示例：

```json
{
  "max_resets": 3,
  "max_active_shards": 1
}
```

建议：稳定且需要查询或校验的配置最终应提升为独立字段；`config` 只保留低频扩展项。

Project 状态约束：

- `active`：允许 attach shard、激活 queue、claim、完成任务和写 receiver；
- `draining`：拒绝 attach、激活、新 claim 和新的独立 receiver 写入，只允许已有 WIP 完成所必需的 outcome 与 receiver 写入；
- `archived`：所有 shard 停止后的只读历史项目。

Project 的 shard membership 可以随时 attach 新 queue，也可以按安全流程 detach 现有 queue。`max_active_shards` 只限制同时占用在线 backend 的 shard 数量，不限制已登记 shard 总数。长期 Project 可以设为 `1`，由操作者逐个激活。

### 5.2 `shards`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `project_id` | `text` | 外键，指向 `projects.id` |
| `id` | `text` | 逻辑分片 ID；与 `project_id` 组成主键 |
| `owner_agent_id` | `text null` | 外键，指向 tracker 当前指派的 shard agent |
| `status` | `text` | `staged`、`loading`、`active`、`draining`、`paused`、`offline`、`recovering`、`completed`、`archived`、`load_failed`、`recovery_failed`、`removed` |
| `source_uri` | `text null` | 预切分任务源，例如 `s3://bucket/jobs-001.jobs.jsonl.zst`；对应原设计中的 `src` |
| `source_format` | `text null` | queue source 格式；第一版固定为 `jobs-jsonl-zstd-v1` |
| `source_etag` | `text null` | source 的版本或内容摘要，防止载入期间被替换 |
| `load_error_code` | `text null` | 最近一次 source 载入失败的稳定错误码；成功或重新指派时清空 |
| `recovery_error_code` | `text null` | 最近一次 checkpoint 恢复失败的稳定错误码；成功或重新指派时清空 |
| `owner_lease_expires_at` | `int64 null` | 当前 owner 的 tracker 租约截止时间，UNIX 秒 |
| `checkpoint_uri` | `text null` | 最新有效 SQLite checkpoint 的 S3 URI；对应原设计中的 `state` |
| `checkpoint_format` | `text null` | 第一版固定为 `sqlite-zstd-v1` |
| `checkpoint_seq` | `bigint null` | checkpoint 单调递增序号 |
| `checkpoint_generation` | `bigint null` | 生成该 checkpoint 的 owner generation |
| `checkpoint_upload_id` | `text null` | 当前唯一在途 checkpoint 的随机 ID；没有上传时为空 |
| `checkpoint_upload_seq` | `bigint null` | 当前在途 checkpoint 的预分配序号 |
| `checkpoint_upload_started_at` | `int64 null` | 当前 checkpoint 开始时间，UNIX 秒 |
| `checkpoint_checksum` | `text null` | `.sqlite.zst` 对象的 SHA-256 |
| `checkpoint_size` | `int64 null` | 压缩对象大小，单位为 byte |
| `checkpoint_at` | `int64 null` | checkpoint 完成时间，UNIX 秒 |
| `archive_uri` | `text null` | 归档位置，例如 S3 URI |
| `generation` | `bigint` | queue owner epoch，只能递增 |
| `activated_at` | `int64 null` | 最近一次激活完成时间，UNIX 秒 |
| `completed_at` | `int64 null` | 任务全部完成时间，UNIX 秒 |
| `removed_at` | `int64 null` | 从 Project membership 移除的时间，UNIX 秒 |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

约束：

- `primary key (project_id, id)`；
- shard 在进入 `loading` 前必须有 `source_uri`、固定的 `source_etag` 和 `source_format=jobs-jsonl-zstd-v1`；
- `active` shard 必须有当前 `owner_agent_id` 和有效 owner lease；对应 shard agent 必须有 tracker 已从公网验证的 HTTP(S) endpoint 和该 endpoint 所需的可选 TLS pin；
- `archived` 分片必须有 `archive_uri`；
- `paused`、`completed`、`archived` 和 `removed` shard 不得保留有效 owner lease；
- shard ID 在同一 Project 内永不复用；detach 后保留 `removed` tombstone，防止旧 client 命中新 queue；
- `(project_id, id, generation)` 唯一标识一次 owner 任期；
- client 的 claim、续租、complete 和 fail 都必须携带 generation；owner 仅在 generation 匹配且自身 tracker lease 未过期时服务写请求；
- 同一 shard 最多有一个 `checkpoint_upload_id`；`checkpoint_upload_id`、`checkpoint_upload_seq` 和 `checkpoint_upload_started_at` 必须同时为空或同时非空，只有当前 owner/generation 可以开始或提交它；
- checkpoint 上传完成并通过 `HeadObject` 大小检查后才能以 CAS 更新为最新版本；恢复节点下载后必须重新计算 SHA-256 并检查 SQLite。

### 5.3 `receivers`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `project_id` | `text` | 外键，指向 `projects.id` |
| `id` | `text` | 与 `project_id` 组成主键 |
| `status` | `text` | `active` 或 `removed` |
| `sink_uri` | `text` | 受信管理员登记的 S3 prefix，不接受 client 覆盖 |
| `format` | `text` | 第一版固定为 `jobs-jsonl-zstd-v1` |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

Receiver 不计入 `max_active_shards`，不参与 assignment，也没有 owner lease、
generation、checkpoint 或本地 daemon。

### 5.4 `vnode_sessions`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `id` | `text` | 会话 ID，主键 |
| `project_id` | `text` | 外键，指向 `projects.id` |
| `agent_id` | `text` | 外键，指向创建该会话的 worker agent |
| `hostname` | `text` | worker 所在主机 |
| `attrs` | `jsonb` | worker 能力、版本等扩展属性 |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `lease_expires_at` | `int64` | 租约过期时间，UNIX 秒；对应原设计中的 `locked_until` |
| `last_heartbeat_at` | `int64` | 最近心跳时间，UNIX 秒 |

`lease_expires_at` 到期后，会话认领但尚未完成的任务可以被回收。会话续租必须使用带条件的原子更新，避免旧会话覆盖新租约。

### 5.5 `users`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `id` | `text` | Saveweb 用户 ID，主键 |
| `github_user_id` | `bigint` | GitHub 不可变的数值用户 ID，唯一；不能用可修改的 login 作为身份主键 |
| `github_login` | `text` | 最近一次登录时的 GitHub login，仅用于显示 |
| `github_avatar_url` | `text null` | 最近一次登录时的头像 URL，仅用于显示 |
| `status` | `text` | `pending`、`active`、`suspended` |
| `roles` | `text[]` | `admin`、`shard_owner`、`worker` 的子集；`pending` 用户可以为空 |
| `machine_token` | `text null` | 该用户所有机器共用的长期 bearer token；后台可查看、复制和重置 |
| `machine_token_updated_at` | `int64 null` | 最近生成或重置 machine token 的时间，UNIX 秒 |
| `last_login_at` | `int64 null` | 最近登录时间，UNIX 秒 |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

首次 GitHub OAuth 登录按 `github_user_id` 创建或更新用户。`github_login` 变化不得创建新用户。首个管理员的 `github_user_id` 由 tracker 受信配置 bootstrap；OAuth 登录不能自行取得 `admin` 或 `shard_owner` role。暂停用户时，tracker 同时撤销其全部 agent。

### 5.6 `agents`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `id` | `text` | agent ID，主键 |
| `user_id` | `text` | 外键，指向 `users.id` |
| `kind` | `text` | `shard` 或 `worker`；创建后不可修改 |
| `name` | `text` | 用户为机器设置的显示名称 |
| `status` | `text` | `registered`、`online`、`offline`、`revoked` |
| `endpoint_uri` | `text null` | 仅 shard 使用的公网 queue endpoint；worker 必须为空 |
| `endpoint_version` | `bigint null` | 仅 shard 使用；endpoint 或 TLS pin 变化时递增 |
| `tls_spki_sha256` | `text null` | 仅 shard 使用；HTTPS 需要 SPKI pin 时填写，其他情况为空 |
| `endpoint_checked_at` | `int64 null` | tracker 最近一次从公网验证 shard endpoint、TLS/cache 行为的时间，UNIX 秒 |
| `attrs` | `jsonb` | hostname、版本、容量和能力等声明信息 |
| `last_heartbeat_at` | `int64 null` | 最近心跳时间，UNIX 秒 |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

一个 agent 只属于一个 user，并使用本机持久化的稳定 agent ID。每次上线和心跳都携带该 user 的 machine token；首次上线时 tracker 创建 agent，后续上线更新同一条记录。一个 user 的多台 agent 共用一个 machine token，不为每台机器额外签发 secret。Machine token 使用至少 256 bit CSPRNG 随机值，第一版保存在受信 PostgreSQL 中以便用户在后台再次查看；不得进入 URL、日志或 metrics。重置 token 会立即使旧 token 对该用户的全部机器失效，但不会删除 agent 记录。

Shard 的 `owner_agent_id` 必须指向 `kind=shard`、user 为 `active` 且具有 `shard_owner` role 的未撤销 agent；vnode session 必须指向 `kind=worker`、user 为 `active` 且具有 `worker` role 的未撤销 agent。

Shard agent 必须填写 endpoint 和非空且只能递增的 `endpoint_version`。校验策略由 URI 与可选 pin 直接推导：HTTPS 有 pin 时执行 certificate pinning；HTTPS 无 pin 时执行标准 CA 与 hostname 校验；HTTP 必须无 pin，且由 shard 和 worker 双方显式允许，会暴露 access token 和 queue 数据，不作为生产默认值。Daemon 直出或四层 TCP port forwarding 通常登记 shard 首次启动时生成的 SPKI pin；Caddy、cloudflared 等 HTTPS 反向代理通常不登记 pin，允许 public CA certificate 正常轮换。Tracker 必须从公网完成 TLS/cache/challenge health check 后才允许 assignment。

Worker agent 的所有 endpoint 字段必须为空。Worker 始终主动连接 tracker 和 shard，不提供入站服务。

## 6. 数据面模型（后端无关）

### 6.1 `jobs`

| 字段 | 类型 | 约束/说明 |
| --- | --- | --- |
| `id` | `text` | 稳定任务 ID；在 shard 内唯一，完整身份为 `(project_id, shard_id, id)` |
| `url` | `text` | 抓取目标；第一版不在 queue 内执行 URL canonicalization |
| `type` | `text` | `seed` 或 `asset` |
| `via` | `text null` | 产生该任务的来源 URL |
| `hops` | `integer` | 从 seed 开始的链接跳数，非负 |
| `attr` | `json` | 自定义数据，例如请求头 |
| `status` | `text` | 见下方状态机 |
| `session_id` | `text null` | 当前认领该任务的会话 |
| `attempt_id` | `text null` | 每次 claim 生成的不可复用 ID，用于拒绝同一 session 的迟到结果 |
| `claim_generation` | `bigint null` | 认领任务时的 shard generation |
| `lease_expires_at` | `int64 null` | 当前任务租约截止时间，UNIX 秒 |
| `reset_count` | `integer` | 已回收/重试次数，初始为 0 |
| `created_at` | `int64` | 创建时间，UNIX 秒 |
| `updated_at` | `int64` | 最后更新时间，UNIX 秒 |

`attr` 示例：

```json
{
  "headers": {
    "Accept-Language": "zh-CN"
  }
}
```

Source 和 derived job 都使用 [Queue API v1](./api-v1.md) 的 `JobSpecV1`。记录必须显式携带 ID；默认 helper 使用 `j1_ + hex(SHA-256(type + NUL + url))`，`via`、`hops` 和 `attr` 不参与。相同 ID 且 `type`、`url`、规范化 `attr` 相同的写入是幂等 no-op；相同 ID 对应不同身份输入时返回 `identity_conflict`，不得覆盖。

第一版 source 固定为 `jobs-jsonl-zstd-v1`：Zstandard 压缩的 UTF-8 JSON Lines，每个非空行是一个完整 `JobSpecV1`。简单 `jobs.txt` 由 source 工具逐行转换 URL、生成默认 ID 并打包。Loader recovery 可以从对象开头重新解压，依靠 job ID 跳过已经导入的记录，不实现 zstd seek index。

建议把 HTTP 状态码与队列状态分开保存。不要直接用 `404` 作为队列状态，可增加以下 outcome 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `outcome_code` | `integer null` | HTTP 状态码，例如 `404` |
| `outcome_kind` | `text null` | 结果分类，例如 `success`、`http_error`、`network_error` |
| `outcome_uri` | `text null` | 抓取结果的有界引用；WARC 的物理最终位置不要求在 complete 时确定 |
| `error` | `json null` | 结构化错误信息 |

Queue API v1 中，`complete` 只接受 `success`、`http_error`、`skipped` 三种
`outcome_kind` 并把 job 置为 `done`；HTTP 404 属于确定的 `http_error` outcome，
不是 queue failure。网络超时、DNS 失败等执行错误使用 `fail(retryable=true)`，
明确不可重试的执行错误使用 `fail(retryable=false)`。WARC 或页面正文不能放入
queue RPC。产生 WARC 的 Worker 必须先从受信 WARC Receiver 取得合法 receipt，
再把文件名和 receipt 作为有界 outcome metadata 交给现有 `complete`；后续物理
归档位置与 job 解耦。

### 6.2 Job 状态机

```text
             claim                  complete
    todo ─────────────► wip ─────────────────────► done
      ▲                  │  │
      │                  │  └── 不可重试错误 ────► failed
      │                  │
      │                  │ lease 过期 / 可重试错误
      │                  ▼
      └────────────── resetting
                            │ 新 reset_count > max_resets
                            ▼
                     reset_exhausted
```

状态定义：

- `todo`：等待消费；
- `wip`：已被某个有效 session 认领；
- `resetting`：正在回收，防止多个回收器重复操作；进入该状态时原子递增 `reset_count`；
- `done`：处理完成；HTTP 404 等业务结果也可以是 `done`；
- `failed`：发生明确的不可重试错误；
- `reset_exhausted`：递增后的 `reset_count` 超过 `max_resets`，不再返回 `todo`。

`done`、`failed` 和 `reset_exhausted` 是终态。将 reset 超限单独建模，便于区分“任务本身不可执行”和“任务反复超时或被回收”。其他业务结果分类应写入 outcome 字段，而不是继续扩展队列状态。

任务从 `wip` 进入 `resetting` 时立即使原 `attempt_id` 失效；返回 `todo` 前清空 `session_id`、`attempt_id`、`claim_generation` 和任务 lease。迟到请求即使来自同一 session 也不能完成新 attempt。

## 7. SQLite Backend 接口边界

第一版只有 SQLite 实现。这里定义接口不是为了立即支持多种 backend，而是为了让队列主逻辑、调度器和 loader 不直接散落 SQLite 操作：

```text
set_fence(generation, now, owner_lease_expires_at)
                                      安装或续租 tracker owner fence；generation 增加时恢复 WIP
enqueue(generation, now, jobs)        仅供 loader 使用，批量写入并按 job.id 幂等
claim_batch(generation, now, session_id, limit, lease_ttl)
                                      原子认领 todo 任务
complete_batch(generation, now, session_id, items)
                                      batch transaction 内逐项完成并写入派生任务
fail_batch(generation, now, session_id, items)
                                      batch transaction 内逐项 reset 或进入 failed
extend_lease_batch(generation, now, session_id, attempts, ttl)
                                      续租长时间运行的任务
requeue_expired(generation, now, max_resets)
                                      回收过期 wip；超限进入 reset_exhausted
scan(cursor, filter)                  归档和校验使用
```

SQLite 实现必须保证：

- `enqueue` 可以安全重试；
- `claim_batch` 在单个分片内使用一个 transaction 原子执行，为每个 job 生成新的 `attempt_id`；同一时刻不能把同一 job 分给两个有效租约；
- 所有在线操作先校验 generation 和 owner lease；`complete`、`fail` 和续租还必须校验 `session_id`、`attempt_id` 与任务租约，拒绝旧 owner、旧 attempt 或迟到 worker 的覆盖；
- 写事务在开始和 commit 前各校验一次 owner lease；commit 前使用服务端可注入时钟，跨过 owner lease 截止点的整个 batch 回滚；
- `explicit` Project 中，完成父任务与写入同 shard 的 `discovered_jobs` 必须在一个事务内提交；这样队列清空判断不会漏掉派生任务；
- complete/fail batch 使用一个外层 SQLite transaction，逐 item 预校验或使用 savepoint 隔离，并返回 `applied`、`already_applied` 或稳定 error code；不能静默部分成功。单个 complete item 内的 outcome、父任务终态和 discovered jobs 是一个原子单元；外层 commit 失败则整个 batch 返回 batch-level error，不得报告已应用。

SQLite 实现可使用事务和 `UPDATE ... RETURNING` 完成认领；一个 SQLite 文件对应一个物理分片，以便独立载入、执行和归档。PostgreSQL、Redis 等 adapter 不属于第一版范围，只有出现实际需求时才按同一语义接口增加。

Receiver-only virtual shard 不使用 SQLite 接口，只暴露：

```text
POST /api/v1/worker/receivers/{receiver_id}/batches
```

Request 绑定当前 `session_id` 并携带 jobs；成功后返回对象 URI、数量、压缩大小、
SHA-256 和 int64 UNIX 时间。每次调用生成一个独立、不可变的
`.jobs.jsonl.zst` 对象。Receiver 不在本地累计待上传任务，也不在 S3 上追加已有
对象；没有 receiver queue RPC。

## 8. 核心流程

### 8.1 确定目标 Shard

第一版不运行分片算法，所有目标都必须显式确定：

- source 文件在 attach 时绑定唯一 `shard_id`，loader 只能写入该 queue；
- worker 处理过程中发现并继续在 queue 执行的新任务继承当前任务的 `shard_id`，与父任务完成状态在同一个 SQLite 事务中提交；
- 多阶段输出必须由调用者显式指定 receiver，并通过 receiver gateway 写入。

第一版不提供向运行中 queue 任意直接 enqueue 的外部 API。需要增加一批独立任务时，操作者将其保存为新的不可变 S3 source，再 attach 为新 shard。这样所有已确认 queue 输入都能从 source 或父任务恢复。

目标为 receiver 时，gateway 从控制面解析 `sink_uri`，限制 batch 大小，在内存中
有界压缩后直接写入 R2。写入失败或结果不确定时不返回成功。

### 8.2 动态管理预切分 Shard

以手动切分后的 S3 文件为例：

1. 操作者创建长期 `explicit` Project，并设置 `max_active_shards`，例如 `1`；
2. 把 `jobs.txt` 切成多份，并由 source 工具生成带稳定 ID 的 `.jobs.jsonl.zst` 后上传 S3；
3. 为每份文件登记一个 shard，保存 `source_uri`、`source_format` 和不可变的 `source_etag`，初始状态为 `staged`；
4. 需要处理下一批任务时，调用 `activate(project_id, shard_id)`；
5. 控制面检查 Project 为 `active`，并确认占用在线资源的 queue shard 数量没有超过限制；`loading`、`active`、`draining`、`offline` 和 `recovering` 都计入；
6. 以 CAS 把 shard 从 `staged` 改为 `loading`，递增 generation，并把它指派给公网 endpoint、可选 TLS pin 及健康检查均有效的审核 owner，签发 owner lease；
7. loader 按 generation 流式读取 S3 source，以稳定 job ID 幂等写入后端；owner 失联时必须等 lease 到期再换 owner；
8. loader 校验 source ETag 和导入统计，成功后把 shard 设为 `active`；失败则设为 `load_failed`。修复后重新进入 `loading` 时从头读取，SQLite 的 job ID 唯一约束忽略已导入项，不维护额外的断点协议；
9. 调度器开始把该 shard 的任务分配给 worker。

第一版由操作者显式激活下一个 shard。以后可以增加“当前 shard 完成后自动激活下一份”的调度策略，但不改变队列后端接口。

`explicit` Project 的 shard membership 支持以下操作：

- `attach_shard`：登记新的 `shard_id`、`source_uri` 和 `source_etag`，创建一个 `staged` queue；不影响其他 shard；
- `attach_receiver`：登记 `sink_uri`，创建一个立即可写的 `active` receiver；不占 active queue 配额；
- `pause_shard`：把 `active` shard 设为 `draining`，停止新 claim；现有 WIP 完成后生成最终 checkpoint、交还 owner lease，再进入 `paused`；
- `resume_shard`：递增 generation，从 checkpoint 恢复 `paused` queue，再进入 `active`；
- `detach_shard`：从 Project membership 移除 queue，并保留状态为 `removed` 的 tombstone。

Detach 规则：

- `staged` shard 尚未执行，可以直接 detach；
- `load_failed` shard 在清理不完整的 SQLite 文件和 loader 状态后可以 detach；
- `completed` 或 `archived` shard 可以直接 detach，但不会自动删除其 S3 source、checkpoint 或 archive；
- `paused` shard 可能仍有未完成 job，只有显式传入 `cancel_pending=true` 才能 detach；
- `active` shard 必须先 pause；`loading`、`draining`、`offline` 或 `recovering` shard 默认拒绝 detach；
- 强制 detach 属于管理操作：必须先使 owner lease 失效并递增 generation，之后才能留下 tombstone；
- 已 detach 的 `shard_id` 不得复用。

Receiver 没有需要 drain 的本地 queue。停止所有生产者后即可 detach；detach 只移除逻辑入口，不自动删除已经写入 `sink_uri` 的对象。

Attach 和 detach 只改变 `explicit` Project 的 queue/receiver membership，不触发其他 shard 的重分片、复制或路由更新。

### 8.3 消费任务

1. worker 创建或续租 vnode session；
2. 调度器为 worker 选择可用 queue shard；
3. worker 从 tracker 取得 shard assignment，其中包含公网 endpoint、generation、可选 TLS pin 和短期 shard access token；worker 按 URI scheme 与 pin 校验 endpoint 后直连 shard 并批量 `claim`，shard 为每个任务生成新的 `attempt_id`；
4. 处理期间按 generation、session 和 attempt ID 续租；
5. 普通单阶段流程中，完成后原子写入 outcome、同 queue 派生任务和 `done` 状态；
   多阶段 Job Receiver 流程按 8.5 节先写 S3；产生 WARC 时按 8.6 节先取得
   Receiver receipt；然后仍由 Worker 使用同一个 `complete` 完成当前 job；
6. 若完成请求失败，client 在原 generation 和 lease 仍有效时重试，并刷新 tracker 元数据；
7. tracker 若已进入 `offline` / `recovering` 并报告新的 generation，或原任务 lease 已失效，client 直接丢弃本次 outcome，不向新 owner 重放；
8. 可重试失败进入 `resetting` 并递增 `reset_count`；新计数未超限则返回 `todo`，超限则进入 `reset_exhausted`；不可重试失败进入 `failed`；
9. 当 shard 不再有 `todo`、`wip` 或 `resetting` 时，owner 生成并发布最终 checkpoint；tracker 再以条件更新把它设为 `completed` 并释放 owner lease。

步骤 9 之所以安全，是因为第一版 queue 输入只来自激活前固定的 source 和同事务派生任务，不接受外部动态写入。

丢弃的是某次执行的 outcome，不是 job 本身。新 owner 恢复 checkpoint 后会再次派发未完成 job，因此队列保持 at-least-once；代价是重复抓取，以及第一次抓取到的页面版本可能不再保留。这是本故障恢复方案明确接受的语义。

### 8.4 Checkpoint 与宕机接管

#### 8.4.1 S3 格式

第一版 checkpoint 固定使用 **`VACUUM INTO` 生成的紧凑 SQLite 快照，再以 Zstandard 压缩**，格式名为 `sqlite-zstd-v1`。

对象命名：

```text
s3://<bucket>/<prefix>/<base64url(project_id)>/<base64url(shard_id)>/<generation>/<checkpoint_upload_id>.sqlite.zst
```

选择这一格式的原因：

- `VACUUM INTO` 会去掉 free pages 和碎片，生成内容一致且接近最小尺寸的 SQLite 文件；
- Zstd 对数据库页和重复字符串有较好压缩效果，同时恢复速度快；
- 恢复时可以直接解压为 SQLite，不需要解析 SQL dump、JSON 或列式格式再重建索引；
- 不保存独立 WAL 文件，避免 DB/WAL 配对、WAL 截断和多段恢复链问题。

Zstd 默认建议使用 level 6，并用真实 shard 在 level 3、6、9 间基准测试后调整。更高等级通常只换来有限空间收益，却显著增加 checkpoint CPU 时间。

Checkpoint 发布流程：

1. 当前 owner 使用 `VACUUM INTO` 生成新的 `.sqlite` 一致快照；输出文件必须是不存在的临时文件；
2. 使用 Zstd 压缩为 `.sqlite.zst`；
3. 计算压缩对象的 byte size 和 SHA-256；multipart 的每个 part 另行计算 `Content-MD5`；
4. Owner 向 tracker 申请上传。Tracker 确认当前没有在途 checkpoint 后，以 CAS 分配递增的 `checkpoint_upload_seq`、随机 `checkpoint_upload_id` 和不可变 S3 key；client 不能指定 bucket、prefix 或 key；
5. Tracker 创建 R2 multipart upload。Owner 为下一 part 申请只绑定 object key、S3 upload ID 和 part number 的短期 presigned URL，并把数据直接上传到 R2；
6. URL TTL 不能修改。若某个 URL 到期或 owner 需要更长总上传时间，只要 owner lease、generation 和 `checkpoint_upload_id` 仍匹配，就可以为该 part 重新签发；已经确认的 part 不需要重传；
7. Owner 提交有序的 part number/ETag 清单；Tracker 在再次验证 owner、generation 和 upload ID 后调用 `CompleteMultipartUpload`，再用 `HeadObject` 确认对象存在且实际大小匹配；
8. 仅当 owner、generation、owner lease、`checkpoint_upload_seq` 和 `checkpoint_upload_id` 仍有效时，以 CAS 发布 `checkpoint_uri`、`checkpoint_format`、`checkpoint_seq`、`checkpoint_generation`、`checkpoint_checksum`、`checkpoint_size` 和 `checkpoint_at`，同时清空在途上传字段；
9. CAS 失败说明 owner 已过期或上传已作废，该对象不得成为恢复基线。完成但未引用的对象由 GC 清理，未完成 multipart 由 R2 lifecycle 清理。

Owner 可以显式放弃当前上传；tracker 仅在 owner、generation 和 `checkpoint_upload_id` 匹配时清空在途字段。Generation 切换也必须清空这些字段，使新 owner 可以开始自己的 checkpoint。

保留策略：

- 第一版只把 tracker 当前指向的一份 checkpoint 作为恢复基线；
- 新 checkpoint 发布成功后，异步删除被替换的旧对象；
- 未被 tracker 引用的完整对象由定期 GC 清理，未完成 multipart 由 R2 lifecycle 清理；
- 第一版 R2 不依赖 bucket versioning，而是用唯一 key 保存不可变对象；未来若切换到支持 versioning 的实现，必须同时配置 noncurrent version 生命周期；
- paused shard 至少保留 1 份可恢复 checkpoint；removed shard 是否删除 checkpoint 由 detach 的数据保留策略决定。

不选择 SQL dump、JSON/JSONL、Parquet 或 DB+WAL 作为第一版 checkpoint。增量 diff 虽然可能进一步节省空间，但会引入恢复依赖链、压缩基线管理和垃圾回收复杂度。

未来如果完整 SQLite 快照仍然过大，可以设计 `state-delta-v1`：继续以不可变 `source_uri` 保存原始 jobs，只 checkpoint 完成状态 bitmap、失败状态、派生 jobs、去重数据和 outcome 引用。该方案需要恢复时重建 SQLite，不属于第一版。

参考：[SQLite `VACUUM INTO`](https://sqlite.org/lang_vacuum.html)、[Zstandard CLI](https://github.com/facebook/zstd/blob/dev/programs/zstd.1.md)。

#### 8.4.2 宕机接管

宕机接管流程：

1. tracker 根据心跳和健康检查判定 owner A 失效，并等待 A 的 owner lease 到期；计划内交接可以由 A 主动交还租约；
2. tracker 原子地把 shard 从 `(active, generation=g, owner_agent_id=A)` 改为 `(offline, generation=g+1, owner_agent_id=NULL)`，同时清空旧 generation 的 checkpoint 在途字段；generation 递增是不可撤销的 fencing 操作；
3. tracker 指派 owner B、签发新 owner lease，并把 shard 设为 `recovering`；
4. Tracker 给 B 的 `recovering` assignment 签发 exact-object GET URL；B 校验压缩 size、SHA-256、Zstd 展开上限、SQLite `quick_check`、schema 和 queue identity，再在空数据目录原子安装；校验失败或没有已发布 checkpoint 时进入独立的 `recovery_failed`，第一版不隐式回退到 source loader；
5. B 推进 SQLite fence，把快照中的 `wip` 恢复为 `todo`，清空 session、attempt ID、lease 和 `claim_generation`；基础设施故障不增加 job 的 `reset_count`；
6. 恢复完成后，tracker 确认 B 的公网 endpoint、可选 TLS pin、健康检查与 owner lease 仍有效，保持 generation `g+1`，再把 shard 设为 `active`；
7. 持有 generation `g` 的 client 刷新 tracker 后发现 epoch 已变化，丢弃本地 outcome；这些 job 会从 B 重新 claim。

SQLite checkpoint 是一致快照，因此父任务的 `done`、outcome 和派生任务要么一起进入 checkpoint，要么一起不进入。后一种情况下，父任务会重新执行并再次生成派生任务，不需要额外 replay 队列。

计划内切换 shard 机器也使用同一协议：旧 owner 主动交还租约，tracker 再递增 generation，并从 checkpoint 启动新 owner。不能让两个 generation 的 owner lease 同时有效。

该恢复方案依赖以下不变量：

- `source_uri` 的内容不可变；
- generation 只能递增，不能回退或复用；
- 两个 owner lease 不能重叠；旧 owner lease 失效后必须停止写入；
- 父任务的 `done`、outcome 和派生 jobs 在同一个 SQLite 事务中提交；
- checkpoint 是 SQLite 一致快照，并且只能由当前 generation 通过 tracker CAS 发布。

只要这些条件成立，checkpoint 之后生成但尚未进入快照的 job，其父任务状态也不会进入快照，恢复后父任务会重新执行并再次生成它。事务外生成且无法重现的任务不受该机制保护，禁止作为正常写入路径。

#### 8.4.3 上传 Checkpoint 时失联

假设 owner A 持有 generation `g`，正在上传 checkpoint 时与 tracker 失联，而 owner B 随后接管：

1. Tracker 可以先把 A 标记为 suspect，但必须等 A 的 owner lease 按控制面时间到期后，才能递增到 `g+1` 并指派 B；不得根据 client 本地时钟提前接管；
2. A 手里的旧 presigned part URL 可能尚未过期，因此它仍可能继续写这个确切 part；但它不能创建或完成 multipart、写其他 key，也不能修改 tracker pointer；
3. A 报告 part 清单后，HQ 再校验 owner lease；只有校验仍成立才完成 multipart、执行 `HeadObject`，最后以 PostgreSQL CAS 发布 checkpoint pointer。R2 complete 与 PostgreSQL 事务不能原子提交，因此 **CAS 才是 checkpoint 的提交点**；
4. Checkpoint CAS 与 tracker 的 `g → g+1` 必须条件更新同一 shard row，使二者形成唯一顺序：
   - 若 checkpoint CAS 先成功，它是 A 在 lease 有效期间发布的合法快照；tracker 随后递增 generation，B 可以从该快照恢复；
   - 若 generation 递增先成功，A 的 CAS 必须影响 0 行；即使 R2 对象已经上传或 complete，也只是未引用对象，B 不得使用，之后由 GC 删除；
5. B 只读取 tracker 明确发布的 `checkpoint_uri`，不得通过列举 R2 prefix 自动选择“序号最大”或“时间最新”的对象；B 使用 generation `g+1` 的新 key 创建后续 checkpoint，不会与 A 的 key 冲突。

因此不会出现两个 checkpoint 同时成为恢复基线。可能发生的只有多余上传、孤儿对象，以及 B 从较旧 checkpoint 恢复后重复执行部分 job；这符合既定的 at-least-once 语义。若绕过 CAS 发布 pointer，或在旧 lease 到期前启动 B，才会产生真正的 split-brain 风险。

### 8.5 Receiver-only 多阶段流程

Receiver 用于“本阶段执行 seed，收集发现的 jobs，下一阶段再重新分片”的流程：

```text
Stage 1 queue shards
        │ discovered jobs
        ▼
receiver-only virtual shard
        │ durable S3 objects
        ▼
合并、去重、split
        │
        ▼
Stage 2 explicit queue shards
```

单个 job 的处理顺序：

1. worker 执行 seed 并得到 discovered jobs；
2. worker 把 jobs 批量 `enqueue` 到指定 receiver；
3. receiver gateway 将 batch 压缩并写入唯一 S3 对象：

   ```text
   <sink_uri>/<project>/<receiver>/<random_id>.jobs.jsonl.zst
   ```

4. S3 写入成功后 receiver 才返回成功；
5. worker 收到明确成功后，再向原 queue shard 提交 outcome 并完成 seed job；
6. receiver 写入失败、请求超时或结果不确定时，worker 不完成 seed job，让 lease 到期后重新执行。

这个顺序不提供 exactly-once。若 S3 已写入但响应丢失，seed 会重新执行并产生重复对象；这是允许的。它保证的是“可能重复，不因写入顺序静默丢失 discovered jobs”。

阶段切换由操作者手动完成：

1. 停止 attach 或激活新的 Stage 1 queue，并让已经激活的 queue shards 运行到 `completed`；
2. 列出 receiver `sink_uri` 下的全部 `.jobs.jsonl.zst`；
3. 按确定的对象顺序调用 `source merge --input ... --output-prefix ...`，按 job ID
   去重；同 ID 的 `type`、`url` 或 `attr` 不同则整体失败；
4. 通过 `--jobs-per-file` 按下一阶段需要重新 split；
5. 把生成的文件上传 S3；
6. 在同一 `explicit` Project 或新 Project 中 attach 为 Stage 2 queue shards。

第一版不增加 outbox、manifest、seal、自动 compaction 或 Pipeline/Stage 数据模型。
普通 worker 始终把 batch 交给 HQ receiver gateway，由 gateway 执行 S3 PUT；worker
不获得 S3 credential。所有已完成的父任务都对应一次明确成功的 receiver 写入。
超时但后来才落入 R2 的未确认对象最多造成额外输入，不属于必须收集的已确认集合，
下一阶段按稳定 job ID 去重。

### 8.6 WARC 接收与 Job 完成

WARC job 不增加队列状态，完整流程为：

```text
todo → wip ── Worker direct upload ──► WARC Receiver
          │                                  │ validate + durable accept
          │◄──────── signed receipt ─────────┘
          └── Worker complete(receipt) ─────► done
```

1. Worker claim job，获得当前 generation、attempt ID 和 lease；
2. Worker 抓取并生成 WARC，在本地暂存 outcome；一个文件可以对应多个当前 jobs；
3. Worker 直接把 WARC 上传到 Saveweb 维护的 WARC Receiver，上传慢时继续续租；
4. Receiver 完整校验 WARC，并在文件和接收记录已经持久化后签发 receipt；非法或
   未完整接收的文件不 ACK；
5. Worker 把 `warc_filename`、receipt 和业务 outcome 放入普通 complete；Shard
   验证 receipt 后，仍按 generation、session、attempt 和 lease 原子完成 job；
6. Receiver 接收后、complete 前 Worker 宕机时，job 重新执行，允许产生重复 WARC；
7. 旧 generation 或旧 attempt 的 receipt/outcome 不能完成新 attempt，已接收的
   WARC 可以继续归档，但不再影响 queue；
8. Receipt ACK 后的 MegaWARC、最终 sink 上传和重试只更新独立文件状态。Tracker
   可以展示该链路，但不能把 sink 失败传播回 job。

第一版 receipt 以整份合法 WARC 为粒度。未来的流式协议可以改为完整 record group
粒度：Receiver 一旦验证并持久化某个 job 对应的全部 records，就返回 receipt，使
Worker 更早 complete；最终文件封装和命名仍在 Receiver 内异步完成。Receipt 应以
稳定的 `receipt_id` / `archive_ref` 为主，物理文件名只作附加信息，以免 MegaWARC
重组改变引用身份。

### 8.7 用户登录、机器上线与撤销

#### 8.7.1 GitHub OAuth 登录 Tracker

Saveweb 管理员、shard owner 志愿者和 worker 志愿者都从 tracker web 发起同一套 GitHub OAuth web flow：

1. tracker 生成不可猜测的 `state` 和 PKCE `code_verifier`，把浏览器重定向到 GitHub；
2. GitHub 把短期 authorization code 回调到 tracker；tracker 校验 `state`，在服务端用 code 和 PKCE 换取 access token；
3. tracker 立即调用 GitHub `GET /user` 重新确认身份，以数值 `id` upsert `users`，`login` 和头像只作为显示信息；
4. tracker 创建自己的短期 web session，以 `HttpOnly`、`Secure`、`SameSite=Lax` cookie 返回；
5. GitHub access token 不发送给 shard/worker，也不能用作 machine token。第一版只做身份登录，不请求 repository 或 email 权限；完成身份确认后不长期保存 access token。

OAuth 只完成 authentication，不能自行增加 `roles`。新用户是否自动得到 `worker` role 可以作为部署策略；`shard_owner` 和 `admin` 必须由现有管理员授予。所有 role、用户状态和 agent 撤销操作写入 audit log。

标准 web flow 已经使 code 换 token、`GET /user` 等后端请求全部由 tracker 发出，因此 shard/worker 主机本身不需要访问 GitHub。用户的浏览器仍必须能打开 GitHub 的授权/登录页；GitHub device flow 同样要求用户访问 `github.com/login/device`，不能解决这一点。第一版不代理 GitHub 登录页面、密码或 cookie。若目标用户连 GitHub 授权页也无法访问，应另行选择受信的国内身份源或管理员一次性邀请码，而不是把 GitHub 页面反向代理进 tracker。

参考：[GitHub OAuth Apps 授权流程](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps)。

#### 8.7.2 Machine Token 与 Agent 上线

Daemon 不直接执行 GitHub OAuth，也不持有 GitHub token。使用以下简单流程：

1. 用户登录 tracker web，在个人后台生成自己的 machine token；一个 user 同时只有一个当前 token；
2. 该 token 长期有效、可重复使用，后台可以随时查看和复制。用户把它粘贴到 shard/worker 本机管理页，或通过 `SAVEWEB_MACHINE_TOKEN` 环境变量提供；
3. daemon 首次启动时在本地生成并持久化一个稳定 agent ID，然后用 machine token、agent ID、`kind`、名称、版本和能力信息连接 tracker；
4. tracker 由 token 找到 user，并校验状态和 role：shard agent 要求 `shard_owner`，worker agent 要求 `worker`；
5. 校验通过后，tracker 创建或更新 `agents` row。后续上线、心跳和控制面请求继续使用同一个 machine token，不再交换另一份每机器 secret。

一个 user 的所有 shard 和 worker 可以复用同一个 machine token。Tracker 仍为每台机器保留独立 agent ID，以便显示心跳、容量、assignment 和单机下线状态，但不要求志愿者管理多份 secret。

用户或管理员可以在 tracker 后台重置 machine token。重置后，tracker 立即拒绝旧 token 发起的上线、心跳、租约续期、checkpoint part URL 请求和 checkpoint pointer CAS，该用户的全部机器需要换成新 token。Worker 不再取得新的 session、assignment 或 shard access token；已经签发的 access token 最多继续有效到自身 `exp`，已有 job lease 自然到期。Shard 的现有 owner lease 不再续期，tracker 停止向新 worker 返回该 endpoint，等待 lease 到期后按 8.4 节递增 generation 并接管。重置 token 本身不能绕过 lease 直接制造两个同时有效的 owner。旧 owner 已取得的精确 part URL 可能继续有效到原 TTL，但无法要求 tracker 完成 multipart 或通过 CAS 发布 checkpoint。

#### 8.7.3 Local Admin Token

每个 shard/worker daemon 都必须有独立的 local admin token：

- 默认在每次启动时使用 CSPRNG 生成至少 256 bit 随机 token；交互模式只显示一次，service 模式写入权限为 `0600` 的 runtime secret 文件并只输出文件路径；
- 运维者也可以通过 `SAVEWEB_LOCAL_ADMIN_TOKEN` 环境变量显式指定；空值或明显过短的值必须拒绝启动；
- 管理 HTTP server 第一版只绑定 `127.0.0.1`，不提供绑定 `0.0.0.0` 的开关；远程管理使用 SSH tunnel；
- token 只通过登录表单或 `Authorization` header 提交，不能放在 URL、日志或浏览器 `localStorage` 中。浏览器登录后换成短期 `HttpOnly`、`SameSite=Strict` local session，并校验 `Origin`；
- local admin token 只能管理当前进程，不能登录 tracker、申请 shard、claim job 或替代 machine token；本机页面允许录入或替换 machine token，但保存后不在状态页回显。

随机 token 在进程重启时自动轮换；环境变量指定值则由操作者负责轮换。本机 token 泄漏的影响被限制在当前主机和当前 daemon，但同机其他用户仍可能访问 `127.0.0.1`，因此 token 不能省略。

## 9. 分片生命周期

### 9.1 Queue Shard

```text
attach → staged → loading → active → completed → archived
            │        ▲  │      ├→ offline → recovering → active
            │        │  ▼      └→ draining → paused → recovering → active
            │        └─ load_failed             ├→ recovery_failed
            └→ removed                          └→ removed

completed / archived → removed
```

- **登记**：只记录 `source_uri` 等元数据，状态为 `staged`，不创建 SQLite backend；
- **激活**：状态变为 `loading`，分配 SQLite 文件并流式载入 source；成功后进入 `active`；
- **载入失败**：进入 `load_failed`，修复问题后从头幂等载入；不维护额外的 source 断点状态；
- **执行**：调度器只从 `active` shard claim 任务；
- **暂停**：进入 `draining` 后停止新 claim；WIP 清零并发布最终 checkpoint 后交还 owner lease，进入 `paused`；
- **恢复**：`paused` shard 递增 generation，经 `recovering` 从 checkpoint 恢复后重新进入 `active`；
- **恢复失败**：checkpoint 缺失、损坏、identity 不符或安装失败时进入 `recovery_failed`，不隐式回退 source；管理员排除问题后用更高 generation 重新指派；
- **故障**：tracker 先递增 generation 并设为 `offline`，再指派新 owner 进入 `recovering`；checkpoint 恢复完成后回到 `active`；
- **完成**：所有任务都进入终态时，发布最终 checkpoint、交还 owner lease，再以条件更新进入 `completed`；
- **归档**：按需上传 SQLite 快照或结果清单，写入 `archive_uri`，删除在线副本后进入 `archived`；
- **移除**：按 8.2 节的 detach 规则进入 `removed`，保留 tombstone，不影响其他 queue。

同一 shard 的 SQLite 文件若需要搬到新位置，使用 8.4 节的计划内接管协议：交还旧 owner lease、递增 generation，再由新 owner 从已校验 checkpoint 启动。这只是物理位置变化，不改变 shard ID。

generation 切换前，旧 owner lease 必须已经到期或被主动交还；切换后旧 owner 不能再次接受写入。更换 `owner_agent_id` 而不递增 generation 会造成 split-brain，属于非法状态转换。同一 owner 只更换公网 endpoint 或 TLS certificate、且仍由同一 SQLite 进程持有 lease 时可以保留 generation，但必须递增 `endpoint_version`；worker 的旧 route 连接失败后回 tracker 刷新。

### 9.2 Receiver Shard

```text
attach → active → removed
```

Receiver 不分配 owner、SQLite 或 checkpoint，也没有 pause、recover 和 completed 状态。`active` 时只接受 `enqueue`；停止生产者后可直接进入 `removed`，已经写入 S3 的对象按独立数据保留策略处理。

## 10. 一致性、容量与可观测性

### 10.1 一致性

- tracker 提供 owner、状态和 generation 的权威版本；
- 数据面只保证单分片内原子性；
- checkpoint 必须来自 SQLite 一致快照，并通过 CAS 发布；
- 恢复后所有旧 WIP 都重新入队；对不可变 source 和同事务派生 job 保证 at-least-once，但不保证保留失效 generation 的 outcome；
- receiver 只有在 S3 PUT 成功后才确认 enqueue；响应失败或不确定时生产 job 不得完成，重复由下一阶段去重；
- WARC Receiver 只有在 WARC 合法且文件和接收记录已持久化后才签发 receipt；只有
  Worker 携 receipt 完成 job，Receiver 和后续归档状态都不直接修改 job；
- job ID 在 shard 内需要唯一约束；每次 claim 的 attempt ID 不得复用，outcome ID 也必须唯一。

### 10.2 容量与背压

- 为每个分片设置容量、水位和最大批量大小；
- queue API 默认 batch 64、最大 256；claim、complete 和 fail 都按 batch 使用一个 SQLite transaction，complete/fail 在 transaction 内用逐项校验或 savepoint 返回逐项结果；
- 100,000 completed jobs/s 是多个公网 shard endpoint 的 aggregate 目标，不是单 tracker 或单 SQLite 文件的承诺；worker 直连 shard，tracker 不转发 queue body；
- 100k 目标只计算 JobSpec、attempt、lease 和 outcome metadata；WARC、页面正文、Job Receiver object 与 checkpoint 流量单独计量；WARC 字节不经过 tracker；
- source 载入或在线 SQLite 达到高水位时暂停激活新 shard，并对生产者限流；
- Project 可以动态 attach/detach shard，S3 承担冷数据容量；`max_active_shards` 控制本地磁盘和 worker 压力；
- SQLite 后端应限制并发写入者数量，并使用 WAL 模式。

### 10.3 最低监控项

- 每个项目和分片的 `todo`、`wip`、`failed`、`reset_exhausted` 数量；
- claim、complete、fail 的吞吐和延迟；
- session 与 job 租约过期数量；
- owner 心跳与租约、generation 切换次数、恢复耗时和被丢弃的旧 generation outcome 数量；
- shard endpoint 公网健康检查、TLS/CA/pin 验证失败、cache misconfiguration、access token 验证失败、assignment 刷新和直连失败数量；
- checkpoint 序号、年龄、大小、上传耗时和校验失败次数；
- 分片容量、SQLite 健康状态与路由 generation；
- staged、loading、active、draining、paused、completed、removed shard 数量；
- receiver enqueue 吞吐、S3 写入失败率、输出对象数和压缩后 byte 数；
- WARC Receiver 的接收、校验、拒绝和 receipt 延迟，以及 accepted、packing、uploading、published、failed 文件数；这些指标不汇入 job 状态；
- S3 source 的载入进度、checkpoint、ETag 变化和失败次数。

## 11. 安全要求

- Saveweb 管理员、shard owner 志愿者和 worker 志愿者都在 tracker 使用 GitHub OAuth 登录；每次登录必须使用 `state`、PKCE S256，并在换取 token 后调用 `GET /user`，以稳定的 GitHub 数值 ID 识别用户；
- GitHub 登录、user machine token、shard access token 和 local admin token 是独立权限域，彼此不得替代；OAuth token 不下发到机器，machine token 只发送给 tracker，local admin token 不离开本机；
- machine token 使用高熵随机值，一个 user 的机器可以复用；不得出现在 URL、日志或 metrics，所有 tracker API 只通过 TLS 暴露；
- shard 公网 queue endpoint 统一为 HTTP/JSON URI。HTTPS 有 pin 时校验 SPKI，无 pin 时校验 public CA 与 hostname；HTTP 只能在 shard 和 worker 双方显式允许后使用，不作为生产默认值；
- tracker 使用 Ed25519 签发短期 shard access token，scope 固定到 worker session、Project、shard、owner 和 generation。公网 shard 在读取大 body 前先验证 token，并执行请求大小、并发和速率限制；
- 四层 TCP forwarding 保留 shard daemon 的 TLS identity，通常登记 pin；Caddy、cloudflared 等七层 HTTPS 反向代理通常使用 public CA 且不登记 pin，必须原样传递认证 header 和 body，queue client 不跟随 redirect；
- shard endpoint 必须解析到 global-unicast，拒绝 loopback、private、link-local 和 metadata 地址，防止 endpoint 注册成为 SSRF；
- shard queue 与 challenge API 一律使用 `POST`；request 设置 `Cache-Control: no-store, no-cache, max-age=0` 和 `Pragma: no-cache`，response 还设置 `Cloudflare-CDN-Cache-Control: no-store`、`CDN-Cache-Control: no-store` 和 `Expires: 0`；
- 使用 Cloudflare 时必须为 `/api/v1/queue/*` 和 `/api/v1/shard/endpoint-challenge` 配置 Bypass Cache。Worker 对 `CF-Cache-Status` 使用 allowlist，只接受 `DYNAMIC`、`BYPASS` 或 header 缺失，其他值均视为 `cache_misconfigured`；
- tracker web session 使用安全 cookie 和 CSRF 防护；role 变更、用户暂停、machine token 生成/重置、agent 下线、Project/shard 状态变更和强制接管必须记录操作者、时间、目标和原因；
- 普通 worker 不得获得任何 S3/R2 credential；经过审核的 shard owner 也不得获得长期或 bucket-wide access key；
- HQ/tracker 决定 storage profile、bucket 和完整 object key；不得接受 client 提供任意 S3 URI；
- Receiver batch 只能交给 HQ gateway，由 gateway 限长、压缩并流式写入 R2；普通 worker 不能直接选择 bucket 或 object key；
- WARC 必须由 Worker 直接上传到 Saveweb 维护的 WARC Receiver，不得经过 tracker
  或 shard；Receiver receipt 必须经过认证并限制大小，Shard 只接受受信 Receiver
  签发的 receipt；
- Checkpoint 使用 multipart upload；审核过的 owner 只获得精确 upload ID/part number 的固定 TTL presigned URL，不能 list、get、delete 或写其他 key；URL 到期后只能重新签发，不能延长原 URL；
- 只有 owner lease、generation 和当前 `checkpoint_upload_id` 仍匹配时才能重新签发 part URL、完成 multipart 或发布 pointer；
- 下载 source 或 checkpoint 使用短期、单对象、只读 URL，或者通过 HQ 代理；
- R2 校验每个 multipart part 的 `Content-MD5`；HQ 通过 `HeadObject` 检查实际大小，恢复节点下载后验证 SHA-256 和 SQLite。解压时必须限制最大展开尺寸，防止压缩炸弹；
- 只有 HQ 服务账号可以 `ListObjectsV2`、`DeleteObject`、发布 checkpoint pointer 或清理对象；
- 对每个用户、Project 和 shard 设置 receiver batch、checkpoint 大小和请求速率上限，避免程序错误消耗存储和请求费用；
- S3 URI 中不得包含 access key 或 secret；受信服务凭据只能引用 secret；
- worker 只能访问被授权的 project 和 shard；
- `attr`、错误信息和抓取请求头可能包含敏感数据，需要限制大小并按策略脱敏；

## 12. Web 管理界面

Tracker contributor portal 与 tracker admin 可以是同一个 Web 应用，按 user role 显示不同页面；shard/worker 的 local admin web 是各自 daemon 内置的独立页面，不共享 tracker cookie 或 local admin token。

### 12.1 Tracker Contributor Portal

所有已登录用户至少可以查看自己的 GitHub 身份、审核状态和 roles。获得相应 role 后还可以：

- 查看、复制或重置自己的 machine token，并查看该 token 下登记的 shard/worker agent；
- 查看 agent 在线状态、版本、最近心跳、容量、当前 vnode session 或 owner shard；
- 重命名或下线单台 agent；重置 machine token 会使自己的全部机器需要更新配置；
- 查看自己机器的历史上下线、token 重置和被拒绝请求等审计信息。

### 12.2 Tracker Admin

具有 `admin` role 的 Saveweb 管理员可以：

- 审核、启用、暂停用户，并授予或收回 `worker`、`shard_owner`、`admin` role；
- 查看、下线或撤销 agent，观察版本、心跳、网络方式和容量；
- 创建和归档 Project，attach/activate/pause/resume/detach shard；
- 查看 shard 的 owner、generation、lease、checkpoint、载入/恢复进度和 receiver 输出统计；
- 请求安全 drain、checkpoint 或重新指派。强制 fencing 必须经过二次确认并写明原因；
- 检索 audit log 和系统告警。

页面不能绕过状态机直接编辑 PostgreSQL row。所有按钮调用与公开 API 相同的 command handler、权限检查和 CAS；高风险操作必须显示目标 Project/shard/generation，防止管理员对旧页面误操作。

### 12.3 Shard Local Admin

Shard 的 `127.0.0.1` 页面显示：本机 agent 身份与 tracker 连接、公网 endpoint/endpoint version/可选 pin 及 tracker TLS/cache 健康检查、当前 Project/shard/generation、owner lease、SQLite 大小与 `quick_check`、job 状态计数、checkpoint 进度、最近错误和日志。

第一版只提供安全的本机操作：设置 machine token、HTTP(S) endpoint 和可选 pin，测试 tracker/R2 连通性及公网 TLS/cache reachability，请求立即 checkpoint、请求 drain/优雅交还、停止接收新 claim、导出诊断信息和停止进程。页面只能“请求” tracker 状态转换，不能自行修改 owner、generation、checkpoint pointer 或 Project/shard 状态，也不能直接编辑 SQLite job。

### 12.4 Worker Local Admin

Worker 的 `127.0.0.1` 页面显示：本机 agent 身份与 tracker 连接、当前 vnode session、正在执行的 job、吞吐、错误、租约剩余时间和日志。允许设置 machine token、暂停新 claim、完成当前任务后退出、测试出站网络和导出诊断信息；worker 不需要公网 endpoint，也不提供手工伪造 complete/fail、修改 job、冒用其他 session 或回显 machine token 的入口。

## 13. 待确认的设计决策

以下信息在原始设计中语义不足，实施前需要定案：

1. checkpoint 的生成周期和最大可接受重复执行窗口；
2. `max_resets` 的默认值、退避策略和不可重试错误分类；
3. shard 完成后由操作者手动激活下一份，还是增加可选的自动激活策略；
4. Job Receiver 与第二阶段 merge/split 的真实规模、内存预算和默认 batch 参数；
5. 对浏览器无法访问 GitHub 授权页的用户，是增加独立的国内身份源，还是管理员一次性邀请码；第一版不代理 GitHub 登录页面、密码或 cookie；
6. 使用目标 tracker 机型、真实 JobSpec 和多台公网 shard 完成 100,000 completed jobs/s 验收时的 shard 数量、延迟与带宽门槛。

## 14. 建议实施顺序

1. 以 [Queue API v1](./api-v1.md) 和 OpenAPI 固定 JobSpec、batch types、error code 和 SDK contract；
2. 实现单个 SQLite shard 的 enqueue / claim / complete / fail / retry，以及 source pack 工具；
3. 实现 users、GitHub OAuth、role、共享 machine token、agent 登记和 local admin token；
4. 实现统一 shard HTTP(S) endpoint、可选 TLS pin、反向代理 cache 检查、tracker health check 和短期 shard access token；
5. 实现 PostgreSQL 控制面、vnode session、assignment，以及 shard 的 attach/activate/pause/resume/detach 状态机；
6. 实现 Go/Python worker SDK，跑通 worker 直连 shard 的 batch claim/complete/fail；
7. 用真实 JobSpec 做单 shard 和多 shard 压测，再决定 100k 目标所需的容量与 shard 数；
8. 实现 Job Receiver gateway、S3 source 载入、checkpoint 和 generation recovery；
9. 以独立协议接入外部 WARC Receiver receipt；WARC body、MegaWARC 和最终 sink
   上传不进入本仓库；
10. 最后补充管理页面诊断功能；其他 backend、computed routing 和逻辑重分片不属于第一版。
