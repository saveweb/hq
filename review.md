# SavewebHQ 第一版设计评审

> 本文只保留第一版真正需要处理的问题，不再把所有未来风险当成当前开发任务。

## 1. 已确认的范围

以下决定已经定案：

1. 第一版只实现 `explicit` Project；
2. Shard owner 必须经过审核和认证；系统假设它可能宕机或失联，但不会主动篡改 SQLite 或 checkpoint；
3. 普通志愿者只作为 job worker，不能直接访问 SQLite，也不持有 S3/R2 credential；
4. Queue 输入只来自不可变 S3 source，以及和父任务在同一个 SQLite transaction 中写入的同 shard derived jobs；
5. 第一版不开放向运行中 queue 任意直接 enqueue；新增独立任务通过“上传新 source → attach 新 shard”进入系统；
6. Receiver batch 由 HQ gateway 有界压缩并写入 R2，普通 worker 不直接上传；
7. 故障恢复只保证 crash fault 下的 at-least-once，允许 checkpoint 回滚造成重复执行；
8. Saveweb 管理员、shard owner 志愿者和 worker 志愿者统一通过 tracker 的 GitHub OAuth 登录，role 由 tracker 单独授权；
9. 每个用户在 tracker 后台拥有一个长期、可重置的 machine token，其全部 shard/worker 机器共用该 token 上线；
10. shard 和 worker 都只在 `127.0.0.1` 提供本机管理页，使用启动时随机生成或由 `SAVEWEB_LOCAL_ADMIN_TOKEN` 指定的独立 local admin token；
11. shard owner 必须提供一个公网 HTTP(S) endpoint URI；它可以由 daemon 直出、四层端口转发，或由 Caddy/cloudflared 等七层反向代理暴露，tracker 不提供 queue relay；
12. worker 从 tracker 取得 endpoint、generation、可选 SPKI pin 和短期 shard access token 后直连 shard；machine token 不发送给 shard；
13. Source 固定为 `jobs-jsonl-zstd-v1`，JobSpec 显式包含 `id` 和 `url`；默认 ID 是 `SHA-256(type + NUL + url)`；
14. claim/complete/fail 使用 [Queue API v1](./api-v1.md) 的 batch-first HTTP/JSON contract、逐项结果和稳定 error code；
15. worker 不需要公网 endpoint，始终只做出站连接；shard endpoint 背后的部署方式不改变 Queue API。
16. WARC 由 worker 直接上传到 Saveweb 自行维护的独立 WARC Receiver，不经过
    tracker、shard 或 Job Receiver；Receiver 在 WARC 合法且已持久接收后签发
    receipt，只有 Worker 可以携 receipt 完成自己持有的 job attempt；
17. Receipt ACK 是 job 与归档链路的边界。MegaWARC、Internet Archive 等最终 sink
    上传、重试和失败只作为文件状态由 tracker 追踪，不反向改变 job；第一版不增加
    `uploading` job 状态，也不实现 Receiver → Shard 回调。

这些决定已经排除了原评审中最复杂的部分：tracker queue relay、自动 NAT traversal、恶意 shard owner、computed routing、跨 shard outbox、算法迁移、在线重分片、receiver 自动 Pipeline 和全局 exactly-once。

## 2. 第一版仍需解决的问题

### 2.1 真实容量压测

100,000 completed jobs/s 是多台公网 shard 的 aggregate 目标，不是单 tracker、单 shard 或 100,000 个独立 HTTP request/s 的承诺。

默认 batch 64 时，100k completed jobs/s 约等于分散在各 shard 上的 3,125 个 claim + complete/fail HTTP requests/s。Tracker 不转发 queue body，只负责 session、assignment、access token 和控制面，因此其 1 Gbps 不是 queue 热路径瓶颈。

实现后仍必须用真实 JobSpec 测量：

- 单 SQLite shard 的 batch claim 和 complete/fail 吞吐；
- 每台 owner 的 CPU、SSD、上下行和 p95/p99 latency；
- 达到 aggregate 100k 所需的 shard 数量；
- Job Receiver、checkpoint 控制流量与 tracker/HQ 1 Gbps 的竞争；WARC body 直达
  独立 Receiver，不占 tracker 带宽。

[本机容量基线](./capacity.md) 已覆盖真实 SQLite transaction、Echo/JSON/access-token
数据路径，并给出默认 batch 64 下 15 台满载或 29 台按 50% 规划的初步模型。它也证明
同一台机器增加逻辑 shard 不会线性扩展。多台公网 owner、TLS/WAN、p95/p99、真实
payload 和 checkpoint 并发仍是生产验收项。

### 2.2 Checkpoint 周期与本地磁盘预算

`VACUUM INTO` 会额外生成一份接近完整 SQLite 大小的文件，之后还有压缩临时文件。需要用真实 shard 定下：

- checkpoint 时间周期；
- 最大可接受重复执行窗口；
- shard 最大 SQLite 大小；
- owner 至少需要预留多少临时磁盘。

这属于容量参数，不需要再增加协议。

### 2.3 Reset 策略

需要给出 `max_resets` 默认值，并区分：

- worker lease 超时；
- 可重试抓取错误；
- owner 宕机恢复。

Owner 宕机恢复不增加 reset count；超限后使用已经定义的 `reset_exhausted`。

### 2.4 WARC Receipt 接入

WARC body 上传已经明确排除在 SavewebHQ 之外。本仓库只需在外部 WARC Receiver
协议稳定后实现有界 receipt 的解析和受信签名验证，并让 Go/Python Worker SDK 在
普通 complete 中携带 receipt。Receiver 已接受但 Worker 未 complete 时允许 job
重跑和 WARC 重复，不为此增加分布式事务、回调或 GC 协议。

### 2.5 Receiver 对象与 Stage 2 工具

Receiver record encoding 已与 source 统一为 `jobs-jsonl-zstd-v1`。第一版固定为
“一次成功请求对应一个不可变对象”，默认最多 1000 条 job、8 MiB HTTP request、
16 MiB 压缩对象；没有后台 flush、manifest 或自动 seal。`source merge` 已按显式
输入顺序实现流式 merge、按 ID 去重、identity conflict 检查和定长 split。仍需
根据真实 discovered jobs 流量调校这些上限及工具的内存预算。

### 2.6 国内用户的登录备用方案

标准 GitHub OAuth 已经由 tracker 负责 code 换 token 和 `GET /user`，因此 shard/worker daemon 不需要访问 GitHub；但用户浏览器仍必须访问 GitHub 授权页。GitHub device flow 也要求访问 `github.com/login/device`，不能解决授权页不可达。

第一版不反向代理 GitHub 登录页面、密码或 cookie。需要根据真实用户情况二选一：

- GitHub OAuth 作为唯一入口，接受用户需要自行解决浏览器访问；或
- 增加一个独立、可审计的国内身份源或管理员邀请码。

若确定需要备用入口，再泛化 `users` 的身份字段；不提前把多身份系统加入第一版 schema。

## 3. 已经补上的必要协议规则

以下规则已经进入 `design.md` 或 `api-v1.md`，不再作为开放问题：

- JobSpec 必须包含 `id` 和 `url`；相同 ID 的身份输入不同时返回 `identity_conflict`，不能覆盖；
- 每次 claim 生成不可复用的 `attempt_id`，迟到结果不能命中新 attempt；
- claim、complete 和 fail 都按 batch 使用一个 SQLite transaction；complete/fail 在 transaction 内用预校验或 savepoint 隔离，逐项返回结果；
- 单个 complete item 的 outcome、parent `done` 和同 shard discovered jobs 在一个 transaction 中提交；
- `stale_generation`、`stale_attempt`、`lease_expired` 和 `session_expired` 的旧 outcome 直接丢弃，不转发到新 owner；
- shard 公网 endpoint 统一为 HTTP/JSON URI；HTTPS 有 pin 时校验 SPKI，无 pin 时校验 public CA 与 hostname；HTTP 只能由双方显式允许且不作为生产默认值；
- worker 直连 shard 使用 tracker Ed25519 签发的短期 access token，scope 绑定 session、Project、shard、owner 和 generation；
- shard queue 与 challenge API 只使用 POST，双向发送 `no-store` cache headers；Cloudflare path 还必须配置 Bypass Cache，worker 只接受 `CF-Cache-Status: DYNAMIC/BYPASS` 或 header 缺失；
- Caddy/cloudflared 等反向代理必须透传认证 header 和 body，queue client 不跟随 redirect；
- machine token 只认证 user 的机器到 tracker，shard access token 只认证 worker 到指定 shard，local admin token 只管理当前进程；
- 同一 shard 同时只允许一个 checkpoint upload；R2 对象上传完成不等于 checkpoint 生效，PostgreSQL generation CAS 才是提交点；
- Shard 进入 `completed` 前必须发布最终 checkpoint 并释放 owner lease；
- Receiver 只有在 HQ gateway 确认 R2 PUT 成功后才向 worker 返回成功。
- WARC Receiver 只有在文件合法且持久接收后才签发 receipt；WARC job 在此之前
  保持 `wip`，之后仍由 Worker 使用普通 complete 进入 `done`；
- 最终 WARC sink 状态与 job 状态解耦，tracker 只追踪，不参与 WARC body 转发；

## 4. 明确不做

以下内容不应出现在第一版代码、schema 或配置中：

- tracker queue relay、反向 WebSocket 和自动 NAT traversal；
- worker 公网 endpoint，以及 tracker/shard 主动 push JobSpec 或等待 worker 执行完成；
- `computed` Project 和分片算法插件；
- 跨 shard derived jobs；
- 运行中替换算法或逻辑重分片；
- 不可信 shard owner 的 Byzantine 防护；
- PostgreSQL、Redis 等其他 queue backend；
- fallback shard；
- receiver manifest、seal、自动 stage/pipeline；
- 任意外部动态 queue enqueue；
- checkpoint 增量 diff、历史 catalog 和自动回退链；
- exactly-once outcome 或 WARC。
- WARC Receiver 直接修改 job、按 filename 回调 Shard，或为 WARC 增加
  `uploading` / `upload_pending` job 状态；

如果未来重新引入其中任何一项，应针对该项单独做设计评审，而不是提前把复杂度留在第一版接口里。

## 5. 建议开工顺序

1. 以 `api-v1.md` 和 OpenAPI 固定 HTTP types、error code 和 SDK contract；
2. 实现单 SQLite shard、source pack，以及 batch claim/complete/fail；
3. 实现 GitHub OAuth、user/role、共享 machine token、agent 登记和 local admin token；
4. 实现统一 shard HTTP(S) endpoint、可选 TLS pin、反向代理 cache 检查、tracker health check 和短期 shard access token；
5. 实现 PostgreSQL Project/shard 状态、vnode session 和 assignment；
6. 实现 Go/Python SDK，跑通 worker 直连 shard；
7. 做单 shard 与多 shard 压测，确定 100k 目标的真实容量；
8. 最后实现 R2 checkpoint/recovery、Job Receiver、WARC receipt 接入和完整管理页；
   WARC body、MegaWARC 与最终 sink 上传在独立 Receiver 项目中实现。
