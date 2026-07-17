# SavewebHQ v1 试运行手册

> 本文只描述当前实现。它不假设自动故障调度、自动 shard completion、WARC 上传或
> 本地持久存储 fallback。

## 1. 组件与信任边界

- Tracker：受信控制面，使用 PostgreSQL 和 R2 credential；生产只允许通过 HTTPS
  反向代理访问。
- Shard：审核过的志愿者运行，一个在线 queue shard 对应一个本机 SQLite 文件；
  Worker 直接访问其公网 HTTP(S) endpoint。
- Worker：只做出站连接，持有用户的 machine token，但不持有 R2 credential。
- WARC Receiver：独立服务，不在本仓库部署；WARC byte 不经过 Tracker 或 Shard。

第一批试运行应让每台 owner 主机只承担一个活跃 shard。多个 SQLite 文件共享同一台
机器不会线性扩容，实测基线见 [capacity.md](./capacity.md)。

## 2. 必需外部服务

1. PostgreSQL；
2. Cloudflare R2 或兼容 S3 API 的对象存储；
3. Tracker 的 HTTPS 域名与反向代理；
4. GitHub OAuth App；callback 为
   `https://TRACKER_HOST/auth/github/callback`；
5. 每个 Shard 的公网 endpoint，可以是 daemon 直出 TLS、Caddy，或 cloudflared。

R2 至少准备 source、checkpoint 和 Job Receiver 输出所需 prefix。为 checkpoint prefix
配置自动清理未完成 multipart upload 的 lifecycle；当前实现不会自动 GC 已完成但因
generation CAS 失败而未被引用的对象。

## 3. Secret 与持久目录

所有 secret 文件必须是 `0600`，且服务用户应独占其目录：

- Tracker signing key、web session secret、GitHub client secret；
- R2 access key ID 和 secret key；
- 每个贡献者自己的 machine token；
- Shard identity、直出 TLS private key；
- Shard `data-dir`，其中包含 SQLite 和 runtime 临时文件。

生成 Tracker secrets：

```bash
tracker keygen --out /var/lib/saveweb/tracker-signing.json --key-id key-2026-01
tracker web-keygen --out /var/lib/saveweb/tracker-web.secret
chmod 600 /var/lib/saveweb/*
```

Shard identity 与可选直出证书：

```bash
shard init --out /var/lib/saveweb/shard/identity.json
shard tls-init --key-out /var/lib/saveweb/shard/tls.key \
  --cert-out /var/lib/saveweb/shard/tls.crt --server-name shard.example
```

`tls-init` 输出的 SPKI pin 需要登记为 `--tls-spki-sha256`。经 Caddy/cloudflared 使用
public CA 时改用 `--tls-terminated-by-proxy`，通常不登记 pin。

## 4. Tracker 上线

每次升级先运行幂等 migration，再启动进程：

```bash
tracker migrate --database-url "$HQ_DATABASE_URL"

tracker serve \
  --listen 127.0.0.1:8080 \
  --database-url "$HQ_DATABASE_URL" \
  --public-url https://tracker.example \
  --signing-key-file /var/lib/saveweb/tracker-signing.json \
  --web-session-secret-file /var/lib/saveweb/tracker-web.secret \
  --github-client-id "$HQ_GITHUB_CLIENT_ID" \
  --github-client-secret-file /var/lib/saveweb/github-client.secret \
  --s3-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
  --s3-region auto \
  --s3-access-key-id-file /var/lib/saveweb/r2-access-key \
  --s3-secret-access-key-file /var/lib/saveweb/r2-secret-key \
  --checkpoint-prefix-uri s3://saveweb-checkpoints/checkpoints
```

Tracker 进程本身不终止公网 TLS；让 Caddy/nginx 只把 HTTPS origin 转发到
`127.0.0.1:8080`。不要把本地端口直接暴露到公网。上线探针为 `GET /healthz`；当前它
是进程 liveness，不替代 PostgreSQL/R2 外部监控。

首次部署用 `bootstrap-user` 创建一个带 GitHub 数值 ID 的管理员。之后用户、roles、
machine token、Project、Shard 和 Job Receiver 均从 Web 页面管理。生产不启用
`--allow-insecure-public-url`、`--allow-private-shard-endpoints` 或 `--allow-http-s3`。

## 5. Shard 上线

直出 TLS 示例：

```bash
shard serve \
  --listen :8081 \
  --admin-listen 127.0.0.1:9081 \
  --tracker-url https://tracker.example \
  --tracker-issuer https://tracker.example \
  --machine-token-file /var/lib/saveweb/shard/machine.token \
  --identity-file /var/lib/saveweb/shard/identity.json \
  --data-dir /var/lib/saveweb/shard/data \
  --endpoint https://shard.example:8081 \
  --endpoint-version 1 \
  --tls-cert-file /var/lib/saveweb/shard/tls.crt \
  --tls-key-file /var/lib/saveweb/shard/tls.key \
  --tls-spki-sha256 PIN_FROM_TLS_INIT \
  --checkpoint-interval-seconds 300
```

若 TLS 在本机反代终止，Queue daemon 必须改为只监听 loopback，例如
`--listen 127.0.0.1:8081 --endpoint https://shard.example --tls-terminated-by-proxy`，防止
绕过反代直接访问明文端口。Cloudflare 对 `/api/v1/queue/*` 和
`/api/v1/shard/endpoint-challenge` 必须 Bypass Cache，并保留 `Authorization`、
`Cache-Control` 和 response cache headers；不能自动重试或 redirect Queue POST。

`--checkpoint-interval-seconds` 默认是 `0`，即明确禁用。需要宕机恢复的生产 shard
必须设置非零周期，并根据可接受的重复执行窗口、SQLite 大小和临时磁盘预算调校。
Checkpoint 临时目录在 `<data-dir>/runtime/checkpoints`，一次 snapshot + 压缩期间要为
SQLite 副本和压缩文件留出空间。

本机管理页位于 `127.0.0.1:9081`。token 默认轮换到
`<data-dir>/runtime/local-admin.token`；远程查看使用 SSH tunnel，不开放管理端口。

## 6. 显式 Project / Shard 操作

### Attach 与运行

1. 用 source 工具生成 `.jobs.jsonl.zst`，上传为不可变 R2 object；
2. 在 Tracker `/admin/projects` 创建 `active` Project；
3. 提交 source URI、ETag、审核 owner 和 generation `1`，状态选 `loading`；
4. 等待状态成为 `active`，再启动 Worker。

冷 source 不必预先注册为 `staged`。长期 Project 可以保留多份 R2 source，在需要运行
时逐份 attach 为不同 shard ID。第一版不自动选择下一份。

### 计划暂停或迁移

1. 在 Tracker 把 `active` shard 设为 `draining`；
2. 等 owner 下一次 heartbeat 后，从 Shard 本机页确认不再增长 WIP，并等待 WIP 为 0；
3. 等至少一份新 checkpoint 成功发布；
4. 在 Tracker 把 `draining` 设为 `paused`。该事务清零 owner lease；
5. 若要恢复，选择新的/原来的审核 owner和更高 generation，提交 `recovering`；
6. 新 owner 校验并安装 checkpoint，成功后自动报告 `active`。

`draining → active` 可在同一 generation 恢复 claim。`active → paused` 被拒绝，避免跳过
显式 drain；没有 published checkpoint 的 pause 也会被拒绝。

### 宕机接管

1. 不修改 PostgreSQL，不手工清 lease；
2. 等管理页中的旧 `owner_lease_expires_at` 到期；
3. 选择 replacement owner，以更高 generation 提交 `recovering`；
4. 若 lease 仍有效，CAS 返回拒绝；等待到期后重试；
5. 若没有 checkpoint，第一版不能隐式回退 source，需人工决定是否建立新 shard。

旧 owner 即使还持有未过期的 presigned part URL，也不能发布 checkpoint pointer；新
owner只读取 Tracker 当前明确指向的 checkpoint。

## 7. 停机、升级与备份

- Tracker：先停止管理变更，再正常发送 SIGTERM；进程有 10 秒 graceful shutdown。
- Shard：先 drain，等待 WIP=0 和 checkpoint，再 pause，最后 SIGTERM；进程有 15 秒
  HTTP graceful shutdown。SIGTERM 本身不会额外触发 checkpoint。
- Shard 二进制原地升级且继续使用同一 data-dir/identity/endpoint 时保持 generation；
  endpoint 或 pin 改变时递增 `endpoint-version`。
- 定期备份 PostgreSQL。R2 source、Tracker 当前 checkpoint pointer 指向的对象和
  Receiver 输出使用独立保留策略；本机 SQLite 不是唯一恢复副本。
- 数据库 migration 只向前。升级前保留 PostgreSQL 备份，不在运行中混用未知新版
  SQLite schema；Shard 会拒绝打开比自身支持版本更高的文件。

## 8. 最低监控与告警

- Tracker/Shard `/healthz` 和进程重启次数；
- owner/worker 最后 heartbeat、owner lease 剩余时间、endpoint status；
- 每 shard 的 todo/wip/done/failed/reset_exhausted；
- checkpoint age、sequence、compressed size、失败日志和 R2 multipart 数量；
- source load/recovery error code；
- Queue 的 `backpressure`、`owner_lease_expired`、`stale_generation`、TLS/cache 错误；
- PostgreSQL 容量/连接，R2 请求错误、存储量和费用。

当前程序输出结构化 JSON 日志，但不内置 Prometheus exporter。第一批试运行可以由
进程管理器收集日志并轮询只读状态；增加 metrics 前先固定实际需要的少量指标。
