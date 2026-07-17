# SavewebHQ 本机容量基线

> 状态：2026-07-17 的开发机基线，不是 100,000 completed jobs/s 的分布式生产验收。

## 1. 测量口径

目标口径是 **completed jobs/s**，不是把每个 job 拆成独立 HTTP 请求后的字面 QPS。
一次正常完成至少包含一个 claim 和一个 complete 请求；batch 为 `B` 时，`N`
completed jobs/s 约产生 `2N/B` HTTP requests/s。WARC、checkpoint 和 Job Receiver
对象不经过 Queue API，也不计入这里。

基准使用真实 SQLite queue transaction、generation/attempt/lease 校验和小型 outcome。
HTTP 基准还包括 Echo handler、Ed25519 access-token 验证、JSON 编解码、keep-alive
loopback TCP client/server。SQLite 使用与生产代码相同的 WAL、`synchronous=NORMAL`、
8 个连接。它没有模拟公网 RTT、TLS/反向代理、WARC 上传、discovered jobs、大型 attrs
或 checkpoint 并发。

测试环境：

- Linux amd64，Go 1.26.5；模块最低版本仍是 Go 1.25；
- Intel Core i5-12450H，8 cores / 12 logical CPUs；
- workspace 位于 NVMe 上的 btrfs；
- 每个样本固定执行 200 次 claim + complete cycle；SQLite 取 5 个样本中位数，
  HTTP 取 3 个样本中位数。

可重跑：

```bash
make bench-capacity
```

`BENCH_TIME` 和 `BENCH_COUNT` 可覆盖默认的 `200x` 和 `3`。基准预先写入所需 jobs，
计时区间只包含 claim + complete。

## 2. 结果

### 2.1 SQLite queue 层

| 模式 | Batch | 中位 jobs/s |
| --- | ---: | ---: |
| 单 SQLite shard，顺序 cycle | 64 | 9,025 |
| 单 SQLite shard，顺序 cycle | 256 | 8,236 |
| 同机 4 个 SQLite shard，各 1 个 goroutine | 64 | 10,601 |
| 同机 4 个 SQLite shard，各 1 个 goroutine | 256 | 10,671 |

同一台机器上把一个 shard 变成四个文件只提高到约 10.6k jobs/s，没有四倍扩展。
这里已经受到共享 CPU、内存和存储的限制；逻辑 shard 数不能代替独立 owner 容量。

### 2.2 完整 HTTP queue 路径

| 模式 | Batch | 中位 jobs/s | 中位 HTTP requests/s |
| --- | ---: | ---: | ---: |
| 单 shard，单顺序 worker | 1 | 911 | 1,821 |
| 单 shard，单顺序 worker | 64 | 6,987 | 218 |
| 单 shard，单顺序 worker | 256 | 7,263 | 57 |
| 单 shard，并发 workers | 1 | 1,055 | 2,109 |
| 单 shard，并发 workers | 64 | 5,218 | 163 |
| 同机 4 shards，各 1 个 worker | 1 | 2,384 | 4,767 |
| 同机 4 shards，各 1 个 worker | 64 | 9,020 | 282 |
| 同机 4 shards，各 1 个 worker | 256 | 9,742 | 76 |

Batch 是决定性因素。Batch 1 主要测量小请求的 transaction、JSON 和 HTTP 固定成本；
默认 batch 64 把它们摊薄。单 SQLite shard 上盲目增加并发 writer 没有提高 batch 64
吞吐，反而增加争用，因此 Worker SDK 应优先攒 batch，而不是无限增加并发请求。

## 3. 100k 容量模型

默认 batch 64 时：

```text
100000 completed jobs/s ÷ 64 × 2 ≈ 3125 HTTP requests/s
```

用完整 HTTP 单-shard 中位数 `6,987 jobs/s` 估算：

- 基准机型满载理论下限：`ceil(100000 / 6987) = 15` 台独立 shard owner；
- 每台只规划到实测值 50%：`ceil(100000 / (6987 × 0.5)) = 29` 台；
- 50% 规划下每台约处理 3,493 jobs/s，即约 109 个 batch HTTP requests/s。

因此第一版可暂用“约 29 台与本基准机相当、每台一个活跃 shard”作为 100k 的容量
起点，而不能承诺 15 台一定够。志愿者硬件、SQLite 文件增长、公网 RTT/TLS、真实
outcome 大小和任务分布都会改变这个数字；性能较差的 owner 应按自身实测权重计入。

若有人把目标误解为 100,000 个独立 HTTP requests/s，batch 1 的单机并发中位数仅
为 2,109 requests/s：满载约需 48 台，按 50% 规划约需 95 台；而 100,000 个
completed jobs/s 若完全不 batch，还会产生约 200,000 requests/s。这不是 v1 的目标。

## 4. 结论与未完成验收

本机结果支持现有拓扑：Queue body 不经过 tracker，横向容量来自独立公网 shard owner，
每个 owner 限制 SQLite writer 并使用批量请求。Tracker 的 1 Gbps 链路不是上述
queue metadata 热路径的一部分。

以下项目仍需在生产候选环境验收，不能由本机 loopback 基准代替：

- 多台公网 owner 的 aggregate 100k 持续压测，以及 p50/p95/p99 latency；
- TLS、Caddy/cloudflared、真实 WAN RTT 和丢包下的吞吐；
- 大型 attrs/outcome、derived jobs、fail/lease extension 混合流量；
- SQLite 随文件增长的性能，以及 checkpoint 与 queue 同时运行时的抖动；
- Tracker、Job Receiver 和 checkpoint 控制流量共享 1 Gbps 时的独立容量测试。
