# Metricstash

简体中文 | [English](README.en.md)

Metricstash 是一个轻量、一次性执行的 Prometheus/OpenMetrics 文本指标采集客户端。它不启动后台服务：每次 `collect` 读取 TOML 配置、并发抓取多个目标、把允许的指标族写入 SQLite，然后退出。

它适合 1 vCPU / 1 GiB 内存 / 500 MiB 可写磁盘这类受限环境中作为缺失监控系统时的补充采样工具，不替代 Prometheus、告警或长期时序数据库。

## 安装与构建

开发环境使用 `uv`：

```bash
uv sync
uv run metricstash --help
uv run pytest -q
uv build
```

目标主机不需要安装 `uv`。构建后可安装 wheel：

```bash
python -m pip install dist/metricstash-0.1.0-py3-none-any.whl
metricstash --help
```

运行时依赖仅为 `aiohttp` 与 `prometheus-client`。不使用 ORM、Pydantic、Rich 或完整 PromQL 解析器。

## 最小配置

```toml
[[tasks]]
system = "prod"
module = "api"
labels = { cluster = "prod" }

[[tasks.metrics]]
name = "http_requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
```

只需指定任务的 `system`、`module`、每个指标的 `name`/`type`，以及目标 `url`。其余采集参数均有默认值。完整配置见 [`docs/config.example.toml`](docs/config.example.toml)。

## 示例

[`examples/`](examples/) 提供可完全离线运行的端到端示例，几分钟内跑通 `collect → query → inspect → prune`，并包含 cron/systemd 调度与 PyPI 发布指引。

通过重复的 `--context KEY=VALUE` 提供动态任务上下文；模板使用 `${context.KEY}`。任务/目标标签还可使用 `${task.system}`、`${task.module}`、`${task.job}`、`${target.dns_name}` 与 `${target.resolved_ip}`。缺失的上下文变量会在发起 HTTP 请求前报错；不会读取环境变量。

## 命令

```bash
# 仅做网络无关的配置检查
metricstash validate --config metricstash.toml --context system=prod

# 一次采集；数据库路径始终显式指定
metricstash collect --config metricstash.toml --db /var/lib/metricstash/metrics.db

# 即时查询：取 --at 时刻或之前的最后一个样本，不带隐式 5 分钟 lookback
metricstash query --db /var/lib/metricstash/metrics.db \
  'http_requests_total{cluster="prod"}' --at now

# 范围查询：返回 (at - 5m, at] 内的原始样本
metricstash query --db /var/lib/metricstash/metrics.db \
  'http_requests_total{cluster="prod"}[5m]' --format json

# 只展示已存的原始 bucket/quantile/count/sum，不计算 rate 或分位数
metricstash inspect histogram --db /var/lib/metricstash/metrics.db \
  'http_request_duration_seconds{cluster="prod"}'
metricstash inspect summary --db /var/lib/metricstash/metrics.db \
  'rpc_duration_seconds{cluster="prod"}'

# 清理独立于采集；不会由 collect 自动触发
metricstash prune --db /var/lib/metricstash/metrics.db --older-than 30d
metricstash prune --db /var/lib/metricstash/metrics.db --before 2026-07-24T00:00:00Z
metricstash db vacuum --db /var/lib/metricstash/metrics.db

# 现有数据库的升级必须显式执行
metricstash db migrate --db /var/lib/metricstash/metrics.db
```

`query` 默认输出表格，也支持 `--format json`。时间参数接受 `now` 或 RFC3339（例如 `2026-07-24T00:00:00Z`）。范围与 `--max-age` 使用 `ms`、`s`、`m`、`h`、`d`、`w` 单位。

## 选择器边界

支持且只支持一个明确指标名、可选标签匹配器和可选范围：

```text
http_requests_total{cluster="prod",pod!~"canary-.*"}[5m]
```

标签操作符为 `=`、`!=`、`=~`、`!~`；正则使用 Python `re.fullmatch`。缺失标签按空字符串处理，因此负匹配具有 PromQL 风格语义。函数、聚合、算术、联接、多指标运算和 HTTP 查询 API 均不支持。结果超过默认 1,000 个 series 或 10,000 个样本时会显式失败，不会静默截断。

## 采集与标签语义

- 每个逻辑目标均会在一次 `collect` 中解析 DNS。只要存在 AAAA 记录，就采集全部去重后的 IPv6 地址，绝不在 IPv6 连接/HTTP 失败后回退 IPv4；只有没有 AAAA 时才使用 A 记录。默认最多 16 个 IP，可在 `[collector] max_resolved_ips` 调整。
- 每个 IP 是独立物理目标。连接 TCP 时使用该 IP，但 HTTP `Host`、HTTPS SNI 和证书验证仍使用原始域名。`resolved_ip` 总是由采集器在标签合并后写入，避免不同 IP 的 series 合并。
- 任务标签会被目标标签覆盖。`system`、`module` 自动成为标签，`job` 默认等于 `module`；缺少 `instance` 时默认使用 `resolved_ip:port`（IPv6 用方括号）。
- 默认 `honor_labels = true`：端点暴露的同名标签优先，采集器只补缺失标签。设为 `false` 时，端点冲突标签改名为 `exported_<name>` 后再应用采集器标签。
- 允许自定义标签名，包括 `job`、`instance` 和 `__*`。

配置中的指标 allow-list 是精确名称匹配。类型由配置决定；端点有 `# TYPE` 时必须一致，缺失 `# TYPE` 可以接受。counter、gauge、untyped、经典 histogram 与 summary 均支持；protobuf 与 native histogram 不支持。histogram 必须包含和 `_count` 相等的 `+Inf` bucket；summary/histogram 不完整时整个物理目标回滚。

解析、HTTP 读取和 SQLite 写入都按流式批次进行。原始响应体和任何认证秘密不会保存到 SQLite。

## SQLite 与资源默认值

SQLite 使用 WAL。查询可以和单个写入者并存，但 `collect`、`prune`、迁移和 `vacuum` 通过文件锁拒绝并发执行。series 身份是“实际样本指标名 + 全部规范化标签”；同一 `series_id + sample_timestamp_ms` 的后来样本直接 UPSERT 覆盖值和 `scrape_id`。

有限浮点数存入 `value REAL`；`NaN`、`+Inf`、`-Inf` 存入 `value_repr TEXT`，两者恰有一个存在。每个物理目标有独立事务：失败目标的业务样本回滚，但成功目标保留，且每个目标会记录 scrape 审计和以下采集器指标：

- `metricstash_up`
- `metricstash_scrape_duration_seconds`
- `metricstash_scrape_samples`
- `metricstash_scrape_attempts`

默认上限：并发 4、请求超时 10 秒、重试 0、解压响应 8 MiB、每物理目标 25,000 样本、每样本 32 标签、标签值 1 KiB、DNS 16 IP。没有自动容量清理或磁盘配额守卫；请按需要显式运行 `prune`。磁盘满、SQLite I/O 错误或锁冲突会停止本次采集。

## HTTP 与退出码

HTTP 支持无认证、静态 headers、Basic Auth 和 Bearer token 文件；TLS 校验默认开启。只对网络暂态错误以及 HTTP 408、429、5xx 进行指数退避重试。显式非文本 `Content-Type` 会被拒绝；目标可单独允许缺失的 `Content-Type`。

| 退出码 | 含义 |
| --- | --- |
| 0 | 所有物理目标成功 |
| 1 | 至少一个物理目标失败，成功结果已保留 |
| 2 | 配置、模板、时间或选择器使用错误；未开始采集变更 |
| 3 | SQLite 锁、磁盘/I/O 或其他不可恢复错误 |
| 130 | SIGINT；活动目标事务回滚，已提交目标保留 |

不管理 cron 或 systemd。可由外部调度器调用，例如：

```text
*/5 * * * * metricstash collect --config /etc/metricstash.toml --db /var/lib/metricstash/metrics.db
```

## 非目标

没有后台服务、调度器管理、告警、remote-write、原始响应保留、代理、OAuth、mTLS、通用服务发现、HTTP 查询 API、完整 PromQL、protobuf exposition 或 native histograms。
