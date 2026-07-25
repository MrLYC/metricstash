# Metricstash 设计

[English](2026-07-24-metricstash-design.md) | 简体中文

**状态：** 已批准的设计，2026-07-24  
**范围：** 一个一次性、轻量的客户端：从多个端点采集选定的 Prometheus/OpenMetrics 文本指标并存入 SQLite。

## 目标

提供一个非守护进程 CLI，可在完整监控栈缺失的环境中由人工、cron 或 systemd timer 调用。它从多个目标采集选定的指标族，保留有用的目标身份与时间戳，并通过 SQLite 提供一个刻意受限、类 PromQL 的读取接口。

## 非目标

- 不提供后台服务、HTTP 查询 API、调度器管理、告警或 remote-write 支持。
- 不提供完整 PromQL 求值器：不支持函数、聚合、算术、联接或跨指标计算。
- 不支持 protobuf exposition、原生直方图采集、代理、OAuth、mTLS 或通用服务发现。
- 不保存原始响应负载，也不自动执行保留期清理。

## 运行时与依赖边界

- Python `>=3.11`。
- 开发和打包使用 `uv`；目标主机不需要 `uv`。
- 运行时依赖为 `aiohttp` 和 `prometheus-client`。
- `google-re2` 是可选依赖，默认不安装，因为表达式由受信任的本地运维人员输入。
- 不引入 ORM、Pydantic、Rich 或完整 PromQL 解析器。
- 交付包含 `metricstash` 控制台命令的 wheel。

## 资源边界

预期部署环境为 1 vCPU、1 GiB 内存、500 MiB 可写磁盘的容器。

实现应将正常 RSS 控制在 128 MiB 以下、受压 RSS 控制在 256 MiB 以下。内建默认值刻意保守，并允许覆盖：

| 设置项 | 默认值 |
|---|---:|
| 并发物理目标请求数 | 4 |
| 请求超时 | 10 秒 |
| 重试次数 | 0 |
| 解压响应上限 | 8 MiB |
| 每个物理目标的样本数 | 25,000 |
| 每个样本的标签数 | 32 |
| 单个标签值大小 | 1 KiB |
| 每个主机名解析出的 IP 数 | 16 |
| 查询 series / 样本结果上限 | 1,000 / 10,000 |

刻意不提供主动 SQLite 容量保护。部署方负责容量管理，并且必须显式调用 `prune`。磁盘写满和不可恢复的 SQLite I/O 错误会作为致命错误终止当前采集。

## 配置模型

配置格式为 TOML，采用 `task -> target` 分组。唯一必需的语义字段是任务的 `system`、任务的 `module`、指标的 `name` 与 `type`，以及每个目标的 `url`。所有运行参数都有默认值。

```toml
[collector]
max_concurrency = 4

[[tasks]]
system = "${context.system}"
module = "api"
job = "api" # 省略时默认等于 module
honor_labels = true
labels = { cluster = "${context.cluster}" }

[tasks.http]
timeout = "10s"
retries = 2
bearer_token_file = "/run/secrets/api-token"

[[tasks.metrics]]
name = "http_requests_total"
type = "counter"

[[tasks.metrics]]
name = "http_request_duration_seconds"
type = "histogram"
required = true

[[tasks.targets]]
url = "https://api.example.com/metrics"
labels = { role = "primary" }
```

### 模板与标签

- 一次运行通过可重复的 `--context key=value` 参数提供动态值。
- 模板使用 `${context.key}`，也可以引用任务与目标字段。DNS 展开后还可以使用 `${target.resolved_ip}` 和 `${target.dns_name}`。
- 模板值缺失属于发起请求之前的配置错误。无网络的 `validate` 会检查静态模板语法和已知名称；DNS 派生的值在采集时校验。
- 任务标签会由目标标签继承；目标标签覆盖任务标签。
- `system` 和 `module` 是必需的业务标签。`job` 默认为 `module`；若未提供，`instance` 默认为物理端点地址。
- 自定义标签可使用任何有效标签名，包括 `job`、`instance` 和 `__*`。
- 默认 `honor_labels=true`：端点提供的标签优先，采集器标签只补齐缺失的键。`honor_labels=false` 时，会先将冲突的端点标签改名为 `exported_<name>`，再应用采集器标签。
- `resolved_ip` 是唯一有意的例外：采集器始终在标签合并后追加它，因此 DNS 展开的 IP 不会折叠为同一个 series。

### HTTP 配置

- 支持的认证方式：无认证、静态 headers、Basic Auth 和 Bearer token 文件。秘密绝不存入 SQLite。
- 默认启用 TLS 校验。
- 仅对网络瞬态失败以及 HTTP 408、429 和 5xx 重试；其他 4xx 响应不重试。
- 目标可以选择接受缺失的 `Content-Type`；显式的非文本内容类型会被拒绝。

## 采集流水线

```text
collect --config --db --context
  -> 校验配置和模板
  -> 将逻辑目标展开为物理 IP 目标
  -> 有界并发的 HTTP 抓取
  -> 流式解析并过滤允许的指标族
  -> 串行 SQLite 写入，每个物理目标一个事务
  -> 审计记录和合成采集指标
```

### DNS 行为

- 每次调用只解析主机名一次。
- 优先使用 AAAA 记录。只要存在至少一条 AAAA 记录，就只采集其 IPv6 地址；IPv6 连接或 HTTP 失败后不回退 IPv4。
- 仅在没有 AAAA 记录时使用 A 记录。
- 对地址去重；当逻辑目标超过配置的地址上限时使其失败；每个结果 IP 独立采集。
- TCP socket 连接所选 IP，同时保留原始主机名用于 HTTP `Host`、HTTPS SNI 和证书验证。

### 指标解析与接纳

- 只支持 Prometheus/OpenMetrics 文本。protobuf 和原生直方图不受支持。
- 指标配置是类型的事实来源。端点提供的 `# TYPE`（若存在）必须与配置一致。
- 对允许的经典 histogram 基础名称，采集 `<base>_bucket`、`<base>_sum` 和 `<base>_count`。对允许的 summary 基础名称，采集 `<base>{quantile=...}`、`<base>_sum` 和 `<base>_count`。
- 指标族默认是必需的。缺失必需指标族会使该物理目标失败。
- 必需的 histogram 和 summary 指标族必须结构完整。histogram 必须包含与 `_count` 相等的 `+Inf` bucket。
- 忽略配置 allow-list 之外的格式错误指标行。允许的指标族出现格式错误或不一致时，该物理目标失败。
- 增量读取和解析。校验 `prometheus-client` 能够支持这一路径；否则只增加一个小型增量适配器，不能缓冲完整响应。

### 成功与失败语义

- 每个物理目标的采集样本会批量写入单个 SQLite 事务。仅在成功时提交；失败则回滚该目标。
- 即使其他目标失败，成功的物理目标仍会提交。任意部分目标失败使 `collect` 以退出码 1 结束。
- 每个物理目标都会创建一条 `scrapes` 审计记录。
- 将固定的合成指标插入同一 series/sample 存储：`metricstash_up`、`metricstash_scrape_duration_seconds`、`metricstash_scrape_samples` 与 `metricstash_scrape_attempts`。
- 合成指标仅使用确定性的采集器标签：任务标签、目标标签、`job`、`instance` 和 `resolved_ip`；无需端点自身标签也能记录失败。

## SQLite 存储模型

SQLite 使用 WAL 模式。读查询可以与一个写入者并行，但文件锁会拒绝并发的 `collect`、`prune` 和迁移命令。每批采集完成后执行 checkpoint，以防 WAL 无限增长。

| 表 | 用途 |
|---|---|
| `schema_migrations` | 已应用的 schema 版本；已有数据库只能通过 `db migrate` 迁移 |
| `runs` | 一次命令调用、其 context、开始时间和工具版本 |
| `scrapes` | 每次物理目标/IP 尝试：耗时、状态、重试、HTTP 状态和截断的错误摘要 |
| `metric_metadata` | 每个 system/module/指标族最新的 `HELP`、`UNIT` 和类型 |
| `series` | 实际指标名、指标族基础名称、配置类型和规范化后的完整标签集 |
| `series_labels` | 用于选择和正则过滤的标签索引行 |
| `samples` | 带时间戳的值及其最新 `scrape_id` |

`series` 由实际指标名和规范化、已排序的标签编码共同唯一确定。`samples` 使用：

```text
PRIMARY KEY(series_id, sample_timestamp_ms)
```

后续采集发出相同 series 和时间戳时，直接使用 UPSERT。有限值存入 `value REAL`；`NaN`、`+Inf` 和 `-Inf` 存入 `value_repr TEXT`。检查约束保证两者恰有一个存在。保留最近的 `scrape_id` 以便审计追溯。

当 `honor_timestamps=true`（默认）时，样本时间戳遵从目标时间戳；否则使用采集时间戳。采集开始和结束时间仍是审计字段。

## 查询与查看模型

选择器语法支持且只支持一个必需指标名、使用 `=`、`!=`、`=~` 和 `!~` 的可选标签匹配器，以及可选范围选择器，例如：

```text
http_requests_total{cluster="prod"}[5m]
```

- 正则使用 Python `re.fullmatch`；由于本地运维人员受信任，这是可接受的。无效正则属于使用错误。
- 缺失标签按空字符串处理，匹配 PromQL 风格的负匹配语义。
- 范围查询返回 `(at - range, at]` 内的原始样本。
- 即时查询返回 `--at` 时刻或之前的最新样本，没有隐式的 5 分钟 lookback。输出展示样本时间、采集时间和样本年龄；`--max-age` 可过滤过旧样本。
- 超出结果上限会显式失败，而不是静默截断。
- 查询默认以表格输出，并支持 `--format json`。
- `inspect histogram` 按除 `le` 外的全部标签对指标族成员分组；`inspect summary` 按除 `quantile` 外的全部标签分组。二者默认显示即时快照，可选展示时间范围，但绝不计算 rate、分位数或聚合。

## 命令接口

```text
metricstash validate --config CONFIG --context key=value
metricstash collect --config CONFIG --db DB --context key=value
metricstash query --db DB 'metric{label="x"}[5m]' --at TIME --format table|json
metricstash inspect histogram|summary --db DB 'metric_family{...}' --at TIME
metricstash prune --db DB --older-than 30d
metricstash db migrate --db DB
```

- 路径始终显式指定；不会在用户主目录下隐式创建状态。
- `validate` 不访问网络。
- `prune` 是显式操作，支持 `--older-than` 或 `--before`；`VACUUM` 是另一项独立的显式操作。
- 没有命令管理 cron 或 systemd。文档只能提供调用示例。

## 退出码与中断行为

| 代码 | 含义 |
|---:|---|
| 0 | 所有请求的目标采集成功 |
| 1 | 至少一个物理目标失败；成功结果已保留 |
| 2 | 配置、模板或选择器错误；没有发生采集变更 |
| 3 | SQLite 锁、磁盘写满或其他不可恢复的内部/I/O 错误 |
| 130 | 被 SIGINT 中断；活动事务回滚，更早的提交保留 |

普通命令结果输出到 stdout。诊断信息输出到 stderr，并可选以 JSON 日志格式输出。绝不在日志或审计行中输出秘密值。

## 验证策略

- 对配置默认值、模板、标签优先级、类型声明和错误信息使用单元测试夹具。
- 使用本地 `aiohttp` 服务器进行 HTTP 集成测试，覆盖超时、重试、响应上限、Content-Type 和认证行为。
- DNS 测试覆盖 IPv6 优先、仅 A 记录的回退、地址上限、IP 身份和保留主机名语义。
- 解析器夹具覆盖 counter、gauge、histogram、summary、时间戳、特殊值、选中的格式错误行和被忽略的未选中格式错误行。
- SQLite 测试覆盖事务回滚、UPSERT 冲突行为、锁、清理、迁移、标签、范围/即时选择器以及 inspect 输出。
- 在 1 CPU / 1 GiB / 500 MiB 容器配置下，使用有代表性的 25,000 样本响应进行端到端测试。
