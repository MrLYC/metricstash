# Metricstash 示例 / Examples

本目录提供可离线运行的端到端示例，帮助你在几分钟内跑通 `collect → query → inspect → prune` 全流程。

## basic：本地静态端点

`basic/` 用一个静态 Prometheus 文本文件模拟被抓取的 exporter，完全离线即可运行。

- [`basic/metricstash.toml`](basic/metricstash.toml)：最小配置，采集一个 counter 和一个 histogram。
- [`basic/sample_metrics.txt`](basic/sample_metrics.txt)：静态的 Prometheus/OpenMetrics 文本样本。

### 1. 准备

在仓库根目录用 `uv` 安装依赖，或直接安装已构建的 wheel：

```bash
uv sync
```

### 2. 启动一个本地指标端点

`python -m http.server` 会把当前目录的 `.txt` 文件以 `text/plain` 暴露，正好满足 metricstash 的 Content-Type 要求：

```bash
cd examples/basic
python -m http.server 9000
```

保持该终端运行，`sample_metrics.txt` 现在可通过 `http://127.0.0.1:9000/sample_metrics.txt` 访问。

### 3. 校验配置（无需网络）

```bash
uv run metricstash validate --config examples/basic/metricstash.toml
```

### 4. 采集一次

```bash
uv run metricstash collect \
  --config examples/basic/metricstash.toml \
  --db /tmp/metricstash-demo.db
```

预期输出类似 `run=1 successful_targets=1 failed_targets=0`。

### 5. 查询

```bash
# 即时查询（表格）
uv run metricstash query --db /tmp/metricstash-demo.db \
  'http_requests_total{cluster="local"}'

# JSON 输出，便于脚本处理
uv run metricstash query --db /tmp/metricstash-demo.db \
  'http_requests_total{cluster="local"}' --format json
```

### 6. 检视 histogram

只展示已存储的原始 bucket/`_count`/`_sum`，不做速率或分位数计算：

```bash
uv run metricstash inspect histogram --db /tmp/metricstash-demo.db \
  'http_request_duration_seconds{cluster="local"}'
```

### 7. 清理（显式，不会由 collect 自动触发）

```bash
uv run metricstash prune --db /tmp/metricstash-demo.db --older-than 30d
```

## 定时调度示例

metricstash 不管理调度，由外部调度器调用即可。

cron：

```cron
*/5 * * * * metricstash collect --config /etc/metricstash/metricstash.toml --db /var/lib/metricstash/metrics.db
```

systemd timer（`metricstash.service` + `metricstash.timer`）：

```ini
# /etc/systemd/system/metricstash.service
[Unit]
Description=Metricstash one-shot collection

[Service]
Type=oneshot
ExecStart=/usr/local/bin/metricstash collect --config /etc/metricstash/metricstash.toml --db /var/lib/metricstash/metrics.db
```

```ini
# /etc/systemd/system/metricstash.timer
[Unit]
Description=Run metricstash every 5 minutes

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

## 发布到 PyPI

发布由 CI 在推送版本 tag 时自动完成（见 [`.github/workflows/publish.yml`](../.github/workflows/publish.yml)）：

```bash
# 1. 在 pyproject.toml 中提升 version，例如 0.1.0 -> 0.1.1
# 2. 提交后打 tag 并推送
git tag v0.1.1
git push origin v0.1.1
```

推送 tag 后，流水线会先运行完整测试套件，通过后用仓库 secret `PYPI_TOKEN` 构建并发布。
