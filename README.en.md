# Metricstash

[简体中文](README.md) | English

Metricstash is a lightweight, one-shot client for collecting Prometheus/OpenMetrics text metrics. It does not run a background service: each `collect` invocation reads a TOML configuration file, fetches multiple targets concurrently, writes the allowed metric families to SQLite, and exits.

It is intended as a supplementary sampler when a monitoring system is absent in constrained environments such as a container with 1 vCPU, 1 GiB of memory, and 500 MiB of writable storage. It is not a replacement for Prometheus, alerting, or a long-term time-series database.

## Installation and build

Use `uv` for development:

```bash
uv sync
uv run metricstash --help
uv run pytest -q
uv build
```

Target hosts do not need `uv`. Install the built wheel instead:

```bash
python -m pip install dist/metricstash-0.1.0-py3-none-any.whl
metricstash --help
```

The only runtime dependencies are `aiohttp` and `prometheus-client`. Metricstash does not use an ORM, Pydantic, Rich, or a full PromQL parser.

## Minimal configuration

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

Only a task's `system` and `module`, each metric's `name` and `type`, and a target `url` are required. All other collection settings have defaults. See the complete example in [`docs/config.example.toml`](docs/config.example.toml).

## Examples

[`examples/`](examples/) contains a fully offline, end-to-end walkthrough of `collect → query → inspect → prune`, plus cron/systemd scheduling and PyPI release guidance.

Provide dynamic task context with repeated `--context KEY=VALUE` flags; templates use `${context.KEY}`. Task and target labels may also use `${task.system}`, `${task.module}`, `${task.job}`, `${target.dns_name}`, and `${target.resolved_ip}`. A missing context variable is an error before the HTTP request begins; environment variables are never read for templating.

## Commands

```bash
# Validate configuration without network access.
metricstash validate --config metricstash.toml --context system=prod

# Collect once; the database path is always explicit.
metricstash collect --config metricstash.toml --db /var/lib/metricstash/metrics.db

# Instant query: return the last sample at or before --at, with no implicit 5-minute lookback.
metricstash query --db /var/lib/metricstash/metrics.db \
  'http_requests_total{cluster="prod"}' --at now

# Range query: return raw samples in (at - 5m, at].
metricstash query --db /var/lib/metricstash/metrics.db \
  'http_requests_total{cluster="prod"}[5m]' --format json

# Show only stored raw bucket/quantile/count/sum values; do not calculate rates or quantiles.
metricstash inspect histogram --db /var/lib/metricstash/metrics.db \
  'http_request_duration_seconds{cluster="prod"}'
metricstash inspect summary --db /var/lib/metricstash/metrics.db \
  'rpc_duration_seconds{cluster="prod"}'

# Cleanup is independent of collection and is never run automatically by collect.
metricstash prune --db /var/lib/metricstash/metrics.db --older-than 30d
metricstash prune --db /var/lib/metricstash/metrics.db --before 2026-07-24T00:00:00Z
metricstash db vacuum --db /var/lib/metricstash/metrics.db

# Existing databases must be upgraded explicitly.
metricstash db migrate --db /var/lib/metricstash/metrics.db
```

`query` uses table output by default and also supports `--format json`. Time arguments accept `now` or RFC3339 (for example, `2026-07-24T00:00:00Z`). Ranges and `--max-age` use the units `ms`, `s`, `m`, `h`, `d`, and `w`.

## Selector boundary

Metricstash supports exactly one explicit metric name, optional label matchers, and an optional range selector:

```text
http_requests_total{cluster="prod",pod!~"canary-.*"}[5m]
```

The supported label operators are `=`, `!=`, `=~`, and `!~`; regular expressions use Python `re.fullmatch`. Missing labels are treated as empty strings, giving negative matchers PromQL-style semantics. Functions, aggregation, arithmetic, joins, multi-metric operations, and an HTTP query API are not supported. Results exceeding the default limit of 1,000 series or 10,000 samples fail explicitly rather than being silently truncated.

## Collection and label semantics

- Each logical target resolves DNS during a `collect` invocation. When any AAAA record exists, Metricstash collects every deduplicated IPv6 address and never falls back to IPv4 after an IPv6 connection or HTTP failure; it uses A records only when no AAAA record exists. The default limit is 16 IPs and can be changed with `[collector] max_resolved_ips`.
- Each IP address is an independent physical target. TCP connects to that IP, while the HTTP `Host` header, HTTPS SNI, and certificate verification continue to use the original hostname. `resolved_ip` is always written by the collector after labels are merged, preventing series from different IPs from collapsing together.
- Target labels override task labels. `system` and `module` automatically become labels, `job` defaults to `module`, and a missing `instance` defaults to `resolved_ip:port` (with brackets for IPv6).
- The default is `honor_labels = true`: endpoint labels with the same name win, while collector labels fill only missing labels. When it is `false`, conflicting endpoint labels are renamed to `exported_<name>` before collector labels are applied.
- Any custom label name is allowed, including `job`, `instance`, and `__*`.

The metrics allow-list in the configuration uses exact name matching. The configured type is authoritative; an endpoint `# TYPE`, if present, must match, while an absent `# TYPE` is accepted. Counter, gauge, untyped, classic histogram, and summary families are supported; protobuf and native histograms are not. A histogram must have a `+Inf` bucket equal to its `_count`; an incomplete summary or histogram rolls back the whole physical target.

Parsing, HTTP reading, and SQLite writes all proceed in streaming batches. Neither raw response bodies nor authentication secrets are stored in SQLite.

## SQLite and resource defaults

SQLite uses WAL. Queries can run alongside one writer, but a file lock rejects concurrent `collect`, `prune`, migration, and `vacuum` operations. Series identity is the actual sample metric name plus all normalized labels; a later sample with the same `series_id + sample_timestamp_ms` directly UPSERTs the value and `scrape_id`.

Finite floating-point values are stored in `value REAL`; `NaN`, `+Inf`, and `-Inf` are stored in `value_repr TEXT`, with exactly one of the two present. Every physical target has its own transaction: business samples from a failed target roll back, successful targets remain, and every target records scrape audit data together with these collector metrics:

- `metricstash_up`
- `metricstash_scrape_duration_seconds`
- `metricstash_scrape_samples`
- `metricstash_scrape_attempts`

Default limits are: concurrency 4, request timeout 10 seconds, retries 0, decompressed response 8 MiB, 25,000 samples per physical target, 32 labels per sample, 1 KiB per label value, and 16 DNS IPs. There is no automatic capacity cleanup or disk-quota guard; run `prune` explicitly when needed. A full disk, SQLite I/O error, or lock conflict stops the collection run.

## HTTP and exit codes

HTTP supports no authentication, static headers, Basic Auth, and Bearer token files; TLS verification is enabled by default. Exponential-backoff retries apply only to transient network errors and HTTP 408, 429, and 5xx responses. An explicitly non-text `Content-Type` is rejected; a target may individually allow a missing `Content-Type`.

| Exit code | Meaning |
| --- | --- |
| 0 | All physical targets succeeded |
| 1 | At least one physical target failed; successful results were retained |
| 2 | Configuration, template, time, or selector usage error; no collection mutation started |
| 3 | SQLite lock, disk/I/O, or another unrecoverable error |
| 130 | SIGINT; the active target transaction rolls back and already committed targets remain |

Metricstash does not manage cron or systemd. Invoke it from an external scheduler, for example:

```text
*/5 * * * * metricstash collect --config /etc/metricstash.toml --db /var/lib/metricstash/metrics.db
```

## Non-goals

Metricstash has no background service, scheduler management, alerting, remote write, raw response retention, proxy support, OAuth, mTLS, generic service discovery, HTTP query API, full PromQL, protobuf exposition, or native histograms.
