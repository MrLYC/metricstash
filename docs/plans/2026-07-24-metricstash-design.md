# Metricstash Design

**Status:** Approved design, 2026-07-24
**Scope:** A one-shot, lightweight client for collecting selected Prometheus/OpenMetrics text metrics from multiple endpoints and storing them in SQLite.

## Goal

Provide a non-daemon CLI that can be invoked manually, by cron, or by a systemd timer in environments where a full monitoring stack is unavailable. It collects selected metric families from multiple targets, preserves useful target identity and timestamps, and offers a deliberately restricted PromQL-like read interface backed by SQLite.

## Non-goals

- No background service, HTTP query API, scheduler management, alerting, or remote-write support.
- No full PromQL evaluator: no functions, aggregation, arithmetic, joins, or cross-metric calculations.
- No protobuf exposition, native histogram ingestion, proxy support, OAuth, mTLS, or generic service discovery.
- No raw response payload retention and no automatic retention cleanup.

## Runtime and dependency boundary

- Python `>=3.11`.
- Development and packaging use `uv`; target hosts do not need `uv`.
- Runtime dependencies are `aiohttp` and `prometheus-client`.
- `google-re2` is optional and not installed by default because expressions are trusted local-operator input.
- Do not add an ORM, Pydantic, Rich, or a full PromQL parser.
- Deliver a wheel with the `metricstash` console command.

## Resource envelope

The expected deployment is a 1 vCPU, 1 GiB memory, 500 MiB writable-disk container.

The implementation should aim for normal RSS below 128 MiB and stressed RSS below 256 MiB. Built-in defaults are intentionally conservative and overrideable:

| Setting | Default |
|---|---:|
| Concurrent physical target requests | 4 |
| Request timeout | 10 seconds |
| Retry count | 0 |
| Decompressed response limit | 8 MiB |
| Samples per physical target | 25,000 |
| Labels per sample | 32 |
| Label-value size | 1 KiB |
| Resolved IPs per hostname | 16 |
| Query series / sample result limit | 1,000 / 10,000 |

There is deliberately no proactive SQLite capacity guard. The deployment owns capacity management and must invoke `prune` explicitly. Disk-full and unrecoverable SQLite I/O errors stop the current collection as fatal errors.

## Configuration model

Configuration is TOML and follows `task -> target` grouping. The only required semantic fields are task `system`, task `module`, a metric `name` and `type`, and each target `url`. All operating values have defaults.

```toml
[collector]
max_concurrency = 4

[[tasks]]
system = "${context.system}"
module = "api"
job = "api" # defaults to module when omitted
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

### Templates and labels

- A run supplies dynamic values with repeatable `--context key=value` flags.
- Templates use `${context.key}` and may refer to task and target fields. After DNS expansion they may also use `${target.resolved_ip}` and `${target.dns_name}`.
- A missing template value is a pre-request configuration error. Network-free `validate` checks static template syntax and known names; DNS-derived values are validated at collection time.
- Task labels are inherited by targets; target labels override task labels.
- `system` and `module` are mandatory business labels. `job` defaults to `module`; `instance` defaults to the physical endpoint address when absent.
- Custom labels may use any valid label name, including `job`, `instance`, and `__*`.
- `honor_labels=true` is the default: endpoint-provided labels win and collector labels fill only missing keys. When `honor_labels=false`, conflicting endpoint labels are renamed to `exported_<name>` before collector labels are applied.
- `resolved_ip` is the one intentional exception: the collector always appends it after label merging so DNS-expanded IPs cannot collapse into the same series.

### HTTP configuration

- Supported authentication: none, static headers, Basic Auth, and Bearer token files. Secrets are never stored in SQLite.
- TLS verification is enabled by default.
- Retry only network-transient failures and HTTP 408, 429, and 5xx; do not retry other 4xx responses.
- A target may opt into accepting a missing text `Content-Type`; explicit non-text content types are rejected.

## Collection pipeline

```text
collect --config --db --context
  -> validate config and templates
  -> expand logical targets into physical IP targets
  -> bounded concurrent HTTP fetches
  -> streaming parse and allowed-family filtering
  -> serialized SQLite writes, one transaction per physical target
  -> audit and synthetic collection metrics
```

### DNS behavior

- Hostnames are resolved once per invocation.
- Prefer AAAA records. If at least one AAAA record exists, only its IPv6 addresses are collected; do not fall back to IPv4 after IPv6 connect or HTTP failures.
- Use A records only when no AAAA record exists.
- Deduplicate addresses, fail a logical target when it exceeds the configured address cap, and collect each resulting IP independently.
- Connect TCP sockets to the selected IP while preserving the original hostname for HTTP `Host`, HTTPS SNI, and certificate validation.

### Metric parsing and acceptance

- Support Prometheus/OpenMetrics text only. Protobuf and native histograms are unsupported.
- Metric configuration is the type source of truth. An endpoint-provided `# TYPE`, when present, must agree with configuration.
- For an allowed classic histogram base name, collect `<base>_bucket`, `<base>_sum`, and `<base>_count`. For an allowed summary base name, collect `<base>{quantile=...}`, `<base>_sum`, and `<base>_count`.
- Required metric families are required by default. A missing required family fails that physical target.
- Required histogram and summary families must be structurally complete. Histograms must include a `+Inf` bucket matching `_count`.
- Ignore malformed metric lines outside the configured allow-list. A malformed or inconsistent allowed family fails the physical target.
- Read and parse incrementally. Validate that `prometheus-client` can support this path; otherwise add only a small incremental adapter rather than buffering full responses.

### Success and failure semantics

- Source samples are batched into a single SQLite transaction for each physical target. Commit only on success; rollback that target on failure.
- Successful physical targets stay committed even if other targets fail. Any partial target failure causes `collect` to exit with code 1.
- Every physical target creates a `scrapes` audit record.
- Insert fixed synthetic metrics into the same series/sample store: `metricstash_up`, `metricstash_scrape_duration_seconds`, `metricstash_scrape_samples`, and `metricstash_scrape_attempts`.
- Synthetic metrics use deterministic collector labels only: task labels, target labels, `job`, `instance`, and `resolved_ip`; they never need endpoint self-labels to record failure.

## SQLite storage model

SQLite uses WAL mode. Read queries may run beside one writer, but a filesystem lock rejects concurrent `collect`, `prune`, and migration commands. Run a checkpoint after collection batches to prevent an unbounded WAL.

| Table | Purpose |
|---|---|
| `schema_migrations` | Applied schema versions; existing databases migrate only through `db migrate` |
| `runs` | One command invocation, its context, start time, and tool version |
| `scrapes` | Each physical target/IP attempt, timings, status, retries, HTTP status, and truncated error summary |
| `metric_metadata` | Latest `HELP`, `UNIT`, and type per `system/module/metric family` |
| `series` | Actual metric name, family base name, configured type, and canonical full label set |
| `series_labels` | Label index rows for selection and regex filtering |
| `samples` | Time-stamped values and their most recent `scrape_id` |

`series` is unique on actual metric name plus a canonical, sorted label encoding. `samples` has:

```text
PRIMARY KEY(series_id, sample_timestamp_ms)
```

Use direct UPSERT when a later scrape emits the same series and timestamp. A finite value goes in `value REAL`; `NaN`, `+Inf`, and `-Inf` go in `value_repr TEXT`. A check constraint guarantees exactly one is present. The most recent `scrape_id` is retained for audit traceability.

Sample timestamps follow target timestamps when `honor_timestamps=true` (the default); otherwise use the collection timestamp. Collection start/end remain audit fields.

## Query and inspection model

The selector grammar supports exactly one required metric name, optional label matchers using `=`, `!=`, `=~`, and `!~`, and an optional range selector such as:

```text
http_requests_total{cluster="prod"}[5m]
```

- Regexes use Python `re.fullmatch`; this is acceptable because local operators are trusted. Invalid regexes are usage errors.
- Missing labels are treated as the empty string, matching PromQL-style negative-matcher behavior.
- A range query returns raw samples from `(at - range, at]`.
- An instant query returns the latest sample at or before `--at`, with no implicit five-minute lookback. Output exposes sample time, collection time, and age; `--max-age` can filter old samples.
- Result limits fail loudly rather than silently truncating.
- Queries default to table output and support `--format json`.
- `inspect histogram` groups family members by all labels except `le`; `inspect summary` groups by all labels except `quantile`. Both default to an instant snapshot, optionally show a time range, and never calculate rates, quantiles, or aggregations.

## Command interface

```text
metricstash validate --config CONFIG --context key=value
metricstash collect --config CONFIG --db DB --context key=value
metricstash query --db DB 'metric{label="x"}[5m]' --at TIME --format table|json
metricstash inspect histogram|summary --db DB 'metric_family{...}' --at TIME
metricstash prune --db DB --older-than 30d
metricstash db migrate --db DB
```

- Paths are always explicit; no state is implicitly created under a home directory.
- `validate` is network-free.
- `prune` is explicit and supports `--older-than` or `--before`; `VACUUM` is a separate explicit action.
- No command manages cron or systemd. Documentation may provide invocation examples only.

## Exit codes and interruption behavior

| Code | Meaning |
|---:|---|
| 0 | All requested target collections succeeded |
| 1 | At least one physical target failed; successes are retained |
| 2 | Configuration, template, or selector error; no collection mutation |
| 3 | SQLite lock, disk-full, or other unrecoverable internal/I/O failure |
| 130 | Interrupted by SIGINT; active transaction rolls back, earlier commits remain |

Normal command results go to stdout. Diagnostics go to stderr, with an optional JSON log format. Never emit secret values in logs or audit rows.

## Verification strategy

- Unit fixtures for configuration defaults, templates, label precedence, type declarations, and error messages.
- HTTP integration tests using local `aiohttp` servers for timeout, retry, response limits, Content-Type, and authentication behavior.
- DNS tests for IPv6 preference, A-only fallback, address caps, IP identity, and preserved hostname semantics.
- Parser fixtures for counter, gauge, histogram, summary, timestamps, special values, selected malformed lines, and ignored unselected malformed lines.
- SQLite tests for transaction rollback, UPSERT collision behavior, locks, pruning, migrations, labels, range/instant selectors, and inspect output.
- End-to-end tests under a 1 CPU / 1 GiB / 500 MiB container profile with representative 25,000-sample responses.
