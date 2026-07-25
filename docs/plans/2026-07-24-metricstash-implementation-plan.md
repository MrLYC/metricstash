# Metricstash Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a non-daemon Python CLI that collects configured Prometheus/OpenMetrics text metrics from multiple endpoints into SQLite and provides restricted selector, histogram, summary, audit, and lifecycle commands.

**Architecture:** Use `aiohttp` for bounded asynchronous collection and `prometheus-client` for metric-family parsing, guarded by a streaming-compatibility test. Normalize metric series and labels in SQLite, write each physical target in one transaction, and expose a deliberately small query evaluator rather than a full PromQL engine.

**Tech Stack:** Python 3.11+, uv, aiohttp, prometheus-client, sqlite3, argparse, pytest, pytest-asyncio.

**Design source:** `docs/plans/2026-07-24-metricstash-design.md`

---

## Implementation rules

- Work in a dedicated feature worktree before touching implementation files.
- Keep runtime dependencies limited to `aiohttp` and `prometheus-client`.
- Use TDD: add the failing test, run it, make the smallest change, rerun it.
- Do not add a daemon, HTTP API, scheduler manager, ORM, Pydantic, Rich, or a full PromQL parser.
- Do not retain raw metric response bodies or secrets.
- Commit only tested, cohesive slices with the commit messages suggested below.

### Task 1: Establish the package, dependencies, and test harness

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Delete: `main.py`
- Create: `src/metricstash/__init__.py`
- Create: `src/metricstash/__main__.py`
- Create: `src/metricstash/cli.py`
- Create: `tests/test_cli_smoke.py`

**Step 1: Write the failing smoke test**

```python
from metricstash.cli import build_parser


def test_cli_exposes_top_level_commands() -> None:
    parser = build_parser()
    commands = parser._subparsers._group_actions[0].choices
    assert {"validate", "collect", "query", "inspect", "prune", "db"} <= commands.keys()
```

**Step 2: Run it to verify failure**

Run: `uv run pytest tests/test_cli_smoke.py -v`

Expected: import failure because the package and parser do not exist.

**Step 3: Add the smallest package shell**

- Run `uv add aiohttp prometheus-client`.
- Run `uv add --dev pytest pytest-asyncio`.
- Configure a `src` layout, console script `metricstash = "metricstash.cli:main"`, and pytest test path in `pyproject.toml`.
- Add `build_parser()` with only command shells and `main()` that dispatches no behavior yet.
- Remove uv's placeholder `main.py` after the console entry point is proven.

```python
def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    return 0
```

**Step 4: Run the smoke test and packaging checks**

Run:

```bash
uv run pytest tests/test_cli_smoke.py -v
uv build
```

Expected: one passing test and a wheel/sdist under `dist/`.

**Step 5: Commit**

```bash
git add -A pyproject.toml uv.lock main.py src tests
git commit -m "build: create metricstash package skeleton"
```

### Task 2: Model configuration defaults and template interpolation

**Files:**
- Create: `src/metricstash/models.py`
- Create: `src/metricstash/config.py`
- Create: `src/metricstash/templates.py`
- Create: `tests/test_config.py`

**Step 1: Write failing configuration tests**

```python
def test_minimal_task_receives_operating_defaults(tmp_path) -> None:
    config = load_config(tmp_path / "minimal.toml", {"system": "prod"})
    task = config.tasks[0]
    assert task.job == task.module
    assert task.honor_labels is True
    assert task.http.timeout_seconds == 10
    assert task.http.retries == 0
    assert task.metrics[0].required is True


def test_missing_template_context_fails_before_network(tmp_path) -> None:
    with pytest.raises(ConfigError, match="context.cluster"):
        load_config(tmp_path / "template.toml", {"system": "prod"})
```

**Step 2: Run the tests to verify failure**

Run: `uv run pytest tests/test_config.py -v`

Expected: import failure.

**Step 3: Implement typed, stdlib-only configuration loading**

- Parse TOML using `tomllib`.
- Use dataclasses for collector, task, HTTP, metric declaration, target, and limits models.
- Make only `system`, `module`, metric `name/type`, and target `url` mandatory.
- Implement `${context.key}`, `${task.*}`, and static `${target.*}` expansion with a resolver that reports the exact missing key.
- Reserve post-DNS `${target.resolved_ip}` and `${target.dns_name}` resolution for Task 7.
- Reject unknown metric types and malformed durations at validation time.

**Step 4: Run focused and full tests**

Run:

```bash
uv run pytest tests/test_config.py -v
uv run pytest -q
```

Expected: all tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/models.py src/metricstash/config.py src/metricstash/templates.py tests/test_config.py
git commit -m "feat: load task configuration with defaults and templates"
```

### Task 3: Implement label merging and metric-family selection rules

**Files:**
- Create: `src/metricstash/labels.py`
- Create: `src/metricstash/metrics.py`
- Create: `tests/test_labels.py`
- Create: `tests/test_metrics.py`

**Step 1: Write failing label and family tests**

```python
def test_honor_labels_fills_only_missing_endpoint_labels() -> None:
    merged = merge_labels(
        endpoint={"job": "exporter", "region": "sh"},
        collector={"job": "api", "cluster": "prod"},
        honor_labels=True,
        resolved_ip="2001:db8::1",
    )
    assert merged == {
        "job": "exporter", "region": "sh", "cluster": "prod",
        "resolved_ip": "2001:db8::1",
    }


def test_histogram_declaration_expands_to_classic_sample_names() -> None:
    assert allowed_sample_names("latency_seconds", "histogram") == {
        "latency_seconds_bucket", "latency_seconds_sum", "latency_seconds_count"
    }
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_labels.py tests/test_metrics.py -v`

Expected: import failure.

**Step 3: Implement deterministic behavior**

- Merge task labels and target labels first, with target values overriding task values.
- Implement `honor_labels=true` filling and `honor_labels=false` `exported_<name>` renaming.
- Always append `resolved_ip` after either merge path.
- Map configured metric-family kinds to accepted actual sample names; keep the family base name for later inspection.
- Treat custom label names as valid input, including `job`, `instance`, and `__*`.

**Step 4: Run tests**

Run: `uv run pytest tests/test_labels.py tests/test_metrics.py -v`

Expected: all pass.

**Step 5: Commit**

```bash
git add src/metricstash/labels.py src/metricstash/metrics.py tests/test_labels.py tests/test_metrics.py
git commit -m "feat: add label precedence and metric-family rules"
```

### Task 4: Create schema, migration, locking, and SQLite repository primitives

**Files:**
- Create: `src/metricstash/db.py`
- Create: `src/metricstash/migrations.py`
- Create: `tests/test_db.py`

**Step 1: Write failing persistence tests**

```python
def test_sample_upsert_replaces_same_series_and_timestamp(db) -> None:
    series_id = db.ensure_series("requests_total", "requests_total", "counter", {"job": "api"})
    db.upsert_sample(series_id, 1_700_000_000_000, value=1.0, value_repr=None, scrape_id=1)
    db.upsert_sample(series_id, 1_700_000_000_000, value=2.0, value_repr=None, scrape_id=2)
    assert db.fetch_sample(series_id, 1_700_000_000_000) == (2.0, None, 2)


def test_special_values_use_value_repr(db) -> None:
    series_id = db.ensure_series("temperature", "temperature", "gauge", {})
    db.upsert_sample(series_id, 1, value=None, value_repr="NaN", scrape_id=1)
    assert db.fetch_sample(series_id, 1) == (None, "NaN", 1)
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_db.py -v`

Expected: import failure.

**Step 3: Implement schema version 1**

- Create `schema_migrations`, `runs`, `scrapes`, `metric_metadata`, `series`, `series_labels`, and `samples` exactly as designed.
- Use canonical JSON (sorted keys, compact separators) for the label identity key and normalized `series_labels` rows for querying.
- Enforce one-of `value` and `value_repr`, and `PRIMARY KEY(series_id, sample_timestamp_ms)`.
- Enable WAL, foreign keys, a bounded busy timeout, and an explicit writer lock.
- Initialize a new database from `collect`; reject schema upgrades until an explicit migration action runs.
- Add repository methods for runs, scrape audit rows, metadata, series, samples, commits, rollbacks, and checkpoints.

**Step 4: Run database tests**

Run: `uv run pytest tests/test_db.py -v`

Expected: all pass against a temporary SQLite file.

**Step 5: Commit**

```bash
git add src/metricstash/db.py src/metricstash/migrations.py tests/test_db.py
git commit -m "feat: add versioned SQLite time-series storage"
```

### Task 5: Build the restricted selector parser and database query executor

**Files:**
- Create: `src/metricstash/query.py`
- Create: `tests/test_query.py`

**Step 1: Write failing parser and query tests**

```python
def test_parse_range_selector() -> None:
    selector = parse_selector('http_requests_total{cluster="prod",pod!~"canary-.*"}[5m]')
    assert selector.metric_name == "http_requests_total"
    assert selector.range_ms == 300_000
    assert [matcher.operator for matcher in selector.matchers] == ["=", "!~"]


def test_missing_label_matches_negative_matcher(db) -> None:
    # Store a series with no cluster label.
    assert query_series(db, 'x{cluster!="prod"}', at_ms=1000)
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_query.py -v`

Expected: import failure.

**Step 3: Implement only the designed grammar**

- Require one unquoted legacy metric name, parse optional `{...}` matchers and optional `[duration]`.
- Support `=`, `!=`, `=~`, and `!~` with `re.fullmatch`.
- Match a missing label as `""`; invalid regexes raise a usage error.
- Use `series_labels` for indexed equality candidates, then apply negative and regex matchers in Python.
- Query `(at - range, at]` for ranges and the newest sample at or before `at` for instant queries.
- Enforce series/sample limits with a specific overflow exception rather than truncation.

**Step 4: Run tests**

Run: `uv run pytest tests/test_query.py -v`

Expected: all parser, missing-label, range, instant, special-value, and limit tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/query.py tests/test_query.py
git commit -m "feat: add restricted metric selector queries"
```

### Task 6: Add histogram and summary presentation from stored samples

**Files:**
- Create: `src/metricstash/inspect.py`
- Create: `tests/test_inspect.py`

**Step 1: Write failing inspector tests**

```python
def test_histogram_inspection_groups_by_labels_except_le(db) -> None:
    view = inspect_histogram(db, "latency_seconds", at_ms=1_000)
    assert view.groups[0].buckets[-1].upper_bound == "+Inf"
    assert view.groups[0].count == view.groups[0].buckets[-1].value


def test_summary_inspection_shows_quantiles_without_calculating_new_ones(db) -> None:
    view = inspect_summary(db, "rpc_seconds", at_ms=1_000)
    assert view.groups[0].quantiles[0].quantile == "0.5"
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_inspect.py -v`

Expected: import failure.

**Step 3: Implement presentation-only reconstruction**

- Select one configured family at a time.
- Group histogram members by all labels except `le`; group summary members by all labels except `quantile`.
- Surface bucket/quantile rows, `_count`, `_sum`, sample timestamp, and collection timestamp.
- Reject incomplete stored groups rather than calculating rates or quantiles.

**Step 4: Run tests**

Run: `uv run pytest tests/test_inspect.py -v`

Expected: all inspection tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/inspect.py tests/test_inspect.py
git commit -m "feat: add histogram and summary inspection"
```

### Task 7: Expand targets through DNS with IPv6-first semantics

**Files:**
- Create: `src/metricstash/dns.py`
- Create: `tests/test_dns.py`

**Step 1: Write failing resolver tests**

```python
@pytest.mark.asyncio
async def test_prefers_all_aaaa_records_without_ipv4_fallback(monkeypatch) -> None:
    resolved = await expand_target("https://api.example.test/metrics", resolver=fake_resolver(
        ipv6=["2001:db8::1", "2001:db8::2"], ipv4=["192.0.2.1"]
    ))
    assert [item.resolved_ip for item in resolved] == ["2001:db8::1", "2001:db8::2"]


@pytest.mark.asyncio
async def test_uses_ipv4_only_when_no_aaaa_records_exist() -> None:
    ...
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_dns.py -v`

Expected: import failure.

**Step 3: Implement logical-to-physical target expansion**

- Resolve hostnames once per collect invocation, deduplicate answers, and cap them at the configured maximum.
- Preserve URL hostname as `dns_name`; create one physical target per selected address.
- Use all IPv6 answers if any exist; use IPv4 only when no IPv6 answer exists.
- Fill a missing `instance` with `resolved_ip:port` and defer dynamic DNS template expansion until this point.
- Make failure to resolve or address-cap overflow a logical target failure.

**Step 4: Run tests**

Run: `uv run pytest tests/test_dns.py -v`

Expected: all DNS policy tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/dns.py tests/test_dns.py
git commit -m "feat: expand DNS targets with IPv6 preference"
```

### Task 8: Implement bounded asynchronous HTTP collection

**Files:**
- Create: `src/metricstash/http.py`
- Create: `tests/test_http.py`

**Step 1: Write failing HTTP tests using local aiohttp servers**

```python
@pytest.mark.asyncio
async def test_retries_only_retryable_statuses(metrics_server) -> None:
    client = MetricsHttpClient(retries=1, timeout_seconds=1)
    result = await client.fetch(metrics_server.url("/returns-503"))
    assert result.attempts == 2


@pytest.mark.asyncio
async def test_rejects_non_text_content_type(metrics_server) -> None:
    client = MetricsHttpClient()
    with pytest.raises(ContentTypeError):
        await client.fetch(metrics_server.url("/binary"))
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_http.py -v`

Expected: import failure.

**Step 3: Implement the aiohttp transport**

- Use one `ClientSession` and a `TCPConnector(limit=max_concurrency, force_close=True)`.
- Implement timeout, retryable failure classification, exponential delay, compressed-byte streaming, response-size counting, and Content-Type policy.
- Add a resolver adapter that connects the selected IP while the URL hostname remains the request host, preserving Host/SNI/certificate behavior.
- Keep credentials in request construction only and redact them from exception text.

**Step 4: Run tests**

Run: `uv run pytest tests/test_http.py -v`

Expected: all retry, timeout, body-limit, auth-header, and Content-Type tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/http.py tests/test_http.py
git commit -m "feat: add bounded asynchronous metrics HTTP client"
```

### Task 9: Prove parser streaming behavior and adapt metric families

**Files:**
- Create: `src/metricstash/exposition.py`
- Create: `tests/test_exposition.py`

**Step 1: Write the streaming gate test**

```python
def test_parser_consumes_line_stream_without_bulk_read() -> None:
    source = NoBulkReadTextStream([
        "# TYPE requests_total counter\\n",
        "requests_total 1\\n",
    ])
    families = list(iter_metric_families(source))
    assert families[0].name == "requests"
```

**Step 2: Run it to determine parser capability**

Run: `uv run pytest tests/test_exposition.py::test_parser_consumes_line_stream_without_bulk_read -v`

Expected: initially fail because `iter_metric_families` is absent. If the dependency cannot pass with a line stream, record that fact in the test and implement the bounded fallback adapter in the next step.

**Step 3: Implement the narrowest valid adapter**

- Prefer `prometheus_client.parser` file/iterator support where it is proven to stream.
- If it needs full text, implement a line-oriented adapter that buffers only one declared metric family and reuses `prometheus-client` data structures where practical.
- Convert emitted samples to internal records with actual sample name, base family name, type, labels, numeric/special value representation, and source timestamp.
- Validate configured type against observed `# TYPE` when present.
- Ignore unconfigured malformed lines, but fail configured families on malformed input, missing required metrics, or invalid classic histogram/summary structure.

**Step 4: Run parser suite**

Run: `uv run pytest tests/test_exposition.py -v`

Expected: fixtures for counters, gauges, summaries, histograms, `NaN`, infinities, timestamps, type mismatch, and selected/unselected malformed lines pass.

**Step 5: Commit**

```bash
git add src/metricstash/exposition.py tests/test_exposition.py
git commit -m "feat: parse selected metric families incrementally"
```

### Task 10: Orchestrate collection, audit, transactions, and synthetic metrics

**Files:**
- Create: `src/metricstash/collector.py`
- Create: `tests/test_collector.py`

**Step 1: Write failing orchestration tests**

```python
@pytest.mark.asyncio
async def test_partial_target_failure_commits_success_and_returns_partial_result(tmp_path, metrics_server) -> None:
    result = await collect(load_test_config(metrics_server), tmp_path / "metrics.db")
    assert result.successful_targets == 1
    assert result.failed_targets == 1
    assert read_metric(tmp_path / "metrics.db", "metricstash_up", {"resolved_ip": "..."}) == 0


@pytest.mark.asyncio
async def test_failed_target_rolls_back_business_samples(tmp_path, metrics_server) -> None:
    ...
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_collector.py -v`

Expected: import failure.

**Step 3: Implement collection orchestration**

- Run config validation, DNS expansion, then bounded physical-target fetches.
- Serialize writes through one async queue/worker while network and parsing remain concurrent.
- In a target transaction, upsert series, labels, samples, metadata, and successful scrape audit.
- On target failure, roll back its business transaction, then record its failed audit and deterministic `metricstash_*` metrics in a separate small transaction.
- Insert four synthetic metrics with collector-side labels only.
- Treat SQLite lock/full/I/O exceptions as fatal and cancel remaining work; make SIGINT cancellation return code 130 after rolling back active work.

**Step 4: Run collector tests**

Run: `uv run pytest tests/test_collector.py -v`

Expected: partial-success, atomic-target, synthetic-metric, disk/I/O error, and cancellation tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/collector.py tests/test_collector.py
git commit -m "feat: collect targets transactionally into SQLite"
```

### Task 11: Wire CLI commands, output formats, prune, and migration

**Files:**
- Modify: `src/metricstash/cli.py`
- Create: `src/metricstash/output.py`
- Create: `tests/test_cli.py`
- Create: `tests/test_prune.py`
- Create: `tests/test_migrate.py`

**Step 1: Write failing CLI acceptance tests**

```python
def test_query_json_output_is_machine_readable(cli_runner, populated_db) -> None:
    result = cli_runner("query", "--db", str(populated_db), "x{job=\"api\"}", "--format", "json")
    assert result.exit_code == 0
    assert json.loads(result.stdout)["series"]


def test_prune_never_runs_implicitly_during_collect(cli_runner, populated_db) -> None:
    ...
```

**Step 2: Run failing tests**

Run: `uv run pytest tests/test_cli.py tests/test_prune.py tests/test_migrate.py -v`

Expected: command behavior is absent or incomplete.

**Step 3: Implement command dispatch**

- Implement `validate`, `collect`, `query`, `inspect histogram`, `inspect summary`, `prune`, and `db migrate`.
- Keep paths explicit and write results to stdout, diagnostics to stderr.
- Produce stable table and JSON serializers without Rich.
- Map expected failures exactly to exit codes 0, 1, 2, 3, and 130.
- Make `prune` require `--older-than` or `--before`; expose `VACUUM` only as an explicit option/action.
- Make existing-database upgrades possible only through `db migrate`.

**Step 4: Run CLI tests**

Run: `uv run pytest tests/test_cli.py tests/test_prune.py tests/test_migrate.py -v`

Expected: command, output, exit-code, prune, and migration tests pass.

**Step 5: Commit**

```bash
git add src/metricstash/cli.py src/metricstash/output.py tests/test_cli.py tests/test_prune.py tests/test_migrate.py
git commit -m "feat: expose collection and query CLI commands"
```

### Task 12: Add docs, example configuration, and constrained end-to-end verification

**Files:**
- Modify: `README.md`
- Create: `docs/config.example.toml`
- Create: `tests/test_e2e.py`
- Create: `tests/test_resource_limits.py`

**Step 1: Write failing end-to-end tests**

```python
@pytest.mark.asyncio
async def test_end_to_end_collect_query_and_histogram_inspect(tmp_path, metrics_server) -> None:
    db = tmp_path / "metricstash.db"
    assert await invoke_collect(metrics_server.config_path, db) == 0
    assert invoke_query(db, 'http_requests_total{cluster="prod"}[5m]').exit_code == 0
    assert invoke_inspect_histogram(db, 'latency_seconds{cluster="prod"}').exit_code == 0


@pytest.mark.asyncio
async def test_sample_limit_prevents_large_response_commit(tmp_path, metrics_server) -> None:
    ...
```

**Step 2: Run failing end-to-end tests**

Run: `uv run pytest tests/test_e2e.py tests/test_resource_limits.py -v`

Expected: failure until all command paths are wired.

**Step 3: Document only supported behavior**

- Document install/build with `uv`, wheel installation without `uv`, minimal configuration, all commands, selector grammar, DNS IPv6 rule, label semantics, error behavior, and explicit pruning.
- Include a cron/systemd timer invocation example without creating or managing schedules.
- State the 1c1g500m resource envelope and configurable defaults.
- Document unsupported formats and query features clearly.

**Step 4: Run full verification**

Run:

```bash
uv run pytest -q
uv build
uv run metricstash --help
```

Expected: all tests pass, wheel builds, and help lists all supported commands.

**Step 5: Commit**

```bash
git add README.md docs/config.example.toml tests/test_e2e.py tests/test_resource_limits.py
git commit -m "docs: document metricstash client workflow"
```

## Final verification checklist

Run these only after every implementation task is complete:

```bash
uv lock --check
uv run pytest -q
uv build
git diff --check
git status --short
```

Verify the resulting wheel in a clean virtual environment, then run a local fixture endpoint through `collect`, `query`, `inspect histogram`, `inspect summary`, and `prune`.
