"""TOML configuration loading, validation, defaults, and static templates."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from metricstash.models import (
    AppConfig,
    BasicAuthConfig,
    CollectorConfig,
    HttpConfig,
    MetricConfig,
    TargetConfig,
    TaskConfig,
)
from metricstash.templates import TemplateError, expand_template


class ConfigError(ValueError):
    """A configuration is invalid before any target is contacted."""


_DURATION = re.compile(r"^(?P<number>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h|d|w)$")
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}
_LABEL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_METRIC_NAME = re.compile(r"^[A-Za-z_:][A-Za-z0-9_:]*$")
_METRIC_TYPES = frozenset({"counter", "gauge", "untyped", "histogram", "summary"})
_DEFERRED_TARGET_VALUES = frozenset({"target.resolved_ip", "target.dns_name"})


def parse_duration(value: str, *, field_name: str = "duration") -> float:
    """Parse the compact duration syntax shared by config and selectors."""
    match = _DURATION.fullmatch(value)
    if match is None:
        raise ConfigError(f"invalid {field_name}: {value!r}")
    seconds = float(match.group("number")) * _DURATION_UNITS[match.group("unit")]
    if seconds <= 0:
        raise ConfigError(f"invalid {field_name}: {value!r}")
    return seconds


def load_config(path: Path, context: Mapping[str, str]) -> AppConfig:
    """Load a network-free TOML configuration with all operating defaults."""
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"cannot read configuration {path}: {error}") from error

    if not isinstance(raw, Mapping):
        raise ConfigError("configuration root must be a TOML table")
    try:
        collector = _parse_collector(_mapping(raw.get("collector", {}), "collector"))
        raw_tasks = raw.get("tasks")
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise ConfigError("configuration requires at least one [[tasks]] entry")
        tasks = tuple(
            _parse_task(_mapping(task, f"tasks[{index}]"), context, path.parent, index)
            for index, task in enumerate(raw_tasks)
        )
    except TemplateError as error:
        raise ConfigError(str(error)) from error
    return AppConfig(collector=collector, tasks=tasks, source_path=path)


def _parse_collector(raw: Mapping[str, object]) -> CollectorConfig:
    defaults = CollectorConfig()
    return CollectorConfig(
        max_concurrency=_positive_int(raw.get("max_concurrency", defaults.max_concurrency), "collector.max_concurrency"),
        body_limit_bytes=_positive_int(raw.get("body_limit_bytes", defaults.body_limit_bytes), "collector.body_limit_bytes"),
        max_samples_per_target=_positive_int(
            raw.get("max_samples_per_target", defaults.max_samples_per_target),
            "collector.max_samples_per_target",
        ),
        max_labels_per_sample=_positive_int(
            raw.get("max_labels_per_sample", defaults.max_labels_per_sample),
            "collector.max_labels_per_sample",
        ),
        max_label_value_bytes=_positive_int(
            raw.get("max_label_value_bytes", defaults.max_label_value_bytes),
            "collector.max_label_value_bytes",
        ),
        max_resolved_ips=_positive_int(
            raw.get("max_resolved_ips", defaults.max_resolved_ips),
            "collector.max_resolved_ips",
        ),
        query_series_limit=_positive_int(
            raw.get("query_series_limit", defaults.query_series_limit),
            "collector.query_series_limit",
        ),
        query_sample_limit=_positive_int(
            raw.get("query_sample_limit", defaults.query_sample_limit),
            "collector.query_sample_limit",
        ),
        busy_timeout_ms=_positive_int(
            raw.get("busy_timeout_ms", defaults.busy_timeout_ms), "collector.busy_timeout_ms"
        ),
    )


def _parse_task(
    raw: Mapping[str, object],
    context: Mapping[str, str],
    config_dir: Path,
    index: int,
) -> TaskConfig:
    task_name = f"tasks[{index}]"
    static_scopes = {"context": context, "task": {}, "target": {}}
    system = _expand_required_string(raw.get("system"), f"{task_name}.system", static_scopes)
    module = _expand_required_string(raw.get("module"), f"{task_name}.module", static_scopes)
    task_scope: dict[str, object] = {"system": system, "module": module}
    job_value = raw.get("job", module)
    job = _expand_required_string(
        job_value,
        f"{task_name}.job",
        {"context": context, "task": task_scope, "target": {}},
    )
    task_scope["job"] = job
    scopes = {"context": context, "task": task_scope, "target": {}}
    labels = _parse_labels(raw.get("labels", {}), f"{task_name}.labels", scopes)
    http = _parse_http(_mapping(raw.get("http", {}), f"{task_name}.http"), scopes, config_dir)
    honor_labels = _bool(raw.get("honor_labels", True), f"{task_name}.honor_labels")
    honor_timestamps = _bool(raw.get("honor_timestamps", True), f"{task_name}.honor_timestamps")
    raw_metrics = raw.get("metrics")
    if not isinstance(raw_metrics, list) or not raw_metrics:
        raise ConfigError(f"{task_name} requires at least one [[tasks.metrics]] entry")
    metrics = tuple(
        _parse_metric(_mapping(metric, f"{task_name}.metrics[{metric_index}]"), scopes, task_name, metric_index)
        for metric_index, metric in enumerate(raw_metrics)
    )
    names = [metric.name for metric in metrics]
    if len(set(names)) != len(names):
        raise ConfigError(f"{task_name} declares a metric family more than once")
    raw_targets = raw.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ConfigError(f"{task_name} requires at least one [[tasks.targets]] entry")
    targets = tuple(
        _parse_target(
            _mapping(target, f"{task_name}.targets[{target_index}]"),
            scopes,
            task_name,
            target_index,
        )
        for target_index, target in enumerate(raw_targets)
    )
    return TaskConfig(
        system=system,
        module=module,
        job=job,
        labels=labels,
        metrics=metrics,
        targets=targets,
        http=http,
        honor_labels=honor_labels,
        honor_timestamps=honor_timestamps,
    )


def _parse_metric(
    raw: Mapping[str, object],
    scopes: Mapping[str, Mapping[str, object]],
    task_name: str,
    index: int,
) -> MetricConfig:
    name = _expand_required_string(raw.get("name"), f"{task_name}.metrics[{index}].name", scopes)
    if _METRIC_NAME.fullmatch(name) is None:
        raise ConfigError(f"invalid metric name: {name!r}")
    metric_type = _expand_required_string(raw.get("type"), f"{task_name}.metrics[{index}].type", scopes)
    if metric_type not in _METRIC_TYPES:
        raise ConfigError(f"invalid metric type: {metric_type!r}")
    return MetricConfig(
        name=name,
        metric_type=metric_type,
        required=_bool(raw.get("required", True), f"{task_name}.metrics[{index}].required"),
    )


def _parse_target(
    raw: Mapping[str, object],
    scopes: Mapping[str, Mapping[str, object]],
    task_name: str,
    index: int,
) -> TargetConfig:
    url = _expand_required_string(raw.get("url"), f"{task_name}.targets[{index}].url", scopes)
    if "${target." in url:
        raise ConfigError(f"{task_name}.targets[{index}].url cannot use DNS-derived templates")
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigError(f"invalid target URL: {url!r}")
    target_scopes = dict(scopes)
    target_scopes["target"] = {"url": url}
    labels = _parse_labels(
        raw.get("labels", {}),
        f"{task_name}.targets[{index}].labels",
        target_scopes,
    )
    missing_content_type = raw.get("allow_missing_content_type")
    if missing_content_type is not None:
        missing_content_type = _bool(
            missing_content_type,
            f"{task_name}.targets[{index}].allow_missing_content_type",
        )
    return TargetConfig(
        url=url,
        labels=labels,
        allow_missing_content_type=missing_content_type,
    )


def _parse_http(
    raw: Mapping[str, object],
    scopes: Mapping[str, Mapping[str, object]],
    config_dir: Path,
) -> HttpConfig:
    defaults = HttpConfig()
    timeout_raw = _expand_string(raw.get("timeout", "10s"), "tasks.http.timeout", scopes)
    timeout_seconds = parse_duration(timeout_raw, field_name="timeout")
    retries = _nonnegative_int(raw.get("retries", defaults.retries), "tasks.http.retries")
    headers_raw = _mapping(raw.get("headers", {}), "tasks.http.headers")
    headers = {
        _expand_string(key, "tasks.http.headers key", scopes): _expand_string(
            value, "tasks.http.headers value", scopes
        )
        for key, value in headers_raw.items()
    }
    basic_auth: BasicAuthConfig | None = None
    if "basic_auth" in raw:
        basic_raw = _mapping(raw["basic_auth"], "tasks.http.basic_auth")
        basic_auth = BasicAuthConfig(
            username=_expand_required_string(basic_raw.get("username"), "tasks.http.basic_auth.username", scopes),
            password=_expand_required_string(basic_raw.get("password"), "tasks.http.basic_auth.password", scopes),
        )
    token_file: Path | None = None
    if "bearer_token_file" in raw:
        token_value = _expand_required_string(raw["bearer_token_file"], "tasks.http.bearer_token_file", scopes)
        token_file = Path(token_value)
        if not token_file.is_absolute():
            token_file = config_dir / token_file
    if basic_auth is not None and token_file is not None:
        raise ConfigError("tasks.http cannot configure both basic_auth and bearer_token_file")
    return HttpConfig(
        timeout_seconds=timeout_seconds,
        retries=retries,
        headers=headers,
        basic_auth=basic_auth,
        bearer_token_file=token_file,
        verify_tls=_bool(raw.get("verify_tls", defaults.verify_tls), "tasks.http.verify_tls"),
        allow_missing_content_type=_bool(
            raw.get("allow_missing_content_type", defaults.allow_missing_content_type),
            "tasks.http.allow_missing_content_type",
        ),
    )


def _parse_labels(
    raw: object,
    field_name: str,
    scopes: Mapping[str, Mapping[str, object]],
) -> dict[str, str]:
    mapping = _mapping(raw, field_name)
    labels: dict[str, str] = {}
    for raw_name, raw_value in mapping.items():
        if not isinstance(raw_name, str) or _LABEL_NAME.fullmatch(raw_name) is None:
            raise ConfigError(f"invalid label name in {field_name}: {raw_name!r}")
        labels[raw_name] = _expand_string(raw_value, f"{field_name}.{raw_name}", scopes)
    return labels


def _expand_required_string(
    value: object,
    field_name: str,
    scopes: Mapping[str, Mapping[str, object]],
) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field_name} must be a non-empty string")
    return _expand_string(value, field_name, scopes)


def _expand_string(
    value: object,
    field_name: str,
    scopes: Mapping[str, Mapping[str, object]],
) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{field_name} must be a string")
    try:
        return expand_template(value, scopes, deferred=_DEFERRED_TARGET_VALUES)
    except TemplateError as error:
        raise ConfigError(f"{field_name}: {error}") from error


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{field_name} must be a table")
    return value


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{field_name} must be a non-negative integer")
    return value


def _bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{field_name} must be a boolean")
    return value
