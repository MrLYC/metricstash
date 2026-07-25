"""Streaming Prometheus/OpenMetrics text parsing for configured metric families."""

from __future__ import annotations

import codecs
import math
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from prometheus_client.parser import text_fd_to_metric_families

from metricstash.metrics import find_configured_metric
from metricstash.models import MetricConfig


class ExpositionError(ValueError):
    """Selected Prometheus/OpenMetrics text is malformed or inconsistent."""


@dataclass(frozen=True)
class ParsedSample:
    actual_name: str
    family_name: str
    metric_type: str
    labels: dict[str, str]
    timestamp_ms: int
    value: float | None
    value_repr: str | None


@dataclass(frozen=True)
class ParsedFamily:
    name: str
    metric_type: str
    help_text: str | None
    unit: str | None
    samples: tuple[ParsedSample, ...]


@dataclass(frozen=True)
class ExpositionResult:
    families: tuple[ParsedFamily, ...]
    sample_count: int


_SAMPLE_NAME = re.compile(r"^\s*([A-Za-z_:][A-Za-z0-9_:]*)")


def iter_metric_families(source: Iterable[str]) -> Iterator[Any]:
    """Yield prometheus-client Metric values from a line iterable without bulk reads."""
    yield from text_fd_to_metric_families(source)  # type: ignore[arg-type]


def parse_exposition(
    source: Iterable[str],
    metrics: tuple[MetricConfig, ...] | list[MetricConfig],
    *,
    collection_timestamp_ms: int,
    honor_timestamps: bool = True,
    max_samples: int = 25_000,
    max_labels: int = 32,
    max_label_value_bytes: int = 1024,
) -> ExpositionResult:
    """Parse only configured families from a streaming line iterable.

    The underlying client parser consumes `for line in source`, not `read()`. A
    filtering stream drops unconfigured lines before parsing, so an invalid
    unrelated metric cannot poison an explicitly allow-listed collection.
    """
    configured = tuple(metrics)
    if max_samples <= 0 or max_labels <= 0 or max_label_value_bytes <= 0:
        raise ValueError("parser limits must be positive")
    filtered = _SelectedLineStream(source, configured)
    by_name: dict[str, list[ParsedSample]] = {metric.name: [] for metric in configured}
    help_text: dict[str, str] = {}
    sample_count = 0
    try:
        parsed_metrics = iter_metric_families(filtered)
        for parsed_metric in parsed_metrics:
            for raw_sample in parsed_metric.samples:
                configured_metric = find_configured_metric(raw_sample.name, configured)
                if configured_metric is None:
                    continue
                _validate_metric_type(parsed_metric.type, configured_metric)
                labels = {str(name): str(value) for name, value in raw_sample.labels.items()}
                _validate_labels(labels, max_labels=max_labels, max_label_value_bytes=max_label_value_bytes)
                value, value_repr = _split_value(raw_sample.value)
                timestamp_ms = _sample_timestamp_ms(
                    raw_sample.timestamp,
                    collection_timestamp_ms=collection_timestamp_ms,
                    honor_timestamps=honor_timestamps,
                )
                by_name[configured_metric.name].append(
                    ParsedSample(
                        actual_name=str(raw_sample.name),
                        family_name=configured_metric.name,
                        metric_type=configured_metric.metric_type,
                        labels=labels,
                        timestamp_ms=timestamp_ms,
                        value=value,
                        value_repr=value_repr,
                    )
                )
                sample_count += 1
                if sample_count > max_samples:
                    raise ExpositionError(f"sample limit ({max_samples}) exceeded")
                if parsed_metric.documentation:
                    help_text[configured_metric.name] = str(parsed_metric.documentation)
    except (ValueError, TypeError) as error:
        if isinstance(error, ExpositionError):
            raise
        raise ExpositionError(f"invalid selected Prometheus text: {error}") from error
    families = tuple(
        ParsedFamily(
            name=metric.name,
            metric_type=metric.metric_type,
            help_text=help_text.get(metric.name),
            unit=filtered.units.get(metric.name),
            samples=tuple(by_name[metric.name]),
        )
        for metric in configured
        if by_name[metric.name]
    )
    _validate_required_and_structures(configured, families)
    return ExpositionResult(families=families, sample_count=sample_count)


class _SelectedLineStream:
    """Filter line-oriented exposition before handing it to prometheus-client."""

    def __init__(self, source: Iterable[str], metrics: tuple[MetricConfig, ...]) -> None:
        self.source = source
        self.metrics = metrics
        self.by_name = {metric.name: metric for metric in metrics}
        self.units: dict[str, str] = {}
        self._current_selected = False

    def __iter__(self) -> Iterator[str]:
        for raw_line in self.source:
            if not isinstance(raw_line, str):
                raise ExpositionError("text parser received a non-string line")
            stripped = raw_line.strip()
            if not stripped:
                if self._current_selected:
                    yield raw_line
                continue
            if stripped.startswith("#"):
                yield from self._handle_comment(raw_line, stripped)
                continue
            sample_match = _SAMPLE_NAME.match(raw_line)
            sample_name = sample_match.group(1) if sample_match else None
            if sample_name is not None and find_configured_metric(sample_name, self.metrics) is not None:
                self._current_selected = True
                yield raw_line
            elif self._current_selected and sample_name is None:
                # It cannot be attributed to an unrelated valid family, so a selected
                # declaration must surface the syntax error rather than silently hide it.
                yield raw_line

    def _handle_comment(self, raw_line: str, stripped: str) -> Iterator[str]:
        parts = stripped.split(None, 3)
        if len(parts) < 2 or parts[0] != "#":
            if self._current_selected:
                yield raw_line
            return
        directive = parts[1]
        if directive not in {"HELP", "TYPE", "UNIT"}:
            return
        name = parts[2] if len(parts) >= 3 else None
        configured = self.by_name.get(name) if name is not None else None
        self._current_selected = configured is not None
        if configured is None:
            return
        if directive == "TYPE":
            if len(parts) < 4:
                yield raw_line
                return
            observed_type = parts[3].split(None, 1)[0]
            if observed_type != configured.metric_type:
                raise ExpositionError(
                    f"type mismatch for {configured.name}: configured {configured.metric_type}, endpoint {observed_type}"
                )
            yield raw_line
            return
        if directive == "UNIT":
            if len(parts) < 4:
                raise ExpositionError(f"invalid UNIT declaration for selected metric {configured.name}")
            self.units[configured.name] = parts[3].split(None, 1)[0]
            return
        yield raw_line


class IncrementalExpositionParser:
    """Parse selected metric samples while an HTTP response is still arriving.

    Only an unfinished UTF-8 line is retained. Each complete selected sample is
    parsed immediately through `prometheus-client` and returned to the caller,
    which can persist batches inside its target transaction. The compact
    validator keeps only the structural state required for histogram/summary
    checks until `finish()`.
    """

    def __init__(
        self,
        metrics: tuple[MetricConfig, ...] | list[MetricConfig],
        *,
        collection_timestamp_ms: int,
        honor_timestamps: bool = True,
        max_samples: int = 25_000,
        max_labels: int = 32,
        max_label_value_bytes: int = 1024,
    ) -> None:
        self.metrics = tuple(metrics)
        self.by_name = {metric.name: metric for metric in self.metrics}
        self.collection_timestamp_ms = collection_timestamp_ms
        self.honor_timestamps = honor_timestamps
        self.max_samples = max_samples
        self.max_labels = max_labels
        self.max_label_value_bytes = max_label_value_bytes
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._pending = ""
        self._current_selected = False
        self._help: dict[str, str] = {}
        self._units: dict[str, str] = {}
        self._sample_count = 0
        self._closed = False
        self._validator = _IncrementalValidator(self.metrics)

    @property
    def sample_count(self) -> int:
        return self._sample_count

    def feed_bytes(self, chunk: bytes) -> tuple[ParsedFamily, ...]:
        """Decode a response chunk and return complete selected samples immediately."""
        if self._closed:
            raise ExpositionError("cannot feed a finished exposition parser")
        try:
            text = self._decoder.decode(chunk)
        except UnicodeDecodeError as error:
            raise ExpositionError(f"metrics response is not valid UTF-8: {error}") from error
        return self._feed_text(text)

    def finish(self) -> tuple[ParsedFamily, ...]:
        """Flush the last line and validate required/compound metric families."""
        if self._closed:
            return ()
        try:
            tail = self._decoder.decode(b"", final=True)
        except UnicodeDecodeError as error:
            raise ExpositionError(f"metrics response is not valid UTF-8: {error}") from error
        emitted = list(self._feed_text(tail))
        if self._pending:
            emitted.extend(self._consume_line(self._pending))
            self._pending = ""
        self._validator.finish()
        self._closed = True
        return tuple(emitted)

    def _feed_text(self, text: str) -> tuple[ParsedFamily, ...]:
        self._pending += text
        emitted: list[ParsedFamily] = []
        while "\n" in self._pending:
            line, self._pending = self._pending.split("\n", 1)
            emitted.extend(self._consume_line(f"{line}\n"))
        return tuple(emitted)

    def _consume_line(self, raw_line: str) -> tuple[ParsedFamily, ...]:
        stripped = raw_line.strip()
        if not stripped:
            return ()
        if stripped.startswith("#"):
            self._consume_comment(stripped)
            return ()
        sample_match = _SAMPLE_NAME.match(raw_line)
        sample_name = sample_match.group(1) if sample_match else None
        configured = find_configured_metric(sample_name, self.metrics) if sample_name is not None else None
        if configured is None:
            if self._current_selected and sample_name is None:
                self._parse_selected_line(raw_line)
            return ()
        self._current_selected = True
        return self._parse_selected_line(raw_line, configured)

    def _consume_comment(self, stripped: str) -> None:
        parts = stripped.split(None, 3)
        if len(parts) < 2 or parts[0] != "#":
            return
        directive = parts[1]
        if directive not in {"HELP", "TYPE", "UNIT"}:
            return
        name = parts[2] if len(parts) >= 3 else None
        configured = self.by_name.get(name) if name is not None else None
        self._current_selected = configured is not None
        if configured is None:
            return
        if directive == "TYPE":
            if len(parts) < 4:
                raise ExpositionError(f"invalid TYPE declaration for selected metric {configured.name}")
            observed_type = parts[3].split(None, 1)[0]
            if observed_type != configured.metric_type:
                raise ExpositionError(
                    f"type mismatch for {configured.name}: configured {configured.metric_type}, endpoint {observed_type}"
                )
        elif directive == "HELP" and len(parts) >= 4:
            self._help[configured.name] = parts[3]
        elif directive == "UNIT":
            if len(parts) < 4:
                raise ExpositionError(f"invalid UNIT declaration for selected metric {configured.name}")
            self._units[configured.name] = parts[3].split(None, 1)[0]

    def _parse_selected_line(
        self, raw_line: str, configured: MetricConfig | None = None
    ) -> tuple[ParsedFamily, ...]:
        try:
            parsed_metrics = tuple(iter_metric_families((raw_line,)))
        except (ValueError, TypeError) as error:
            raise ExpositionError(f"invalid selected Prometheus text: {error}") from error
        samples: list[ParsedSample] = []
        for parsed_metric in parsed_metrics:
            for raw_sample in parsed_metric.samples:
                metric = configured or find_configured_metric(raw_sample.name, self.metrics)
                if metric is None:
                    continue
                labels = {str(name): str(value) for name, value in raw_sample.labels.items()}
                _validate_labels(
                    labels,
                    max_labels=self.max_labels,
                    max_label_value_bytes=self.max_label_value_bytes,
                )
                value, value_repr = _split_value(raw_sample.value)
                timestamp_ms = _sample_timestamp_ms(
                    raw_sample.timestamp,
                    collection_timestamp_ms=self.collection_timestamp_ms,
                    honor_timestamps=self.honor_timestamps,
                )
                sample = ParsedSample(
                    actual_name=str(raw_sample.name),
                    family_name=metric.name,
                    metric_type=metric.metric_type,
                    labels=labels,
                    timestamp_ms=timestamp_ms,
                    value=value,
                    value_repr=value_repr,
                )
                self._sample_count += 1
                if self._sample_count > self.max_samples:
                    raise ExpositionError(f"sample limit ({self.max_samples}) exceeded")
                self._validator.observe(sample)
                samples.append(sample)
        if not samples:
            raise ExpositionError("invalid selected Prometheus text: line did not yield a sample")
        family_name = samples[0].family_name
        return (
            ParsedFamily(
                name=family_name,
                metric_type=samples[0].metric_type,
                help_text=self._help.get(family_name),
                unit=self._units.get(family_name),
                samples=tuple(samples),
            ),
        )


class _IncrementalValidator:
    def __init__(self, metrics: tuple[MetricConfig, ...]) -> None:
        self.metrics = {metric.name: metric for metric in metrics}
        self.seen: set[str] = set()
        self.histograms: dict[tuple[str, tuple[tuple[str, str], ...], int], dict[str, object]] = {}
        self.summaries: dict[tuple[str, tuple[tuple[str, str], ...], int], dict[str, object]] = {}

    def observe(self, sample: ParsedSample) -> None:
        self.seen.add(sample.family_name)
        if sample.metric_type == "histogram":
            self._observe_histogram(sample)
        elif sample.metric_type == "summary":
            self._observe_summary(sample)

    def finish(self) -> None:
        for metric in self.metrics.values():
            if metric.required and metric.name not in self.seen:
                raise ExpositionError(f"required metric family is missing: {metric.name}")
        for (family_name, _, _), group in self.histograms.items():
            if group.get("inf") is None or group.get("count") is None or group.get("sum") is None:
                raise ExpositionError(f"incomplete histogram {family_name}")
            if group["inf"] != group["count"]:
                raise ExpositionError(f"histogram {family_name} +Inf bucket does not equal _count")
        for (family_name, _, _), group in self.summaries.items():
            if not group["quantile"] or group.get("count") is None or group.get("sum") is None:
                raise ExpositionError(f"incomplete summary {family_name}")

    def _observe_histogram(self, sample: ParsedSample) -> None:
        labels = dict(sample.labels)
        if sample.actual_name.endswith("_bucket"):
            if "le" not in labels:
                raise ExpositionError(f"incomplete histogram {sample.family_name}: bucket lacks le")
            upper_bound = labels.pop("le")
            key = (sample.family_name, tuple(sorted(labels.items())), sample.timestamp_ms)
            group = self.histograms.setdefault(key, {"inf": None, "count": None, "sum": None})
            if upper_bound == "+Inf":
                group["inf"] = (sample.value, sample.value_repr)
        else:
            key = (sample.family_name, tuple(sorted(labels.items())), sample.timestamp_ms)
            group = self.histograms.setdefault(key, {"inf": None, "count": None, "sum": None})
            if sample.actual_name.endswith("_count"):
                group["count"] = (sample.value, sample.value_repr)
            elif sample.actual_name.endswith("_sum"):
                group["sum"] = (sample.value, sample.value_repr)

    def _observe_summary(self, sample: ParsedSample) -> None:
        labels = dict(sample.labels)
        if sample.actual_name == sample.family_name:
            if "quantile" not in labels:
                raise ExpositionError(f"incomplete summary {sample.family_name}: sample lacks quantile")
            labels.pop("quantile")
            key = (sample.family_name, tuple(sorted(labels.items())), sample.timestamp_ms)
            group = self.summaries.setdefault(key, {"quantile": False, "count": None, "sum": None})
            group["quantile"] = True
        else:
            key = (sample.family_name, tuple(sorted(labels.items())), sample.timestamp_ms)
            group = self.summaries.setdefault(key, {"quantile": False, "count": None, "sum": None})
            if sample.actual_name.endswith("_count"):
                group["count"] = (sample.value, sample.value_repr)
            elif sample.actual_name.endswith("_sum"):
                group["sum"] = (sample.value, sample.value_repr)


def _validate_metric_type(observed_type: str, configured: MetricConfig) -> None:
    if observed_type not in {"untyped", "unknown"} and observed_type != configured.metric_type:
        raise ExpositionError(
            f"type mismatch for {configured.name}: configured {configured.metric_type}, endpoint {observed_type}"
        )


def _validate_labels(labels: dict[str, str], *, max_labels: int, max_label_value_bytes: int) -> None:
    if len(labels) > max_labels:
        raise ExpositionError(f"label limit ({max_labels}) exceeded")
    for name, value in labels.items():
        if len(value.encode("utf-8")) > max_label_value_bytes:
            raise ExpositionError(f"label value limit ({max_label_value_bytes}) exceeded for {name}")


def _split_value(raw_value: object) -> tuple[float | None, str | None]:
    value = float(raw_value)
    if math.isnan(value):
        return None, "NaN"
    if math.isinf(value):
        return (None, "+Inf") if value > 0 else (None, "-Inf")
    return value, None


def _sample_timestamp_ms(
    source_timestamp: float | None,
    *,
    collection_timestamp_ms: int,
    honor_timestamps: bool,
) -> int:
    if source_timestamp is None or not honor_timestamps:
        return collection_timestamp_ms
    return int(round(source_timestamp * 1000))


def _validate_required_and_structures(
    configured: tuple[MetricConfig, ...], families: tuple[ParsedFamily, ...]
) -> None:
    found = {family.name: family for family in families}
    for metric in configured:
        family = found.get(metric.name)
        if family is None:
            if metric.required:
                raise ExpositionError(f"required metric family is missing: {metric.name}")
            continue
        if metric.metric_type == "histogram":
            _validate_histogram(family)
        elif metric.metric_type == "summary":
            _validate_summary(family)


def _validate_histogram(family: ParsedFamily) -> None:
    groups: dict[tuple[tuple[tuple[str, str], ...], int], dict[str, object]] = {}
    for sample in family.samples:
        labels = dict(sample.labels)
        if sample.actual_name.endswith("_bucket"):
            if "le" not in labels:
                raise ExpositionError(f"incomplete histogram {family.name}: bucket lacks le")
            upper_bound = labels.pop("le")
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            group = groups.setdefault(key, {"buckets": {}, "count": None, "sum": None})
            buckets = group["buckets"]
            assert isinstance(buckets, dict)
            buckets[upper_bound] = sample
        elif sample.actual_name.endswith("_count"):
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            groups.setdefault(key, {"buckets": {}, "count": None, "sum": None})["count"] = sample
        elif sample.actual_name.endswith("_sum"):
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            groups.setdefault(key, {"buckets": {}, "count": None, "sum": None})["sum"] = sample
    for group in groups.values():
        buckets = group["buckets"]
        count = group["count"]
        total = group["sum"]
        if not isinstance(buckets, dict) or "+Inf" not in buckets or not isinstance(count, ParsedSample) or not isinstance(total, ParsedSample):
            raise ExpositionError(f"incomplete histogram {family.name}")
        infinity = buckets["+Inf"]
        assert isinstance(infinity, ParsedSample)
        if infinity.value != count.value or infinity.value_repr != count.value_repr:
            raise ExpositionError(f"histogram {family.name} +Inf bucket does not equal _count")


def _validate_summary(family: ParsedFamily) -> None:
    groups: dict[tuple[tuple[tuple[str, str], ...], int], dict[str, object]] = {}
    for sample in family.samples:
        labels = dict(sample.labels)
        if sample.actual_name == family.name:
            if "quantile" not in labels:
                raise ExpositionError(f"incomplete summary {family.name}: sample lacks quantile")
            labels.pop("quantile")
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            group = groups.setdefault(key, {"quantiles": [], "count": None, "sum": None})
            quantiles = group["quantiles"]
            assert isinstance(quantiles, list)
            quantiles.append(sample)
        elif sample.actual_name.endswith("_count"):
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            groups.setdefault(key, {"quantiles": [], "count": None, "sum": None})["count"] = sample
        elif sample.actual_name.endswith("_sum"):
            key = (tuple(sorted(labels.items())), sample.timestamp_ms)
            groups.setdefault(key, {"quantiles": [], "count": None, "sum": None})["sum"] = sample
    for group in groups.values():
        if not group["quantiles"] or not isinstance(group["count"], ParsedSample) or not isinstance(group["sum"], ParsedSample):
            raise ExpositionError(f"incomplete summary {family.name}")
