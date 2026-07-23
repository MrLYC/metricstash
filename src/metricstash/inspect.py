"""Presentation-only reconstruction of stored classic histograms and summaries."""

from __future__ import annotations

import json
from dataclasses import dataclass

from metricstash.db import Database
from metricstash.query import QuerySample, Selector, _compile_matchers, _labels_match, parse_selector


class InspectError(ValueError):
    """Stored family data cannot be presented as a structurally valid type."""


@dataclass(frozen=True)
class HistogramBucket:
    upper_bound: str
    sample: QuerySample


@dataclass(frozen=True)
class HistogramGroup:
    labels: dict[str, str]
    buckets: tuple[HistogramBucket, ...]
    count: QuerySample | None
    sum: QuerySample | None
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class HistogramView:
    selector: Selector
    at_ms: int
    groups: tuple[HistogramGroup, ...]


@dataclass(frozen=True)
class SummaryQuantile:
    quantile: str
    sample: QuerySample


@dataclass(frozen=True)
class SummaryGroup:
    labels: dict[str, str]
    quantiles: tuple[SummaryQuantile, ...]
    count: QuerySample | None
    sum: QuerySample | None
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class SummaryView:
    selector: Selector
    at_ms: int
    groups: tuple[SummaryGroup, ...]


def inspect_histogram(database: Database, selector: str | Selector, *, at_ms: int) -> HistogramView:
    """Show stored bucket, count, and sum components without deriving new values."""
    parsed = parse_selector(selector) if isinstance(selector, str) else selector
    components = _load_family_components(database, parsed, at_ms, "histogram")
    groups: dict[tuple[tuple[str, str], ...] | tuple[tuple[tuple[str, str], ...], int], dict[str, object]] = {}
    for metric_name, labels, sample in components:
        group_labels = {name: value for name, value in labels.items() if name != "le"}
        key = _group_key(group_labels, sample, parsed)
        group = groups.setdefault(key, {"labels": group_labels, "buckets": {}, "count": None, "sum": None, "sample": sample})
        if metric_name == f"{parsed.metric_name}_bucket":
            if "le" not in labels:
                raise InspectError("histogram bucket is missing its le label")
            buckets = group["buckets"]
            assert isinstance(buckets, dict)
            buckets[labels["le"]] = sample
        elif metric_name == f"{parsed.metric_name}_count":
            group["count"] = sample
        elif metric_name == f"{parsed.metric_name}_sum":
            group["sum"] = sample
        else:
            raise InspectError(f"unexpected histogram sample {metric_name!r}")
    rendered: list[HistogramGroup] = []
    for group in groups.values():
        buckets = group["buckets"]
        count = group["count"]
        total = group["sum"]
        if not isinstance(buckets, dict) or not buckets or not isinstance(count, QuerySample) or not isinstance(total, QuerySample):
            raise InspectError("incomplete stored histogram group")
        infinity = buckets.get("+Inf")
        if not isinstance(infinity, QuerySample):
            raise InspectError("incomplete stored histogram group: missing +Inf bucket")
        if not _same_sample_value(infinity, count):
            raise InspectError("histogram +Inf bucket does not equal _count")
        ordered_buckets = tuple(
            HistogramBucket(upper_bound=upper_bound, sample=bucket_sample)
            for upper_bound, bucket_sample in sorted(buckets.items(), key=lambda item: _bucket_sort_key(item[0]))
        )
        labels = group["labels"]
        sample = group["sample"]
        assert isinstance(labels, dict) and isinstance(sample, QuerySample)
        rendered.append(
            HistogramGroup(
                labels=labels,
                buckets=ordered_buckets,
                count=count,
                sum=total,
                timestamp_ms=sample.timestamp_ms if parsed.range_ms is not None else None,
            )
        )
    return HistogramView(selector=parsed, at_ms=at_ms, groups=tuple(_sort_groups(rendered)))


def inspect_summary(database: Database, selector: str | Selector, *, at_ms: int) -> SummaryView:
    """Show stored quantile, count, and sum components without calculating quantiles."""
    parsed = parse_selector(selector) if isinstance(selector, str) else selector
    components = _load_family_components(database, parsed, at_ms, "summary")
    groups: dict[tuple[tuple[str, str], ...] | tuple[tuple[tuple[str, str], ...], int], dict[str, object]] = {}
    for metric_name, labels, sample in components:
        group_labels = {name: value for name, value in labels.items() if name != "quantile"}
        key = _group_key(group_labels, sample, parsed)
        group = groups.setdefault(key, {"labels": group_labels, "quantiles": {}, "count": None, "sum": None, "sample": sample})
        if metric_name == parsed.metric_name:
            if "quantile" not in labels:
                raise InspectError("summary sample is missing its quantile label")
            quantiles = group["quantiles"]
            assert isinstance(quantiles, dict)
            quantiles[labels["quantile"]] = sample
        elif metric_name == f"{parsed.metric_name}_count":
            group["count"] = sample
        elif metric_name == f"{parsed.metric_name}_sum":
            group["sum"] = sample
        else:
            raise InspectError(f"unexpected summary sample {metric_name!r}")
    rendered: list[SummaryGroup] = []
    for group in groups.values():
        quantiles = group["quantiles"]
        count = group["count"]
        total = group["sum"]
        if not isinstance(quantiles, dict) or not quantiles or not isinstance(count, QuerySample) or not isinstance(total, QuerySample):
            raise InspectError("incomplete stored summary group")
        ordered_quantiles = tuple(
            SummaryQuantile(quantile=quantile, sample=sample)
            for quantile, sample in sorted(quantiles.items(), key=lambda item: _quantile_sort_key(item[0]))
        )
        labels = group["labels"]
        sample = group["sample"]
        assert isinstance(labels, dict) and isinstance(sample, QuerySample)
        rendered.append(
            SummaryGroup(
                labels=labels,
                quantiles=ordered_quantiles,
                count=count,
                sum=total,
                timestamp_ms=sample.timestamp_ms if parsed.range_ms is not None else None,
            )
        )
    return SummaryView(selector=parsed, at_ms=at_ms, groups=tuple(_sort_groups(rendered)))


def _load_family_components(
    database: Database,
    selector: Selector,
    at_ms: int,
    metric_type: str,
) -> list[tuple[str, dict[str, str], QuerySample]]:
    compiled = _compile_matchers(selector.matchers)
    rows = database.connection.execute(
        """
        SELECT id, metric_name, labels_json FROM series
        WHERE family_name = ? AND metric_type = ? ORDER BY id
        """,
        (selector.metric_name, metric_type),
    )
    results: list[tuple[str, dict[str, str], QuerySample]] = []
    for row in rows:
        labels = {str(name): str(value) for name, value in json.loads(row["labels_json"]).items()}
        if not _labels_match(labels, compiled):
            continue
        for sample in _load_component_samples(database, int(row["id"]), selector, at_ms):
            results.append((str(row["metric_name"]), labels, sample))
    return results


def _load_component_samples(
    database: Database, series_id: int, selector: Selector, at_ms: int
) -> list[QuerySample]:
    if selector.range_ms is None:
        sql = """
            SELECT sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms
            FROM samples WHERE series_id = ? AND sample_timestamp_ms <= ?
            ORDER BY sample_timestamp_ms DESC LIMIT 1
        """
        parameters = (series_id, at_ms)
    else:
        sql = """
            SELECT sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms
            FROM samples
            WHERE series_id = ? AND sample_timestamp_ms > ? AND sample_timestamp_ms <= ?
            ORDER BY sample_timestamp_ms
        """
        parameters = (series_id, at_ms - selector.range_ms, at_ms)
    rows = database.connection.execute(sql, parameters).fetchall()
    return [
        QuerySample(
            timestamp_ms=int(row["sample_timestamp_ms"]),
            value=row["value"],
            value_repr=row["value_repr"],
            scrape_id=int(row["scrape_id"]),
            collected_at_ms=int(row["collected_at_ms"]),
            age_ms=at_ms - int(row["sample_timestamp_ms"]),
        )
        for row in rows
    ]


def _group_key(
    labels: dict[str, str], sample: QuerySample, selector: Selector
) -> tuple[tuple[str, str], ...] | tuple[tuple[tuple[str, str], ...], int]:
    base = tuple(sorted(labels.items()))
    if selector.range_ms is None:
        return base
    return (base, sample.timestamp_ms)


def _same_sample_value(left: QuerySample, right: QuerySample) -> bool:
    return left.value == right.value and left.value_repr == right.value_repr


def _bucket_sort_key(value: str) -> tuple[int, float | str]:
    if value == "+Inf":
        return (1, 0.0)
    try:
        return (0, float(value))
    except ValueError:
        return (0, value)


def _quantile_sort_key(value: str) -> tuple[int, float | str]:
    try:
        return (0, float(value))
    except ValueError:
        return (1, value)


def _sort_groups(groups: list[HistogramGroup] | list[SummaryGroup]) -> list[HistogramGroup] | list[SummaryGroup]:
    return sorted(groups, key=lambda group: (tuple(sorted(group.labels.items())), group.timestamp_ms or -1))
