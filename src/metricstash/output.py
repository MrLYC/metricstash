"""Stable table and JSON rendering without a terminal UI dependency."""

from __future__ import annotations

import json
from typing import Any

from metricstash.inspect import HistogramView, SummaryView
from metricstash.query import QueryResult


def render_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def query_to_dict(result: QueryResult) -> dict[str, object]:
    return {
        "selector": _selector_to_dict(result.selector),
        "at_ms": result.at_ms,
        "series": [
            {
                "id": series.series_id,
                "metric_name": series.metric_name,
                "family_name": series.family_name,
                "metric_type": series.metric_type,
                "labels": series.labels,
                "samples": [_sample_to_dict(sample) for sample in series.samples],
            }
            for series in result.series
        ],
    }


def render_query_table(result: QueryResult) -> str:
    rows = [["metric", "labels", "sample_ms", "collected_ms", "age_ms", "value"]]
    for series in result.series:
        for sample in series.samples:
            rows.append(
                [
                    series.metric_name,
                    json.dumps(series.labels, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    str(sample.timestamp_ms),
                    str(sample.collected_at_ms),
                    str(sample.age_ms),
                    _display_value(sample.value, sample.value_repr),
                ]
            )
    return _render_table(rows)


def histogram_to_dict(view: HistogramView) -> dict[str, object]:
    return {
        "kind": "histogram",
        "selector": _selector_to_dict(view.selector),
        "at_ms": view.at_ms,
        "groups": [
            {
                "labels": group.labels,
                "timestamp_ms": group.timestamp_ms,
                "buckets": [
                    {"le": bucket.upper_bound, "sample": _sample_to_dict(bucket.sample)}
                    for bucket in group.buckets
                ],
                "count": _sample_to_dict(group.count) if group.count is not None else None,
                "sum": _sample_to_dict(group.sum) if group.sum is not None else None,
            }
            for group in view.groups
        ],
    }


def summary_to_dict(view: SummaryView) -> dict[str, object]:
    return {
        "kind": "summary",
        "selector": _selector_to_dict(view.selector),
        "at_ms": view.at_ms,
        "groups": [
            {
                "labels": group.labels,
                "timestamp_ms": group.timestamp_ms,
                "quantiles": [
                    {"quantile": item.quantile, "sample": _sample_to_dict(item.sample)}
                    for item in group.quantiles
                ],
                "count": _sample_to_dict(group.count) if group.count is not None else None,
                "sum": _sample_to_dict(group.sum) if group.sum is not None else None,
            }
            for group in view.groups
        ],
    }


def render_histogram_table(view: HistogramView) -> str:
    rows = [["labels", "timestamp_ms", "component", "bound", "value"]]
    for group in view.groups:
        labels = json.dumps(group.labels, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        timestamp = "" if group.timestamp_ms is None else str(group.timestamp_ms)
        for bucket in group.buckets:
            rows.append([labels, timestamp, "bucket", bucket.upper_bound, _display_value(bucket.sample.value, bucket.sample.value_repr)])
        if group.count is not None:
            rows.append([labels, timestamp, "count", "", _display_value(group.count.value, group.count.value_repr)])
        if group.sum is not None:
            rows.append([labels, timestamp, "sum", "", _display_value(group.sum.value, group.sum.value_repr)])
    return _render_table(rows)


def render_summary_table(view: SummaryView) -> str:
    rows = [["labels", "timestamp_ms", "component", "quantile", "value"]]
    for group in view.groups:
        labels = json.dumps(group.labels, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        timestamp = "" if group.timestamp_ms is None else str(group.timestamp_ms)
        for item in group.quantiles:
            rows.append([labels, timestamp, "quantile", item.quantile, _display_value(item.sample.value, item.sample.value_repr)])
        if group.count is not None:
            rows.append([labels, timestamp, "count", "", _display_value(group.count.value, group.count.value_repr)])
        if group.sum is not None:
            rows.append([labels, timestamp, "sum", "", _display_value(group.sum.value, group.sum.value_repr)])
    return _render_table(rows)


def _selector_to_dict(selector: Any) -> dict[str, object]:
    return {
        "metric_name": selector.metric_name,
        "range_ms": selector.range_ms,
        "matchers": [
            {"name": matcher.name, "operator": matcher.operator, "value": matcher.value}
            for matcher in selector.matchers
        ],
    }


def _sample_to_dict(sample: Any) -> dict[str, object]:
    return {
        "timestamp_ms": sample.timestamp_ms,
        "value": sample.value,
        "value_repr": sample.value_repr,
        "scrape_id": sample.scrape_id,
        "collected_at_ms": sample.collected_at_ms,
        "age_ms": sample.age_ms,
    }


def _display_value(value: float | None, value_repr: str | None) -> str:
    return value_repr if value_repr is not None else str(value)


def _render_table(rows: list[list[str]]) -> str:
    if len(rows) == 1:
        return "(no matching samples)\n"
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    rendered = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip() for row in rows]
    return "\n".join(rendered) + "\n"
