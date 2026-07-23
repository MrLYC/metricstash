"""Configured metric-family rules and classic sample-name expansion."""

from __future__ import annotations

from collections.abc import Iterable

from metricstash.models import MetricConfig


def allowed_sample_names(name: str, metric_type: str) -> set[str]:
    """Return actual Prometheus sample names belonging to a configured family."""
    if metric_type == "histogram":
        return {f"{name}_bucket", f"{name}_sum", f"{name}_count"}
    if metric_type == "summary":
        return {name, f"{name}_sum", f"{name}_count"}
    return {name}


def family_base_for_sample(sample_name: str, configured_name: str, metric_type: str) -> str:
    """Return the configured base family for a known accepted sample."""
    if sample_name not in allowed_sample_names(configured_name, metric_type):
        raise ValueError(f"{sample_name!r} is not part of {configured_name!r}")
    return configured_name


def find_configured_metric(
    sample_name: str, metrics: Iterable[MetricConfig]
) -> MetricConfig | None:
    """Find the configured family owning an actual exposition sample name."""
    for metric in metrics:
        if sample_name in allowed_sample_names(metric.name, metric.metric_type):
            return metric
    return None
