"""Prometheus-compatible target and endpoint label precedence."""

from __future__ import annotations

from collections.abc import Mapping


def merge_labels(
    *,
    endpoint: Mapping[str, str],
    collector: Mapping[str, str],
    honor_labels: bool,
    resolved_ip: str,
) -> dict[str, str]:
    """Merge endpoint labels with collector identity labels.

    This follows Prometheus' `honor_labels` behavior, except that `resolved_ip`
    is always appended by Metricstash to preserve per-IP collection identity.
    """
    merged = dict(endpoint)
    if honor_labels:
        for name, value in collector.items():
            merged.setdefault(name, value)
    else:
        for name, value in collector.items():
            if name in merged:
                endpoint_value = merged.pop(name)
                exported_name = _available_exported_name(name, merged, collector)
                merged[exported_name] = endpoint_value
            merged[name] = value
    merged["resolved_ip"] = resolved_ip
    return merged


def _available_exported_name(
    name: str,
    existing: Mapping[str, str],
    collector: Mapping[str, str],
) -> str:
    candidate = f"exported_{name}"
    while candidate in existing or candidate in collector:
        candidate = f"exported_{candidate}"
    return candidate
