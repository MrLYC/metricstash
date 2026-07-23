from __future__ import annotations

import pytest

from metricstash.db import Database
from metricstash.inspect import InspectError, inspect_histogram, inspect_summary


def _store(db: Database, name: str, family: str, metric_type: str, labels: dict[str, str], value: float) -> None:
    series_id = db.ensure_series(name, family, metric_type, labels)
    db.upsert_sample(series_id, 1_000, value=value, value_repr=None, scrape_id=1)


def test_histogram_inspection_groups_by_labels_except_le(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        _store(db, "latency_seconds_bucket", "latency_seconds", "histogram", {"job": "api", "le": "0.5"}, 3)
        _store(db, "latency_seconds_bucket", "latency_seconds", "histogram", {"job": "api", "le": "+Inf"}, 5)
        _store(db, "latency_seconds_count", "latency_seconds", "histogram", {"job": "api"}, 5)
        _store(db, "latency_seconds_sum", "latency_seconds", "histogram", {"job": "api"}, 1.25)

        view = inspect_histogram(db, "latency_seconds", at_ms=1_000)

        assert view.groups[0].labels == {"job": "api"}
        assert view.groups[0].buckets[-1].upper_bound == "+Inf"
        assert view.groups[0].count is not None
        assert view.groups[0].count.value == view.groups[0].buckets[-1].sample.value
    finally:
        db.close()


def test_summary_inspection_shows_quantiles_without_calculating_new_ones(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        _store(db, "rpc_seconds", "rpc_seconds", "summary", {"job": "api", "quantile": "0.5"}, 0.2)
        _store(db, "rpc_seconds_count", "rpc_seconds", "summary", {"job": "api"}, 4)
        _store(db, "rpc_seconds_sum", "rpc_seconds", "summary", {"job": "api"}, 1.1)

        view = inspect_summary(db, "rpc_seconds", at_ms=1_000)

        assert view.groups[0].labels == {"job": "api"}
        assert view.groups[0].quantiles[0].quantile == "0.5"
        assert view.groups[0].quantiles[0].sample.value == 0.2
    finally:
        db.close()


def test_histogram_inspection_rejects_incomplete_stored_group(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        _store(db, "latency_seconds_bucket", "latency_seconds", "histogram", {"le": "+Inf"}, 5)

        with pytest.raises(InspectError, match="incomplete"):
            inspect_histogram(db, "latency_seconds", at_ms=1_000)
    finally:
        db.close()
