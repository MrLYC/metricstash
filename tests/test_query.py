from __future__ import annotations

import pytest

from metricstash.db import Database
from metricstash.query import ResultLimitError, parse_selector, query_series


def test_parse_range_selector() -> None:
    selector = parse_selector('http_requests_total{cluster="prod",pod!~"canary-.*"}[5m]')

    assert selector.metric_name == "http_requests_total"
    assert selector.range_ms == 300_000
    assert [matcher.operator for matcher in selector.matchers] == ["=", "!~"]


def test_missing_label_matches_negative_matcher(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("x", "x", "gauge", {"job": "api"})
        db.upsert_sample(series_id, 1_000, value=1.0, value_repr=None, scrape_id=1)

        result = query_series(db, 'x{cluster!="prod"}', at_ms=1_000)

        assert [series.labels for series in result.series] == [{"job": "api"}]
    finally:
        db.close()


def test_range_query_uses_open_left_closed_right_boundary(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("x", "x", "gauge", {})
        for timestamp in (500, 501, 1_000):
            db.upsert_sample(series_id, timestamp, value=float(timestamp), value_repr=None, scrape_id=1)

        result = query_series(db, "x[500ms]", at_ms=1_000)

        assert [sample.timestamp_ms for sample in result.series[0].samples] == [501, 1_000]
    finally:
        db.close()


def test_instant_query_returns_latest_sample_at_or_before_evaluation_time(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("x", "x", "gauge", {})
        db.upsert_sample(series_id, 900, value=1.0, value_repr=None, scrape_id=1, collected_at_ms=910)
        db.upsert_sample(series_id, 1_100, value=2.0, value_repr=None, scrape_id=2, collected_at_ms=1_110)

        result = query_series(db, "x", at_ms=1_000)

        sample = result.series[0].samples[0]
        assert sample.timestamp_ms == 900
        assert sample.collected_at_ms == 910
        assert sample.age_ms == 100
    finally:
        db.close()


def test_instant_query_max_age_filters_old_samples(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("x", "x", "gauge", {})
        db.upsert_sample(series_id, 900, value=1.0, value_repr=None, scrape_id=1)

        result = query_series(db, "x", at_ms=1_000, max_age_ms=50)

        assert result.series == ()
    finally:
        db.close()


def test_result_limit_fails_instead_of_truncating(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        for instance in ("a", "b"):
            series_id = db.ensure_series("x", "x", "gauge", {"instance": instance})
            db.upsert_sample(series_id, 1_000, value=1.0, value_repr=None, scrape_id=1)

        with pytest.raises(ResultLimitError, match="series limit"):
            query_series(db, "x", at_ms=1_000, series_limit=1)
    finally:
        db.close()
