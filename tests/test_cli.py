from __future__ import annotations

import json

from metricstash.cli import main
from metricstash.db import Database


def test_query_json_output_is_machine_readable(tmp_path, capsys) -> None:
    db_path = tmp_path / "metrics.db"
    db = Database.open(db_path)
    try:
        series_id = db.ensure_series("x", "x", "gauge", {"job": "api"})
        db.upsert_sample(series_id, 1_000, value=1.0, value_repr=None, scrape_id=1, collected_at_ms=1_001)
    finally:
        db.close()

    exit_code = main(
        [
            "query",
            "--db",
            str(db_path),
            'x{job="api"}',
            "--at",
            "1970-01-01T00:00:01Z",
            "--format",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["series"][0]["labels"] == {"job": "api"}
    assert payload["series"][0]["samples"][0]["value"] == 1.0


def test_invalid_selector_returns_usage_exit_code(tmp_path, capsys) -> None:
    db_path = tmp_path / "metrics.db"
    Database.open(db_path).close()

    exit_code = main(["query", "--db", str(db_path), "{job=\"api\"}"])

    assert exit_code == 2
    assert "metric name" in capsys.readouterr().err


def test_missing_command_returns_usage_exit_code(capsys) -> None:
    assert main([]) == 2
    assert "a command is required" in capsys.readouterr().err
