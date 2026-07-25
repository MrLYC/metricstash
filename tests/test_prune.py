from __future__ import annotations

from metricstash.cli import main
from metricstash.db import Database


def test_prune_requires_an_explicit_retention_boundary(tmp_path, capsys) -> None:
    db_path = tmp_path / "metrics.db"
    Database.open(db_path).close()

    exit_code = main(["prune", "--db", str(db_path)])

    assert exit_code == 2
    assert "one of the arguments" in capsys.readouterr().err


def test_prune_deletes_only_when_invoked_explicitly(tmp_path, capsys) -> None:
    db_path = tmp_path / "metrics.db"
    db = Database.open(db_path)
    try:
        series_id = db.ensure_series("x", "x", "gauge", {})
        db.upsert_sample(series_id, 1, value=1.0, value_repr=None, scrape_id=1)
        db.upsert_sample(series_id, 2_000, value=2.0, value_repr=None, scrape_id=1)
    finally:
        db.close()

    exit_code = main(["prune", "--db", str(db_path), "--before", "1970-01-01T00:00:01.500Z"])

    assert exit_code == 0
    db = Database.open(db_path)
    try:
        rows = db.connection.execute("SELECT sample_timestamp_ms FROM samples ORDER BY sample_timestamp_ms").fetchall()
        assert [row[0] for row in rows] == [2_000]
    finally:
        db.close()
