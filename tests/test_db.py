from __future__ import annotations

import sqlite3

import pytest

from metricstash.db import Database
from metricstash.migrations import MigrationRequired


def test_sample_upsert_replaces_same_series_and_timestamp(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("requests_total", "requests_total", "counter", {"job": "api"})
        db.upsert_sample(series_id, 1_700_000_000_000, value=1.0, value_repr=None, scrape_id=1)
        db.upsert_sample(series_id, 1_700_000_000_000, value=2.0, value_repr=None, scrape_id=2)

        assert db.fetch_sample(series_id, 1_700_000_000_000) == (2.0, None, 2)
    finally:
        db.close()


def test_special_values_use_value_repr(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        series_id = db.ensure_series("temperature", "temperature", "gauge", {})
        db.upsert_sample(series_id, 1, value=None, value_repr="NaN", scrape_id=1)

        assert db.fetch_sample(series_id, 1) == (None, "NaN", 1)
    finally:
        db.close()


def test_series_identity_uses_canonical_full_label_set(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        first = db.ensure_series("x", "x", "gauge", {"b": "2", "a": "1"})
        second = db.ensure_series("x", "x", "gauge", {"a": "1", "b": "2"})

        assert first == second
        assert db.series_labels(first) == {"a": "1", "b": "2"}
    finally:
        db.close()


def test_database_initializes_new_schema_automatically(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        assert db.schema_version == 1
    finally:
        db.close()


def test_failed_target_transaction_rolls_back_business_rows(tmp_path) -> None:
    db = Database.open(tmp_path / "metrics.db")
    try:
        try:
            with db.transaction():
                series_id = db.ensure_series("x", "x", "gauge", {"job": "api"})
                db.upsert_sample(series_id, 1, value=1.0, value_repr=None, scrape_id=1)
                raise RuntimeError("target failed")
        except RuntimeError:
            pass

        assert db.connection.execute("SELECT COUNT(*) FROM series").fetchone()[0] == 0
        assert db.connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 0
    finally:
        db.close()


def test_mutating_database_open_rejects_another_writer(tmp_path) -> None:
    path = tmp_path / "metrics.db"
    first = Database.open(path, writer_lock=True)
    try:
        with pytest.raises(RuntimeError, match="database is busy"):
            Database.open(path, writer_lock=True)
    finally:
        first.close()


def test_existing_old_schema_requires_explicit_migration(tmp_path) -> None:
    path = tmp_path / "metrics.db"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(MigrationRequired, match="db migrate"):
        Database.open(path)

    db = Database.open(path, allow_migrate=True)
    try:
        assert db.schema_version == 1
    finally:
        db.close()


def test_read_only_open_does_not_create_a_missing_database(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        Database.open(tmp_path / "missing.db", create=False)


def test_read_only_open_does_not_initialize_an_empty_existing_file(tmp_path) -> None:
    path = tmp_path / "empty.db"
    path.touch()

    with pytest.raises(FileNotFoundError, match="not initialized"):
        Database.open(path, create=False)
