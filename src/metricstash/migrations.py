"""Versioned SQLite schema for Metricstash."""

from __future__ import annotations

import sqlite3
import time


LATEST_SCHEMA_VERSION = 1


class MigrationRequired(RuntimeError):
    """An existing database needs an explicit `db migrate` invocation."""


_MIGRATIONS: dict[int, str] = {
    1: """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at_ms INTEGER NOT NULL
    );

    CREATE TABLE runs (
        id INTEGER PRIMARY KEY,
        started_at_ms INTEGER NOT NULL,
        finished_at_ms INTEGER,
        status TEXT,
        context_json TEXT NOT NULL,
        tool_version TEXT NOT NULL
    );

    CREATE TABLE scrapes (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES runs(id),
        system TEXT NOT NULL,
        module TEXT NOT NULL,
        logical_url TEXT NOT NULL,
        resolved_ip TEXT NOT NULL,
        instance TEXT NOT NULL,
        started_at_ms INTEGER NOT NULL,
        finished_at_ms INTEGER,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        http_status INTEGER,
        sample_count INTEGER NOT NULL DEFAULT 0,
        error TEXT
    );

    CREATE TABLE metric_metadata (
        system TEXT NOT NULL,
        module TEXT NOT NULL,
        family_name TEXT NOT NULL,
        metric_type TEXT NOT NULL,
        help TEXT,
        unit TEXT,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (system, module, family_name)
    );

    CREATE TABLE series (
        id INTEGER PRIMARY KEY,
        metric_name TEXT NOT NULL,
        family_name TEXT NOT NULL,
        metric_type TEXT NOT NULL,
        labels_json TEXT NOT NULL,
        labels_key TEXT NOT NULL,
        UNIQUE (metric_name, labels_key)
    );

    CREATE TABLE series_labels (
        series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (series_id, name)
    );

    CREATE TABLE samples (
        series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
        sample_timestamp_ms INTEGER NOT NULL,
        value REAL,
        value_repr TEXT,
        scrape_id INTEGER NOT NULL,
        collected_at_ms INTEGER NOT NULL,
        PRIMARY KEY (series_id, sample_timestamp_ms),
        CHECK (
            (value IS NOT NULL AND value_repr IS NULL)
            OR (value IS NULL AND value_repr IN ('NaN', '+Inf', '-Inf'))
        )
    );

    CREATE INDEX idx_scrapes_run_id ON scrapes(run_id);
    CREATE INDEX idx_series_metric_name ON series(metric_name);
    CREATE INDEX idx_series_family_name ON series(family_name);
    CREATE INDEX idx_series_labels_name_value ON series_labels(name, value, series_id);
    CREATE INDEX idx_samples_timestamp ON samples(sample_timestamp_ms);
    """,
}


def migrate_schema(connection: sqlite3.Connection, *, allow_upgrade: bool) -> int:
    """Initialize new databases or apply explicitly authorized upgrades."""
    current = _current_version(connection)
    is_new_database = current is None
    if current is None:
        if _has_user_tables(connection):
            raise MigrationRequired("database has no Metricstash schema; refusing to modify it")
        current = 0
    if current > LATEST_SCHEMA_VERSION:
        raise MigrationRequired("database schema is newer than this Metricstash version")
    if current < LATEST_SCHEMA_VERSION and not is_new_database and not allow_upgrade:
        raise MigrationRequired("database schema upgrade required; run `metricstash db migrate`")
    for version in range(current + 1, LATEST_SCHEMA_VERSION + 1):
        connection.executescript(_MIGRATIONS[version])
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (?, ?)",
            (version, int(time.time() * 1000)),
        )
    return LATEST_SCHEMA_VERSION


def schema_version(connection: sqlite3.Connection) -> int:
    version = _current_version(connection)
    if version is None:
        return 0
    return version


def _current_version(connection: sqlite3.Connection) -> int | None:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if exists is None:
        return None
    row = connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
    return int(row[0])


def _has_user_tables(connection: sqlite3.Connection) -> bool:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return bool(rows)
