"""SQLite repository, transactions, schema lifecycle, and writer locking."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterator, Mapping

from metricstash.migrations import migrate_schema, schema_version


class DatabaseLockError(RuntimeError):
    """Another mutating Metricstash process holds the database lock."""


class FileLock:
    """A non-blocking advisory lock stored beside a SQLite database."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: IO[str] | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise DatabaseLockError(f"database is busy: {self.path}") from error
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


class Database:
    """A small explicit repository over Metricstash's versioned SQLite schema."""

    def __init__(self, path: Path, connection: sqlite3.Connection, lock: FileLock | None) -> None:
        self.path = path
        self.connection = connection
        self._lock = lock

    @classmethod
    def open(
        cls,
        path: Path,
        *,
        allow_migrate: bool = False,
        writer_lock: bool = False,
        busy_timeout_ms: int = 5_000,
    ) -> "Database":
        """Open a database, initializing a new schema and checking old schemas."""
        path = Path(path)
        lock: FileLock | None = None
        if writer_lock:
            lock = FileLock(path.with_name(f"{path.name}.lock"))
            lock.acquire()
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(path, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
            migrate_schema(connection, allow_upgrade=allow_migrate)
        except BaseException:
            if connection is not None:
                connection.close()
            if lock is not None:
                lock.release()
            raise
        return cls(path, connection, lock)

    @property
    def schema_version(self) -> int:
        return schema_version(self.connection)

    def close(self) -> None:
        try:
            self.connection.close()
        finally:
            if self._lock is not None:
                self._lock.release()
                self._lock = None

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run a single all-or-nothing immediate write transaction."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def ensure_series(
        self,
        metric_name: str,
        family_name: str,
        metric_type: str,
        labels: Mapping[str, str],
    ) -> int:
        """Return the stable ID for one actual metric sample and full label set."""
        normalized = {str(name): str(value) for name, value in labels.items()}
        labels_json = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        self.connection.execute(
            """
            INSERT INTO series(metric_name, family_name, metric_type, labels_json, labels_key)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(metric_name, labels_key) DO NOTHING
            """,
            (metric_name, family_name, metric_type, labels_json, labels_json),
        )
        row = self.connection.execute(
            "SELECT id FROM series WHERE metric_name = ? AND labels_key = ?",
            (metric_name, labels_json),
        ).fetchone()
        if row is None:  # pragma: no cover - protected by the unique constraint
            raise RuntimeError("unable to create metric series")
        series_id = int(row["id"])
        self.connection.executemany(
            """
            INSERT INTO series_labels(series_id, name, value) VALUES (?, ?, ?)
            ON CONFLICT(series_id, name) DO UPDATE SET value = excluded.value
            """,
            [(series_id, name, value) for name, value in normalized.items()],
        )
        return series_id

    def series_labels(self, series_id: int) -> dict[str, str]:
        rows = self.connection.execute(
            "SELECT name, value FROM series_labels WHERE series_id = ? ORDER BY name", (series_id,)
        ).fetchall()
        return {str(row["name"]): str(row["value"]) for row in rows}

    def upsert_sample(
        self,
        series_id: int,
        sample_timestamp_ms: int,
        *,
        value: float | None,
        value_repr: str | None,
        scrape_id: int,
        collected_at_ms: int | None = None,
    ) -> None:
        """Store a finite float or one special-value representation by overwrite."""
        _validate_sample_value(value, value_repr)
        if collected_at_ms is None:
            collected_at_ms = int(time.time() * 1000)
        self.connection.execute(
            """
            INSERT INTO samples(
                series_id, sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(series_id, sample_timestamp_ms) DO UPDATE SET
                value = excluded.value,
                value_repr = excluded.value_repr,
                scrape_id = excluded.scrape_id,
                collected_at_ms = excluded.collected_at_ms
            """,
            (series_id, sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms),
        )

    def fetch_sample(self, series_id: int, sample_timestamp_ms: int) -> tuple[float | None, str | None, int] | None:
        row = self.connection.execute(
            """
            SELECT value, value_repr, scrape_id FROM samples
            WHERE series_id = ? AND sample_timestamp_ms = ?
            """,
            (series_id, sample_timestamp_ms),
        ).fetchone()
        if row is None:
            return None
        return (row["value"], row["value_repr"], int(row["scrape_id"]))

    def create_run(
        self,
        context: Mapping[str, str],
        *,
        started_at_ms: int | None = None,
        tool_version: str = "0.1.0",
    ) -> int:
        if started_at_ms is None:
            started_at_ms = int(time.time() * 1000)
        cursor = self.connection.execute(
            "INSERT INTO runs(started_at_ms, context_json, tool_version) VALUES (?, ?, ?)",
            (
                started_at_ms,
                json.dumps(dict(context), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                tool_version,
            ),
        )
        return int(cursor.lastrowid)

    def finish_run(self, run_id: int, status: str, *, finished_at_ms: int | None = None) -> None:
        if finished_at_ms is None:
            finished_at_ms = int(time.time() * 1000)
        self.connection.execute(
            "UPDATE runs SET status = ?, finished_at_ms = ? WHERE id = ?",
            (status, finished_at_ms, run_id),
        )

    def create_scrape(
        self,
        run_id: int,
        *,
        system: str,
        module: str,
        logical_url: str,
        resolved_ip: str,
        instance: str,
        started_at_ms: int | None = None,
        status: str = "running",
    ) -> int:
        if started_at_ms is None:
            started_at_ms = int(time.time() * 1000)
        cursor = self.connection.execute(
            """
            INSERT INTO scrapes(
                run_id, system, module, logical_url, resolved_ip, instance, started_at_ms, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, system, module, logical_url, resolved_ip, instance, started_at_ms, status),
        )
        return int(cursor.lastrowid)

    def finish_scrape(
        self,
        scrape_id: int,
        *,
        status: str,
        attempts: int,
        sample_count: int,
        http_status: int | None = None,
        error: str | None = None,
        finished_at_ms: int | None = None,
    ) -> None:
        if finished_at_ms is None:
            finished_at_ms = int(time.time() * 1000)
        self.connection.execute(
            """
            UPDATE scrapes
            SET status = ?, attempts = ?, sample_count = ?, http_status = ?, error = ?, finished_at_ms = ?
            WHERE id = ?
            """,
            (status, attempts, sample_count, http_status, _truncate_error(error), finished_at_ms, scrape_id),
        )

    def upsert_metadata(
        self,
        *,
        system: str,
        module: str,
        family_name: str,
        metric_type: str,
        help_text: str | None,
        unit: str | None,
        updated_at_ms: int | None = None,
    ) -> None:
        if updated_at_ms is None:
            updated_at_ms = int(time.time() * 1000)
        self.connection.execute(
            """
            INSERT INTO metric_metadata(
                system, module, family_name, metric_type, help, unit, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(system, module, family_name) DO UPDATE SET
                metric_type = excluded.metric_type,
                help = excluded.help,
                unit = excluded.unit,
                updated_at_ms = excluded.updated_at_ms
            """,
            (system, module, family_name, metric_type, help_text, unit, updated_at_ms),
        )

    def checkpoint(self) -> None:
        self.connection.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def prune_samples_before(self, cutoff_ms: int) -> int:
        cursor = self.connection.execute("DELETE FROM samples WHERE sample_timestamp_ms < ?", (cutoff_ms,))
        self.connection.execute(
            "DELETE FROM series WHERE id NOT IN (SELECT DISTINCT series_id FROM samples)"
        )
        return int(cursor.rowcount)

    def vacuum(self) -> None:
        self.connection.execute("VACUUM")



def _validate_sample_value(value: float | None, value_repr: str | None) -> None:
    if (value is None) == (value_repr is None):
        raise ValueError("exactly one of value and value_repr must be present")
    if value is not None:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("value must be a finite number")
    elif value_repr not in {"NaN", "+Inf", "-Inf"}:
        raise ValueError("value_repr must be NaN, +Inf, or -Inf")


def _truncate_error(value: str | None, limit: int = 1024) -> str | None:
    if value is None:
        return None
    return value[:limit]
