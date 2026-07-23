from __future__ import annotations

import sqlite3

from metricstash.cli import main
from metricstash.db import Database


def test_db_migrate_upgrades_only_when_explicitly_requested(tmp_path, capsys) -> None:
    db_path = tmp_path / "metrics.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)")
        connection.commit()
    finally:
        connection.close()

    exit_code = main(["db", "migrate", "--db", str(db_path)])

    assert exit_code == 0
    assert "migrated" in capsys.readouterr().out
    db = Database.open(db_path)
    try:
        assert db.schema_version == 1
    finally:
        db.close()
