from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from aiohttp import web

from metricstash.collector import collect
from metricstash.db import Database
from metricstash.models import AppConfig, CollectorConfig, MetricConfig, TargetConfig, TaskConfig


@asynccontextmanager
async def serve(app: web.Application):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[union-attr]
    port = sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        await runner.cleanup()


def config_for(port: int, targets: tuple[TargetConfig, ...]) -> AppConfig:
    task = TaskConfig(
        system="prod",
        module="api",
        job="api",
        labels={"cluster": "prod"},
        metrics=(MetricConfig("requests_total", "counter"),),
        targets=targets,
    )
    return AppConfig(
        collector=CollectorConfig(max_concurrency=2, body_limit_bytes=1024 * 1024),
        tasks=(task,),
        source_path=Path("metricstash.toml"),
    )


def sample_value(db_path: Path, metric_name: str, required_labels: dict[str, str]) -> float | None:
    db = Database.open(db_path)
    try:
        rows = db.connection.execute(
            """
            SELECT s.value, s.value_repr, series.labels_json
            FROM samples AS s JOIN series ON series.id = s.series_id
            WHERE series.metric_name = ?
            """,
            (metric_name,),
        ).fetchall()
        for row in rows:
            labels = json.loads(row["labels_json"])
            if all(labels.get(key) == value for key, value in required_labels.items()):
                return row["value"]
    finally:
        db.close()
    return None


@pytest.mark.asyncio
async def test_partial_target_failure_commits_success_and_records_synthetic_failure(tmp_path: Path) -> None:
    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="# TYPE requests_total counter\nrequests_total 3\n", content_type="text/plain")

    async def fail(request: web.Request) -> web.Response:
        return web.Response(status=503, text="unavailable")

    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/fail", fail)
    db_path = tmp_path / "metrics.db"
    async with serve(app) as port:
        config = config_for(
            port,
            (
                TargetConfig(f"http://127.0.0.1:{port}/ok", labels={"role": "ok"}),
                TargetConfig(f"http://127.0.0.1:{port}/fail", labels={"role": "fail"}),
            ),
        )
        result = await collect(config, db_path, context={})

    assert result.successful_targets == 1
    assert result.failed_targets == 1
    assert sample_value(db_path, "requests_total", {"role": "ok"}) == 3.0
    assert sample_value(db_path, "metricstash_up", {"role": "ok"}) == 1.0
    assert sample_value(db_path, "metricstash_up", {"role": "fail"}) == 0.0


@pytest.mark.asyncio
async def test_failed_target_rolls_back_business_samples_but_keeps_audit_metrics(tmp_path: Path) -> None:
    async def malformed(request: web.Request) -> web.Response:
        return web.Response(
            text="# TYPE requests_total counter\nrequests_total 3\nrequests_total{broken 4\n",
            content_type="text/plain",
        )

    app = web.Application()
    app.router.add_get("/bad", malformed)
    db_path = tmp_path / "metrics.db"
    async with serve(app) as port:
        config = config_for(
            port,
            (TargetConfig(f"http://127.0.0.1:{port}/bad", labels={"role": "bad"}),),
        )
        result = await collect(config, db_path, context={})

    assert result.successful_targets == 0
    assert result.failed_targets == 1
    assert sample_value(db_path, "requests_total", {"role": "bad"}) is None
    assert sample_value(db_path, "metricstash_up", {"role": "bad"}) == 0.0
    db = Database.open(db_path)
    try:
        scrape = db.connection.execute("SELECT status, attempts, error FROM scrapes").fetchone()
        assert scrape["status"] == "failed"
        assert scrape["attempts"] == 1
        assert scrape["error"]
    finally:
        db.close()
