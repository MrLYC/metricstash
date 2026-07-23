from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from aiohttp import web

from metricstash.collector import collect
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


@pytest.mark.asyncio
async def test_sample_limit_prevents_large_response_commit(tmp_path: Path) -> None:
    async def metrics(request: web.Request) -> web.Response:
        return web.Response(text="# TYPE x gauge\nx{n=\"1\"} 1\nx{n=\"2\"} 2\nx{n=\"3\"} 3\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    db_path = tmp_path / "metrics.db"
    async with serve(app) as port:
        config = AppConfig(
            collector=CollectorConfig(max_samples_per_target=2, body_limit_bytes=1024 * 1024),
            tasks=(
                TaskConfig(
                    system="prod",
                    module="api",
                    job="api",
                    labels={},
                    metrics=(MetricConfig("x", "gauge"),),
                    targets=(TargetConfig(f"http://127.0.0.1:{port}/metrics"),),
                ),
            ),
            source_path=Path("metricstash.toml"),
        )
        result = await collect(config, db_path, context={})

    assert result.successful_targets == 0
    assert result.failed_targets == 1
    import sqlite3

    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute("SELECT metric_name, labels_json FROM series").fetchall()
    finally:
        connection.close()
    assert all(name != "x" for name, _ in rows)
    assert any(name == "metricstash_up" and json.loads(labels)["resolved_ip"] == "127.0.0.1" for name, labels in rows)
