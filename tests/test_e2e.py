from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from aiohttp import web

from metricstash.cli import main


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
async def test_end_to_end_collect_query_and_histogram_inspect(tmp_path: Path, capsys) -> None:
    async def metrics(request: web.Request) -> web.Response:
        return web.Response(
            text="""# TYPE http_requests_total counter
http_requests_total{status=\"200\"} 7
# TYPE latency_seconds histogram
latency_seconds_bucket{le=\"0.5\"} 3
latency_seconds_bucket{le=\"+Inf\"} 5
latency_seconds_sum 1.2
latency_seconds_count 5
""",
            content_type="text/plain",
        )

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    db_path = tmp_path / "metrics.db"
    config_path = tmp_path / "metricstash.toml"
    async with serve(app) as port:
        config_path.write_text(
            f"""
[collector]
max_concurrency = 2

[[tasks]]
system = "prod"
module = "api"
labels = {{ cluster = "prod" }}

[[tasks.metrics]]
name = "http_requests_total"
type = "counter"

[[tasks.metrics]]
name = "latency_seconds"
type = "histogram"

[[tasks.targets]]
url = "http://127.0.0.1:{port}/metrics"
""",
            encoding="utf-8",
        )
        assert await asyncio.to_thread(main, ["collect", "--config", str(config_path), "--db", str(db_path)]) == 0

    assert main(["query", "--db", str(db_path), 'http_requests_total{cluster="prod"}', "--format", "json"]) == 0
    assert main(["inspect", "histogram", "--db", str(db_path), 'latency_seconds{cluster="prod"}']) == 0
    output = capsys.readouterr().out
    assert "http_requests_total" in output
    assert "bucket" in output
