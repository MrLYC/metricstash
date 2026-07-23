from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from metricstash.dns import PhysicalTarget
from metricstash.http import BodyLimitError, ContentTypeError, MetricsHttpClient
from metricstash.models import MetricConfig, TargetConfig, TaskConfig


@asynccontextmanager
async def serve(app: web.Application):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[union-attr]
    port = sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}", port
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_retries_only_retryable_statuses() -> None:
    calls = 0

    async def returns_503(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=503, text="unavailable")
        return web.Response(text="metric 1\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/returns-503", returns_503)
    async with serve(app) as (base_url, _):
        async with MetricsHttpClient(retries=1, timeout_seconds=1, retry_backoff_seconds=0) as client:
            result = await client.fetch(f"{base_url}/returns-503")

    assert result.attempts == 2
    assert calls == 2


@pytest.mark.asyncio
async def test_rejects_non_text_content_type() -> None:
    async def binary(request: web.Request) -> web.Response:
        return web.Response(body=b"x", content_type="application/octet-stream")

    app = web.Application()
    app.router.add_get("/binary", binary)
    async with serve(app) as (base_url, _):
        async with MetricsHttpClient() as client:
            with pytest.raises(ContentTypeError):
                await client.fetch(f"{base_url}/binary")


@pytest.mark.asyncio
async def test_streams_decompressed_bytes_to_consumer_without_retaining_body() -> None:
    async def metrics(request: web.Request) -> web.Response:
        return web.Response(text="first 1\nsecond 2\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    received: list[bytes] = []

    async def consume(chunk: bytes) -> None:
        received.append(chunk)

    async with serve(app) as (base_url, _):
        async with MetricsHttpClient(body_limit_bytes=1024) as client:
            result = await client.fetch(f"{base_url}/metrics", on_chunk=consume)

    assert result.bytes_received == len(b"first 1\nsecond 2\n")
    assert b"".join(received) == b"first 1\nsecond 2\n"


@pytest.mark.asyncio
async def test_connects_to_selected_ip_but_sends_original_host_header() -> None:
    observed_hosts: list[str] = []

    async def metrics(request: web.Request) -> web.Response:
        observed_hosts.append(request.headers["Host"])
        return web.Response(text="x 1\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    async with serve(app) as (_, port):
        task = TaskConfig(
            system="prod",
            module="api",
            job="api",
            labels={},
            metrics=(MetricConfig("x", "gauge"),),
            targets=(),
        )
        target = TargetConfig(f"http://metrics.example.test:{port}/metrics")
        physical = PhysicalTarget(
            task=task,
            target=target,
            dns_name="metrics.example.test",
            resolved_ip="127.0.0.1",
            port=port,
            collector_labels={"instance": f"127.0.0.1:{port}"},
        )
        async with MetricsHttpClient() as client:
            await client.fetch(physical)

    assert observed_hosts == [f"metrics.example.test:{port}"]


@pytest.mark.asyncio
async def test_response_limit_fails_before_large_body_is_accepted() -> None:
    async def metrics(request: web.Request) -> web.Response:
        return web.Response(text="x" * 32, content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    async with serve(app) as (base_url, _):
        async with MetricsHttpClient(body_limit_bytes=8) as client:
            with pytest.raises(BodyLimitError):
                await client.fetch(f"{base_url}/metrics")
