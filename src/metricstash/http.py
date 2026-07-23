"""Bounded asynchronous HTTP transport for physical metric targets."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aiohttp

from metricstash.dns import PhysicalTarget
from metricstash.models import BasicAuthConfig, HttpConfig


class HttpError(RuntimeError):
    """A metrics endpoint request could not produce an acceptable response."""


class HttpStatusError(HttpError):
    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"metrics endpoint returned HTTP {status}: {url}")
        self.status = status
        self.url = url


class ContentTypeError(HttpError):
    """The endpoint did not declare Prometheus/OpenMetrics text."""


class BodyLimitError(HttpError):
    """The decompressed response exceeds the configured cap."""


class RequestConfigurationError(HttpError):
    """Local request credentials or settings cannot be loaded safely."""


class _RetryableResponse(HttpStatusError):
    pass


ChunkConsumer = Callable[[bytes], Awaitable[None] | None]


@dataclass(frozen=True)
class HttpFetchResult:
    url: str
    status: int
    attempts: int
    bytes_received: int
    duration_ms: int
    headers: dict[str, str]


@dataclass(frozen=True)
class _RequestSettings:
    timeout_seconds: float
    retries: int
    headers: dict[str, str]
    basic_auth: BasicAuthConfig | None
    bearer_token_file: Path | None
    verify_tls: bool
    allow_missing_content_type: bool


class MetricsHttpClient:
    """One reusable, bounded aiohttp session for a collection invocation."""

    def __init__(
        self,
        *,
        max_concurrency: int = 4,
        retries: int = 0,
        timeout_seconds: float = 10.0,
        body_limit_bytes: int = 8 * 1024 * 1024,
        retry_backoff_seconds: float = 0.1,
        allow_missing_content_type: bool = False,
    ) -> None:
        if max_concurrency <= 0 or retries < 0 or timeout_seconds <= 0 or body_limit_bytes <= 0:
            raise ValueError("HTTP client limits must be positive (and retries non-negative)")
        self.max_concurrency = max_concurrency
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.body_limit_bytes = body_limit_bytes
        self.retry_backoff_seconds = retry_backoff_seconds
        self.allow_missing_content_type = allow_missing_content_type
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "MetricsHttpClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def fetch(
        self,
        target: str | PhysicalTarget,
        *,
        on_chunk: ChunkConsumer | None = None,
    ) -> HttpFetchResult:
        """Fetch an endpoint and stream decompressed response bytes to a consumer."""
        session = await self._ensure_session()
        request_url, server_hostname, settings = _prepare_request(target, self)
        headers, auth = _request_credentials(settings)
        started = time.monotonic()
        for attempt in range(1, settings.retries + 2):
            try:
                timeout = aiohttp.ClientTimeout(total=settings.timeout_seconds)
                async with session.get(
                    request_url,
                    headers=headers,
                    auth=auth,
                    allow_redirects=False,
                    timeout=timeout,
                    ssl=settings.verify_tls,
                    server_hostname=server_hostname,
                ) as response:
                    if response.status in {408, 429} or response.status >= 500:
                        raise _RetryableResponse(response.status, request_url)
                    if response.status < 200 or response.status >= 300:
                        raise HttpStatusError(response.status, request_url)
                    _validate_content_type(
                        response.headers.get("Content-Type"),
                        allow_missing=settings.allow_missing_content_type,
                        url=request_url,
                    )
                    bytes_received = 0
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        bytes_received += len(chunk)
                        if bytes_received > self.body_limit_bytes:
                            raise BodyLimitError(
                                f"metrics response exceeds {self.body_limit_bytes} decompressed bytes: {request_url}"
                            )
                        if on_chunk is not None:
                            consumed = on_chunk(chunk)
                            if asyncio.iscoroutine(consumed):
                                await consumed
                    return HttpFetchResult(
                        url=request_url,
                        status=response.status,
                        attempts=attempt,
                        bytes_received=bytes_received,
                        duration_ms=int((time.monotonic() - started) * 1000),
                        headers={str(name): str(value) for name, value in response.headers.items()},
                    )
            except _RetryableResponse as error:
                retry_error: BaseException = error
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as error:
                if _is_permanent_tls_error(error):
                    raise HttpError(f"network request failed: {request_url}: {error}") from error
                retry_error = error
            if attempt > settings.retries:
                if isinstance(retry_error, HttpStatusError):
                    raise retry_error
                raise HttpError(f"network request failed after {attempt} attempts: {request_url}: {retry_error}") from retry_error
            await asyncio.sleep(self.retry_backoff_seconds * (2 ** (attempt - 1)))
        raise AssertionError("retry loop unexpectedly exhausted")  # pragma: no cover

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=self.max_concurrency, force_close=True)
            self._session = aiohttp.ClientSession(connector=connector, auto_decompress=True)
        return self._session


def _prepare_request(
    target: str | PhysicalTarget, client: MetricsHttpClient
) -> tuple[str, str | None, _RequestSettings]:
    if isinstance(target, str):
        return (
            target,
            None,
            _RequestSettings(
                timeout_seconds=client.timeout_seconds,
                retries=client.retries,
                headers={},
                basic_auth=None,
                bearer_token_file=None,
                verify_tls=True,
                allow_missing_content_type=client.allow_missing_content_type,
            ),
        )
    config: HttpConfig = target.task.http
    allow_missing = (
        target.target.allow_missing_content_type
        if target.target.allow_missing_content_type is not None
        else config.allow_missing_content_type
    )
    original = urlsplit(target.url)
    request_url = _url_for_resolved_ip(original, target.resolved_ip, target.port)
    headers = dict(config.headers)
    headers["Host"] = _original_host_header(original)
    return (
        request_url,
        target.dns_name,
        _RequestSettings(
            timeout_seconds=config.timeout_seconds,
            retries=config.retries,
            headers=headers,
            basic_auth=config.basic_auth,
            bearer_token_file=config.bearer_token_file,
            verify_tls=config.verify_tls,
            allow_missing_content_type=allow_missing,
        ),
    )


def _request_credentials(settings: _RequestSettings) -> tuple[dict[str, str], aiohttp.BasicAuth | None]:
    headers = dict(settings.headers)
    if settings.bearer_token_file is not None:
        try:
            token = settings.bearer_token_file.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise RequestConfigurationError("cannot read bearer token file") from error
        if not token:
            raise RequestConfigurationError("bearer token file is empty")
        headers["Authorization"] = f"Bearer {token}"
    auth = None
    if settings.basic_auth is not None:
        auth = aiohttp.BasicAuth(settings.basic_auth.username, settings.basic_auth.password)
    return headers, auth


def _validate_content_type(content_type: str | None, *, allow_missing: bool, url: str) -> None:
    if content_type is None:
        if allow_missing:
            return
        raise ContentTypeError(f"metrics endpoint omitted Content-Type: {url}")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type.startswith("text/") or media_type == "application/openmetrics-text":
        return
    raise ContentTypeError(f"metrics endpoint returned non-text Content-Type {media_type!r}: {url}")


def _url_for_resolved_ip(original: Any, resolved_ip: str, port: int) -> str:
    host = f"[{resolved_ip}]" if ":" in resolved_ip else resolved_ip
    return urlunsplit((original.scheme, f"{host}:{port}", original.path, original.query, ""))


def _original_host_header(original: Any) -> str:
    hostname = original.hostname
    if hostname is None:  # pragma: no cover - config validates URLs before this boundary
        raise RequestConfigurationError("target URL has no hostname")
    host = f"[{hostname}]" if ":" in hostname else hostname
    try:
        port = original.port
    except ValueError as error:  # pragma: no cover - config validates URLs before this boundary
        raise RequestConfigurationError("target URL has an invalid port") from error
    default_port = 443 if original.scheme == "https" else 80
    return host if port is None or port == default_port else f"{host}:{port}"


def _is_permanent_tls_error(error: BaseException) -> bool:
    return isinstance(
        error,
        (
            aiohttp.ClientConnectorCertificateError,
            aiohttp.ClientConnectorSSLError,
            aiohttp.ServerFingerprintMismatch,
        ),
    )
