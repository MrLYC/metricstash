"""Concurrent one-shot collection with target-atomic SQLite persistence."""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from metricstash.db import Database
from metricstash.dns import PhysicalTarget, ResolutionError, Resolver, expand_target
from metricstash.exposition import IncrementalExpositionParser, ParsedFamily
from metricstash.http import HttpFetchResult, MetricsHttpClient
from metricstash.labels import merge_labels
from metricstash.models import AppConfig, TargetConfig, TaskConfig


class CollectionFatalError(RuntimeError):
    """SQLite or another process-wide failure stopped the complete invocation."""


@dataclass(frozen=True)
class TargetFailure:
    url: str
    resolved_ip: str | None
    error: str


@dataclass(frozen=True)
class CollectionResult:
    run_id: int
    successful_targets: int
    failed_targets: int
    failures: tuple[TargetFailure, ...]


@dataclass(frozen=True)
class _Batch:
    families: tuple[ParsedFamily, ...]


@dataclass(frozen=True)
class _End:
    result: HttpFetchResult | None
    error: Exception | None
    started_at_ms: int
    finished_at_ms: int
    attempts: int


@dataclass
class _TargetStream:
    physical: PhysicalTarget
    queue: asyncio.Queue[_Batch | _End]
    started_at_ms: int


class _TargetStreamFailure(Exception):
    def __init__(self, end: _End) -> None:
        self.end = end
        super().__init__(str(end.error))


async def collect(
    config: AppConfig,
    db_path: Path,
    *,
    context: Mapping[str, str],
    resolver: Resolver | None = None,
) -> CollectionResult:
    """Collect configured physical targets while retaining partial successes."""
    database = Database.open(
        db_path,
        writer_lock=True,
        busy_timeout_ms=config.collector.busy_timeout_ms,
    )
    run_id = database.create_run(context)
    try:
        physical_targets, resolution_failures = await _expand_targets(config, resolver)
        failures: list[TargetFailure] = []
        for task, target, error in resolution_failures:
            _record_resolution_failure(database, run_id, task, target, error)
            failures.append(TargetFailure(target.url, None, str(error)))
        streams = [
            _TargetStream(
                physical=physical,
                queue=asyncio.Queue(maxsize=8),
                started_at_ms=_now_ms(),
            )
            for physical in physical_targets
        ]
        async with MetricsHttpClient(
            max_concurrency=config.collector.max_concurrency,
            body_limit_bytes=config.collector.body_limit_bytes,
        ) as client:
            fetch_tasks = [asyncio.create_task(_fetch_target(client, stream, config)) for stream in streams]
            writer_task = asyncio.create_task(_write_targets(database, run_id, streams, config))
            try:
                outcomes = await writer_task
                await asyncio.gather(*fetch_tasks)
            except asyncio.CancelledError:
                await _cancel_tasks(fetch_tasks, writer_task)
                database.finish_run(run_id, "interrupted")
                raise
            except Exception as error:
                await _cancel_tasks(fetch_tasks, writer_task)
                database.finish_run(run_id, "fatal")
                if isinstance(error, (sqlite3.Error, OSError)):
                    raise CollectionFatalError(str(error)) from error
                raise
        for outcome in outcomes:
            if outcome is not None:
                failures.append(outcome)
        successful = len(physical_targets) - sum(1 for outcome in outcomes if outcome is not None)
        failed = len(failures)
        status = "success" if failed == 0 else "partial" if successful else "failed"
        database.finish_run(run_id, status)
        database.checkpoint()
        return CollectionResult(
            run_id=run_id,
            successful_targets=successful,
            failed_targets=failed,
            failures=tuple(failures),
        )
    finally:
        database.close()


async def _expand_targets(
    config: AppConfig, resolver: Resolver | None
) -> tuple[list[PhysicalTarget], list[tuple[TaskConfig, TargetConfig, ResolutionError]]]:
    physical: list[PhysicalTarget] = []
    failures: list[tuple[TaskConfig, TargetConfig, ResolutionError]] = []
    for task in config.tasks:
        for target in task.targets:
            try:
                physical.extend(
                    await expand_target(
                        task,
                        target,
                        max_resolved_ips=config.collector.max_resolved_ips,
                        resolver=resolver,
                    )
                )
            except ResolutionError as error:
                failures.append((task, target, error))
    return physical, failures


async def _fetch_target(client: MetricsHttpClient, stream: _TargetStream, config: AppConfig) -> None:
    physical = stream.physical
    parser = IncrementalExpositionParser(
        physical.task.metrics,
        collection_timestamp_ms=stream.started_at_ms,
        honor_timestamps=physical.task.honor_timestamps,
        max_samples=config.collector.max_samples_per_target,
        max_labels=config.collector.max_labels_per_sample,
        max_label_value_bytes=config.collector.max_label_value_bytes,
    )

    async def consume(chunk: bytes) -> None:
        families = parser.feed_bytes(chunk)
        if families:
            await stream.queue.put(_Batch(families))

    try:
        result = await client.fetch(physical, on_chunk=consume)
        final_families = parser.finish()
        if final_families:
            await stream.queue.put(_Batch(final_families))
        await stream.queue.put(
            _End(
                result=result,
                error=None,
                started_at_ms=stream.started_at_ms,
                finished_at_ms=_now_ms(),
                attempts=result.attempts,
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        await stream.queue.put(
            _End(
                result=None,
                error=error,
                started_at_ms=stream.started_at_ms,
                finished_at_ms=_now_ms(),
                attempts=int(getattr(error, "attempts", 1)),
            )
        )


async def _write_targets(
    database: Database,
    run_id: int,
    streams: list[_TargetStream],
    config: AppConfig,
) -> list[TargetFailure | None]:
    outcomes: list[TargetFailure | None] = []
    for stream in streams:
        outcomes.append(await _write_target(database, run_id, stream, config))
    return outcomes


async def _write_target(
    database: Database,
    run_id: int,
    stream: _TargetStream,
    config: AppConfig,
) -> TargetFailure | None:
    physical = stream.physical
    sample_count = 0
    end: _End | None = None
    try:
        with database.transaction():
            scrape_id = database.create_scrape(
                run_id,
                system=physical.task.system,
                module=physical.task.module,
                logical_url=physical.url,
                resolved_ip=physical.resolved_ip,
                instance=physical.collector_labels["instance"],
                started_at_ms=stream.started_at_ms,
            )
            while True:
                event = await stream.queue.get()
                if isinstance(event, _Batch):
                    sample_count += _write_families(database, scrape_id, physical, event.families, config)
                    continue
                end = event
                if end.error is not None:
                    raise _TargetStreamFailure(end)
                database.finish_scrape(
                    scrape_id,
                    status="success",
                    attempts=end.attempts,
                    sample_count=sample_count,
                    http_status=end.result.status if end.result is not None else None,
                    finished_at_ms=end.finished_at_ms,
                )
                _write_synthetic_metrics(
                    database,
                    scrape_id,
                    physical,
                    timestamp_ms=end.finished_at_ms,
                    up=1.0,
                    duration_ms=end.finished_at_ms - stream.started_at_ms,
                    sample_count=sample_count,
                    attempts=end.attempts,
                )
                break
        return None
    except _TargetStreamFailure as failure:
        end = failure.end
        with database.transaction():
            scrape_id = database.create_scrape(
                run_id,
                system=physical.task.system,
                module=physical.task.module,
                logical_url=physical.url,
                resolved_ip=physical.resolved_ip,
                instance=physical.collector_labels["instance"],
                started_at_ms=end.started_at_ms,
                status="failed",
            )
            database.finish_scrape(
                scrape_id,
                status="failed",
                attempts=end.attempts,
                sample_count=0,
                http_status=getattr(end.error, "status", None),
                error=str(end.error),
                finished_at_ms=end.finished_at_ms,
            )
            _write_synthetic_metrics(
                database,
                scrape_id,
                physical,
                timestamp_ms=end.finished_at_ms,
                up=0.0,
                duration_ms=end.finished_at_ms - end.started_at_ms,
                sample_count=0,
                attempts=end.attempts,
            )
        return TargetFailure(physical.url, physical.resolved_ip, str(end.error))


def _write_families(
    database: Database,
    scrape_id: int,
    physical: PhysicalTarget,
    families: tuple[ParsedFamily, ...],
    config: AppConfig,
) -> int:
    count = 0
    for family in families:
        database.upsert_metadata(
            system=physical.task.system,
            module=physical.task.module,
            family_name=family.name,
            metric_type=family.metric_type,
            help_text=family.help_text,
            unit=family.unit,
        )
        for sample in family.samples:
            labels = merge_labels(
                endpoint=sample.labels,
                collector=physical.collector_labels,
                honor_labels=physical.task.honor_labels,
                resolved_ip=physical.resolved_ip,
            )
            _validate_merged_labels(labels, config)
            series_id = database.ensure_series(
                sample.actual_name,
                sample.family_name,
                sample.metric_type,
                labels,
            )
            database.upsert_sample(
                series_id,
                sample.timestamp_ms,
                value=sample.value,
                value_repr=sample.value_repr,
                scrape_id=scrape_id,
            )
            count += 1
    return count


def _write_synthetic_metrics(
    database: Database,
    scrape_id: int,
    physical: PhysicalTarget,
    *,
    timestamp_ms: int,
    up: float,
    duration_ms: int,
    sample_count: int,
    attempts: int,
) -> None:
    labels = merge_labels(
        endpoint={},
        collector=physical.collector_labels,
        honor_labels=True,
        resolved_ip=physical.resolved_ip,
    )
    values = {
        "metricstash_up": up,
        "metricstash_scrape_duration_seconds": max(duration_ms, 0) / 1000.0,
        "metricstash_scrape_samples": float(sample_count),
        "metricstash_scrape_attempts": float(attempts),
    }
    for name, value in values.items():
        series_id = database.ensure_series(name, name, "gauge", labels)
        database.upsert_sample(
            series_id,
            timestamp_ms,
            value=value,
            value_repr=None,
            scrape_id=scrape_id,
        )


def _record_resolution_failure(
    database: Database,
    run_id: int,
    task: TaskConfig,
    target: TargetConfig,
    error: ResolutionError,
) -> None:
    timestamp_ms = _now_ms()
    labels = {"system": task.system, "module": task.module, "job": task.job, "resolved_ip": ""}
    with database.transaction():
        scrape_id = database.create_scrape(
            run_id,
            system=task.system,
            module=task.module,
            logical_url=target.url,
            resolved_ip="",
            instance=target.url,
            started_at_ms=timestamp_ms,
            status="failed",
        )
        database.finish_scrape(
            scrape_id,
            status="failed",
            attempts=0,
            sample_count=0,
            error=str(error),
            finished_at_ms=timestamp_ms,
        )
        for name, value in {
            "metricstash_up": 0.0,
            "metricstash_scrape_duration_seconds": 0.0,
            "metricstash_scrape_samples": 0.0,
            "metricstash_scrape_attempts": 0.0,
        }.items():
            series_id = database.ensure_series(name, name, "gauge", labels)
            database.upsert_sample(
                series_id,
                timestamp_ms,
                value=value,
                value_repr=None,
                scrape_id=scrape_id,
            )


def _validate_merged_labels(labels: Mapping[str, str], config: AppConfig) -> None:
    if len(labels) > config.collector.max_labels_per_sample:
        raise CollectionFatalError(f"merged label limit ({config.collector.max_labels_per_sample}) exceeded")
    for name, value in labels.items():
        if len(value.encode("utf-8")) > config.collector.max_label_value_bytes:
            raise CollectionFatalError(
                f"merged label value limit ({config.collector.max_label_value_bytes}) exceeded for {name}"
            )


async def _cancel_tasks(fetch_tasks: list[asyncio.Task[None]], writer_task: asyncio.Task[object]) -> None:
    for task in [*fetch_tasks, writer_task]:
        if not task.done():
            task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await asyncio.gather(*fetch_tasks, writer_task)


def _now_ms() -> int:
    return int(time.time() * 1000)
