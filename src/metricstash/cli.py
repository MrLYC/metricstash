"""Command-line entry point and exit-code boundary for Metricstash."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sqlite3
import sys
import time
from pathlib import Path

from metricstash.collector import CollectionFatalError, collect
from metricstash.config import ConfigError, load_config, parse_duration
from metricstash.db import Database, DatabaseLockError
from metricstash.inspect import InspectError, inspect_histogram, inspect_summary
from metricstash.migrations import MigrationRequired
from metricstash.output import (
    histogram_to_dict,
    query_to_dict,
    render_histogram_table,
    render_json,
    render_query_table,
    render_summary_table,
    summary_to_dict,
)
from metricstash.query import ResultLimitError, SelectorError, query_series


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command parser."""
    parser = argparse.ArgumentParser(prog="metricstash")
    subparsers = parser.add_subparsers(dest="command")
    validate_parser = subparsers.add_parser("validate", help="validate TOML configuration without network access")
    _add_config_arguments(validate_parser)
    collect_parser = subparsers.add_parser("collect", help="collect configured metrics once")
    _add_config_arguments(collect_parser)
    collect_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
    query_parser = subparsers.add_parser("query", help="query one metric selector from SQLite")
    query_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
    query_parser.add_argument("selector", help="single-metric selector")
    query_parser.add_argument("--at", default="now", help="evaluation time (RFC3339 or now)")
    query_parser.add_argument("--max-age", help="maximum instant-sample age, e.g. 5m")
    query_parser.add_argument("--format", choices=("table", "json"), default="table")
    inspect_parser = subparsers.add_parser("inspect")
    inspect_subparsers = inspect_parser.add_subparsers(dest="inspect_command", required=True)
    for kind in ("histogram", "summary"):
        kind_parser = inspect_subparsers.add_parser(kind, help=f"present stored {kind} components")
        kind_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
        kind_parser.add_argument("selector", help="configured family selector")
        kind_parser.add_argument("--at", default="now", help="evaluation time (RFC3339 or now)")
        kind_parser.add_argument("--format", choices=("table", "json"), default="table")
    prune_parser = subparsers.add_parser("prune", help="explicitly delete old samples")
    prune_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
    retention = prune_parser.add_mutually_exclusive_group(required=True)
    retention.add_argument("--older-than", help="delete samples older than this duration")
    retention.add_argument("--before", help="delete samples before this RFC3339 time")
    db_parser = subparsers.add_parser("db")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)
    migrate_parser = db_subparsers.add_parser("migrate", help="apply pending SQLite migrations")
    migrate_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
    vacuum_parser = db_subparsers.add_parser("vacuum", help="explicitly compact a SQLite database")
    vacuum_parser.add_argument("--db", required=True, type=Path, help="SQLite database path")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run a command and map expected failures to documented process statuses."""
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code)
    try:
        if args.command == "validate":
            load_config(args.config, _parse_context(args.context))
            print("configuration is valid")
            return 0
        if args.command == "collect":
            config = load_config(args.config, _parse_context(args.context))
            result = asyncio.run(collect(config, args.db, context=_parse_context(args.context)))
            print(
                f"run={result.run_id} successful_targets={result.successful_targets} "
                f"failed_targets={result.failed_targets}"
            )
            for failure in result.failures:
                print(f"target failure: {failure.url}: {failure.error}", file=sys.stderr)
            return 1 if result.failed_targets else 0
        if args.command == "query":
            return _run_query(args)
        if args.command == "inspect":
            return _run_inspect(args)
        if args.command == "prune":
            return _run_prune(args)
        if args.command == "db":
            return _run_database_command(args)
        parser.print_usage(sys.stderr)
        print("metricstash: error: a command is required", file=sys.stderr)
        return 2
    except (ConfigError, SelectorError, InspectError, ResultLimitError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except (DatabaseLockError, MigrationRequired, CollectionFatalError, sqlite3.Error, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    return 2  # pragma: no cover - argparse and command dispatch make this unreachable


def _add_config_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True, type=Path, help="TOML configuration path")
    parser.add_argument("--context", action="append", default=[], metavar="KEY=VALUE", help="dynamic task context")


def _parse_context(values: list[str]) -> dict[str, str]:
    context: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ConfigError(f"invalid context value {value!r}; expected KEY=VALUE")
        key, item = value.split("=", 1)
        if not key:
            raise ConfigError("context key cannot be empty")
        context[key] = item
    return context


def _run_query(args: argparse.Namespace) -> int:
    at_ms = _parse_time_ms(args.at)
    max_age_ms = _parse_duration_ms(args.max_age, "max age") if args.max_age else None
    with Database.open(args.db, create=False) as database:
        result = query_series(database, args.selector, at_ms=at_ms, max_age_ms=max_age_ms)
    if args.format == "json":
        print(render_json(query_to_dict(result)), end="")
    else:
        print(render_query_table(result), end="")
    return 0


def _run_inspect(args: argparse.Namespace) -> int:
    at_ms = _parse_time_ms(args.at)
    with Database.open(args.db, create=False) as database:
        if args.inspect_command == "histogram":
            view = inspect_histogram(database, args.selector, at_ms=at_ms)
            rendered = render_json(histogram_to_dict(view)) if args.format == "json" else render_histogram_table(view)
        else:
            view = inspect_summary(database, args.selector, at_ms=at_ms)
            rendered = render_json(summary_to_dict(view)) if args.format == "json" else render_summary_table(view)
    print(rendered, end="")
    return 0


def _run_prune(args: argparse.Namespace) -> int:
    if args.older_than:
        cutoff_ms = int(time.time() * 1000) - _parse_duration_ms(args.older_than, "older-than")
    else:
        cutoff_ms = _parse_time_ms(args.before)
    with Database.open(args.db, writer_lock=True, create=False) as database:
        with database.transaction():
            deleted = database.prune_samples_before(cutoff_ms)
        database.checkpoint()
    print(f"pruned_samples={deleted}")
    return 0


def _run_database_command(args: argparse.Namespace) -> int:
    if args.db_command == "migrate":
        with Database.open(args.db, allow_migrate=True, writer_lock=True) as database:
            print(f"migrated schema to version {database.schema_version}")
        return 0
    if args.db_command == "vacuum":
        with Database.open(args.db, writer_lock=True, create=False) as database:
            database.vacuum()
        print("vacuum complete")
        return 0
    raise ValueError(f"unknown database command {args.db_command!r}")


def _parse_duration_ms(value: str, field_name: str) -> int:
    return int(round(parse_duration(value, field_name=field_name) * 1000))


def _parse_time_ms(value: str) -> int:
    if value == "now":
        return int(time.time() * 1000)
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"invalid time {value!r}; use RFC3339 or now") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(round(parsed.timestamp() * 1000))
