"""A deliberately small, single-metric PromQL-like selector evaluator."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Pattern

from metricstash.config import ConfigError, parse_duration
from metricstash.db import Database


class SelectorError(ValueError):
    """A selector is outside Metricstash's supported grammar."""


class ResultLimitError(RuntimeError):
    """A query exceeded its explicit series or sample limit."""


@dataclass(frozen=True)
class Matcher:
    name: str
    operator: str
    value: str


@dataclass(frozen=True)
class Selector:
    metric_name: str
    matchers: tuple[Matcher, ...] = ()
    range_ms: int | None = None


@dataclass(frozen=True)
class QuerySample:
    timestamp_ms: int
    value: float | None
    value_repr: str | None
    scrape_id: int
    collected_at_ms: int
    age_ms: int


@dataclass(frozen=True)
class QuerySeries:
    series_id: int
    metric_name: str
    family_name: str
    metric_type: str
    labels: dict[str, str]
    samples: tuple[QuerySample, ...]


@dataclass(frozen=True)
class QueryResult:
    selector: Selector
    at_ms: int
    series: tuple[QuerySeries, ...]


_METRIC_PREFIX = re.compile(r"[A-Za-z_:][A-Za-z0-9_:]*")
_LABEL_PREFIX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_OPERATORS = ("!~", "=~", "!=", "=")


def parse_selector(expression: str) -> Selector:
    """Parse one metric name, optional matchers, and an optional range selector."""
    text = expression.strip()
    if not text:
        raise SelectorError("selector must include a metric name")
    match = _METRIC_PREFIX.match(text)
    if match is None:
        raise SelectorError("selector must begin with a metric name")
    metric_name = match.group(0)
    position = match.end()
    matchers: tuple[Matcher, ...] = ()
    position = _skip_whitespace(text, position)
    if position < len(text) and text[position] == "{":
        matchers, position = _parse_matchers(text, position + 1)
    position = _skip_whitespace(text, position)
    range_ms: int | None = None
    if position < len(text) and text[position] == "[":
        close = text.find("]", position + 1)
        if close < 0:
            raise SelectorError("unterminated range selector")
        duration = text[position + 1 : close].strip()
        try:
            range_ms = int(round(parse_duration(duration, field_name="range selector") * 1000))
        except ConfigError as error:
            raise SelectorError(str(error)) from error
        position = close + 1
    if text[_skip_whitespace(text, position) :]:
        raise SelectorError("unexpected text after selector")
    return Selector(metric_name=metric_name, matchers=matchers, range_ms=range_ms)


def query_series(
    database: Database,
    selector: str | Selector,
    *,
    at_ms: int,
    series_limit: int = 1_000,
    sample_limit: int = 10_000,
    max_age_ms: int | None = None,
) -> QueryResult:
    """Evaluate a selector without functions, aggregation, or implicit lookback."""
    if series_limit <= 0 or sample_limit <= 0:
        raise ValueError("query limits must be positive")
    if max_age_ms is not None and max_age_ms < 0:
        raise ValueError("max_age_ms must be non-negative")
    parsed = parse_selector(selector) if isinstance(selector, str) else selector
    compiled = _compile_matchers(parsed.matchers)
    results: list[QuerySeries] = []
    sample_count = 0
    cursor = database.connection.execute(
        """
        SELECT id, metric_name, family_name, metric_type, labels_json
        FROM series WHERE metric_name = ? ORDER BY id
        """,
        (parsed.metric_name,),
    )
    for row in cursor:
        labels = {str(name): str(value) for name, value in json.loads(row["labels_json"]).items()}
        if not _labels_match(labels, compiled):
            continue
        samples = _load_samples(database, int(row["id"]), parsed, at_ms, max_age_ms)
        if not samples:
            continue
        if len(results) >= series_limit:
            raise ResultLimitError(f"series limit ({series_limit}) exceeded")
        sample_count += len(samples)
        if sample_count > sample_limit:
            raise ResultLimitError(f"sample limit ({sample_limit}) exceeded")
        results.append(
            QuerySeries(
                series_id=int(row["id"]),
                metric_name=str(row["metric_name"]),
                family_name=str(row["family_name"]),
                metric_type=str(row["metric_type"]),
                labels=labels,
                samples=tuple(samples),
            )
        )
    return QueryResult(selector=parsed, at_ms=at_ms, series=tuple(results))


def _parse_matchers(text: str, position: int) -> tuple[tuple[Matcher, ...], int]:
    matchers: list[Matcher] = []
    position = _skip_whitespace(text, position)
    if position < len(text) and text[position] == "}":
        return (), position + 1
    while True:
        label_match = _LABEL_PREFIX.match(text, position)
        if label_match is None:
            raise SelectorError("expected label name in matcher")
        name = label_match.group(0)
        position = _skip_whitespace(text, label_match.end())
        operator = next((candidate for candidate in _OPERATORS if text.startswith(candidate, position)), None)
        if operator is None:
            raise SelectorError("expected matcher operator")
        position = _skip_whitespace(text, position + len(operator))
        if position >= len(text) or text[position] != '"':
            raise SelectorError("matcher values must be quoted strings")
        try:
            value, consumed = json.JSONDecoder().raw_decode(text[position:])
        except json.JSONDecodeError as error:
            raise SelectorError(f"invalid matcher string: {error.msg}") from error
        if not isinstance(value, str):
            raise SelectorError("matcher values must be strings")
        matchers.append(Matcher(name=name, operator=operator, value=value))
        position = _skip_whitespace(text, position + consumed)
        if position >= len(text):
            raise SelectorError("unterminated matcher list")
        if text[position] == "}":
            return tuple(matchers), position + 1
        if text[position] != ",":
            raise SelectorError("expected ',' or '}' after matcher")
        position = _skip_whitespace(text, position + 1)


def _skip_whitespace(text: str, position: int) -> int:
    while position < len(text) and text[position].isspace():
        position += 1
    return position


def _compile_matchers(matchers: tuple[Matcher, ...]) -> tuple[tuple[Matcher, Pattern[str] | None], ...]:
    compiled: list[tuple[Matcher, Pattern[str] | None]] = []
    for matcher in matchers:
        pattern: Pattern[str] | None = None
        if matcher.operator in {"=~", "!~"}:
            try:
                pattern = re.compile(matcher.value)
            except re.error as error:
                raise SelectorError(f"invalid regular expression for {matcher.name}: {error}") from error
        compiled.append((matcher, pattern))
    return tuple(compiled)


def _labels_match(
    labels: dict[str, str], compiled: tuple[tuple[Matcher, Pattern[str] | None], ...]
) -> bool:
    for matcher, pattern in compiled:
        actual = labels.get(matcher.name, "")
        if matcher.operator == "=" and actual != matcher.value:
            return False
        if matcher.operator == "!=" and actual == matcher.value:
            return False
        if matcher.operator == "=~" and (pattern is None or pattern.fullmatch(actual) is None):
            return False
        if matcher.operator == "!~" and pattern is not None and pattern.fullmatch(actual) is not None:
            return False
    return True


def _load_samples(
    database: Database,
    series_id: int,
    selector: Selector,
    at_ms: int,
    max_age_ms: int | None,
) -> list[QuerySample]:
    if selector.range_ms is not None:
        rows = database.connection.execute(
            """
            SELECT sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms
            FROM samples
            WHERE series_id = ? AND sample_timestamp_ms > ? AND sample_timestamp_ms <= ?
            ORDER BY sample_timestamp_ms
            """,
            (series_id, at_ms - selector.range_ms, at_ms),
        ).fetchall()
    else:
        rows = database.connection.execute(
            """
            SELECT sample_timestamp_ms, value, value_repr, scrape_id, collected_at_ms
            FROM samples
            WHERE series_id = ? AND sample_timestamp_ms <= ?
            ORDER BY sample_timestamp_ms DESC LIMIT 1
            """,
            (series_id, at_ms),
        ).fetchall()
    samples = [
        QuerySample(
            timestamp_ms=int(row["sample_timestamp_ms"]),
            value=row["value"],
            value_repr=row["value_repr"],
            scrape_id=int(row["scrape_id"]),
            collected_at_ms=int(row["collected_at_ms"]),
            age_ms=at_ms - int(row["sample_timestamp_ms"]),
        )
        for row in rows
    ]
    if selector.range_ms is None and max_age_ms is not None:
        samples = [sample for sample in samples if sample.age_ms <= max_age_ms]
    return samples
