from __future__ import annotations

import pytest

from metricstash.exposition import (
    ExpositionError,
    IncrementalExpositionParser,
    iter_metric_families,
    parse_exposition,
)
from metricstash.models import MetricConfig


class NoBulkReadTextStream:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def __iter__(self):
        return iter(self._lines)

    def read(self, *args, **kwargs):  # pragma: no cover - the parser must never call this
        raise AssertionError("bulk read is forbidden")


def test_parser_consumes_line_stream_without_bulk_read() -> None:
    source = NoBulkReadTextStream([
        "# TYPE requests_total counter\n",
        "requests_total 1\n",
    ])

    families = list(iter_metric_families(source))

    assert families[0].name == "requests"


def test_parse_selected_counter_and_source_timestamp() -> None:
    result = parse_exposition(
        NoBulkReadTextStream([
            "# HELP requests_total total requests\n",
            "# TYPE requests_total counter\n",
            'requests_total{cluster="prod"} 3 1234\n',
        ]),
        (MetricConfig("requests_total", "counter"),),
        collection_timestamp_ms=9_999,
    )

    family = result.families[0]
    sample = family.samples[0]
    assert family.help_text == "total requests"
    assert sample.actual_name == "requests_total"
    assert sample.timestamp_ms == 1_234
    assert sample.value == 3.0


def test_special_values_use_text_representation_and_collection_time_when_timestamps_disabled() -> None:
    result = parse_exposition(
        NoBulkReadTextStream([
            "# TYPE temperature gauge\n",
            "temperature NaN 1234\n",
        ]),
        (MetricConfig("temperature", "gauge"),),
        collection_timestamp_ms=9_999,
        honor_timestamps=False,
    )

    sample = result.families[0].samples[0]
    assert (sample.value, sample.value_repr, sample.timestamp_ms) == (None, "NaN", 9_999)


def test_type_mismatch_for_selected_family_fails() -> None:
    with pytest.raises(ExpositionError, match="type mismatch"):
        parse_exposition(
            NoBulkReadTextStream([
                "# TYPE requests_total gauge\n",
                "requests_total 1\n",
            ]),
            (MetricConfig("requests_total", "counter"),),
            collection_timestamp_ms=1_000,
        )


def test_unselected_malformed_line_is_ignored_but_selected_one_fails() -> None:
    result = parse_exposition(
        NoBulkReadTextStream([
            "# TYPE selected gauge\n",
            "selected 1\n",
            "unselected{not-valid 2\n",
        ]),
        (MetricConfig("selected", "gauge"),),
        collection_timestamp_ms=1_000,
    )
    assert result.families[0].samples[0].actual_name == "selected"

    with pytest.raises(ExpositionError):
        parse_exposition(
            NoBulkReadTextStream([
                "# TYPE selected gauge\n",
                "selected{not-valid 2\n",
            ]),
            (MetricConfig("selected", "gauge"),),
            collection_timestamp_ms=1_000,
        )


def test_histogram_and_summary_must_be_structurally_complete() -> None:
    with pytest.raises(ExpositionError, match="incomplete histogram"):
        parse_exposition(
            NoBulkReadTextStream([
                "# TYPE latency_seconds histogram\n",
                'latency_seconds_bucket{le="+Inf"} 2\n',
            ]),
            (MetricConfig("latency_seconds", "histogram"),),
            collection_timestamp_ms=1_000,
        )

    result = parse_exposition(
        NoBulkReadTextStream([
            "# TYPE rpc_seconds summary\n",
            'rpc_seconds{quantile="0.5"} 0.2\n',
            "rpc_seconds_sum 1\n",
            "rpc_seconds_count 5\n",
        ]),
        (MetricConfig("rpc_seconds", "summary"),),
        collection_timestamp_ms=1_000,
    )
    assert len(result.families[0].samples) == 3


def test_incremental_adapter_emits_selected_samples_before_stream_end() -> None:
    parser = IncrementalExpositionParser(
        (MetricConfig("selected", "gauge"),),
        collection_timestamp_ms=1_000,
    )

    emitted = parser.feed_bytes(b"# TYPE selected gauge\nselected{job=\"api\"} 1")
    assert emitted == ()
    emitted = parser.feed_bytes(b"\nother{broken 2\n")

    assert emitted[0].samples[0].actual_name == "selected"
    parser.finish()
