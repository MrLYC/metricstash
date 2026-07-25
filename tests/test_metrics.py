from metricstash.metrics import allowed_sample_names, family_base_for_sample


def test_histogram_declaration_expands_to_classic_sample_names() -> None:
    assert allowed_sample_names("latency_seconds", "histogram") == {
        "latency_seconds_bucket",
        "latency_seconds_sum",
        "latency_seconds_count",
    }
    assert family_base_for_sample("latency_seconds_bucket", "latency_seconds", "histogram") == "latency_seconds"


def test_summary_declaration_keeps_quantile_sample_and_components() -> None:
    assert allowed_sample_names("rpc_seconds", "summary") == {
        "rpc_seconds",
        "rpc_seconds_sum",
        "rpc_seconds_count",
    }


def test_scalar_metric_declaration_accepts_only_its_configured_name() -> None:
    assert allowed_sample_names("http_requests_total", "counter") == {"http_requests_total"}
