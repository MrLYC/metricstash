from metricstash.labels import merge_labels


def test_honor_labels_fills_only_missing_endpoint_labels() -> None:
    merged = merge_labels(
        endpoint={"job": "exporter", "region": "sh"},
        collector={"job": "api", "cluster": "prod"},
        honor_labels=True,
        resolved_ip="2001:db8::1",
    )

    assert merged == {
        "job": "exporter",
        "region": "sh",
        "cluster": "prod",
        "resolved_ip": "2001:db8::1",
    }


def test_collector_wins_by_renaming_conflicting_endpoint_label() -> None:
    merged = merge_labels(
        endpoint={"job": "exporter", "cluster": "edge"},
        collector={"job": "api", "cluster": "prod"},
        honor_labels=False,
        resolved_ip="192.0.2.8",
    )

    assert merged == {
        "exported_job": "exporter",
        "exported_cluster": "edge",
        "job": "api",
        "cluster": "prod",
        "resolved_ip": "192.0.2.8",
    }


def test_resolved_ip_is_a_hard_collector_label() -> None:
    merged = merge_labels(
        endpoint={"resolved_ip": "not-the-peer"},
        collector={},
        honor_labels=True,
        resolved_ip="192.0.2.8",
    )

    assert merged["resolved_ip"] == "192.0.2.8"
