from __future__ import annotations

from pathlib import Path

import pytest

from metricstash.config import ConfigError, load_config


def write_config(tmp_path: Path, contents: str) -> Path:
    path = tmp_path / "metricstash.toml"
    path.write_text(contents, encoding="utf-8")
    return path


def test_minimal_task_receives_operating_defaults(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[tasks]]
system = "${context.system}"
module = "api"

[[tasks.metrics]]
name = "requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
""",
    )

    config = load_config(path, {"system": "prod"})
    task = config.tasks[0]

    assert task.system == "prod"
    assert task.job == task.module == "api"
    assert task.honor_labels is True
    assert task.honor_timestamps is True
    assert task.http.timeout_seconds == 10
    assert task.http.retries == 0
    assert task.metrics[0].required is True
    assert config.collector.max_concurrency == 4
    assert config.collector.max_resolved_ips == 16


def test_missing_template_context_fails_before_network(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[tasks]]
system = "${context.system}"
module = "api"
labels = { cluster = "${context.cluster}" }

[[tasks.metrics]]
name = "requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
""",
    )

    with pytest.raises(ConfigError, match="context.cluster"):
        load_config(path, {"system": "prod"})


def test_dns_templates_are_deferred_until_target_expansion(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[tasks]]
system = "prod"
module = "api"
labels = { peer = "${target.resolved_ip}" }

[[tasks.metrics]]
name = "requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
labels = { hostname = "${target.dns_name}" }
""",
    )

    task = load_config(path, {}).tasks[0]
    assert task.labels["peer"] == "${target.resolved_ip}"
    assert task.targets[0].labels["hostname"] == "${target.dns_name}"


@pytest.mark.parametrize("duration", ["", "5", "5q", "-1s"])
def test_invalid_http_timeout_is_a_configuration_error(tmp_path: Path, duration: str) -> None:
    path = write_config(
        tmp_path,
        f"""
[[tasks]]
system = "prod"
module = "api"

[tasks.http]
timeout = "{duration}"

[[tasks.metrics]]
name = "requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
""",
    )

    with pytest.raises(ConfigError, match="timeout"):
        load_config(path, {})


def test_basic_auth_and_bearer_token_are_mutually_exclusive(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[tasks]]
system = "prod"
module = "api"

[tasks.http]
basic_auth = { username = "user", password = "pass" }
bearer_token_file = "token"

[[tasks.metrics]]
name = "requests_total"
type = "counter"

[[tasks.targets]]
url = "https://api.example.test/metrics"
""",
    )

    with pytest.raises(ConfigError, match="both basic_auth and bearer_token_file"):
        load_config(path, {})
