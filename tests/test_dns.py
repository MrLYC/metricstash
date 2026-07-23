from __future__ import annotations

import pytest

from metricstash.dns import AddressAnswers, ResolutionError, expand_target
from metricstash.models import MetricConfig, TargetConfig, TaskConfig


def make_task() -> TaskConfig:
    return TaskConfig(
        system="prod",
        module="api",
        job="api",
        labels={"cluster": "prod", "peer": "${target.resolved_ip}"},
        metrics=(MetricConfig("requests_total", "counter"),),
        targets=(),
    )


@pytest.mark.asyncio
async def test_prefers_all_aaaa_records_without_ipv4_fallback() -> None:
    async def fake_resolver(host: str, port: int) -> AddressAnswers:
        assert (host, port) == ("api.example.test", 443)
        return AddressAnswers(ipv6=("2001:db8::1", "2001:db8::2"), ipv4=("192.0.2.1",))

    resolved = await expand_target(
        make_task(),
        TargetConfig("https://api.example.test/metrics"),
        max_resolved_ips=16,
        resolver=fake_resolver,
    )

    assert [item.resolved_ip for item in resolved] == ["2001:db8::1", "2001:db8::2"]
    assert all(item.dns_name == "api.example.test" for item in resolved)
    assert resolved[0].collector_labels["peer"] == "2001:db8::1"
    assert resolved[0].collector_labels["instance"] == "[2001:db8::1]:443"


@pytest.mark.asyncio
async def test_uses_ipv4_only_when_no_aaaa_records_exist() -> None:
    async def fake_resolver(host: str, port: int) -> AddressAnswers:
        return AddressAnswers(ipv6=(), ipv4=("192.0.2.1", "192.0.2.2"))

    resolved = await expand_target(
        make_task(),
        TargetConfig("http://api.example.test/metrics"),
        max_resolved_ips=16,
        resolver=fake_resolver,
    )

    assert [item.resolved_ip for item in resolved] == ["192.0.2.1", "192.0.2.2"]
    assert resolved[0].collector_labels["instance"] == "192.0.2.1:80"


@pytest.mark.asyncio
async def test_address_cap_is_a_logical_target_failure() -> None:
    async def fake_resolver(host: str, port: int) -> AddressAnswers:
        return AddressAnswers(ipv6=("2001:db8::1", "2001:db8::2"), ipv4=())

    with pytest.raises(ResolutionError, match="address cap"):
        await expand_target(
            make_task(),
            TargetConfig("https://api.example.test/metrics"),
            max_resolved_ips=1,
            resolver=fake_resolver,
        )
