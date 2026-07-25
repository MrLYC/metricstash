"""Logical-target DNS expansion with deliberate IPv6-first behavior."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

from metricstash.models import TargetConfig, TaskConfig
from metricstash.templates import TemplateError, expand_template


class ResolutionError(RuntimeError):
    """A logical target cannot be expanded into physical IP targets."""


@dataclass(frozen=True)
class AddressAnswers:
    ipv6: tuple[str, ...]
    ipv4: tuple[str, ...]


@dataclass(frozen=True)
class PhysicalTarget:
    task: TaskConfig
    target: TargetConfig
    dns_name: str
    resolved_ip: str
    port: int
    collector_labels: dict[str, str]

    @property
    def url(self) -> str:
        """The original hostname URL, retained for Host/SNI/certificate semantics."""
        return self.target.url


Resolver = Callable[[str, int], Awaitable[AddressAnswers]]


async def expand_target(
    task: TaskConfig,
    target: TargetConfig,
    *,
    max_resolved_ips: int,
    resolver: Resolver | None = None,
) -> tuple[PhysicalTarget, ...]:
    """Expand one hostname to IP-specific collection targets.

    AAAA answers are preferred as a set: if any exist, A answers are intentionally
    ignored, including after an IPv6 connection failure. IPv4 is used only when no
    IPv6 answer exists at resolution time.
    """
    parsed = urlsplit(target.url)
    hostname = parsed.hostname
    if hostname is None:
        raise ResolutionError(f"target URL has no hostname: {target.url!r}")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as error:
        raise ResolutionError(f"target URL has invalid port: {target.url!r}") from error
    addresses = await _resolve(hostname, port, resolver)
    selected = _selected_addresses(addresses)
    if not selected:
        raise ResolutionError(f"DNS returned no A or AAAA address for {hostname}")
    if len(selected) > max_resolved_ips:
        raise ResolutionError(
            f"DNS address cap ({max_resolved_ips}) exceeded for {hostname}: {len(selected)} addresses"
        )
    physical: list[PhysicalTarget] = []
    for address in selected:
        labels = _expand_collector_labels(task, target, hostname, address)
        labels.setdefault("instance", _format_instance(address, port))
        physical.append(
            PhysicalTarget(
                task=task,
                target=target,
                dns_name=hostname,
                resolved_ip=address,
                port=port,
                collector_labels=labels,
            )
        )
    return tuple(physical)


async def default_resolver(hostname: str, port: int) -> AddressAnswers:
    """Resolve A and AAAA records through the host event loop resolver."""
    loop = asyncio.get_running_loop()
    try:
        records = await loop.getaddrinfo(
            hostname,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except OSError as error:
        raise ResolutionError(f"cannot resolve {hostname}: {error}") from error
    ipv6: list[str] = []
    ipv4: list[str] = []
    for family, _, _, _, sockaddr in records:
        if family == socket.AF_INET6:
            ipv6.append(str(sockaddr[0]))
        elif family == socket.AF_INET:
            ipv4.append(str(sockaddr[0]))
    return AddressAnswers(ipv6=tuple(ipv6), ipv4=tuple(ipv4))


async def _resolve(hostname: str, port: int, resolver: Resolver | None) -> AddressAnswers:
    try:
        literal = ipaddress.ip_address(hostname)
    except ValueError:
        resolve = resolver or default_resolver
        answers = await resolve(hostname, port)
    else:
        if literal.version == 6:
            answers = AddressAnswers(ipv6=(str(literal),), ipv4=())
        else:
            answers = AddressAnswers(ipv6=(), ipv4=(str(literal),))
    return AddressAnswers(
        ipv6=tuple(_validated_unique(answers.ipv6, 6)),
        ipv4=tuple(_validated_unique(answers.ipv4, 4)),
    )


def _selected_addresses(answers: AddressAnswers) -> tuple[str, ...]:
    if answers.ipv6:
        return answers.ipv6
    return answers.ipv4


def _validated_unique(addresses: tuple[str, ...], version: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in addresses:
        try:
            address = ipaddress.ip_address(raw)
        except ValueError as error:
            raise ResolutionError(f"resolver returned invalid IP address {raw!r}") from error
        if address.version != version:
            raise ResolutionError(f"resolver returned an address in the wrong family: {raw!r}")
        normalized = str(address)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _expand_collector_labels(
    task: TaskConfig,
    target: TargetConfig,
    dns_name: str,
    resolved_ip: str,
) -> dict[str, str]:
    values = {
        "context": {},
        "task": {"system": task.system, "module": task.module, "job": task.job},
        "target": {"url": target.url, "dns_name": dns_name, "resolved_ip": resolved_ip},
    }
    try:
        labels = {
            name: expand_template(value, values)
            for name, value in task.collector_labels().items()
        }
        labels.update(
            {name: expand_template(value, values) for name, value in target.labels.items()}
        )
    except TemplateError as error:
        raise ResolutionError(str(error)) from error
    return labels


def _format_instance(address: str, port: int) -> str:
    if ":" in address:
        return f"[{address}]:{port}"
    return f"{address}:{port}"
