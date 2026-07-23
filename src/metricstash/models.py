"""Typed configuration and collection data structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class CollectorConfig:
    max_concurrency: int = 4
    body_limit_bytes: int = 8 * 1024 * 1024
    max_samples_per_target: int = 25_000
    max_labels_per_sample: int = 32
    max_label_value_bytes: int = 1024
    max_resolved_ips: int = 16
    query_series_limit: int = 1_000
    query_sample_limit: int = 10_000
    busy_timeout_ms: int = 5_000


@dataclass(frozen=True)
class BasicAuthConfig:
    username: str
    password: str


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: float = 10.0
    retries: int = 0
    headers: Mapping[str, str] = field(default_factory=dict)
    basic_auth: BasicAuthConfig | None = None
    bearer_token_file: Path | None = None
    verify_tls: bool = True
    allow_missing_content_type: bool = False


@dataclass(frozen=True)
class MetricConfig:
    name: str
    metric_type: str
    required: bool = True


@dataclass(frozen=True)
class TargetConfig:
    url: str
    labels: Mapping[str, str] = field(default_factory=dict)
    allow_missing_content_type: bool | None = None


@dataclass(frozen=True)
class TaskConfig:
    system: str
    module: str
    job: str
    labels: Mapping[str, str]
    metrics: tuple[MetricConfig, ...]
    targets: tuple[TargetConfig, ...]
    http: HttpConfig = field(default_factory=HttpConfig)
    honor_labels: bool = True
    honor_timestamps: bool = True

    def collector_labels(self) -> dict[str, str]:
        """Return task-level labels with required business identity enforced."""
        labels = dict(self.labels)
        labels["system"] = self.system
        labels["module"] = self.module
        labels["job"] = self.job
        return labels


@dataclass(frozen=True)
class AppConfig:
    collector: CollectorConfig
    tasks: tuple[TaskConfig, ...]
    source_path: Path
