from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from pipeline_platform.validator import validate_config_dict


@dataclass
class PipelineInfoConfig:
    name: str
    description: str | None = None
    owner: str | None = None
    domain: str | None = None
    version: str | float | None = None


@dataclass
class SourceConnectionConfig:
    path: str | None = None
    base_url: str | None = None
    endpoint: str | None = None
    method: str | None = None
    auth_type: str | None = None
    auth_env_var: str | None = None


@dataclass
class PaginationConfig:
    type: str | None = None
    page_size: int | None = None


@dataclass
class ExtractionConfig:
    mode: str | None = None
    cursor_field: str | None = None
    pagination: PaginationConfig = field(default_factory=PaginationConfig)


@dataclass
class SourceConfig:
    type: str
    connection: SourceConnectionConfig = field(default_factory=SourceConnectionConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    response_format: str | None = None


@dataclass
class DestinationConfig:
    type: str
    warehouse: str
    schema: str
    table: str
    write_mode: str
    primary_key: str | None = None


@dataclass
class ScheduleConfig:
    cron: str
    timezone: str | None = None
    start_date: str | None = None
    catchup: bool = False


@dataclass
class TransformConfig:
    enabled: bool = False
    type: str | None = None
    sql_file: str | None = None
    target_schema: str | None = None
    target_table: str | None = None


@dataclass
class RuntimeConfig:
    retries: int = 0
    retry_delay_minutes: int = 0
    timeout_minutes: int = 60
    alert_email: list[str] = field(default_factory=list)


@dataclass
class LoggingConfig:
    log_level: str = "INFO"
    capture_row_counts: bool = True
    capture_runtime_metrics: bool = True
    enable_pipeline_audit: bool = True


@dataclass
class DataQualityCheckConfig:
    name: str
    column: str
    rule: str
    value: Any = None


@dataclass
class DataQualityConfig:
    enabled: bool = False
    checks: list[DataQualityCheckConfig] = field(default_factory=list)


@dataclass
class PipelineConfig:
    pipeline: PipelineInfoConfig
    source: SourceConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    transform: TransformConfig = field(default_factory=TransformConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)


def load_pipeline_config(path: str) -> PipelineConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)

    validate_config_dict(raw)

    dq_checks = [
        DataQualityCheckConfig(**check)
        for check in raw.get("data_quality", {}).get("checks", [])
    ]

    return PipelineConfig(
        pipeline=PipelineInfoConfig(**raw["pipeline"]),
        source=SourceConfig(
            type=raw["source"]["type"],
            connection=SourceConnectionConfig(**raw["source"].get("connection", {})),
            extraction=ExtractionConfig(
                mode=raw["source"].get("extraction", {}).get("mode"),
                cursor_field=raw["source"].get("extraction", {}).get("cursor_field"),
                pagination=PaginationConfig(
                    **raw["source"].get("extraction", {}).get("pagination", {})
                ),
            ),
            response_format=raw["source"].get("response_format"),
        ),
        destination=DestinationConfig(**raw["destination"]),
        schedule=ScheduleConfig(**raw["schedule"]),
        transform=TransformConfig(**raw.get("transform", {})),
        runtime=RuntimeConfig(**raw.get("runtime", {})),
        logging=LoggingConfig(**raw.get("logging", {})),
        data_quality=DataQualityConfig(
            enabled=raw.get("data_quality", {}).get("enabled", False),
            checks=dq_checks,
        ),
    )