from __future__ import annotations

from pathlib import Path

from croniter import croniter

SUPPORTED_SOURCE_TYPES = {"csv", "api"}
SUPPORTED_LOAD_MODES = {"append", "overwrite"}
SUPPORTED_DESTINATION_TYPES = {"warehouse"}
SUPPORTED_WAREHOUSE_TYPES = {"duckdb"}
SUPPORTED_TRANSFORM_TYPES = {"sql"}
SUPPORTED_EXTRACTION_MODES = {"full", "incremental"}
SUPPORTED_RESPONSE_FORMATS = {"json", "csv"}
SUPPORTED_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
SUPPORTED_DQ_RULES = {"not_null", "timestamp_format", "greater_than"}

REQUIRED_TOP_LEVEL_FIELDS = {
    "pipeline",
    "source",
    "destination",
    "schedule",
}


class ConfigValidationError(ValueError):
    """Raised when a pipeline configuration is invalid."""


def validate_config_dict(config: dict) -> None:
    missing_fields = REQUIRED_TOP_LEVEL_FIELDS - set(config.keys())
    if missing_fields:
        raise ConfigValidationError(
            f"Missing required top-level fields: {sorted(missing_fields)}"
        )

    _validate_pipeline(config["pipeline"])
    _validate_source(config["source"])
    _validate_destination(config["destination"])
    _validate_schedule(config["schedule"])
    _validate_transform(config.get("transform", {}))
    _validate_runtime(config.get("runtime", {}))
    _validate_logging(config.get("logging", {}))
    _validate_data_quality(config.get("data_quality", {}))


def _validate_pipeline(pipeline: dict) -> None:
    if not pipeline.get("name"):
        raise ConfigValidationError("pipeline.name is required")
    if not pipeline.get("owner"):
        raise ConfigValidationError("pipeline.owner is required")


def _validate_source(source: dict) -> None:
    source_type = source.get("type")
    if source_type not in SUPPORTED_SOURCE_TYPES:
        raise ConfigValidationError(
            f"Unsupported source.type: {source_type}. Supported: {sorted(SUPPORTED_SOURCE_TYPES)}"
        )

    connection = source.get("connection", {})

    if source_type == "csv":
        if not connection.get("path"):
            raise ConfigValidationError(
                "source.connection.path is required when source.type = csv"
            )

    if source_type == "api":
        required_api_fields = ["base_url", "endpoint", "method"]
        missing = [field for field in required_api_fields if not connection.get(field)]
        if missing:
            raise ConfigValidationError(
                f"Missing API connection fields: {missing}"
            )

        if connection.get("auth_type") == "bearer_token" and not connection.get("auth_env_var"):
            raise ConfigValidationError(
                "source.connection.auth_env_var is required when auth_type = bearer_token"
            )

    extraction = source.get("extraction", {})
    mode = extraction.get("mode", "full")
    if mode not in SUPPORTED_EXTRACTION_MODES:
        raise ConfigValidationError(
            f"Unsupported extraction mode: {mode}. Supported: {sorted(SUPPORTED_EXTRACTION_MODES)}"
        )

    if mode == "incremental" and not extraction.get("cursor_field"):
        raise ConfigValidationError(
            "source.extraction.cursor_field is required when extraction.mode = incremental"
        )

    response_format = source.get("response_format")
    if response_format and response_format not in SUPPORTED_RESPONSE_FORMATS:
        raise ConfigValidationError(
            f"Unsupported response_format: {response_format}. Supported: {sorted(SUPPORTED_RESPONSE_FORMATS)}"
        )


def _validate_destination(destination: dict) -> None:
    if destination.get("type") not in SUPPORTED_DESTINATION_TYPES:
        raise ConfigValidationError(
            f"Unsupported destination.type: {destination.get('type')}. "
            f"Supported: {sorted(SUPPORTED_DESTINATION_TYPES)}"
        )

    if destination.get("warehouse") not in SUPPORTED_WAREHOUSE_TYPES:
        raise ConfigValidationError(
            f"Unsupported destination.warehouse: {destination.get('warehouse')}. "
            f"Supported: {sorted(SUPPORTED_WAREHOUSE_TYPES)}"
        )

    if not destination.get("schema"):
        raise ConfigValidationError("destination.schema is required")

    if not destination.get("table"):
        raise ConfigValidationError("destination.table is required")

    if destination.get("write_mode") not in SUPPORTED_LOAD_MODES:
        raise ConfigValidationError(
            f"Unsupported destination.write_mode: {destination.get('write_mode')}. "
            f"Supported: {sorted(SUPPORTED_LOAD_MODES)}"
        )


def _validate_schedule(schedule: dict) -> None:
    cron_value = schedule.get("cron")
    if not cron_value or not isinstance(cron_value, str):
        raise ConfigValidationError("schedule.cron must be a non-empty string")

    if not croniter.is_valid(cron_value):
        raise ConfigValidationError(f"Invalid cron expression: {cron_value}")

    catchup = schedule.get("catchup", False)
    if not isinstance(catchup, bool):
        raise ConfigValidationError("schedule.catchup must be true or false")


def _validate_transform(transform: dict) -> None:
    if not transform:
        return

    enabled = transform.get("enabled", False)
    if not enabled:
        return

    if transform.get("type") not in SUPPORTED_TRANSFORM_TYPES:
        raise ConfigValidationError(
            f"Unsupported transform.type: {transform.get('type')}. "
            f"Supported: {sorted(SUPPORTED_TRANSFORM_TYPES)}"
        )

    sql_file = transform.get("sql_file")
    if not sql_file:
        raise ConfigValidationError("transform.sql_file is required when transform.enabled = true")

    if not Path(sql_file).exists():
        raise ConfigValidationError(f"transform.sql_file not found: {sql_file}")

    if not transform.get("target_schema"):
        raise ConfigValidationError("transform.target_schema is required")

    if not transform.get("target_table"):
        raise ConfigValidationError("transform.target_table is required")


def _validate_runtime(runtime: dict) -> None:
    if not runtime:
        return

    retries = runtime.get("retries", 0)
    retry_delay_minutes = runtime.get("retry_delay_minutes", 0)
    timeout_minutes = runtime.get("timeout_minutes", 60)

    if not isinstance(retries, int) or retries < 0:
        raise ConfigValidationError("runtime.retries must be an integer >= 0")

    if not isinstance(retry_delay_minutes, int) or retry_delay_minutes < 0:
        raise ConfigValidationError("runtime.retry_delay_minutes must be an integer >= 0")

    if not isinstance(timeout_minutes, int) or timeout_minutes <= 0:
        raise ConfigValidationError("runtime.timeout_minutes must be an integer > 0")


def _validate_logging(logging_config: dict) -> None:
    if not logging_config:
        return

    log_level = logging_config.get("log_level", "INFO")
    if log_level not in SUPPORTED_LOG_LEVELS:
        raise ConfigValidationError(
            f"Unsupported logging.log_level: {log_level}. "
            f"Supported: {sorted(SUPPORTED_LOG_LEVELS)}"
        )


def _validate_data_quality(data_quality: dict) -> None:
    if not data_quality:
        return

    enabled = data_quality.get("enabled", False)
    if not enabled:
        return

    checks = data_quality.get("checks", [])
    if not isinstance(checks, list) or not checks:
        raise ConfigValidationError(
            "data_quality.checks must be a non-empty list when data_quality.enabled = true"
        )

    for check in checks:
        if not check.get("name"):
            raise ConfigValidationError("Each data quality check must have a name")
        if not check.get("column"):
            raise ConfigValidationError("Each data quality check must have a column")
        if check.get("rule") not in SUPPORTED_DQ_RULES:
            raise ConfigValidationError(
                f"Unsupported data quality rule: {check.get('rule')}. "
                f"Supported: {sorted(SUPPORTED_DQ_RULES)}"
            )
        if check.get("rule") == "greater_than" and "value" not in check:
            raise ConfigValidationError(
                "Data quality rule 'greater_than' requires a value"
            )