from __future__ import annotations

from datetime import datetime, timezone

from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


def _escape_sql(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("'", "''")


class RunLogger:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse

    def log_run(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        rows_extracted: int,
        rows_loaded: int,
        execution_time_seconds: float,
        error_message: str | None,
    ) -> None:
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

        safe_run_id = _escape_sql(run_id)
        safe_pipeline_name = _escape_sql(pipeline_name)
        safe_status = _escape_sql(status)
        safe_error = "NULL" if error_message is None else f"'{_escape_sql(error_message)}'"

        self.warehouse.execute(
            f"""
            INSERT INTO metadata_pipeline_runs VALUES (
                '{safe_run_id}',
                '{safe_pipeline_name}',
                TIMESTAMP '{timestamp}',
                '{safe_status}',
                {rows_extracted},
                {rows_loaded},
                {execution_time_seconds},
                {safe_error}
            )
            """
        )