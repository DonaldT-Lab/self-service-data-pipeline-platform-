from __future__ import annotations

from pathlib import Path

from pipeline_platform.config_parser import PipelineConfig
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


class TransformRunner:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse

    def run_transform(self, config: PipelineConfig) -> None:
        if not config.transform.enabled:
            return

        if config.transform.type != "sql":
            raise ValueError(f"Unsupported transform type: {config.transform.type}")

        if not config.transform.sql_file:
            raise ValueError("Transform is enabled but no sql_file was provided")

        sql_path = Path(config.transform.sql_file)
        if not sql_path.exists():
            raise FileNotFoundError(f"Transform SQL file not found: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8")

        source_table = f"{config.destination.schema}_{config.destination.table}"
        target_table = f"{config.transform.target_schema}_{config.transform.target_table}"

        sql = sql.replace("{{ source_table }}", source_table)
        sql = sql.replace("{{ target_table }}", target_table)

        self.warehouse.execute(sql)