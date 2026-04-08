from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import pandas as pd
import requests

from pipeline_platform.config_parser import PipelineConfig
from pipeline_platform.metadata.registry import PipelineRegistry
from pipeline_platform.metadata.run_logger import RunLogger
from pipeline_platform.orchestration.dag_template import render_dag_file
from pipeline_platform.sources.csv_ingestor import CSVIngestor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse
from pipeline_platform.transforms.transform_runner import TransformRunner
from pipeline_platform.pipeline_platform.data_quality.checks import DataQualityRunner


class PipelineExecutor:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse
        self.registry = PipelineRegistry(warehouse)
        self.run_logger = RunLogger(warehouse)
        self.csv_ingestor = CSVIngestor()
        self.transform_runner = TransformRunner(warehouse)
        self.data_quality_runner = DataQualityRunner()

    def execute(self, config: PipelineConfig) -> None:
        self.registry.ensure_metadata_tables()
        self.registry.register_pipeline(config)

        run_id = str(uuid.uuid4())
        start = time.time()
        rows_extracted = 0
        rows_loaded = 0

        try:
            dataframe = self._extract(config)
            rows_extracted = len(dataframe)

            if config.data_quality.enabled:
                self.data_quality_runner.run_checks(dataframe, config.data_quality.checks)

            rows_loaded = self._load(config, dataframe)
            
            if config.transform.enabled:
                self.transform_runner.run_transform(config)
                
            duration = round(time.time() - start, 2)
           
            self.run_logger.log_run(
                run_id=run_id,
                pipeline_name=config.pipeline.name,
                status="SUCCESS",
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                execution_time_seconds=duration,
                error_message=None,
            )
            self.registry.update_last_run_status(config.pipeline.name, "SUCCESS")

        except Exception as exc:  # noqa: BLE001
            duration = round(time.time() - start, 2)

            self.run_logger.log_run(
                run_id=run_id,
                pipeline_name=config.pipeline.name,
                status="FAILED",
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                execution_time_seconds=duration,
                error_message=str(exc),
            )
            self.registry.update_last_run_status(config.pipeline.name, "FAILED")
            raise

    def _extract(self, config: PipelineConfig) -> pd.DataFrame:
        if config.source.type == "csv":
            if not config.source.connection.path:
                raise ValueError("CSV source requires source.connection.path")
            return self.csv_ingestor.read(config.source.connection.path)

        if config.source.type == "api":
            return self._extract_from_api(config)

        raise NotImplementedError(f"Unsupported source type: {config.source.type}")

    def _extract_from_api(self, config: PipelineConfig) -> pd.DataFrame:
        conn = config.source.connection
        url = f"{conn.base_url}{conn.endpoint}"

        headers: dict[str, str] = {}

        if conn.auth_type == "bearer_token":
            if not conn.auth_env_var:
                raise ValueError(
                    "API source with bearer_token auth requires auth_env_var"
                )
            token = os.getenv(conn.auth_env_var)
            if not token:
                raise ValueError(
                    f"Missing environment variable for API token: {conn.auth_env_var}"
                )
            headers["Authorization"] = f"Bearer {token}"

        method = (conn.method or "GET").upper()

        response = requests.request(method=method, url=url, headers=headers, timeout=60)

        if response.status_code != 200:
            raise ValueError(
                f"API request failed with status {response.status_code}: {response.text}"
            )

        data = response.json()

        if isinstance(data, list):
            return pd.DataFrame(data)

        if isinstance(data, dict):
            records = data.get("data", data.get("results", data))
            if isinstance(records, list):
                return pd.DataFrame(records)
            if isinstance(records, dict):
                return pd.DataFrame([records])

        raise ValueError("API response format not supported for DataFrame conversion")

    def _load(self, config: PipelineConfig, dataframe: pd.DataFrame) -> int:
        table_name = self._physical_table_name(config)
        return self.warehouse.load_dataframe(
            table_name=table_name,
            dataframe=dataframe,
            load_mode=config.destination.write_mode,
        )

    @staticmethod
    def _physical_table_name(config: PipelineConfig) -> str:
        return f"{config.destination.schema}_{config.destination.table}"


def generate_dag_file(config: PipelineConfig) -> str:
    output_dir = Path("generated_dags")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"{config.pipeline.name}_dag.py"
    output_path.write_text(render_dag_file(config), encoding="utf-8")
    return str(output_path)