from __future__ import annotations

from pathlib import Path

from pipeline_platform.config_parser import PipelineConfig


DAG_TEMPLATE = '''import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

_PROJECT_DIR = os.getenv("PIPELINE_PROJECT_DIR", ".")

with DAG(
    dag_id="{pipeline_name}",
    start_date=datetime(2026, 1, 1),
    schedule="{schedule}",
    catchup=False,
    tags=["starter-platform", "generated"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd " + _PROJECT_DIR + " && python main.py run --config {config_path}",
    )
'''


def render_dag_file(config: PipelineConfig) -> str:
    config_filename = Path(f"{config.pipeline.name}.yaml").name
    return DAG_TEMPLATE.format(
        pipeline_name=config.pipeline.name,
        schedule=config.schedule.cron,
        config_path=f"configs/{config_filename}",
    )