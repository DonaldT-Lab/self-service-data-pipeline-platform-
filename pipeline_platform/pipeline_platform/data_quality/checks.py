from __future__ import annotations

import pandas as pd

from pipeline_platform.config_parser import DataQualityCheckConfig


class DataQualityRunner:
    def run_checks(
        self,
        dataframe: pd.DataFrame,
        checks: list[DataQualityCheckConfig],
    ) -> None:
        for check in checks:
            if check.rule == "not_null":
                self._check_not_null(dataframe, check.column, check.name)

            elif check.rule == "timestamp_format":
                self._check_timestamp_format(dataframe, check.column, check.name)

            elif check.rule == "greater_than":
                self._check_greater_than(dataframe, check.column, check.value, check.name)

            else:
                raise ValueError(f"Unsupported data quality rule: {check.rule}")

    def _check_not_null(self, dataframe: pd.DataFrame, column: str, check_name: str) -> None:
        if dataframe[column].isnull().any():
            raise ValueError(f"Data quality check failed [{check_name}]: column '{column}' contains nulls")

    def _check_timestamp_format(self, dataframe: pd.DataFrame, column: str, check_name: str) -> None:
        try:
            pd.to_datetime(dataframe[column], errors="raise")
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"Data quality check failed [{check_name}]: column '{column}' has invalid timestamp format"
            ) from exc

    def _check_greater_than(
        self,
        dataframe: pd.DataFrame,
        column: str,
        value: int | float,
        check_name: str,
    ) -> None:
        if not (dataframe[column] > value).all():
            raise ValueError(
                f"Data quality check failed [{check_name}]: column '{column}' must be greater than {value}"
            )