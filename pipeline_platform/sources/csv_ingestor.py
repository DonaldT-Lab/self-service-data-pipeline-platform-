from __future__ import annotations

from pathlib import Path

import pandas as pd


class CSVIngestor:
    def read(self, path: str) -> pd.DataFrame:
        file_path = Path(path)

        if not file_path.is_absolute():
            file_path = Path.cwd() / file_path

        file_path = file_path.resolve()

        if not file_path.exists():
            raise FileNotFoundError(f"CSV source not found: {file_path}")

        return pd.read_csv(file_path)