from __future__ import annotations

from pathlib import Path

import duckdb


def default_db_path(base_dir: Path | None = None) -> Path:
    base = base_dir or Path.cwd()
    data_dir = base / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "nfl.duckdb"


def connect(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(db_path) if db_path is not None else default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))