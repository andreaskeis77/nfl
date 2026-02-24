from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from nfl_rag_db.run_registry import ensure_schema as ensure_run_schema


def _utc_now_naive() -> datetime:
    # DuckDB TIMESTAMP is naive here; we store "naive UTC" consistently.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    # Ensures schema audit + audit.ingest_run exists
    ensure_run_schema(con)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS audit.ingest_source_file (
            file_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            dataset TEXT NOT NULL,
            url TEXT NOT NULL,
            local_path TEXT NOT NULL,
            retrieved_at TIMESTAMP NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes BIGINT NOT NULL
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS audit.ingest_table_stat (
            stat_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            table_fqn TEXT NOT NULL,
            captured_at TIMESTAMP NOT NULL,
            row_count BIGINT NOT NULL,
            previous_row_count BIGINT,
            delta_row_count BIGINT,
            note TEXT
        );
        """
    )


def record_source_file(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    source: str,
    dataset: str,
    url: str,
    local_path: Path | str,
    sha256: str,
    size_bytes: int,
    retrieved_at: datetime | None = None,
) -> str:
    ensure_schema(con)
    file_id = str(uuid4())
    ts = retrieved_at or _utc_now_naive()

    con.execute(
        """
        INSERT INTO audit.ingest_source_file
        (file_id, run_id, source, dataset, url, local_path, retrieved_at, sha256, size_bytes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            file_id,
            run_id,
            source,
            dataset,
            url,
            str(local_path),
            ts,
            sha256,
            int(size_bytes),
        ],
    )
    return file_id


def record_table_stat(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    table_fqn: str,
    row_count: int,
    previous_row_count: int | None = None,
    note: str | None = None,
) -> str:
    ensure_schema(con)
    stat_id = str(uuid4())
    captured_at = _utc_now_naive()
    delta = None if previous_row_count is None else int(row_count) - int(previous_row_count)

    con.execute(
    """
    INSERT INTO audit.ingest_table_stat
    (
        stat_id,
        run_id,
        table_fqn,
        captured_at,
        row_count,
        previous_row_count,
        delta_row_count,
        note
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    [
        stat_id,
        run_id,
        table_fqn,
        captured_at,
        int(row_count),
        (int(previous_row_count) if previous_row_count is not None else None),
        delta,
        note,
    ],
)
    return stat_id