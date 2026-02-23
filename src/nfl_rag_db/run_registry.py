from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import duckdb

OUTCOME_OK = "ok"
OUTCOME_PARTIAL = "partial"
OUTCOME_FAIL = "fail"


@dataclass(frozen=True)
class RunError:
    error_class: str
    error_code: str
    error_message: str


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS audit.ingest_run (
            run_id TEXT PRIMARY KEY,
            component TEXT NOT NULL,
            source TEXT,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP,
            outcome TEXT,
            duration_ms BIGINT,
            params_json TEXT NOT NULL,
            counts_json TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            error_class TEXT,
            error_code TEXT,
            error_message TEXT
        );
        """
    )


def start_run(
    con: duckdb.DuckDBPyConnection,
    *,
    component: str,
    source: str | None = None,
    params: dict | None = None,
) -> str:
    ensure_schema(con)
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    params_json = json.dumps(params or {}, ensure_ascii=False, separators=(",", ":"))

    con.execute(
        """
        INSERT INTO audit.ingest_run
        (run_id, component, source, started_at, params_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        [run_id, component, source, started_at, params_json],
    )
    return run_id


def finish_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    outcome: str,
    counts: dict | None = None,
    retry_count: int = 0,
    error: RunError | None = None,
) -> None:
    row = con.execute(
        "SELECT started_at FROM audit.ingest_run WHERE run_id = ?",
        [run_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown run_id: {run_id}")

    started_at: datetime = row[0]
    ended_at = datetime.now(timezone.utc).replace(tzinfo=None)
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)

    counts_json = (
        json.dumps(counts or {}, ensure_ascii=False, separators=(",", ":"))
        if counts is not None
        else None
    )

    con.execute(
        """
        UPDATE audit.ingest_run
        SET ended_at = ?,
            outcome = ?,
            duration_ms = ?,
            counts_json = ?,
            retry_count = ?,
            error_class = ?,
            error_code = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        [
            ended_at,
            outcome,
            duration_ms,
            counts_json,
            int(retry_count),
            (error.error_class if error else None),
            (error.error_code if error else None),
            (error.error_message if error else None),
            run_id,
        ],
    )