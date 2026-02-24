from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from nfl_rag_db.audit_log import record_source_file, record_table_stat
from nfl_rag_db.change_detection import compute_change_counts
from nfl_rag_db.http_download import download_to_file
from nfl_rag_db.run_registry import OUTCOME_FAIL, OUTCOME_OK, RunError, finish_run, start_run

PLAYER_STATS_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats.parquet"
)


def _utc_ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")


def _count_if_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int | None:
    exists = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    if int(exists) == 0:
        return None
    return int(con.execute(f"SELECT COUNT(*) FROM {schema}.{table};").fetchone()[0])


def _get_cols(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def _infer_key_cols(cols: list[str]) -> list[str]:
    """
    Construct a stable uniqueness key for weekly player stats.

    We include season_type (REG/POST) if present to avoid collisions.
    """
    required = {"season", "player_id"}
    missing = required.difference(cols)
    if missing:
        raise ValueError(f"Required columns missing: {sorted(missing)}. Have: {cols}")

    key: list[str] = ["season"]

    if "season_type" in cols:
        key.append("season_type")
    elif "game_type" in cols:
        key.append("game_type")

    if "week" in cols:
        key.append("week")
    elif "game_id" in cols:
        key.append("game_id")

    key.append("player_id")
    return key


def _format_change_note(season_min: int, changes: dict[str, int], key_cols: list[str]) -> str:
    keys = ",".join(key_cols)
    return (
        f"season >= {season_min}; keys={keys}; "
        f"ins={changes['inserted']} upd={changes['updated']} del={changes['deleted']}"
    )


def ingest_player_stats(
    con: duckdb.DuckDBPyConnection,
    *,
    season_min: int = 2011,
    url: str = PLAYER_STATS_URL,
    base_dir: Path | None = None,
) -> str:
    base = base_dir or Path.cwd()
    run_id = start_run(
        con,
        component="ingest_player_stats",
        source="nflverse_player_stats",
        params={"season_min": season_min, "url": url},
    )

    try:
        ensure_schemas(con)

        prev_core_rows = _count_if_exists(con, "core", "player_week_stats")

        ts = _utc_ts_compact()
        raw_base = base / "data" / "raw" / "player_stats"
        raw_path = raw_base / f"{ts}_player_stats.parquet"

        dl = download_to_file(url, raw_path, timeout_s=300.0, retries=3)

        record_source_file(
            con,
            run_id=run_id,
            source="nflverse_player_stats",
            dataset="player_stats",
            url=url,
            local_path=raw_path,
            sha256=dl.sha256,
            size_bytes=dl.size_bytes,
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE stg.player_stats AS
            SELECT * FROM read_parquet(?);
            """,
            [str(raw_path)],
        )

        stg_rows = int(con.execute("SELECT COUNT(*) FROM stg.player_stats;").fetchone()[0])
        if stg_rows < 10_000:
            raise ValueError(f"stg.player_stats too small ({stg_rows}); parquet load likely failed")

        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="stg.player_stats",
            row_count=stg_rows,
            note="loaded from raw snapshot",
        )

        season_min_i = int(season_min)
        con.execute(
            "CREATE OR REPLACE TEMP VIEW tmp_incoming_player_week_stats AS "
            f"SELECT * FROM stg.player_stats WHERE season >= {season_min_i};"
        )

        incoming_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_incoming_player_week_stats;").fetchone()[0]
        )

        cols = _get_cols(con, "stg", "player_stats")
        key_cols = _infer_key_cols(cols)

        changes = compute_change_counts(
            con,
            existing_fqn="core.player_week_stats",
            incoming_fqn="tmp_incoming_player_week_stats",
            key_cols=key_cols,
            hash_cols=cols,
        )

        con.execute(
            "CREATE OR REPLACE TABLE core.player_week_stats AS "
            "SELECT * FROM tmp_incoming_player_week_stats;"
        )

        core_rows = int(con.execute("SELECT COUNT(*) FROM core.player_week_stats;").fetchone()[0])

        key_expr = ", ".join(key_cols)
        dup = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                  SELECT {key_expr}
                  FROM core.player_week_stats
                  GROUP BY {key_expr}
                  HAVING COUNT(*) > 1
                );
                """
            ).fetchone()[0]
        )
        if dup != 0:
            raise ValueError(f"Duplicate key rows detected in core.player_week_stats: {dup}")

        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="core.player_week_stats",
            row_count=core_rows,
            previous_row_count=prev_core_rows,
            note=_format_change_note(season_min_i, changes, key_cols),
        )

        finish_run(
            con,
            run_id=run_id,
            outcome=OUTCOME_OK,
            counts={
                "raw_bytes": dl.size_bytes,
                "raw_sha256": dl.sha256,
                "stg_rows": stg_rows,
                "incoming_rows": incoming_rows,
                "core_rows": core_rows,
                "prev_core_rows": prev_core_rows,
                "inserted": changes["inserted"],
                "updated": changes["updated"],
                "deleted": changes["deleted"],
                "key_cols": ",".join(key_cols),
            },
        )
        return run_id

    except Exception as e:
        finish_run(
            con,
            run_id=run_id,
            outcome=OUTCOME_FAIL,
            error=RunError(type(e).__name__, "EXCEPTION", str(e)),
        )
        raise