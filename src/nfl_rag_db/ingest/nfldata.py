from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from nfl_rag_db.audit_log import record_source_file, record_table_stat
from nfl_rag_db.http_download import download_to_file
from nfl_rag_db.run_registry import (
    OUTCOME_FAIL,
    OUTCOME_OK,
    RunError,
    finish_run,
    start_run,
)

GAMES_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
TEAMS_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/teams.csv"


def _utc_ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")


def _count_if_exists(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
) -> int | None:
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


def ingest_nfldata_core(
    con: duckdb.DuckDBPyConnection,
    *,
    season_min: int = 2011,
    games_url: str = GAMES_URL,
    teams_url: str = TEAMS_URL,
    base_dir: Path | None = None,
) -> str:
    """
    Ingest nfldata (games.csv + teams.csv) into:
      - raw snapshots: data/raw/nfldata/...
      - staging: stg.nfldata_games / stg.nfldata_teams
      - canonical (v0): core.game / core.team_season (filtered by season_min)

    Monitoring:
      - audit.ingest_run (already in run_registry)
      - audit.ingest_source_file (per snapshot)
      - audit.ingest_table_stat (row counts + delta for core tables)
    """
    base = base_dir or Path.cwd()

    run_id = start_run(
        con,
        component="ingest_nfldata_core",
        source="nfldata",
        params={
            "season_min": season_min,
            "games_url": games_url,
            "teams_url": teams_url,
        },
    )

    try:
        ensure_schemas(con)

        # previous core counts (for delta monitoring)
        prev_core_games = _count_if_exists(con, "core", "game")
        prev_core_teams = _count_if_exists(con, "core", "team_season")

        # download raw snapshots
        ts = _utc_ts_compact()
        raw_base = base / "data" / "raw" / "nfldata"
        games_path = raw_base / "games" / f"{ts}_games.csv"
        teams_path = raw_base / "teams" / f"{ts}_teams.csv"

        games_dl = download_to_file(games_url, games_path)
        teams_dl = download_to_file(teams_url, teams_path)

        # record source files (snapshot metadata)
        record_source_file(
            con,
            run_id=run_id,
            source="nfldata",
            dataset="games",
            url=games_url,
            local_path=games_path,
            sha256=games_dl.sha256,
            size_bytes=games_dl.size_bytes,
        )
        record_source_file(
            con,
            run_id=run_id,
            source="nfldata",
            dataset="teams",
            url=teams_url,
            local_path=teams_path,
            sha256=teams_dl.sha256,
            size_bytes=teams_dl.size_bytes,
        )

        # load staging tables
        con.execute(
            """
            CREATE OR REPLACE TABLE stg.nfldata_games AS
            SELECT * FROM read_csv_auto(?, header=True);
            """,
            [str(games_path)],
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE stg.nfldata_teams AS
            SELECT * FROM read_csv_auto(?, header=True);
            """,
            [str(teams_path)],
        )

        stg_games_rows = int(con.execute("SELECT COUNT(*) FROM stg.nfldata_games;").fetchone()[0])
        stg_teams_rows = int(con.execute("SELECT COUNT(*) FROM stg.nfldata_teams;").fetchone()[0])

        # fail-fast sanity checks: catch parse/truncation early
        if stg_games_rows < 1000:
            raise ValueError(
                f"stg.nfldata_games too small ({stg_games_rows}); CSV parse likely failed"
            )
        if stg_teams_rows < 100:
            raise ValueError(
                f"stg.nfldata_teams too small ({stg_teams_rows}); CSV parse likely failed"
            )

        # record staging stats
        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="stg.nfldata_games",
            row_count=stg_games_rows,
            note="loaded from raw snapshot",
        )
        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="stg.nfldata_teams",
            row_count=stg_teams_rows,
            note="loaded from raw snapshot",
        )

        # create/replace canonical tables (v0)
        con.execute(
            """
            CREATE OR REPLACE TABLE core.game AS
            SELECT * FROM stg.nfldata_games
            WHERE season >= ?;
            """,
            [season_min],
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE core.team_season AS
            SELECT * FROM stg.nfldata_teams
            WHERE season >= ?;
            """,
            [season_min],
        )

        dup_game_ids = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT game_id
                    FROM core.game
                    GROUP BY game_id
                    HAVING COUNT(*) > 1
                );
                """
            ).fetchone()[0]
        )
        if dup_game_ids != 0:
            raise ValueError(f"Duplicate game_id detected: {dup_game_ids}")

        core_games_rows = int(con.execute("SELECT COUNT(*) FROM core.game;").fetchone()[0])
        core_teams_rows = int(con.execute("SELECT COUNT(*) FROM core.team_season;").fetchone()[0])

        # record canonical stats (+delta vs previous)
        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="core.game",
            row_count=core_games_rows,
            previous_row_count=prev_core_games,
            note=f"filtered season >= {season_min}",
        )
        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="core.team_season",
            row_count=core_teams_rows,
            previous_row_count=prev_core_teams,
            note=f"filtered season >= {season_min}",
        )

        finish_run(
            con,
            run_id=run_id,
            outcome=OUTCOME_OK,
            counts={
                "raw_games_bytes": games_dl.size_bytes,
                "raw_teams_bytes": teams_dl.size_bytes,
                "raw_games_sha256": games_dl.sha256,
                "raw_teams_sha256": teams_dl.sha256,
                "stg_games_rows": stg_games_rows,
                "stg_teams_rows": stg_teams_rows,
                "core_games_rows": core_games_rows,
                "core_teams_rows": core_teams_rows,
                "prev_core_games_rows": prev_core_games,
                "prev_core_teams_rows": prev_core_teams,
                "dup_game_ids": dup_game_ids,
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