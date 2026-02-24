from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from nfl_rag_db.audit_log import record_source_file, record_table_stat
from nfl_rag_db.change_detection import compute_change_counts
from nfl_rag_db.http_download import download_to_file
from nfl_rag_db.run_registry import OUTCOME_FAIL, OUTCOME_OK, RunError, finish_run, start_run

PBP_URL_TEMPLATE = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
    "play_by_play_{season}.parquet"
)


def pbp_url_for_season(season: int) -> str:
    return PBP_URL_TEMPLATE.format(season=int(season))


def _utc_ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")


def _qid(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return int(n) > 0


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


def _col_or_null(cols: list[str], name: str, out_name: str | None = None) -> str:
    out = out_name or name
    if name in cols:
        if out != name:
            return f"{_qid(name)} AS {_qid(out)}"
        return _qid(name)
    return f"NULL AS {_qid(out)}"


def _score_select(cols: list[str]) -> tuple[str, str]:
    if "total_home_score" in cols and "total_away_score" in cols:
        home = f"{_qid('total_home_score')} AS home_score"
        away = f"{_qid('total_away_score')} AS away_score"
        return home, away

    if "home_score" in cols and "away_score" in cols:
        home = f"{_qid('home_score')} AS home_score"
        away = f"{_qid('away_score')} AS away_score"
        return home, away

    return "NULL AS home_score", "NULL AS away_score"


def _build_scoring_event_sql(source_fqn: str, cols: list[str], season_i: int) -> str:
    conds: list[str] = []
    if "touchdown" in cols:
        conds.append(f"coalesce({_qid('touchdown')}, 0) = 1")
    if "safety" in cols:
        conds.append(f"coalesce({_qid('safety')}, 0) = 1")
    if "field_goal_result" in cols:
        conds.append(f"{_qid('field_goal_result')} = 'made'")
    if "extra_point_result" in cols:
        conds.append(f"{_qid('extra_point_result')} = 'good'")
    if "two_point_conv_result" in cols:
        conds.append(f"{_qid('two_point_conv_result')} = 'success'")

    where_clause = " OR ".join(conds) if conds else "FALSE"

    case_lines: list[str] = []
    if "safety" in cols:
        case_lines.append(f"WHEN coalesce({_qid('safety')}, 0) = 1 THEN 'SAFETY'")
    if "touchdown" in cols:
        case_lines.append(f"WHEN coalesce({_qid('touchdown')}, 0) = 1 THEN 'TD'")
    if "field_goal_result" in cols:
        case_lines.append(f"WHEN {_qid('field_goal_result')} = 'made' THEN 'FG'")
    if "extra_point_result" in cols:
        case_lines.append(f"WHEN {_qid('extra_point_result')} = 'good' THEN 'XP'")
    if "two_point_conv_result" in cols:
        case_lines.append(f"WHEN {_qid('two_point_conv_result')} = 'success' THEN '2P'")
    scoring_type_expr = "CASE " + " ".join(case_lines) + " ELSE NULL END"

    points_lines: list[str] = []
    if "safety" in cols:
        points_lines.append(f"WHEN coalesce({_qid('safety')}, 0) = 1 THEN 2")
    if "touchdown" in cols:
        points_lines.append(f"WHEN coalesce({_qid('touchdown')}, 0) = 1 THEN 6")
    if "field_goal_result" in cols:
        points_lines.append(f"WHEN {_qid('field_goal_result')} = 'made' THEN 3")
    if "extra_point_result" in cols:
        points_lines.append(f"WHEN {_qid('extra_point_result')} = 'good' THEN 1")
    if "two_point_conv_result" in cols:
        points_lines.append(f"WHEN {_qid('two_point_conv_result')} = 'success' THEN 2")
    points_expr = "CASE " + " ".join(points_lines) + " ELSE NULL END"

    posteam_expr = _qid("posteam") if "posteam" in cols else "NULL"
    defteam_expr = _qid("defteam") if "defteam" in cols else "NULL"

    safety_when = ""
    if "safety" in cols:
        safety_when = f"WHEN coalesce({_qid('safety')}, 0) = 1 THEN {defteam_expr}"

    td_team_when = ""
    if "touchdown" in cols and "td_team" in cols:
        td_team_when = (
            f"WHEN coalesce({_qid('touchdown')}, 0) = 1 AND {_qid('td_team')} IS NOT NULL "
            f"THEN {_qid('td_team')}"
        )

    scoring_team_expr = (
        "CASE "
        f"{safety_when} "
        f"{td_team_when} "
        f"ELSE {posteam_expr} "
        "END"
    )

    season_select = f"{_qid('season')} AS season" if "season" in cols else f"{season_i} AS season"
    home_score_expr, away_score_expr = _score_select(cols)

    select_cols = [
        season_select,
        _col_or_null(cols, "season_type"),
        _col_or_null(cols, "week"),
        _qid("game_id"),
        _qid("play_id"),
        _col_or_null(cols, "qtr"),
        _col_or_null(cols, "game_seconds_remaining"),
        _col_or_null(cols, "quarter_seconds_remaining"),
        _col_or_null(cols, "posteam"),
        _col_or_null(cols, "defteam"),
        _col_or_null(cols, "desc", out_name="play_desc"),
        home_score_expr,
        away_score_expr,
        f"{scoring_team_expr} AS scoring_team",
        f"{scoring_type_expr} AS scoring_type",
        f"{points_expr} AS points",
    ]

    return (
        "SELECT "
        + ", ".join(select_cols)
        + f" FROM {source_fqn} WHERE {where_clause}"
    )


def ingest_pbp_and_scoring(
    con: duckdb.DuckDBPyConnection,
    *,
    season: int,
    url: str | None = None,
    base_dir: Path | None = None,
) -> str:
    season_i = int(season)
    url_final = url or pbp_url_for_season(season_i)
    base = base_dir or Path.cwd()

    run_id = start_run(
        con,
        component="ingest_pbp_and_scoring",
        source="nflverse_pbp",
        params={"season": season_i, "url": url_final},
    )

    try:
        ensure_schemas(con)

        ts = _utc_ts_compact()
        raw_base = base / "data" / "raw" / "pbp" / f"season={season_i}"
        raw_path = raw_base / f"{ts}_play_by_play_{season_i}.parquet"

        dl = download_to_file(url_final, raw_path, timeout_s=600.0, retries=3)

        record_source_file(
            con,
            run_id=run_id,
            source="nflverse_pbp",
            dataset=f"pbp_{season_i}",
            url=url_final,
            local_path=raw_path,
            sha256=dl.sha256,
            size_bytes=dl.size_bytes,
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE stg.pbp AS
            SELECT * FROM read_parquet(?);
            """,
            [str(raw_path)],
        )

        stg_rows = int(con.execute("SELECT COUNT(*) FROM stg.pbp;").fetchone()[0])
        if stg_rows < 10_000:
            raise ValueError(f"stg.pbp too small ({stg_rows}); parquet load likely failed")

        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="stg.pbp",
            row_count=stg_rows,
            note=f"loaded from raw snapshot (season={season_i})",
        )

        cols = _get_cols(con, "stg", "pbp")
        if "game_id" not in cols or "play_id" not in cols:
            raise ValueError("Required columns missing in pbp: game_id/play_id")

        if "season" in cols:
            con.execute(
                "CREATE OR REPLACE TEMP VIEW tmp_incoming_pbp AS "
                f"SELECT * FROM stg.pbp WHERE {_qid('season')} = {season_i};"
            )
        else:
            con.execute("CREATE OR REPLACE TEMP VIEW tmp_incoming_pbp AS SELECT * FROM stg.pbp;")

        incoming_rows = int(con.execute("SELECT COUNT(*) FROM tmp_incoming_pbp;").fetchone()[0])

        if not _table_exists(con, "core", "pbp"):
            pbp_changes = {
                "incoming_rows": incoming_rows,
                "existing_rows": 0,
                "inserted": incoming_rows,
                "updated": 0,
                "deleted": 0,
            }
            prev_season_rows = None
            con.execute("CREATE TABLE core.pbp AS SELECT * FROM tmp_incoming_pbp;")
        else:
            if "season" not in cols:
                raise ValueError("core.pbp exists but incoming pbp has no 'season' column")

            con.execute(
                "CREATE OR REPLACE TEMP VIEW tmp_existing_pbp_season AS "
                f"SELECT * FROM core.pbp WHERE {_qid('season')} = {season_i};"
            )
            prev_season_rows = int(
                con.execute("SELECT COUNT(*) FROM tmp_existing_pbp_season;").fetchone()[0]
            )

            pbp_changes = compute_change_counts(
                con,
                existing_fqn="tmp_existing_pbp_season",
                incoming_fqn="tmp_incoming_pbp",
                key_cols=["game_id", "play_id"],
                hash_cols=cols,
            )

            con.execute(f"DELETE FROM core.pbp WHERE season = {season_i};")
            con.execute("INSERT INTO core.pbp SELECT * FROM tmp_incoming_pbp;")

        if _table_exists(con, "core", "pbp") and "season" in cols:
            season_rows = int(
                con.execute(
                    "SELECT COUNT(*) FROM core.pbp WHERE season = ?",
                    [season_i],
                ).fetchone()[0]
            )
        else:
            season_rows = incoming_rows

        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="core.pbp",
            row_count=season_rows,
            previous_row_count=prev_season_rows,
            note=(
                f"season={season_i}; "
                f"ins={pbp_changes['inserted']} "
                f"upd={pbp_changes['updated']} "
                f"del={pbp_changes['deleted']}"
            ),
        )

        scoring_sql = _build_scoring_event_sql("tmp_incoming_pbp", cols, season_i)
        con.execute(
            "CREATE OR REPLACE TEMP VIEW tmp_incoming_scoring_event AS " + scoring_sql
        )

        scoring_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_incoming_scoring_event;").fetchone()[0]
        )

        if not _table_exists(con, "core", "scoring_event"):
            scoring_changes = {
                "incoming_rows": scoring_rows,
                "existing_rows": 0,
                "inserted": scoring_rows,
                "updated": 0,
                "deleted": 0,
            }
            prev_scoring_rows = None
            con.execute(
                "CREATE TABLE core.scoring_event AS "
                "SELECT * FROM tmp_incoming_scoring_event;"
            )
        else:
            prev_scoring_rows = int(
                con.execute(
                    "SELECT COUNT(*) FROM core.scoring_event WHERE season = ?",
                    [season_i],
                ).fetchone()[0]
            )
            con.execute(
                "CREATE OR REPLACE TEMP VIEW tmp_existing_scoring_season AS "
                f"SELECT * FROM core.scoring_event WHERE season = {season_i};"
            )

            scoring_cols = _get_cols(con, "core", "scoring_event")
            scoring_changes = compute_change_counts(
                con,
                existing_fqn="tmp_existing_scoring_season",
                incoming_fqn="tmp_incoming_scoring_event",
                key_cols=["game_id", "play_id", "scoring_type"],
                hash_cols=scoring_cols,
            )

            con.execute(f"DELETE FROM core.scoring_event WHERE season = {season_i};")
            con.execute("INSERT INTO core.scoring_event SELECT * FROM tmp_incoming_scoring_event;")

        record_table_stat(
            con,
            run_id=run_id,
            table_fqn="core.scoring_event",
            row_count=scoring_rows,
            previous_row_count=prev_scoring_rows,
            note=(
                f"season={season_i}; "
                f"ins={scoring_changes['inserted']} "
                f"upd={scoring_changes['updated']} "
                f"del={scoring_changes['deleted']}"
            ),
        )

        finish_run(
            con,
            run_id=run_id,
            outcome=OUTCOME_OK,
            counts={
                "season": season_i,
                "raw_bytes": dl.size_bytes,
                "raw_sha256": dl.sha256,
                "stg_rows": stg_rows,
                "incoming_pbp_rows": incoming_rows,
                "pbp_inserted": pbp_changes["inserted"],
                "pbp_updated": pbp_changes["updated"],
                "pbp_deleted": pbp_changes["deleted"],
                "scoring_rows": scoring_rows,
                "scoring_inserted": scoring_changes["inserted"],
                "scoring_updated": scoring_changes["updated"],
                "scoring_deleted": scoring_changes["deleted"],
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