from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


GAME_TABLE_CANDIDATES = (("core", "game"), ("core", "games"))
TEAM_TABLE_CANDIDATES = (("core", "team"), ("core", "teams"))
PLAYER_DIMENSION_CANDIDATES = (("core", "players"), ("core", "player"))
PLAYER_WEEK_STATS_CANDIDATES = (("core", "player_week_stats"),)
TEAM_WEEK_STATS_CANDIDATES = (("core", "team_week_stats"),)
ROSTERS_CANDIDATES = (("core", "rosters"), ("core", "rosters_weekly"), ("core", "roster"))
PBP_TABLE_CANDIDATES = (("core", "pbp"), ("core", "play_by_play"))

LOGICAL_DATASETS = [
    ("Games", GAME_TABLE_CANDIDATES),
    ("Play-by-play", PBP_TABLE_CANDIDATES),
    ("Teams", TEAM_TABLE_CANDIDATES),
    ("Player week stats", PLAYER_WEEK_STATS_CANDIDATES),
    ("Team week stats", TEAM_WEEK_STATS_CANDIDATES),
    ("Rosters", ROSTERS_CANDIDATES),
]

GAME_FIELD_ALIASES = {
    "game_id": ["game_id"],
    "season": ["season_year", "season"],
    "season_type": ["season_type"],
    "week": ["week"],
    "home_team": ["home_team", "home_team_abbr"],
    "away_team": ["away_team", "away_team_abbr"],
    "home_score": ["home_score"],
    "away_score": ["away_score"],
    "status": ["status", "game_status"],
    "gameday": ["gameday", "game_date", "event_date"],
    "kickoff": ["kickoff", "kickoff_time", "kickoff_utc", "gametime", "start_time"],
    "venue": ["stadium", "venue", "stadium_name", "location"],
}

TEAM_FIELD_ALIASES = {
    "team_code": ["team_abbr", "team", "team_code", "abbr"],
    "team_name": ["team_name", "name", "team_full_name"],
    "conference": ["conference", "conf"],
    "division": ["division"],
}

PLAYER_DIMENSION_ALIASES = {
    "player_id": ["player_id"],
    "player_name": ["player_name", "full_name", "display_name", "name"],
    "birth_date": ["birth_date", "dob"],
    "college": ["college_name", "college"],
    "status": ["status", "injury_status", "game_status"],
    "height": ["height", "height_inches", "height_cm"],
    "weight": ["weight", "weight_lb", "weight_kg"],
    "rookie_year": ["rookie_year", "entry_year"],
    "draft_team": ["draft_team", "draft_team_abbr", "draft_club"],
    "draft_round": ["draft_round"],
    "draft_pick": ["draft_pick", "draft_number", "draft_overall"],
}

PLAYER_WEEK_FIELD_ALIASES = {
    "player_id": ["player_id"],
    "player_name": ["player_name", "full_name", "display_name", "name"],
    "team_code": ["recent_team", "team", "team_abbr"],
    "position": ["position"],
    "season": ["season"],
    "week": ["week"],
    "passing_yards": ["passing_yards"],
    "rushing_yards": ["rushing_yards"],
    "receiving_yards": ["receiving_yards"],
    "fantasy_points_ppr": ["fantasy_points_ppr"],
}

TEAM_WEEK_FIELD_ALIASES = {
    "team_code": ["team", "team_abbr", "team_code"],
    "season": ["season"],
    "week": ["week"],
    "points_for": ["points_for"],
    "points_against": ["points_against"],
}

ROSTER_FIELD_ALIASES = {
    "player_id": ["player_id"],
    "player_name": ["player_name", "full_name", "display_name", "name"],
    "team_code": ["team", "recent_team", "team_abbr"],
    "position": ["position"],
    "jersey_number": ["jersey_number", "jersey_no"],
}

PBP_FIELD_ALIASES = {
    "game_id": ["game_id"],
    "season": ["season_year", "season"],
    "week": ["week"],
    "play_id": ["play_id", "play_index"],
    "quarter": ["qtr", "quarter"],
    "posteam": ["posteam", "offense_team"],
    "down": ["down"],
    "ydstogo": ["ydstogo"],
    "yardline_100": ["yardline_100"],
    "description": ["desc", "play_description", "play_desc", "description"],
    "epa": ["epa"],
    "home_team": ["home_team", "home_team_abbr"],
    "away_team": ["away_team", "away_team_abbr"],
}

PROFILE_FIELD_LABELS = {
    "birth_date": "Geburtsdatum",
    "college": "College",
    "status": "Status",
    "height": "Größe",
    "weight": "Gewicht",
    "rookie_year": "Rookie Year",
    "draft_team": "Draft Team",
    "draft_round": "Draft Round",
    "draft_pick": "Draft Pick",
}

POSITION_FOCUS = {
    "QB": ["Passing Yards", "Fantasy PPR", "Rushing Yards"],
    "RB": ["Rushing Yards", "Receiving Yards", "Fantasy PPR"],
    "WR": ["Receiving Yards", "Fantasy PPR", "Rushing Yards"],
    "TE": ["Receiving Yards", "Fantasy PPR"],
    "K": ["Fantasy PPR"],
}

STAT_LABELS = {
    "passing_yards_total": "Passing Yards",
    "rushing_yards_total": "Rushing Yards",
    "receiving_yards_total": "Receiving Yards",
    "fantasy_points_ppr_total": "Fantasy PPR",
    "fantasy_points_ppr_avg": "Fantasy PPR / Week",
}


def qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def qfqn(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def _is_populated(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _stringify(value: Any) -> str:
    return "" if value is None else str(value)


def _lower(value: Any) -> str:
    return _stringify(value).strip().lower()


def table_exists(con: Any, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def table_columns(con: Any, schema: str, table: str) -> list[str]:
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


def first_existing_table(con: Any, candidates: Iterable[tuple[str, str]]) -> tuple[str, str] | None:
    for schema, table in candidates:
        if table_exists(con, schema, table):
            return (schema, table)
    return None


def first_matching_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def resolve_alias_map(columns: list[str], alias_map: dict[str, list[str]]) -> dict[str, str | None]:
    return {logical: first_matching_column(columns, candidates) for logical, candidates in alias_map.items()}


def fetch_dicts(con: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cur = con.execute(sql, params or [])
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return [dict(zip(colnames, row)) for row in rows]


def safe_row_count(con: Any, schema: str, table: str) -> int | None:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {qfqn(schema, table)};").fetchone()
    except Exception:
        return None
    return None if row is None else int(row[0])


def _distinct_values(con: Any, schema: str, table: str, column: str) -> list[Any]:
    rows = con.execute(
        f"SELECT DISTINCT {qident(column)} FROM {qfqn(schema, table)} WHERE {qident(column)} IS NOT NULL ORDER BY 1"
    ).fetchall()
    return [row[0] for row in rows if row and row[0] is not None]


def _metric_value(row: dict[str, Any], metric_alias: str) -> Any:
    value = row.get(metric_alias)
    if value is None:
        return None
    try:
        if float(value).is_integer():
            return int(value)
    except Exception:
        return value
    return round(float(value), 2)


class DatasetMeta(dict):
    @property
    def table(self) -> tuple[str, str] | None:
        return self.get("table")

    @property
    def columns(self) -> list[str]:
        return self.get("columns", [])

    @property
    def fields(self) -> dict[str, str | None]:
        return self.get("fields", {})



def _dataset_meta(con: Any, candidates: Iterable[tuple[str, str]], field_aliases: dict[str, list[str]]) -> DatasetMeta:
    resolved = first_existing_table(con, candidates)
    if not resolved:
        return DatasetMeta(table=None, columns=[], fields={})
    schema, table = resolved
    columns = table_columns(con, schema, table)
    fields = resolve_alias_map(columns, field_aliases)
    return DatasetMeta(table=(schema, table), columns=columns, fields=fields)



def list_tables(con: Any, schemas: tuple[str, ...] = ("audit", "stg", "core")) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for schema in schemas:
        rows = con.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_name
            """,
            [schema],
        ).fetchall()
        for sch, tbl in rows:
            cols = table_columns(con, sch, tbl)
            tables.append(
                {
                    "schema": sch,
                    "table": tbl,
                    "fqn": f"{sch}.{tbl}",
                    "rows": safe_row_count(con, sch, tbl),
                    "columns": len(cols),
                }
            )
    return tables



def summarize_tables(tables: list[dict[str, Any]]) -> dict[str, Any]:
    by_schema = Counter(t["schema"] for t in tables)
    return {
        "table_count": len(tables),
        "schema_counts": dict(sorted(by_schema.items())),
    }



def latest_runs(con: Any, limit: int = 25) -> list[dict[str, Any]]:
    if not table_exists(con, "audit", "ingest_run"):
        return []

    cols = table_columns(con, "audit", "ingest_run")
    if not cols:
        return []

    timestamp_col = first_matching_column(cols, ["started_at", "captured_at", "created_at", "updated_at"])
    component_col = first_matching_column(cols, ["component"])
    source_col = first_matching_column(cols, ["source"])
    outcome_col = first_matching_column(cols, ["outcome"])
    duration_col = first_matching_column(cols, ["duration_ms", "duration_millis"])
    counts_col = first_matching_column(cols, ["counts_json", "counts"])
    error_class_col = first_matching_column(cols, ["error_class"])
    error_message_col = first_matching_column(cols, ["error_message", "error"])

    select_bits = [
        f"{qident(timestamp_col)} AS started_at" if timestamp_col else "CAST(NULL AS TIMESTAMP) AS started_at",
        f"{qident(component_col)} AS component" if component_col else "CAST(NULL AS VARCHAR) AS component",
        f"{qident(source_col)} AS source" if source_col else "CAST(NULL AS VARCHAR) AS source",
        f"{qident(outcome_col)} AS outcome" if outcome_col else "CAST(NULL AS VARCHAR) AS outcome",
        f"{qident(duration_col)} AS duration_ms" if duration_col else "CAST(NULL AS BIGINT) AS duration_ms",
        f"{qident(counts_col)} AS counts_json" if counts_col else "CAST(NULL AS VARCHAR) AS counts_json",
        f"{qident(error_class_col)} AS error_class" if error_class_col else "CAST(NULL AS VARCHAR) AS error_class",
        f"{qident(error_message_col)} AS error_message" if error_message_col else "CAST(NULL AS VARCHAR) AS error_message",
    ]

    order_bits = []
    if timestamp_col:
        order_bits.append(f"{qident(timestamp_col)} DESC")
    if component_col:
        order_bits.append(f"{qident(component_col)}")
    if not order_bits:
        order_bits.append("1")

    return fetch_dicts(
        con,
        f"""
        SELECT {', '.join(select_bits)}
        FROM {qfqn('audit', 'ingest_run')}
        ORDER BY {', '.join(order_bits)}
        LIMIT ?
        """,
        [int(limit)],
    )



def latest_table_stats(con: Any) -> list[dict[str, Any]]:
    if not table_exists(con, "audit", "ingest_table_stat"):
        return []

    cols = table_columns(con, "audit", "ingest_table_stat")
    if not cols:
        return []

    colset = set(cols)
    timestamp_col = first_matching_column(cols, ["started_at", "captured_at", "created_at", "updated_at"])
    component_col = first_matching_column(cols, ["component"])
    source_col = first_matching_column(cols, ["source"])
    row_count_col = first_matching_column(cols, ["row_count", "rows"])
    delta_row_count_col = first_matching_column(cols, ["delta_row_count"])
    note_col = first_matching_column(cols, ["note"])
    stat_id_col = first_matching_column(cols, ["stat_id", "id"])

    if {"table_schema", "table_name"}.issubset(colset):
        schema_expr = qident("table_schema")
        table_expr = qident("table_name")
        fqn_expr = f"{schema_expr} || '.' || {table_expr}"
    elif "table_fqn" in colset:
        fqn_expr = qident("table_fqn")
        schema_expr = f"split_part({fqn_expr}, '.', 1)"
        table_expr = f"split_part({fqn_expr}, '.', 2)"
    else:
        return []

    order_expr = qident(timestamp_col) if timestamp_col else (qident(stat_id_col) if stat_id_col else fqn_expr)
    started_at_expr = qident(timestamp_col) if timestamp_col else "CAST(NULL AS TIMESTAMP)"
    component_expr = qident(component_col) if component_col else "CAST(NULL AS VARCHAR)"
    source_expr = qident(source_col) if source_col else "CAST(NULL AS VARCHAR)"
    row_count_expr = qident(row_count_col) if row_count_col else "CAST(NULL AS BIGINT)"
    delta_expr = qident(delta_row_count_col) if delta_row_count_col else "CAST(NULL AS BIGINT)"
    note_expr = qident(note_col) if note_col else "CAST(NULL AS VARCHAR)"

    return fetch_dicts(
        con,
        f"""
        WITH ranked AS (
            SELECT
                {started_at_expr} AS started_at,
                {component_expr} AS component,
                {source_expr} AS source,
                {schema_expr} AS table_schema,
                {table_expr} AS table_name,
                {fqn_expr} AS fqn,
                {row_count_expr} AS row_count,
                {delta_expr} AS delta_row_count,
                {note_expr} AS note,
                ROW_NUMBER() OVER (
                    PARTITION BY {fqn_expr}
                    ORDER BY {order_expr} DESC
                ) AS rn
            FROM {qfqn('audit', 'ingest_table_stat')}
        )
        SELECT
            started_at,
            component,
            source,
            table_schema,
            table_name,
            fqn,
            row_count,
            delta_row_count,
            note
        FROM ranked
        WHERE rn = 1
        ORDER BY fqn
        """,
    )



def coverage_overview(con: Any) -> dict[str, Any]:
    latest_stats_map = {row["fqn"]: row for row in latest_table_stats(con)}
    datasets: list[dict[str, Any]] = []

    for label, candidates in LOGICAL_DATASETS:
        resolved = first_existing_table(con, candidates)
        present = resolved is not None
        schema, table = resolved if resolved else (None, None)
        fqn = f"{schema}.{table}" if schema and table else None
        current_rows = safe_row_count(con, schema, table) if schema and table else None
        audited = latest_stats_map.get(fqn) if fqn else None
        audited_rows = audited["row_count"] if audited else None
        datasets.append(
            {
                "label": label,
                "present": present,
                "schema": schema,
                "table": table,
                "fqn": fqn,
                "current_rows": current_rows,
                "audited_rows": audited_rows,
                "last_seen": audited["started_at"] if audited else None,
            }
        )

    present_count = sum(1 for row in datasets if row["present"])
    total_count = len(datasets)
    pct = round((present_count / total_count) * 100, 1) if total_count else 0.0
    missing = [row for row in datasets if not row["present"]]

    return {
        "datasets": datasets,
        "present_count": present_count,
        "total_count": total_count,
        "coverage_pct": pct,
        "missing": missing,
    }



def _team_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, TEAM_TABLE_CANDIDATES, TEAM_FIELD_ALIASES)



def _player_dimension_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, PLAYER_DIMENSION_CANDIDATES, PLAYER_DIMENSION_ALIASES)



def _player_week_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, PLAYER_WEEK_STATS_CANDIDATES, PLAYER_WEEK_FIELD_ALIASES)



def _team_week_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, TEAM_WEEK_STATS_CANDIDATES, TEAM_WEEK_FIELD_ALIASES)



def _roster_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, ROSTERS_CANDIDATES, ROSTER_FIELD_ALIASES)



def _pbp_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, PBP_TABLE_CANDIDATES, PBP_FIELD_ALIASES)



def _game_meta(con: Any) -> DatasetMeta:
    return _dataset_meta(con, GAME_TABLE_CANDIDATES, GAME_FIELD_ALIASES)



def _team_name_map(con: Any) -> dict[str, str]:
    meta = _team_meta(con)
    if not meta.table:
        return {}
    schema, table = meta.table
    code_col = meta.fields.get("team_code")
    name_col = meta.fields.get("team_name")
    if not code_col:
        return {}
    select_parts = [qident(code_col)]
    if name_col:
        select_parts.append(qident(name_col))
    else:
        select_parts.append(f"{qident(code_col)} AS team_name")
    rows = con.execute(
        f"SELECT {', '.join(select_parts)} FROM {qfqn(schema, table)} ORDER BY 1"
    ).fetchall()
    return {str(code): str(name) for code, name in rows if code}



def _enrich_game_row(row: dict[str, Any], team_names: dict[str, str]) -> dict[str, Any]:
    home_team = _stringify(row.get("home_team"))
    away_team = _stringify(row.get("away_team"))
    row["home_team_name"] = team_names.get(home_team, home_team)
    row["away_team_name"] = team_names.get(away_team, away_team)
    row["matchup"] = f"{away_team} @ {home_team}".strip()

    away_score = row.get("away_score")
    home_score = row.get("home_score")
    if away_score is not None or home_score is not None:
        row["score_display"] = f"{'' if away_score is None else away_score} - {'' if home_score is None else home_score}".strip()
    else:
        row["score_display"] = ""

    winner_team = None
    if away_score is not None and home_score is not None:
        if away_score > home_score:
            winner_team = away_team
        elif home_score > away_score:
            winner_team = home_team
    row["winner_team"] = winner_team
    row["winner_team_name"] = team_names.get(winner_team, winner_team) if winner_team else None
    row["margin"] = abs(int(home_score) - int(away_score)) if away_score is not None and home_score is not None else None
    return row



def game_filter_options(con: Any) -> dict[str, Any]:
    meta = _game_meta(con)
    pbp_meta = _pbp_meta(con)

    seasons: list[Any] = []
    weeks: list[Any] = []
    season_types: list[Any] = []

    source_meta = meta if meta.table else pbp_meta
    if source_meta.table:
        schema, table = source_meta.table
        season_col = source_meta.fields.get("season")
        week_col = source_meta.fields.get("week")
        season_type_col = meta.fields.get("season_type") if meta.table else None
        if season_col:
            seasons = list(reversed(_distinct_values(con, schema, table, season_col)))
        if week_col:
            weeks = _distinct_values(con, schema, table, week_col)
        if meta.table and season_type_col:
            m_schema, m_table = meta.table
            season_types = _distinct_values(con, m_schema, m_table, season_type_col)

    teams = list_teams(con)
    return {
        "seasons": seasons,
        "weeks": weeks,
        "season_types": season_types,
        "teams": teams,
    }



def list_games(
    con: Any,
    season: int | None = None,
    week: int | None = None,
    team: str | None = None,
    season_type: str | None = None,
    q: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    team_names = _team_name_map(con)
    rows: list[dict[str, Any]] = []
    game_meta = _game_meta(con)

    if game_meta.table:
        schema, table = game_meta.table
        fields = game_meta.fields
        clauses: list[str] = []
        params: list[Any] = []

        season_col = fields.get("season")
        week_col = fields.get("week")
        season_type_col = fields.get("season_type")
        home_team_col = fields.get("home_team")
        away_team_col = fields.get("away_team")
        if season is not None and season_col:
            clauses.append(f"{qident(season_col)} = ?")
            params.append(int(season))
        if week is not None and week_col:
            clauses.append(f"{qident(week_col)} = ?")
            params.append(int(week))
        if season_type and season_type_col:
            clauses.append(f"UPPER({qident(season_type_col)}) = ?")
            params.append(str(season_type).upper())
        if team and home_team_col and away_team_col:
            clauses.append(f"({qident(home_team_col)} = ? OR {qident(away_team_col)} = ?)")
            params.extend([str(team).upper(), str(team).upper()])

        select_bits = [
            f"{qident(fields['game_id'])} AS game_id" if fields.get("game_id") else "NULL AS game_id",
            f"{qident(season_col)} AS season" if season_col else "NULL AS season",
            f"{qident(season_type_col)} AS season_type" if season_type_col else "NULL AS season_type",
            f"{qident(week_col)} AS week" if week_col else "NULL AS week",
            f"{qident(home_team_col)} AS home_team" if home_team_col else "NULL AS home_team",
            f"{qident(away_team_col)} AS away_team" if away_team_col else "NULL AS away_team",
            f"{qident(fields['home_score'])} AS home_score" if fields.get("home_score") else "NULL AS home_score",
            f"{qident(fields['away_score'])} AS away_score" if fields.get("away_score") else "NULL AS away_score",
            f"{qident(fields['status'])} AS status" if fields.get("status") else "NULL AS status",
            f"{qident(fields['gameday'])} AS gameday" if fields.get("gameday") else "NULL AS gameday",
            f"{qident(fields['kickoff'])} AS kickoff" if fields.get("kickoff") else "NULL AS kickoff",
            f"{qident(fields['venue'])} AS venue" if fields.get("venue") else "NULL AS venue",
        ]

        order_bits = []
        if season_col:
            order_bits.append(f"{qident(season_col)} DESC")
        if week_col:
            order_bits.append(f"{qident(week_col)} DESC")
        if fields.get("game_id"):
            order_bits.append(f"{qident(fields['game_id'])}")

        sql = [f"SELECT {', '.join(select_bits)} FROM {qfqn(schema, table)}"]
        if clauses:
            sql.append("WHERE " + " AND ".join(clauses))
        if order_bits:
            sql.append("ORDER BY " + ", ".join(order_bits))

        rows = fetch_dicts(con, "\n".join(sql), params)
    else:
        pbp_meta = _pbp_meta(con)
        if pbp_meta.table:
            schema, table = pbp_meta.table
            fields = pbp_meta.fields
            season_col = fields.get("season")
            week_col = fields.get("week")
            game_id_col = fields.get("game_id")
            home_team_col = fields.get("home_team")
            away_team_col = fields.get("away_team")
            if season_col and week_col and game_id_col:
                clauses: list[str] = []
                params = []
                if season is not None:
                    clauses.append(f"{qident(season_col)} = ?")
                    params.append(int(season))
                if week is not None:
                    clauses.append(f"{qident(week_col)} = ?")
                    params.append(int(week))
                if team and home_team_col and away_team_col:
                    clauses.append(f"({qident(home_team_col)} = ? OR {qident(away_team_col)} = ?)")
                    params.extend([str(team).upper(), str(team).upper()])
                sql = [
                    f"SELECT {qident(game_id_col)} AS game_id, {qident(season_col)} AS season, NULL AS season_type, {qident(week_col)} AS week,"
                    f" MIN({qident(home_team_col)}) AS home_team, MIN({qident(away_team_col)}) AS away_team,"
                    " NULL AS home_score, NULL AS away_score, NULL AS status, NULL AS gameday, NULL AS kickoff, NULL AS venue"
                    f" FROM {qfqn(schema, table)}"
                ]
                if clauses:
                    sql.append("WHERE " + " AND ".join(clauses))
                sql.append("GROUP BY 1, 2, 4 ORDER BY 2 DESC, 4 DESC, 1")
                rows = fetch_dicts(con, "\n".join(sql), params)

    enriched = [_enrich_game_row(row, team_names) for row in rows]
    query_text = _lower(q)
    if query_text:
        enriched = [
            row
            for row in enriched
            if query_text in " ".join(
                [
                    _stringify(row.get("game_id")),
                    _stringify(row.get("season")),
                    _stringify(row.get("week")),
                    _stringify(row.get("home_team")),
                    _stringify(row.get("away_team")),
                    _stringify(row.get("home_team_name")),
                    _stringify(row.get("away_team_name")),
                    _stringify(row.get("venue")),
                    _stringify(row.get("status")),
                ]
            ).lower()
        ]
    return enriched[: int(limit)]



def game_explorer_payload(
    con: Any,
    season: int | None = None,
    week: int | None = None,
    team: str | None = None,
    season_type: str | None = None,
    q: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    rows = list_games(
        con,
        season=season,
        week=week,
        team=team,
        season_type=season_type,
        q=q,
        limit=limit,
    )
    seasons_visible = len({row.get("season") for row in rows if row.get("season") is not None})
    teams_visible = len(
        {
            code
            for row in rows
            for code in [row.get("home_team"), row.get("away_team")]
            if _is_populated(code)
        }
    )
    filters = game_filter_options(con)
    return {
        "summary": {
            "games_visible": len(rows),
            "seasons_visible": seasons_visible,
            "teams_visible": teams_visible,
            "has_scores": sum(1 for row in rows if row.get("home_score") is not None or row.get("away_score") is not None),
        },
        "games": rows,
        "filters": filters,
    }



def season_week_overview(con: Any) -> list[dict[str, Any]]:
    game_meta = _game_meta(con)
    if game_meta.table:
        schema, table = game_meta.table
        season_col = game_meta.fields.get("season")
        season_type_col = game_meta.fields.get("season_type")
        week_col = game_meta.fields.get("week")
        if season_col and week_col:
            season_expr = qident(season_col)
            week_expr = qident(week_col)
            season_type_expr = qident(season_type_col) if season_type_col else "NULL"
            return fetch_dicts(
                con,
                f"""
                SELECT
                    {season_expr} AS season,
                    {season_type_expr} AS season_type,
                    {week_expr} AS week,
                    COUNT(*) AS games
                FROM {qfqn(schema, table)}
                GROUP BY 1, 2, 3
                ORDER BY 1 DESC, 2 NULLS LAST, 3
                """,
            )

    pbp_meta = _pbp_meta(con)
    if pbp_meta.table:
        schema, table = pbp_meta.table
        season_col = pbp_meta.fields.get("season")
        week_col = pbp_meta.fields.get("week")
        game_id_col = pbp_meta.fields.get("game_id")
        if season_col and week_col:
            games_expr = f"COUNT(DISTINCT {qident(game_id_col)})" if game_id_col else "COUNT(*)"
            return fetch_dicts(
                con,
                f"""
                SELECT
                    {qident(season_col)} AS season,
                    NULL AS season_type,
                    {qident(week_col)} AS week,
                    {games_expr} AS games
                FROM {qfqn(schema, table)}
                GROUP BY 1, 2, 3
                ORDER BY 1 DESC, 3
                """,
            )
    return []



def season_detail_payload(con: Any, season: int) -> dict[str, Any]:
    rows = [row for row in season_week_overview(con) if int(row["season"]) == int(season)]
    games_visible = sum(int(row.get("games") or 0) for row in rows)
    weeks_visible = len(rows)
    season_types = sorted({row.get("season_type") for row in rows if row.get("season_type")})
    total_by_type = Counter(_stringify(row.get("season_type") or "UNK") for row in rows)
    return {
        "season": int(season),
        "summary": {
            "games_visible": games_visible,
            "weeks_visible": weeks_visible,
            "season_types": season_types,
        },
        "weeks": rows,
        "season_type_breakdown": [
            {"season_type": season_type, "weeks": count}
            for season_type, count in sorted(total_by_type.items())
        ],
    }



def week_detail_payload(con: Any, season: int, week: int) -> dict[str, Any]:
    rows = list_games(con, season=season, week=week, limit=500)
    return {
        "season": int(season),
        "week": int(week),
        "summary": {
            "games_visible": len(rows),
            "teams_visible": len(
                {team for row in rows for team in [row.get("home_team"), row.get("away_team")] if team}
            ),
        },
        "games": rows,
    }



def list_teams(con: Any, search: str | None = None) -> list[dict[str, Any]]:
    name_map = _team_name_map(con)
    team_codes = set(name_map)

    game_rows = list_games(con, limit=10000)
    games_visible: Counter[str] = Counter()
    weeks_visible: dict[str, set[tuple[int, int]]] = {}
    for row in game_rows:
        season = row.get("season")
        week = row.get("week")
        for code in [row.get("home_team"), row.get("away_team")]:
            if _is_populated(code):
                team_code = str(code)
                team_codes.add(team_code)
                games_visible[team_code] += 1
                if season is not None and week is not None:
                    weeks_visible.setdefault(team_code, set()).add((int(season), int(week)))

    player_counts: Counter[str] = Counter()
    roster_meta = _roster_meta(con)
    if roster_meta.table:
        schema, table = roster_meta.table
        team_col = roster_meta.fields.get("team_code")
        player_id_col = roster_meta.fields.get("player_id")
        if team_col and player_id_col:
            rows = con.execute(
                f"SELECT {qident(team_col)}, COUNT(DISTINCT {qident(player_id_col)}) FROM {qfqn(schema, table)} GROUP BY 1"
            ).fetchall()
            for team_code, count in rows:
                if team_code:
                    code = str(team_code)
                    team_codes.add(code)
                    player_counts[code] = int(count)

    if not player_counts:
        player_meta = _player_week_meta(con)
        if player_meta.table:
            schema, table = player_meta.table
            team_col = player_meta.fields.get("team_code")
            player_id_col = player_meta.fields.get("player_id")
            if team_col and player_id_col:
                rows = con.execute(
                    f"SELECT {qident(team_col)}, COUNT(DISTINCT {qident(player_id_col)}) FROM {qfqn(schema, table)} GROUP BY 1"
                ).fetchall()
                for team_code, count in rows:
                    if team_code:
                        code = str(team_code)
                        team_codes.add(code)
                        player_counts[code] = int(count)

    q = _lower(search)
    result: list[dict[str, Any]] = []
    for code in sorted(team_codes):
        row = {
            "team_code": code,
            "team_name": name_map.get(code, code),
            "games_visible": int(games_visible.get(code, 0)),
            "weeks_visible": len(weeks_visible.get(code, set())),
            "players_visible": int(player_counts.get(code, 0)),
        }
        haystack = f"{code} {row['team_name']}".lower()
        if not q or q in haystack:
            result.append(row)
    return result



def team_explorer_payload(con: Any, search: str | None = None) -> dict[str, Any]:
    teams = list_teams(con, search=search)
    return {
        "summary": {
            "teams_visible": len(teams),
            "games_visible": sum(int(row.get("games_visible") or 0) for row in teams),
            "players_visible": sum(int(row.get("players_visible") or 0) for row in teams),
            "weeks_visible": sum(int(row.get("weeks_visible") or 0) for row in teams),
        },
        "teams": teams,
    }


def _team_season_summary(con: Any, team_code: str) -> list[dict[str, Any]]:
    game_meta = _game_meta(con)
    if not game_meta.table:
        return []
    season_col = game_meta.fields.get("season")
    home_team_col = game_meta.fields.get("home_team")
    away_team_col = game_meta.fields.get("away_team")
    home_score_col = game_meta.fields.get("home_score")
    away_score_col = game_meta.fields.get("away_score")
    if not season_col or not home_team_col or not away_team_col:
        return []
    schema, table = game_meta.table

    wins_expr = "NULL"
    losses_expr = "NULL"
    points_for_expr = "NULL"
    points_against_expr = "NULL"
    if home_score_col and away_score_col:
        wins_expr = (
            f"SUM(CASE WHEN ({qident(home_team_col)} = ? AND {qident(home_score_col)} > {qident(away_score_col)}) "
            f"OR ({qident(away_team_col)} = ? AND {qident(away_score_col)} > {qident(home_score_col)}) THEN 1 ELSE 0 END)"
        )
        losses_expr = (
            f"SUM(CASE WHEN ({qident(home_team_col)} = ? AND {qident(home_score_col)} < {qident(away_score_col)}) "
            f"OR ({qident(away_team_col)} = ? AND {qident(away_score_col)} < {qident(home_score_col)}) THEN 1 ELSE 0 END)"
        )
        points_for_expr = (
            f"SUM(CASE WHEN {qident(home_team_col)} = ? THEN {qident(home_score_col)} "
            f"WHEN {qident(away_team_col)} = ? THEN {qident(away_score_col)} ELSE 0 END)"
        )
        points_against_expr = (
            f"SUM(CASE WHEN {qident(home_team_col)} = ? THEN {qident(away_score_col)} "
            f"WHEN {qident(away_team_col)} = ? THEN {qident(home_score_col)} ELSE 0 END)"
        )
        params = [team_code, team_code, team_code, team_code, team_code, team_code, team_code, team_code, team_code, team_code]
    else:
        params = [team_code, team_code]

    rows = fetch_dicts(
        con,
        f"""
        SELECT
            {qident(season_col)} AS season,
            COUNT(*) AS games,
            {wins_expr} AS wins,
            {losses_expr} AS losses,
            {points_for_expr} AS points_for,
            {points_against_expr} AS points_against
        FROM {qfqn(schema, table)}
        WHERE {qident(home_team_col)} = ? OR {qident(away_team_col)} = ?
        GROUP BY 1
        ORDER BY 1 DESC
        """,
        params,
    )
    return rows



def team_detail_payload(con: Any, team_code: str) -> dict[str, Any]:
    team_code = str(team_code).upper()
    teams = list_teams(con)
    team = next((row for row in teams if row["team_code"] == team_code), None)
    if not team:
        return {
            "team": None,
            "summary": {},
            "recent_games": [],
            "season_rows": [],
            "weekly_columns": [],
            "weekly_rows": [],
            "roster_preview": [],
            "roster_groups": [],
            "player_leaders": [],
        }

    recent_games = list_games(con, team=team_code, limit=12)
    season_rows = _team_season_summary(con, team_code)

    weekly_columns: list[str] = []
    weekly_rows: list[tuple[Any, ...]] = []
    summary: dict[str, Any] = {
        "seasons_visible": len(season_rows),
        "weeks_visible": team.get("weeks_visible", 0),
        "points_for_total": None,
        "points_against_total": None,
        "wins_total": None,
        "losses_total": None,
    }
    team_week_meta = _team_week_meta(con)
    if team_week_meta.table:
        schema, table = team_week_meta.table
        team_col = team_week_meta.fields.get("team_code")
        season_col = team_week_meta.fields.get("season")
        week_col = team_week_meta.fields.get("week")
        if team_col and season_col and week_col:
            preferred = [season_col, week_col, team_week_meta.fields.get("points_for"), team_week_meta.fields.get("points_against")]
            weekly_columns = [col for col in preferred if col]
            select_sql = ", ".join(qident(col) for col in weekly_columns)
            weekly_rows = con.execute(
                f"SELECT {select_sql} FROM {qfqn(schema, table)} WHERE {qident(team_col)} = ? ORDER BY {qident(season_col)} DESC, {qident(week_col)} DESC",
                [team_code],
            ).fetchall()
            if team_week_meta.fields.get("points_for") and team_week_meta.fields.get("points_against"):
                summary_row = con.execute(
                    f"""
                    SELECT
                        COUNT(DISTINCT {qident(season_col)}) AS seasons_visible,
                        COUNT(*) AS weeks_visible,
                        SUM({qident(team_week_meta.fields['points_for'])}) AS points_for_total,
                        SUM({qident(team_week_meta.fields['points_against'])}) AS points_against_total
                    FROM {qfqn(schema, table)}
                    WHERE {qident(team_col)} = ?
                    """,
                    [team_code],
                ).fetchone()
                if summary_row:
                    summary.update(
                        {
                            "seasons_visible": int(summary_row[0] or 0),
                            "weeks_visible": int(summary_row[1] or 0),
                            "points_for_total": summary_row[2],
                            "points_against_total": summary_row[3],
                        }
                    )

    if season_rows:
        wins_total = sum(int(row.get("wins") or 0) for row in season_rows if row.get("wins") is not None)
        losses_total = sum(int(row.get("losses") or 0) for row in season_rows if row.get("losses") is not None)
        summary["wins_total"] = wins_total
        summary["losses_total"] = losses_total
        if summary["points_for_total"] is None:
            summary["points_for_total"] = sum(int(row.get("points_for") or 0) for row in season_rows if row.get("points_for") is not None)
        if summary["points_against_total"] is None:
            summary["points_against_total"] = sum(int(row.get("points_against") or 0) for row in season_rows if row.get("points_against") is not None)

    roster_preview: list[dict[str, Any]] = []
    roster_meta = _roster_meta(con)
    if roster_meta.table:
        schema, table = roster_meta.table
        team_col = roster_meta.fields.get("team_code")
        player_id_col = roster_meta.fields.get("player_id")
        player_name_col = roster_meta.fields.get("player_name")
        position_col = roster_meta.fields.get("position")
        if team_col and player_id_col:
            select_bits = [f"{qident(player_id_col)} AS player_id"]
            select_bits.append(
                f"{qident(player_name_col)} AS player_name" if player_name_col else f"{qident(player_id_col)} AS player_name"
            )
            select_bits.append(f"{qident(position_col)} AS position" if position_col else "NULL AS position")
            roster_preview = fetch_dicts(
                con,
                f"SELECT {', '.join(select_bits)} FROM {qfqn(schema, table)} WHERE {qident(team_col)} = ? ORDER BY 2, 1 LIMIT 30",
                [team_code],
            )

    roster_groups = [
        {"position": position or "n/a", "players": count}
        for position, count in sorted(Counter(_stringify(row.get("position") or "n/a") for row in roster_preview).items())
    ]

    player_leaders: list[dict[str, Any]] = []
    player_meta = _player_week_meta(con)
    if player_meta.table:
        schema, table = player_meta.table
        player_id_col = player_meta.fields.get("player_id")
        player_name_col = player_meta.fields.get("player_name")
        team_col = player_meta.fields.get("team_code")
        metrics = [
            ("Passing Yards", player_meta.fields.get("passing_yards")),
            ("Rushing Yards", player_meta.fields.get("rushing_yards")),
            ("Receiving Yards", player_meta.fields.get("receiving_yards")),
            ("Fantasy PPR", player_meta.fields.get("fantasy_points_ppr")),
        ]
        if player_id_col and player_name_col and team_col:
            for label, metric_col in metrics:
                if not metric_col:
                    continue
                leaders = fetch_dicts(
                    con,
                    f"""
                    SELECT
                        {qident(player_id_col)} AS player_id,
                        {qident(player_name_col)} AS player_name,
                        SUM(COALESCE({qident(metric_col)}, 0)) AS value
                    FROM {qfqn(schema, table)}
                    WHERE {qident(team_col)} = ?
                    GROUP BY 1, 2
                    ORDER BY value DESC, player_name
                    LIMIT 5
                    """,
                    [team_code],
                )
                if leaders:
                    player_leaders.append({"label": label, "leaders": leaders})

    return {
        "team": team,
        "summary": summary,
        "recent_games": recent_games,
        "season_rows": season_rows,
        "weekly_columns": weekly_columns,
        "weekly_rows": weekly_rows,
        "roster_preview": roster_preview,
        "roster_groups": roster_groups,
        "player_leaders": player_leaders,
    }



def player_filter_options(con: Any) -> dict[str, Any]:
    meta = _player_week_meta(con)
    if meta.table:
        schema, table = meta.table
        seasons = list(reversed(_distinct_values(con, schema, table, meta.fields["season"]))) if meta.fields.get("season") else []
        positions = _distinct_values(con, schema, table, meta.fields["position"]) if meta.fields.get("position") else []
        team_codes = _distinct_values(con, schema, table, meta.fields["team_code"]) if meta.fields.get("team_code") else []
    else:
        seasons = []
        positions = []
        team_codes = []
    team_lookup = {row["team_code"]: row["team_name"] for row in list_teams(con)}
    teams = [{"team_code": code, "team_name": team_lookup.get(str(code), str(code))} for code in team_codes]
    return {
        "seasons": seasons,
        "positions": positions,
        "teams": teams,
        "sort_options": [
            {"value": "player_name", "label": "Name"},
            {"value": "latest", "label": "Letzte Sichtung"},
            {"value": "passing_yards", "label": "Passing Yards"},
            {"value": "rushing_yards", "label": "Rushing Yards"},
            {"value": "receiving_yards", "label": "Receiving Yards"},
            {"value": "fantasy_points_ppr", "label": "Fantasy PPR"},
        ],
    }



def list_players(
    con: Any,
    search: str | None = None,
    limit: int = 200,
    team: str | None = None,
    position: str | None = None,
    season: int | None = None,
    sort: str = "player_name",
) -> list[dict[str, Any]]:
    q = _lower(search)
    player_meta = _player_week_meta(con)
    if player_meta.table:
        schema, table = player_meta.table
        player_id_col = player_meta.fields.get("player_id")
        player_name_col = player_meta.fields.get("player_name")
        team_col = player_meta.fields.get("team_code")
        position_col = player_meta.fields.get("position")
        season_col = player_meta.fields.get("season")
        week_col = player_meta.fields.get("week")
        if player_id_col and player_name_col:
            where_bits = []
            params: list[Any] = []
            if team and team_col:
                where_bits.append(f"{qident(team_col)} = ?")
                params.append(str(team).upper())
            if position and position_col:
                where_bits.append(f"UPPER({qident(position_col)}) = ?")
                params.append(str(position).upper())
            if season is not None and season_col:
                where_bits.append(f"{qident(season_col)} = ?")
                params.append(int(season))
            if q:
                search_exprs = [
                    f"LOWER(CAST({qident(player_id_col)} AS VARCHAR)) LIKE ?",
                    f"LOWER(CAST({qident(player_name_col)} AS VARCHAR)) LIKE ?",
                ]
                params.extend([f"%{q}%", f"%{q}%"])
                if team_col:
                    search_exprs.append(f"LOWER(CAST({qident(team_col)} AS VARCHAR)) LIKE ?")
                    params.append(f"%{q}%")
                where_bits.append("(" + " OR ".join(search_exprs) + ")")

            metrics = {
                "passing_yards_total": player_meta.fields.get("passing_yards"),
                "rushing_yards_total": player_meta.fields.get("rushing_yards"),
                "receiving_yards_total": player_meta.fields.get("receiving_yards"),
                "fantasy_points_ppr_total": player_meta.fields.get("fantasy_points_ppr"),
            }
            agg_metric_bits = []
            for alias, column in metrics.items():
                if column:
                    agg_metric_bits.append(f"SUM(COALESCE({qident(column)}, 0)) AS {qident(alias)}")
                else:
                    agg_metric_bits.append(f"CAST(NULL AS DOUBLE) AS {qident(alias)}")

            order_sql = {
                "player_name": "player_name, agg.player_id",
                "latest": "latest.latest_season DESC NULLS LAST, latest.latest_week DESC NULLS LAST, player_name, agg.player_id",
                "passing_yards": "agg.passing_yards_total DESC NULLS LAST, player_name, agg.player_id",
                "rushing_yards": "agg.rushing_yards_total DESC NULLS LAST, player_name, agg.player_id",
                "receiving_yards": "agg.receiving_yards_total DESC NULLS LAST, player_name, agg.player_id",
                "fantasy_points_ppr": "agg.fantasy_points_ppr_total DESC NULLS LAST, player_name, agg.player_id",
            }.get(sort, "player_name, agg.player_id")

            base_select = [
                f"{qident(player_id_col)} AS player_id",
                f"{qident(player_name_col)} AS player_name",
                f"{qident(team_col)} AS team_code" if team_col else "NULL AS team_code",
                f"{qident(position_col)} AS position" if position_col else "NULL AS position",
                f"{qident(season_col)} AS season" if season_col else "NULL AS season",
                f"{qident(week_col)} AS week" if week_col else "NULL AS week",
            ]
            for alias, column in metrics.items():
                if column:
                    base_select.append(f"{qident(column)} AS {qident(alias.replace('_total', ''))}")
                else:
                    base_select.append(f"CAST(NULL AS DOUBLE) AS {qident(alias.replace('_total', ''))}")

            sql = f"""
                WITH base AS (
                    SELECT {', '.join(base_select)}
                    FROM {qfqn(schema, table)}
                    {'WHERE ' + ' AND '.join(where_bits) if where_bits else ''}
                ),
                latest AS (
                    SELECT
                        player_id,
                        player_name,
                        team_code,
                        position,
                        season AS latest_season,
                        week AS latest_week,
                        ROW_NUMBER() OVER (
                            PARTITION BY player_id
                            ORDER BY season DESC NULLS LAST, week DESC NULLS LAST, team_code NULLS LAST
                        ) AS rn
                    FROM base
                ),
                agg AS (
                    SELECT
                        player_id,
                        MIN(player_name) AS player_name,
                        COUNT(DISTINCT season) AS seasons_visible,
                        MIN(season) AS first_season,
                        COUNT(*) AS weekly_rows,
                        {', '.join(agg_metric_bits)}
                    FROM base
                    GROUP BY 1
                )
                SELECT
                    agg.player_id,
                    COALESCE(latest.player_name, agg.player_name) AS player_name,
                    latest.team_code,
                    latest.position,
                    agg.seasons_visible,
                    agg.first_season,
                    latest.latest_season,
                    latest.latest_week,
                    agg.weekly_rows,
                    agg.passing_yards_total,
                    agg.rushing_yards_total,
                    agg.receiving_yards_total,
                    agg.fantasy_points_ppr_total,
                    CASE WHEN agg.weekly_rows > 0 THEN ROUND(agg.fantasy_points_ppr_total / agg.weekly_rows, 2) ELSE NULL END AS fantasy_points_ppr_avg
                FROM agg
                LEFT JOIN latest
                    ON latest.player_id = agg.player_id
                   AND latest.rn = 1
                ORDER BY {order_sql}
                LIMIT ?
            """
            rows = fetch_dicts(con, sql, params + [int(limit)])
            return rows

    roster_meta = _roster_meta(con)
    if not roster_meta.table:
        return []
    schema, table = roster_meta.table
    player_id_col = roster_meta.fields.get("player_id")
    player_name_col = roster_meta.fields.get("player_name")
    team_col = roster_meta.fields.get("team_code")
    position_col = roster_meta.fields.get("position")
    if not player_id_col:
        return []
    where_bits = []
    params = []
    if team and team_col:
        where_bits.append(f"{qident(team_col)} = ?")
        params.append(str(team).upper())
    if position and position_col:
        where_bits.append(f"UPPER({qident(position_col)}) = ?")
        params.append(str(position).upper())
    if q:
        search_bits = [f"LOWER(CAST({qident(player_id_col)} AS VARCHAR)) LIKE ?"]
        params.append(f"%{q}%")
        if player_name_col:
            search_bits.append(f"LOWER(CAST({qident(player_name_col)} AS VARCHAR)) LIKE ?")
            params.append(f"%{q}%")
        if team_col:
            search_bits.append(f"LOWER(CAST({qident(team_col)} AS VARCHAR)) LIKE ?")
            params.append(f"%{q}%")
        where_bits.append("(" + " OR ".join(search_bits) + ")")

    rows = fetch_dicts(
        con,
        f"""
        SELECT
            {qident(player_id_col)} AS player_id,
            {qident(player_name_col)} AS player_name,
            {qident(team_col)} AS team_code,
            {qident(position_col)} AS position,
            0 AS seasons_visible,
            NULL AS first_season,
            NULL AS latest_season,
            NULL AS latest_week,
            0 AS weekly_rows,
            NULL AS passing_yards_total,
            NULL AS rushing_yards_total,
            NULL AS receiving_yards_total,
            NULL AS fantasy_points_ppr_total,
            NULL AS fantasy_points_ppr_avg
        FROM {qfqn(schema, table)}
        {'WHERE ' + ' AND '.join(where_bits) if where_bits else ''}
        ORDER BY player_name, player_id
        LIMIT ?
        """,
        params + [int(limit)],
    )
    return rows



def player_explorer_payload(
    con: Any,
    search: str | None = None,
    limit: int = 200,
    team: str | None = None,
    position: str | None = None,
    season: int | None = None,
    sort: str = "player_name",
) -> dict[str, Any]:
    players = list_players(
        con,
        search=search,
        limit=limit,
        team=team,
        position=position,
        season=season,
        sort=sort,
    )
    filters = player_filter_options(con)
    teams_visible = len({row.get("team_code") for row in players if _is_populated(row.get("team_code"))})
    positions_visible = len({row.get("position") for row in players if _is_populated(row.get("position"))})
    seasons_visible = len(
        {
            row.get("latest_season")
            for row in players
            if row.get("latest_season") is not None
        }
    )
    return {
        "summary": {
            "players_visible": len(players),
            "teams_visible": teams_visible,
            "positions_visible": positions_visible,
            "seasons_visible": seasons_visible,
        },
        "players": players,
        "filters": filters,
    }


def _collect_player_profile(con: Any, player_id: str) -> list[dict[str, Any]]:
    profile_rows: list[dict[str, Any]] = []
    candidates = [
        _player_dimension_meta(con),
        _roster_meta(con),
        _player_week_meta(con),
    ]
    for meta in candidates:
        if not meta.table:
            continue
        schema, table = meta.table
        player_id_col = meta.fields.get("player_id")
        if not player_id_col:
            continue
        order_cols = []
        if "season" in meta.fields and meta.fields.get("season"):
            order_cols.append(f"{qident(meta.fields['season'])} DESC NULLS LAST")
        if "week" in meta.fields and meta.fields.get("week"):
            order_cols.append(f"{qident(meta.fields['week'])} DESC NULLS LAST")
        order_sql = ", ".join(order_cols) if order_cols else qident(player_id_col)
        row = fetch_dicts(
            con,
            f"SELECT * FROM {qfqn(schema, table)} WHERE {qident(player_id_col)} = ? ORDER BY {order_sql} LIMIT 1",
            [player_id],
        )
        if row:
            profile_rows.append(row[0])
    return profile_rows



def _first_profile_value(profile_rows: list[dict[str, Any]], candidates: list[str]) -> Any:
    for row in profile_rows:
        for candidate in candidates:
            if candidate in row and _is_populated(row[candidate]):
                return row[candidate]
    return None



def _player_team_history(con: Any, player_id: str) -> list[dict[str, Any]]:
    meta = _player_week_meta(con)
    if not meta.table:
        return []
    schema, table = meta.table
    player_id_col = meta.fields.get("player_id")
    season_col = meta.fields.get("season")
    team_col = meta.fields.get("team_code")
    if not player_id_col or not season_col or not team_col:
        return []
    rows = fetch_dicts(
        con,
        f"""
        SELECT
            {qident(team_col)} AS team_code,
            MIN({qident(season_col)}) AS first_season,
            MAX({qident(season_col)}) AS last_season,
            COUNT(*) AS weekly_rows
        FROM {qfqn(schema, table)}
        WHERE {qident(player_id_col)} = ?
        GROUP BY 1
        ORDER BY last_season DESC, team_code
        """,
        [player_id],
    )
    team_names = _team_name_map(con)
    for row in rows:
        row["team_name"] = team_names.get(_stringify(row.get("team_code")), row.get("team_code"))
    return rows



def player_detail_payload(con: Any, player_id: str) -> dict[str, Any]:
    player_id = str(player_id)
    player_rows = list_players(con, search=player_id, limit=10000)
    player = next((row for row in player_rows if row["player_id"] == player_id), None)
    if not player:
        return {
            "player": None,
            "profile_fields": [],
            "team_history_rows": [],
            "weekly_columns": [],
            "weekly_rows": [],
            "season_summary_columns": [],
            "season_summary_rows": [],
            "stat_totals": [],
        }

    weekly_columns: list[str] = []
    weekly_rows: list[tuple[Any, ...]] = []
    season_summary_columns: list[str] = []
    season_summary_rows: list[tuple[Any, ...]] = []
    stat_totals: list[dict[str, Any]] = []

    player_meta = _player_week_meta(con)
    if player_meta.table:
        schema, table = player_meta.table
        cols = player_meta.columns
        player_id_col = player_meta.fields.get("player_id")
        season_col = player_meta.fields.get("season")
        week_col = player_meta.fields.get("week")
        preferred_weekly = [
            season_col,
            week_col,
            player_meta.fields.get("team_code"),
            player_meta.fields.get("position"),
            player_meta.fields.get("passing_yards"),
            player_meta.fields.get("rushing_yards"),
            player_meta.fields.get("receiving_yards"),
            player_meta.fields.get("fantasy_points_ppr"),
        ]
        weekly_columns = [col for col in preferred_weekly if col]
        if player_id_col and weekly_columns:
            weekly_rows = con.execute(
                f"SELECT {', '.join(qident(col) for col in weekly_columns)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(player_id_col)} = ? ORDER BY {qident(season_col)} DESC NULLS LAST, {qident(week_col)} DESC NULLS LAST",
                [player_id],
            ).fetchall()

            agg_metrics = [
                player_meta.fields.get("passing_yards"),
                player_meta.fields.get("rushing_yards"),
                player_meta.fields.get("receiving_yards"),
                player_meta.fields.get("fantasy_points_ppr"),
            ]
            agg_metrics = [col for col in agg_metrics if col]
            season_summary_columns = [season_col, "games"] + agg_metrics
            season_select = [
                f"{qident(season_col)} AS {qident(season_col)}",
                "COUNT(*) AS games",
            ] + [f"SUM(COALESCE({qident(col)}, 0)) AS {qident(col)}" for col in agg_metrics]
            season_summary_rows = con.execute(
                f"SELECT {', '.join(season_select)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(player_id_col)} = ? GROUP BY 1 ORDER BY 1 DESC",
                [player_id],
            ).fetchall()

            totals_row = con.execute(
                f"SELECT {', '.join(f'SUM(COALESCE({qident(col)}, 0))' for col in agg_metrics)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(player_id_col)} = ?",
                [player_id],
            ).fetchone()
            if totals_row:
                label_lookup = {
                    player_meta.fields.get("passing_yards"): "Passing Yards",
                    player_meta.fields.get("rushing_yards"): "Rushing Yards",
                    player_meta.fields.get("receiving_yards"): "Receiving Yards",
                    player_meta.fields.get("fantasy_points_ppr"): "Fantasy PPR",
                }
                stat_totals = [
                    {"label": label_lookup[col], "value": totals_row[idx]}
                    for idx, col in enumerate(agg_metrics)
                    if col in label_lookup
                ]

    profile_rows = _collect_player_profile(con, player_id)
    profile_fields = []
    for field, candidates in PLAYER_DIMENSION_ALIASES.items():
        if field in {"player_id", "player_name"}:
            continue
        value = _first_profile_value(profile_rows, candidates)
        if _is_populated(value):
            profile_fields.append({"label": PROFILE_FIELD_LABELS.get(field, field), "value": value})

    team_history_rows = _player_team_history(con, player_id)

    preferred_labels = POSITION_FOCUS.get(_stringify(player.get("position")).upper(), ["Passing Yards", "Rushing Yards", "Receiving Yards", "Fantasy PPR"])
    ordered_totals = []
    by_label = {row["label"]: row for row in stat_totals}
    for label in preferred_labels:
        if label in by_label:
            ordered_totals.append(by_label[label])
    for row in stat_totals:
        if row["label"] not in {item["label"] for item in ordered_totals}:
            ordered_totals.append(row)

    return {
        "player": player,
        "profile_fields": profile_fields,
        "team_history_rows": team_history_rows,
        "weekly_columns": weekly_columns,
        "weekly_rows": weekly_rows,
        "season_summary_columns": season_summary_columns,
        "season_summary_rows": season_summary_rows,
        "stat_totals": ordered_totals,
    }



def _pbp_team_summary(con: Any, game_id: str) -> list[dict[str, Any]]:
    meta = _pbp_meta(con)
    if not meta.table:
        return []
    schema, table = meta.table
    game_id_col = meta.fields.get("game_id")
    posteam_col = meta.fields.get("posteam")
    if not game_id_col or not posteam_col:
        return []
    select_bits = [
        f"{qident(posteam_col)} AS team_code",
        "COUNT(*) AS plays",
    ]
    if meta.fields.get("epa"):
        select_bits.append(f"ROUND(AVG(COALESCE({qident(meta.fields['epa'])}, 0)), 3) AS avg_epa")
        select_bits.append(f"ROUND(SUM(COALESCE({qident(meta.fields['epa'])}, 0)), 3) AS total_epa")
    else:
        select_bits.append("CAST(NULL AS DOUBLE) AS avg_epa")
        select_bits.append("CAST(NULL AS DOUBLE) AS total_epa")
    rows = fetch_dicts(
        con,
        f"SELECT {', '.join(select_bits)} FROM {qfqn(schema, table)} WHERE {qident(game_id_col)} = ? GROUP BY 1 ORDER BY plays DESC, team_code",
        [game_id],
    )
    team_names = _team_name_map(con)
    for row in rows:
        row["team_name"] = team_names.get(_stringify(row.get("team_code")), row.get("team_code"))
    return rows



def game_detail_payload(con: Any, game_id: str) -> dict[str, Any]:
    game_id = str(game_id)
    title = f"Game {game_id}"
    game = next((row for row in list_games(con, q=game_id, limit=2000) if _stringify(row.get("game_id")) == game_id), None)
    if game:
        title = f"{game.get('away_team')} @ {game.get('home_team')}"

    pbp_columns: list[str] = []
    pbp_rows: list[tuple[Any, ...]] = []
    pbp_meta = _pbp_meta(con)
    if pbp_meta.table:
        schema, table = pbp_meta.table
        game_id_col = pbp_meta.fields.get("game_id")
        play_order_col = pbp_meta.fields.get("play_id")
        preferred = [
            play_order_col,
            pbp_meta.fields.get("quarter"),
            pbp_meta.fields.get("posteam"),
            pbp_meta.fields.get("down"),
            pbp_meta.fields.get("ydstogo"),
            pbp_meta.fields.get("yardline_100"),
            pbp_meta.fields.get("description"),
            pbp_meta.fields.get("epa"),
        ]
        pbp_columns = [col for col in preferred if col]
        if game_id_col and pbp_columns:
            order_sql = qident(play_order_col) if play_order_col else qident(pbp_columns[0])
            pbp_rows = con.execute(
                f"SELECT {', '.join(qident(col) for col in pbp_columns)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(game_id_col)} = ? ORDER BY {order_sql} LIMIT 25",
                [game_id],
            ).fetchall()

    player_week_columns: list[str] = []
    player_week_rows: list[tuple[Any, ...]] = []
    player_spotlight_groups: list[dict[str, Any]] = []
    player_meta = _player_week_meta(con)
    if game and player_meta.table and game.get("season") is not None and game.get("week") is not None:
        schema, table = player_meta.table
        team_col = player_meta.fields.get("team_code")
        season_col = player_meta.fields.get("season")
        week_col = player_meta.fields.get("week")
        player_name_col = player_meta.fields.get("player_name")
        position_col = player_meta.fields.get("position")
        if team_col and season_col and week_col and player_name_col:
            preferred_cols = [
                player_name_col,
                team_col,
                position_col,
                player_meta.fields.get("passing_yards"),
                player_meta.fields.get("rushing_yards"),
                player_meta.fields.get("receiving_yards"),
                player_meta.fields.get("fantasy_points_ppr"),
            ]
            player_week_columns = [col for col in preferred_cols if col]
            order_metric = player_meta.fields.get("fantasy_points_ppr") or player_meta.fields.get("passing_yards") or player_name_col
            player_week_rows = con.execute(
                f"SELECT {', '.join(qident(col) for col in player_week_columns)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(season_col)} = ? AND {qident(week_col)} = ? AND {qident(team_col)} IN (?, ?) "
                f"ORDER BY {qident(order_metric)} DESC NULLS LAST, {qident(player_name_col)} LIMIT 24",
                [int(game['season']), int(game['week']), game.get("home_team"), game.get("away_team")],
            ).fetchall()

            team_index = player_week_columns.index(team_col)
            for team_code in [game.get("away_team"), game.get("home_team")]:
                if not team_code:
                    continue
                grouped_rows = [row for row in player_week_rows if row[team_index] == team_code][:12]
                if grouped_rows:
                    player_spotlight_groups.append(
                        {
                            "team_code": team_code,
                            "team_name": _team_name_map(con).get(_stringify(team_code), team_code),
                            "rows": grouped_rows,
                        }
                    )

    return {
        "title": title,
        "game": game,
        "pbp_columns": pbp_columns,
        "pbp_rows": pbp_rows,
        "player_week_columns": player_week_columns,
        "player_week_rows": player_week_rows,
        "player_spotlight_groups": player_spotlight_groups,
        "pbp_team_summary_rows": _pbp_team_summary(con, game_id),
    }



def schema_diagnostics_payload(con: Any) -> dict[str, Any]:
    specs = [
        {
            "label": "Games",
            "candidates": GAME_TABLE_CANDIDATES,
            "aliases": GAME_FIELD_ALIASES,
            "required": ["game_id", "season", "week", "home_team", "away_team"],
        },
        {
            "label": "Teams",
            "candidates": TEAM_TABLE_CANDIDATES,
            "aliases": TEAM_FIELD_ALIASES,
            "required": ["team_code", "team_name"],
        },
        {
            "label": "Player Dimension",
            "candidates": PLAYER_DIMENSION_CANDIDATES,
            "aliases": PLAYER_DIMENSION_ALIASES,
            "required": ["player_id", "player_name"],
        },
        {
            "label": "Player Week Stats",
            "candidates": PLAYER_WEEK_STATS_CANDIDATES,
            "aliases": PLAYER_WEEK_FIELD_ALIASES,
            "required": ["player_id", "player_name", "team_code", "season", "week"],
        },
        {
            "label": "Team Week Stats",
            "candidates": TEAM_WEEK_STATS_CANDIDATES,
            "aliases": TEAM_WEEK_FIELD_ALIASES,
            "required": ["team_code", "season", "week"],
        },
        {
            "label": "Rosters",
            "candidates": ROSTERS_CANDIDATES,
            "aliases": ROSTER_FIELD_ALIASES,
            "required": ["player_id", "team_code"],
        },
        {
            "label": "Play-by-Play",
            "candidates": PBP_TABLE_CANDIDATES,
            "aliases": PBP_FIELD_ALIASES,
            "required": ["game_id", "season", "week", "description"],
        },
    ]

    datasets = []
    for spec in specs:
        meta = _dataset_meta(con, spec["candidates"], spec["aliases"])
        resolved_fqn = None
        row_count = None
        if meta.table:
            schema, table = meta.table
            resolved_fqn = f"{schema}.{table}"
            row_count = safe_row_count(con, schema, table)
        fields = []
        for logical, actual in meta.fields.items():
            fields.append(
                {
                    "logical": logical,
                    "column": actual,
                    "required": logical in set(spec["required"]),
                }
            )
        missing_required = [field["logical"] for field in fields if field["required"] and not field["column"]]
        datasets.append(
            {
                "label": spec["label"],
                "resolved_fqn": resolved_fqn,
                "row_count": row_count,
                "candidates": [f"{schema}.{table}" for schema, table in spec["candidates"]],
                "fields": fields,
                "missing_required": missing_required,
                "column_count": len(meta.columns),
                "columns_preview": ", ".join(meta.columns[:16]),
            }
        )

    return {
        "summary": {
            "datasets_visible": len(datasets),
            "datasets_resolved": sum(1 for row in datasets if row["resolved_fqn"]),
            "datasets_with_missing_required": sum(1 for row in datasets if row["missing_required"]),
        },
        "datasets": datasets,
    }



def dashboard_payload(con: Any, db_path: str) -> dict[str, Any]:
    tables = list_tables(con)
    runs = latest_runs(con, limit=10)
    seasons = season_week_overview(con)
    summary = summarize_tables(tables)
    coverage = coverage_overview(con)
    latest_success = next((run for run in runs if run.get("outcome") == "ok"), None)

    return {
        "db_path": db_path,
        "tables": tables,
        "runs": runs,
        "seasons": seasons,
        "summary": summary,
        "latest_success": latest_success,
        "coverage": coverage,
        "latest_table_stats": latest_table_stats(con),
    }
