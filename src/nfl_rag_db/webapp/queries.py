from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


GAME_TABLE_CANDIDATES = (("core", "game"), ("core", "games"))
TEAM_TABLE_CANDIDATES = (("core", "team"), ("core", "teams"))
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


def qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def qfqn(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


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

    return fetch_dicts(
        con,
        f"""
        SELECT
            started_at,
            component,
            source,
            outcome,
            duration_ms,
            counts_json,
            error_class,
            error_message
        FROM {qfqn('audit', 'ingest_run')}
        ORDER BY started_at DESC
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

    rows = fetch_dicts(
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
    return rows


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


def _team_name_map(con: Any) -> dict[str, str]:
    team_table = first_existing_table(con, TEAM_TABLE_CANDIDATES)
    if not team_table:
        return {}
    schema, table = team_table
    cols = table_columns(con, schema, table)
    code_col = first_matching_column(cols, ["team_abbr", "team", "team_code", "abbr"])
    name_col = first_matching_column(cols, ["team_name", "name", "team_full_name"])
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


def _game_meta(con: Any) -> tuple[tuple[str, str] | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]:
    game_table = first_existing_table(con, GAME_TABLE_CANDIDATES)
    if not game_table:
        return (None, None, None, None, None, None, None, None)
    schema, table = game_table
    cols = table_columns(con, schema, table)
    game_id_col = first_matching_column(cols, ["game_id"])
    season_col = first_matching_column(cols, ["season_year", "season"])
    season_type_col = first_matching_column(cols, ["season_type"])
    week_col = first_matching_column(cols, ["week"])
    home_team_col = first_matching_column(cols, ["home_team", "home_team_abbr"])
    away_team_col = first_matching_column(cols, ["away_team", "away_team_abbr"])
    home_score_col = first_matching_column(cols, ["home_score"])
    away_score_col = first_matching_column(cols, ["away_score"])
    return (
        game_table,
        game_id_col,
        season_col,
        season_type_col,
        week_col,
        home_team_col,
        away_team_col,
        home_score_col,
        away_score_col,
    )


def season_week_overview(con: Any) -> list[dict[str, Any]]:
    game_meta = _game_meta(con)
    game_table = game_meta[0]
    if game_table:
        schema, table = game_table
        _, _, season_col, season_type_col, week_col, *_ = game_meta
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

    pbp_table = first_existing_table(con, PBP_TABLE_CANDIDATES)
    if pbp_table:
        schema, table = pbp_table
        cols = table_columns(con, schema, table)
        season_col = first_matching_column(cols, ["season"])
        week_col = first_matching_column(cols, ["week"])
        game_id_col = first_matching_column(cols, ["game_id"])
        if season_col and week_col:
            if game_id_col:
                games_expr = f"COUNT(DISTINCT {qident(game_id_col)})"
            else:
                games_expr = "COUNT(*)"
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
    return {
        "season": int(season),
        "summary": {
            "games_visible": games_visible,
            "weeks_visible": weeks_visible,
            "season_types": season_types,
        },
        "weeks": rows,
    }


def week_detail_payload(con: Any, season: int, week: int) -> dict[str, Any]:
    team_names = _team_name_map(con)
    games: list[dict[str, Any]] = []

    game_meta = _game_meta(con)
    game_table = game_meta[0]
    if game_table:
        schema, table = game_table
        _, game_id_col, season_col, season_type_col, week_col, home_team_col, away_team_col, home_score_col, away_score_col = game_meta
        if season_col and week_col and home_team_col and away_team_col:
            select_bits = [
                f"{qident(game_id_col)} AS game_id" if game_id_col else "NULL AS game_id",
                f"{qident(season_col)} AS season",
                f"{qident(season_type_col)} AS season_type" if season_type_col else "NULL AS season_type",
                f"{qident(week_col)} AS week",
                f"{qident(home_team_col)} AS home_team",
                f"{qident(away_team_col)} AS away_team",
                f"{qident(home_score_col)} AS home_score" if home_score_col else "NULL AS home_score",
                f"{qident(away_score_col)} AS away_score" if away_score_col else "NULL AS away_score",
            ]
            games = fetch_dicts(
                con,
                f"""
                SELECT {', '.join(select_bits)}
                FROM {qfqn(schema, table)}
                WHERE {qident(season_col)} = ? AND {qident(week_col)} = ?
                ORDER BY {qident(away_team_col)}, {qident(home_team_col)}
                """,
                [int(season), int(week)],
            )
    else:
        pbp_table = first_existing_table(con, PBP_TABLE_CANDIDATES)
        if pbp_table:
            schema, table = pbp_table
            cols = table_columns(con, schema, table)
            season_col = first_matching_column(cols, ["season"])
            week_col = first_matching_column(cols, ["week"])
            game_id_col = first_matching_column(cols, ["game_id"])
            home_team_col = first_matching_column(cols, ["home_team"])
            away_team_col = first_matching_column(cols, ["away_team"])
            if season_col and week_col and game_id_col:
                select_bits = [
                    f"{qident(game_id_col)} AS game_id",
                    f"{qident(season_col)} AS season",
                    f"{qident(week_col)} AS week",
                    f"MIN({qident(home_team_col)}) AS home_team" if home_team_col else "NULL AS home_team",
                    f"MIN({qident(away_team_col)}) AS away_team" if away_team_col else "NULL AS away_team",
                    "NULL AS home_score",
                    "NULL AS away_score",
                    "NULL AS season_type",
                ]
                games = fetch_dicts(
                    con,
                    f"""
                    SELECT {', '.join(select_bits)}
                    FROM {qfqn(schema, table)}
                    WHERE {qident(season_col)} = ? AND {qident(week_col)} = ?
                    GROUP BY 1, 2, 3
                    ORDER BY 1
                    """,
                    [int(season), int(week)],
                )

    for row in games:
        row["home_team_name"] = team_names.get(str(row.get("home_team") or ""), row.get("home_team"))
        row["away_team_name"] = team_names.get(str(row.get("away_team") or ""), row.get("away_team"))

    return {
        "season": int(season),
        "week": int(week),
        "summary": {
            "games_visible": len(games),
            "teams_visible": len(
                {team for row in games for team in [row.get("home_team"), row.get("away_team")] if team}
            ),
        },
        "games": games,
    }


def list_teams(con: Any, search: str | None = None) -> list[dict[str, Any]]:
    name_map = _team_name_map(con)
    team_codes = set(name_map)

    game_meta = _game_meta(con)
    game_table = game_meta[0]
    if game_table:
        schema, table = game_table
        _, _, _, _, _, home_team_col, away_team_col, _, _ = game_meta
        if home_team_col and away_team_col:
            rows = con.execute(
                f"SELECT DISTINCT {qident(home_team_col)} AS team_code FROM {qfqn(schema, table)} "
                f"UNION SELECT DISTINCT {qident(away_team_col)} AS team_code FROM {qfqn(schema, table)}"
            ).fetchall()
            team_codes.update(str(row[0]) for row in rows if row[0])

    games_visible: Counter[str] = Counter()
    weeks_visible: dict[str, set[tuple[int, int]]] = {}
    if game_table:
        schema, table = game_table
        _, _, season_col, _, week_col, home_team_col, away_team_col, _, _ = game_meta
        if home_team_col and away_team_col:
            game_rows = con.execute(
                f"SELECT {qident(home_team_col)}, {qident(away_team_col)}, "
                f"{qident(season_col)} AS season, {qident(week_col)} AS week "
                f"FROM {qfqn(schema, table)}"
            ).fetchall()
            for home_team, away_team, season, week in game_rows:
                for team_code in [home_team, away_team]:
                    if team_code:
                        code = str(team_code)
                        team_codes.add(code)
                        games_visible[code] += 1
                        weeks_visible.setdefault(code, set()).add((int(season), int(week)))

    player_counts: Counter[str] = Counter()
    roster_table = first_existing_table(con, ROSTERS_CANDIDATES)
    if roster_table:
        schema, table = roster_table
        cols = table_columns(con, schema, table)
        team_col = first_matching_column(cols, ["team", "recent_team"])
        player_id_col = first_matching_column(cols, ["player_id"])
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
        player_table = first_existing_table(con, PLAYER_WEEK_STATS_CANDIDATES)
        if player_table:
            schema, table = player_table
            cols = table_columns(con, schema, table)
            team_col = first_matching_column(cols, ["recent_team", "team"])
            player_id_col = first_matching_column(cols, ["player_id"])
            if team_col and player_id_col:
                rows = con.execute(
                    f"SELECT {qident(team_col)}, COUNT(DISTINCT {qident(player_id_col)}) FROM {qfqn(schema, table)} GROUP BY 1"
                ).fetchall()
                for team_code, count in rows:
                    if team_code:
                        code = str(team_code)
                        team_codes.add(code)
                        player_counts[code] = int(count)

    result: list[dict[str, Any]] = []
    q = (search or "").strip().lower()
    for code in sorted(team_codes):
        name = name_map.get(code, code)
        row = {
            "team_code": code,
            "team_name": name,
            "games_visible": int(games_visible.get(code, 0)),
            "weeks_visible": len(weeks_visible.get(code, set())),
            "players_visible": int(player_counts.get(code, 0)),
        }
        haystack = f"{code} {name}".lower()
        if not q or q in haystack:
            result.append(row)
    return result


def team_detail_payload(con: Any, team_code: str) -> dict[str, Any]:
    team_code = str(team_code).upper()
    teams = list_teams(con)
    team = next((row for row in teams if row["team_code"] == team_code), None)
    if not team:
        return {
            "team": None,
            "summary": {},
            "recent_games": [],
            "weekly_columns": [],
            "weekly_rows": [],
            "roster_preview": [],
            "player_leaders": [],
        }

    game_meta = _game_meta(con)
    recent_games: list[dict[str, Any]] = []
    if game_meta[0]:
        schema, table = game_meta[0]
        _, game_id_col, season_col, _, week_col, home_team_col, away_team_col, home_score_col, away_score_col = game_meta
        if season_col and week_col and home_team_col and away_team_col:
            select_bits = [
                f"{qident(game_id_col)} AS game_id" if game_id_col else "NULL AS game_id",
                f"{qident(season_col)} AS season",
                f"{qident(week_col)} AS week",
                f"{qident(home_team_col)} AS home_team",
                f"{qident(away_team_col)} AS away_team",
                f"{qident(home_score_col)} AS home_score" if home_score_col else "NULL AS home_score",
                f"{qident(away_score_col)} AS away_score" if away_score_col else "NULL AS away_score",
            ]
            recent_games = fetch_dicts(
                con,
                f"""
                SELECT {', '.join(select_bits)}
                FROM {qfqn(schema, table)}
                WHERE {qident(home_team_col)} = ? OR {qident(away_team_col)} = ?
                ORDER BY {qident(season_col)} DESC, {qident(week_col)} DESC
                LIMIT 12
                """,
                [team_code, team_code],
            )

    weekly_columns: list[str] = []
    weekly_rows: list[tuple[Any, ...]] = []
    summary: dict[str, Any] = {"seasons_visible": 0, "weeks_visible": 0, "points_for_total": None, "points_against_total": None}
    team_week_table = first_existing_table(con, TEAM_WEEK_STATS_CANDIDATES)
    if team_week_table:
        schema, table = team_week_table
        cols = table_columns(con, schema, table)
        team_col = first_matching_column(cols, ["team", "team_abbr", "team_code"])
        season_col = first_matching_column(cols, ["season"])
        week_col = first_matching_column(cols, ["week"])
        if team_col and season_col and week_col:
            weekly_columns = [col for col in [season_col, week_col, "points_for", "points_against"] if col in cols]
            select_sql = ", ".join(qident(col) for col in weekly_columns)
            weekly_rows = con.execute(
                f"SELECT {select_sql} FROM {qfqn(schema, table)} WHERE {qident(team_col)} = ? ORDER BY {qident(season_col)} DESC, {qident(week_col)} DESC",
                [team_code],
            ).fetchall()
            summary_row = con.execute(
                f"""
                SELECT
                    COUNT(DISTINCT {qident(season_col)}) AS seasons_visible,
                    COUNT(*) AS weeks_visible,
                    SUM({qident('points_for')}) AS points_for_total,
                    SUM({qident('points_against')}) AS points_against_total
                FROM {qfqn(schema, table)}
                WHERE {qident(team_col)} = ?
                """,
                [team_code],
            ).fetchone()
            if summary_row:
                summary = {
                    "seasons_visible": int(summary_row[0] or 0),
                    "weeks_visible": int(summary_row[1] or 0),
                    "points_for_total": summary_row[2],
                    "points_against_total": summary_row[3],
                }

    roster_preview: list[dict[str, Any]] = []
    roster_table = first_existing_table(con, ROSTERS_CANDIDATES)
    if roster_table:
        schema, table = roster_table
        cols = table_columns(con, schema, table)
        team_col = first_matching_column(cols, ["team", "recent_team"])
        player_id_col = first_matching_column(cols, ["player_id"])
        player_name_col = first_matching_column(cols, ["player_name", "full_name"])
        position_col = first_matching_column(cols, ["position"])
        if team_col and player_id_col:
            select_bits = [f"{qident(player_id_col)} AS player_id"]
            select_bits.append(
                f"{qident(player_name_col)} AS player_name" if player_name_col else f"{qident(player_id_col)} AS player_name"
            )
            select_bits.append(
                f"{qident(position_col)} AS position" if position_col else "NULL AS position"
            )
            roster_preview = fetch_dicts(
                con,
                f"SELECT {', '.join(select_bits)} FROM {qfqn(schema, table)} WHERE {qident(team_col)} = ? ORDER BY 2, 1 LIMIT 20",
                [team_code],
            )

    player_leaders: list[dict[str, Any]] = []
    player_table = first_existing_table(con, PLAYER_WEEK_STATS_CANDIDATES)
    if player_table:
        schema, table = player_table
        cols = table_columns(con, schema, table)
        player_id_col = first_matching_column(cols, ["player_id"])
        player_name_col = first_matching_column(cols, ["player_name", "full_name"])
        team_col = first_matching_column(cols, ["recent_team", "team"])
        metrics = [
            ("Passing Yards", "passing_yards"),
            ("Rushing Yards", "rushing_yards"),
            ("Receiving Yards", "receiving_yards"),
            ("Fantasy PPR", "fantasy_points_ppr"),
        ]
        if player_id_col and player_name_col and team_col:
            for label, metric_col in metrics:
                if metric_col not in cols:
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
        "weekly_columns": weekly_columns,
        "weekly_rows": weekly_rows,
        "roster_preview": roster_preview,
        "player_leaders": player_leaders,
    }


def list_players(con: Any, search: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    q = (search or "").strip().lower()
    player_table = first_existing_table(con, PLAYER_WEEK_STATS_CANDIDATES)
    if player_table:
        schema, table = player_table
        cols = table_columns(con, schema, table)
        player_id_col = first_matching_column(cols, ["player_id"])
        player_name_col = first_matching_column(cols, ["player_name", "full_name"])
        team_col = first_matching_column(cols, ["recent_team", "team"])
        position_col = first_matching_column(cols, ["position"])
        season_col = first_matching_column(cols, ["season"])
        week_col = first_matching_column(cols, ["week"])
        if player_id_col and player_name_col:
            rows = fetch_dicts(
                con,
                f"""
                SELECT
                    {qident(player_id_col)} AS player_id,
                    {qident(player_name_col)} AS player_name,
                    {qident(team_col)} AS team_code,
                    {qident(position_col)} AS position,
                    COUNT(DISTINCT {qident(season_col)}) AS seasons_visible,
                    MAX({qident(season_col)}) AS latest_season,
                    MAX({qident(week_col)}) AS latest_week,
                    COUNT(*) AS weekly_rows
                FROM {qfqn(schema, table)}
                GROUP BY 1, 2, 3, 4
                ORDER BY player_name, player_id
                LIMIT ?
                """,
                [int(limit)],
            )
            if q:
                rows = [row for row in rows if q in f"{row['player_id']} {row['player_name']} {row.get('team_code') or ''}".lower()]
            return rows

    roster_table = first_existing_table(con, ROSTERS_CANDIDATES)
    if not roster_table:
        return []
    schema, table = roster_table
    cols = table_columns(con, schema, table)
    player_id_col = first_matching_column(cols, ["player_id"])
    player_name_col = first_matching_column(cols, ["player_name", "full_name"])
    team_col = first_matching_column(cols, ["team", "recent_team"])
    position_col = first_matching_column(cols, ["position"])
    if not player_id_col:
        return []
    rows = fetch_dicts(
        con,
        f"""
        SELECT
            {qident(player_id_col)} AS player_id,
            {qident(player_name_col)} AS player_name,
            {qident(team_col)} AS team_code,
            {qident(position_col)} AS position,
            0 AS seasons_visible,
            NULL AS latest_season,
            NULL AS latest_week,
            0 AS weekly_rows
        FROM {qfqn(schema, table)}
        ORDER BY 2, 1
        LIMIT ?
        """,
        [int(limit)],
    )
    if q:
        rows = [row for row in rows if q in f"{row['player_id']} {row.get('player_name') or ''} {row.get('team_code') or ''}".lower()]
    return rows


def player_detail_payload(con: Any, player_id: str) -> dict[str, Any]:
    player_id = str(player_id)
    player_rows = list_players(con, search=None, limit=10000)
    player = next((row for row in player_rows if row["player_id"] == player_id), None)
    if not player:
        return {
            "player": None,
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

    player_table = first_existing_table(con, PLAYER_WEEK_STATS_CANDIDATES)
    if player_table:
        schema, table = player_table
        cols = table_columns(con, schema, table)
        player_id_col = first_matching_column(cols, ["player_id"])
        season_col = first_matching_column(cols, ["season"])
        week_col = first_matching_column(cols, ["week"])
        preferred_weekly = [
            season_col,
            week_col,
            "recent_team" if "recent_team" in cols else ("team" if "team" in cols else None),
            "position" if "position" in cols else None,
            "passing_yards" if "passing_yards" in cols else None,
            "rushing_yards" if "rushing_yards" in cols else None,
            "receiving_yards" if "receiving_yards" in cols else None,
            "fantasy_points_ppr" if "fantasy_points_ppr" in cols else None,
        ]
        weekly_columns = [col for col in preferred_weekly if col]
        if player_id_col and weekly_columns:
            weekly_rows = con.execute(
                f"SELECT {', '.join(qident(col) for col in weekly_columns)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(player_id_col)} = ? ORDER BY {qident(season_col)} DESC, {qident(week_col)} DESC",
                [player_id],
            ).fetchall()

            agg_metrics = [col for col in ["passing_yards", "rushing_yards", "receiving_yards", "fantasy_points_ppr"] if col in cols]
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
                labels = {
                    "passing_yards": "Passing Yards",
                    "rushing_yards": "Rushing Yards",
                    "receiving_yards": "Receiving Yards",
                    "fantasy_points_ppr": "Fantasy PPR",
                }
                stat_totals = [
                    {"label": labels[col], "value": totals_row[idx]}
                    for idx, col in enumerate(agg_metrics)
                ]

    return {
        "player": player,
        "weekly_columns": weekly_columns,
        "weekly_rows": weekly_rows,
        "season_summary_columns": season_summary_columns,
        "season_summary_rows": season_summary_rows,
        "stat_totals": stat_totals,
    }


def game_detail_payload(con: Any, game_id: str) -> dict[str, Any]:
    game_id = str(game_id)
    team_names = _team_name_map(con)
    title = f"Game {game_id}"
    game: dict[str, Any] | None = None

    game_meta = _game_meta(con)
    game_table = game_meta[0]
    if game_table:
        schema, table = game_table
        _, game_id_col, season_col, season_type_col, week_col, home_team_col, away_team_col, home_score_col, away_score_col = game_meta
        if game_id_col:
            select_bits = [
                f"{qident(game_id_col)} AS game_id",
                f"{qident(season_col)} AS season" if season_col else "NULL AS season",
                f"{qident(season_type_col)} AS season_type" if season_type_col else "NULL AS season_type",
                f"{qident(week_col)} AS week" if week_col else "NULL AS week",
                f"{qident(home_team_col)} AS home_team" if home_team_col else "NULL AS home_team",
                f"{qident(away_team_col)} AS away_team" if away_team_col else "NULL AS away_team",
                f"{qident(home_score_col)} AS home_score" if home_score_col else "NULL AS home_score",
                f"{qident(away_score_col)} AS away_score" if away_score_col else "NULL AS away_score",
            ]
            rows = fetch_dicts(
                con,
                f"SELECT {', '.join(select_bits)} FROM {qfqn(schema, table)} WHERE {qident(game_id_col)} = ? LIMIT 1",
                [game_id],
            )
            if rows:
                game = rows[0]
                game["home_team_name"] = team_names.get(str(game.get("home_team") or ""), game.get("home_team"))
                game["away_team_name"] = team_names.get(str(game.get("away_team") or ""), game.get("away_team"))
                title = f"{game.get('away_team')} @ {game.get('home_team')}"

    pbp_columns: list[str] = []
    pbp_rows: list[tuple[Any, ...]] = []
    pbp_table = first_existing_table(con, PBP_TABLE_CANDIDATES)
    if pbp_table:
        schema, table = pbp_table
        cols = table_columns(con, schema, table)
        game_id_col = first_matching_column(cols, ["game_id"])
        play_order_col = first_matching_column(cols, ["play_id", "play_index"])
        preferred = [
            play_order_col,
            "qtr" if "qtr" in cols else ("quarter" if "quarter" in cols else None),
            "posteam" if "posteam" in cols else None,
            "down" if "down" in cols else None,
            "ydstogo" if "ydstogo" in cols else None,
            "yardline_100" if "yardline_100" in cols else None,
            "desc" if "desc" in cols else ("play_description" if "play_description" in cols else ("play_desc" if "play_desc" in cols else ("description" if "description" in cols else None))),
            "epa" if "epa" in cols else None,
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
    player_table = first_existing_table(con, PLAYER_WEEK_STATS_CANDIDATES)
    if game and player_table and game.get("season") is not None and game.get("week") is not None:
        schema, table = player_table
        cols = table_columns(con, schema, table)
        team_col = first_matching_column(cols, ["recent_team", "team"])
        season_col = first_matching_column(cols, ["season"])
        week_col = first_matching_column(cols, ["week"])
        player_name_col = first_matching_column(cols, ["player_name", "full_name"])
        position_col = first_matching_column(cols, ["position"])
        if team_col and season_col and week_col and player_name_col:
            player_week_columns = [
                player_name_col,
                team_col,
                position_col if position_col else None,
                "passing_yards" if "passing_yards" in cols else None,
                "rushing_yards" if "rushing_yards" in cols else None,
                "receiving_yards" if "receiving_yards" in cols else None,
                "fantasy_points_ppr" if "fantasy_points_ppr" in cols else None,
            ]
            player_week_columns = [col for col in player_week_columns if col]
            player_week_rows = con.execute(
                f"SELECT {', '.join(qident(col) for col in player_week_columns)} FROM {qfqn(schema, table)} "
                f"WHERE {qident(season_col)} = ? AND {qident(week_col)} = ? AND {qident(team_col)} IN (?, ?) "
                f"ORDER BY {qident('fantasy_points_ppr')} DESC NULLS LAST, {qident(player_name_col)} LIMIT 20",
                [int(game['season']), int(game['week']), game.get("home_team"), game.get("away_team")],
            ).fetchall()

    return {
        "title": title,
        "game": game,
        "pbp_columns": pbp_columns,
        "pbp_rows": pbp_rows,
        "player_week_columns": player_week_columns,
        "player_week_rows": player_week_rows,
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
