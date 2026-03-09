from __future__ import annotations

from pathlib import Path

import duckdb

from nfl_rag_db.webapp.queries import (
    coverage_overview,
    dashboard_payload,
    game_detail_payload,
    game_explorer_payload,
    list_games,
    list_players,
    list_teams,
    player_detail_payload,
    player_explorer_payload,
    schema_diagnostics_payload,
    season_detail_payload,
    season_week_overview,
    team_detail_payload,
    team_explorer_payload,
    week_detail_payload,
)


def test_coverage_overview_counts_logical_datasets(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        coverage = coverage_overview(con)
    finally:
        con.close()

    assert coverage["present_count"] == 6
    assert coverage["total_count"] == 6
    assert coverage["coverage_pct"] == 100.0


def test_season_week_overview_from_core_game(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        rows = season_week_overview(con)
    finally:
        con.close()

    assert rows == [
        {"season": 2024, "season_type": "REG", "week": 1, "games": 2},
        {"season": 2024, "season_type": "REG", "week": 2, "games": 1},
        {"season": 2023, "season_type": "REG", "week": 18, "games": 1},
    ]


def test_game_explorer_payload_filters_games(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = game_explorer_payload(con, season=2024, team="DAL", week=1)
    finally:
        con.close()

    assert payload["summary"]["games_visible"] == 1
    assert payload["games"][0]["game_id"] == "2024_01_DAL_NYG"
    assert payload["games"][0]["venue"] == "AT&T Stadium"
    assert payload["filters"]["seasons"] == [2024, 2023]


def test_list_games_falls_back_to_pbp_when_game_table_missing(tmp_path):
    db_path = tmp_path / "pbp_only.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA core;")
        con.execute(
            """
            CREATE TABLE core.pbp (
                game_id VARCHAR,
                season INTEGER,
                week INTEGER,
                play_id INTEGER,
                qtr INTEGER,
                posteam VARCHAR,
                home_team VARCHAR,
                away_team VARCHAR,
                "desc" VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.pbp VALUES
                ('2024_01_BUF_MIA', 2024, 1, 1, 1, 'BUF', 'BUF', 'MIA', 'Josh Allen pass complete'),
                ('2024_01_BUF_MIA', 2024, 1, 2, 1, 'MIA', 'BUF', 'MIA', 'Tua Tagovailoa pass complete');
            """
        )
        rows = list_games(con, season=2024, week=1)
    finally:
        con.close()

    assert rows == [
        {
            "game_id": "2024_01_BUF_MIA",
            "season": 2024,
            "season_type": None,
            "week": 1,
            "home_team": "BUF",
            "away_team": "MIA",
            "home_score": None,
            "away_score": None,
            "status": None,
            "gameday": None,
            "kickoff": None,
            "venue": None,
            "home_team_name": "BUF",
            "away_team_name": "MIA",
            "matchup": "MIA @ BUF",
            "score_display": "",
            "winner_team": None,
            "winner_team_name": None,
            "margin": None,
        }
    ]


def test_season_detail_payload_exposes_weeks(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = season_detail_payload(con, season=2024)
    finally:
        con.close()

    assert payload["summary"]["games_visible"] == 3
    assert payload["summary"]["weeks_visible"] == 2
    assert [row["week"] for row in payload["weeks"]] == [1, 2]


def test_week_detail_payload_lists_games(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = week_detail_payload(con, season=2024, week=1)
    finally:
        con.close()

    assert payload["summary"]["games_visible"] == 2
    game_ids = [row["game_id"] for row in payload["games"]]
    assert "2024_01_DAL_NYG" in game_ids
    assert "2024_01_BUF_MIA" in game_ids


def test_game_detail_payload_includes_pbp_team_summary_and_player_spotlight(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = game_detail_payload(con, game_id="2024_01_DAL_NYG")
    finally:
        con.close()

    assert payload["game"]["home_team"] == "DAL"
    assert payload["game"]["away_team"] == "NYG"
    assert payload["game"]["status"] == "final"
    assert payload["game"]["venue"] == "AT&T Stadium"
    assert len(payload["pbp_rows"]) == 3
    assert len(payload["player_week_rows"]) >= 2
    assert payload["pbp_team_summary_rows"][0]["team_code"] == "DAL"
    assert payload["player_spotlight_groups"][0]["team_code"] == "NYG" or payload["player_spotlight_groups"][0]["team_code"] == "DAL"


def test_team_and_player_payloads_are_browseable_and_enriched(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        teams = list_teams(con)
        players = list_players(con)
        team_payload = team_detail_payload(con, team_code="DAL")
        player_payload = player_detail_payload(con, player_id="p_dak")
    finally:
        con.close()

    assert teams[0]["team_code"] == "BUF"
    assert any(row["player_id"] == "p_dak" for row in players)
    assert team_payload["team"]["team_code"] == "DAL"
    assert team_payload["summary"]["wins_total"] == 3
    assert team_payload["summary"]["points_for_total"] == 80
    assert len(team_payload["recent_games"]) == 3
    assert team_payload["roster_groups"] == [
        {"position": "QB", "players": 1},
        {"position": "WR", "players": 1},
    ]
    assert player_payload["player"]["player_id"] == "p_dak"
    assert len(player_payload["weekly_rows"]) == 3
    assert player_payload["profile_fields"][0]["label"] == "Geburtsdatum"
    assert {row["label"] for row in player_payload["stat_totals"]} >= {"Passing Yards", "Fantasy PPR"}


def test_player_and_team_explorer_payloads_include_summaries(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        player_payload = player_explorer_payload(con, team="DAL", position="QB", season=2024, sort="passing_yards")
        team_payload = team_explorer_payload(con, search="dallas")
    finally:
        con.close()

    assert player_payload["summary"]["players_visible"] == 1
    assert player_payload["summary"]["positions_visible"] == 1
    assert player_payload["players"][0]["player_id"] == "p_dak"
    assert team_payload["summary"]["teams_visible"] == 1
    assert team_payload["teams"][0]["team_code"] == "DAL"


def test_list_players_supports_default_and_metric_sort_orders(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        default_rows = list_players(con)
        passing_rows = list_players(con, team="DAL", position="QB", season=2024, sort="passing_yards")
    finally:
        con.close()

    assert any(row["player_id"] == "p_dak" for row in default_rows)
    assert passing_rows[0]["player_id"] == "p_dak"
    assert passing_rows[0]["passing_yards_total"] == 576


def test_schema_diagnostics_payload_resolves_player_dimension(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = schema_diagnostics_payload(con)
    finally:
        con.close()

    assert payload["summary"]["datasets_visible"] == 7
    assert payload["summary"]["datasets_resolved"] == 7
    player_dimension = next(row for row in payload["datasets"] if row["label"] == "Player Dimension")
    assert player_dimension["resolved_fqn"] == "core.players"
    assert player_dimension["missing_required"] == []
    resolved_fields = {row["logical"]: row["column"] for row in player_dimension["fields"]}
    assert resolved_fields["birth_date"] == "birth_date"
    assert resolved_fields["college"] == "college_name"


def test_dashboard_payload_contains_coverage_and_latest_table_stats(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = dashboard_payload(con, db_path=str(sample_db_path))
    finally:
        con.close()

    assert payload["summary"]["table_count"] == 9
    assert payload["coverage"]["present_count"] == 6
    assert payload["latest_success"]["component"] == "ingest_player_stats"
    assert len(payload["latest_table_stats"]) == 7


def test_latest_table_stats_supports_current_audit_schema(tmp_path):
    db_path = tmp_path / "nfl_alt_audit.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA audit;")
        con.execute(
            """
            CREATE TABLE audit.ingest_table_stat (
                stat_id BIGINT,
                captured_at TIMESTAMP,
                table_fqn VARCHAR,
                delta_row_count BIGINT,
                note VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO audit.ingest_table_stat VALUES
                (1, '2026-03-07 09:00:00', 'core.game', 3, 'initial load'),
                (2, '2026-03-07 10:00:00', 'core.game', 1, 'delta load'),
                (3, '2026-03-07 10:05:00', 'core.player_week_stats', 5, 'initial load');
            """
        )
        rows = dashboard_payload(con, db_path=str(db_path))["latest_table_stats"]
    finally:
        con.close()

    by_fqn = {row["fqn"]: row for row in rows}
    assert by_fqn["core.game"]["started_at"] is not None
    assert by_fqn["core.game"]["row_count"] is None
    assert by_fqn["core.game"]["delta_row_count"] == 1
    assert by_fqn["core.player_week_stats"]["delta_row_count"] == 5
