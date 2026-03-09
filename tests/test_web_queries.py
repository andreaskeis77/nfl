import duckdb

from nfl_rag_db.webapp.queries import (
    coverage_overview,
    dashboard_payload,
    game_detail_payload,
    list_players,
    list_teams,
    player_detail_payload,
    season_detail_payload,
    season_week_overview,
    team_detail_payload,
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


def test_game_detail_payload_includes_pbp_and_player_preview(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = game_detail_payload(con, game_id="2024_01_DAL_NYG")
    finally:
        con.close()

    assert payload["game"]["home_team"] == "DAL"
    assert payload["game"]["away_team"] == "NYG"
    assert len(payload["pbp_rows"]) == 3
    assert len(payload["player_week_rows"]) >= 2


def test_team_and_player_payloads_are_browseable(sample_db_path):
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
    assert len(team_payload["recent_games"]) == 2
    assert player_payload["player"]["player_id"] == "p_dak"
    assert len(player_payload["weekly_rows"]) == 2


def test_dashboard_payload_contains_coverage_and_latest_table_stats(sample_db_path):
    con = duckdb.connect(str(sample_db_path), read_only=True)
    try:
        payload = dashboard_payload(con, db_path=str(sample_db_path))
    finally:
        con.close()

    assert payload["summary"]["table_count"] == 8
    assert payload["coverage"]["present_count"] == 6
    assert payload["latest_success"]["component"] == "ingest_player_stats"
    assert len(payload["latest_table_stats"]) == 6


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
