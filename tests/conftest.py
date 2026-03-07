from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.fixture()
def sample_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "nfl_test.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA audit;")
        con.execute("CREATE SCHEMA stg;")
        con.execute("CREATE SCHEMA core;")

        con.execute(
            """
            CREATE TABLE audit.ingest_run (
                started_at TIMESTAMP,
                component VARCHAR,
                source VARCHAR,
                outcome VARCHAR,
                duration_ms BIGINT,
                counts_json VARCHAR,
                error_class VARCHAR,
                error_message VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO audit.ingest_run VALUES
                ('2026-03-07 10:00:00', 'ingest_player_stats', 'nflverse', 'ok', 1234, '{"rows": 5}', NULL, NULL),
                ('2026-03-07 09:30:00', 'ingest_pbp', 'nflverse', 'ok', 1540, '{"rows": 4}', NULL, NULL),
                ('2026-03-07 09:00:00', 'ingest_games', 'nflverse', 'ok', 420, '{"rows": 3}', NULL, NULL);
            """
        )

        con.execute(
            """
            CREATE TABLE audit.ingest_table_stat (
                started_at TIMESTAMP,
                component VARCHAR,
                source VARCHAR,
                table_schema VARCHAR,
                table_name VARCHAR,
                row_count BIGINT
            );
            """
        )
        con.execute(
            """
            INSERT INTO audit.ingest_table_stat VALUES
                ('2026-03-07 09:05:00', 'ingest_games', 'nflverse', 'core', 'game', 3),
                ('2026-03-07 09:40:00', 'ingest_pbp', 'nflverse', 'core', 'pbp', 4),
                ('2026-03-07 10:06:00', 'ingest_teams', 'nflverse', 'core', 'team', 5),
                ('2026-03-07 10:07:00', 'ingest_player_stats', 'nflverse', 'core', 'player_week_stats', 5),
                ('2026-03-07 10:08:00', 'ingest_team_week_stats', 'nflverse', 'core', 'team_week_stats', 3),
                ('2026-03-07 10:09:00', 'ingest_rosters', 'nflverse', 'core', 'rosters', 3);
            """
        )

        con.execute(
            """
            CREATE TABLE core.game (
                game_id VARCHAR,
                season_year INTEGER,
                season_type VARCHAR,
                week INTEGER,
                home_team VARCHAR,
                away_team VARCHAR,
                home_score INTEGER,
                away_score INTEGER
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.game VALUES
                ('2024_01_DAL_NYG', 2024, 'REG', 1, 'DAL', 'NYG', 28, 14),
                ('2024_02_DAL_WAS', 2024, 'REG', 2, 'DAL', 'WAS', 21, 17),
                ('2024_01_BUF_MIA', 2024, 'REG', 1, 'BUF', 'MIA', 24, 20);
            """
        )

        con.execute(
            """
            CREATE TABLE core.team (
                team_abbr VARCHAR,
                team_name VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.team VALUES
                ('DAL', 'Dallas Cowboys'),
                ('NYG', 'New York Giants'),
                ('WAS', 'Washington Commanders'),
                ('BUF', 'Buffalo Bills'),
                ('MIA', 'Miami Dolphins');
            """
        )

        con.execute(
            """
            CREATE TABLE core.player_week_stats (
                player_id VARCHAR,
                player_name VARCHAR,
                recent_team VARCHAR,
                season INTEGER,
                week INTEGER,
                position VARCHAR,
                passing_yards INTEGER,
                rushing_yards INTEGER,
                receiving_yards INTEGER,
                fantasy_points_ppr DOUBLE
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.player_week_stats VALUES
                ('p_dak', 'Dak Prescott', 'DAL', 2024, 1, 'QB', 275, 10, 0, 23.5),
                ('p_dak', 'Dak Prescott', 'DAL', 2024, 2, 'QB', 301, 8, 0, 26.1),
                ('p_cd', 'CeeDee Lamb', 'DAL', 2024, 1, 'WR', 0, 0, 110, 21.0),
                ('p_cd', 'CeeDee Lamb', 'DAL', 2024, 2, 'WR', 0, 0, 95, 18.8),
                ('p_allen', 'Josh Allen', 'BUF', 2024, 1, 'QB', 255, 42, 0, 24.7);
            """
        )

        con.execute(
            """
            CREATE TABLE core.team_week_stats (
                team VARCHAR,
                season INTEGER,
                week INTEGER,
                points_for INTEGER,
                points_against INTEGER
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.team_week_stats VALUES
                ('DAL', 2024, 1, 28, 14),
                ('DAL', 2024, 2, 21, 17),
                ('BUF', 2024, 1, 24, 20);
            """
        )

        con.execute(
            """
            CREATE TABLE core.rosters (
                player_id VARCHAR,
                player_name VARCHAR,
                team VARCHAR,
                position VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.rosters VALUES
                ('p_dak', 'Dak Prescott', 'DAL', 'QB'),
                ('p_cd', 'CeeDee Lamb', 'DAL', 'WR'),
                ('p_allen', 'Josh Allen', 'BUF', 'QB');
            """
        )

        con.execute(
            """
            CREATE TABLE core.pbp (
                game_id VARCHAR,
                season INTEGER,
                week INTEGER,
                play_id INTEGER,
                qtr INTEGER,
                posteam VARCHAR,
                down INTEGER,
                ydstogo INTEGER,
                yardline_100 INTEGER,
                "desc" VARCHAR,
                epa DOUBLE
            );
            """
        )
        con.execute(
            """
            INSERT INTO core.pbp (
                game_id,
                season,
                week,
                play_id,
                qtr,
                posteam,
                down,
                ydstogo,
                yardline_100,
                "desc",
                epa
            ) VALUES
                ('2024_01_DAL_NYG', 2024, 1, 1, 1, 'DAL', 1, 10, 75, 'Dak Prescott pass short right to CeeDee Lamb for 12 yards', 0.45),
                ('2024_01_DAL_NYG', 2024, 1, 2, 1, 'DAL', 1, 10, 63, 'Tony Pollard left tackle for 6 yards', -0.02),
                ('2024_01_DAL_NYG', 2024, 1, 3, 1, 'NYG', 2, 7, 48, 'Daniel Jones pass incomplete deep left', -0.31),
                ('2024_02_DAL_WAS', 2024, 2, 1, 1, 'DAL', 1, 10, 75, 'Dak Prescott scrambles right end for 8 yards', 0.18);
            """
        )
    finally:
        con.close()

    return db_path
