from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from nfl_rag_db.webapp import app as webapp_app


def build_connection(db_path: Path):
    return duckdb.connect(str(db_path), read_only=True)


def test_dashboard_and_datasets_smoke(monkeypatch, sample_db_path):
    monkeypatch.setattr(webapp_app, "connect", lambda db_path=None: build_connection(sample_db_path))
    monkeypatch.setattr(webapp_app, "default_db_path", lambda base_dir=None: Path(sample_db_path))

    client = TestClient(webapp_app.app)

    dashboard = client.get("/dashboard")
    datasets = client.get("/datasets")

    assert dashboard.status_code == 200
    assert datasets.status_code == 200
    assert "Logical Coverage" in dashboard.text
    assert "core.pbp" in datasets.text


def test_season_week_game_routes(monkeypatch, sample_db_path):
    monkeypatch.setattr(webapp_app, "connect", lambda db_path=None: build_connection(sample_db_path))
    monkeypatch.setattr(webapp_app, "default_db_path", lambda base_dir=None: Path(sample_db_path))

    client = TestClient(webapp_app.app)

    seasons = client.get("/seasons")
    season = client.get("/seasons/2024")
    week = client.get("/seasons/2024/weeks/1")
    game = client.get("/games/2024_01_DAL_NYG")

    assert seasons.status_code == 200
    assert season.status_code == 200
    assert week.status_code == 200
    assert game.status_code == 200
    assert "2024" in season.text
    assert "2024_01_DAL_NYG" in week.text
    assert "Play-by-Play Vorschau" in game.text


def test_team_and_player_routes(monkeypatch, sample_db_path):
    monkeypatch.setattr(webapp_app, "connect", lambda db_path=None: build_connection(sample_db_path))
    monkeypatch.setattr(webapp_app, "default_db_path", lambda base_dir=None: Path(sample_db_path))

    client = TestClient(webapp_app.app)

    teams = client.get("/teams")
    team = client.get("/teams/DAL")
    players = client.get("/players")
    player = client.get("/players/p_dak")
    health = client.get("/health")

    assert teams.status_code == 200
    assert team.status_code == 200
    assert players.status_code == 200
    assert player.status_code == 200
    assert health.status_code == 200
    assert "Dallas Cowboys" in team.text
    assert "Dak Prescott" in player.text
    assert health.json()["ok"] is True
