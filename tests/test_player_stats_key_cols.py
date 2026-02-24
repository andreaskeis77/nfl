from nfl_rag_db.ingest.player_stats import _infer_key_cols


def test_infer_key_cols_prefers_season_type_and_week():
    cols = ["season", "week", "season_type", "player_id", "x"]
    assert _infer_key_cols(cols) == ["season", "season_type", "week", "player_id"]


def test_infer_key_cols_falls_back_to_game_type():
    cols = ["season", "week", "game_type", "player_id", "x"]
    assert _infer_key_cols(cols) == ["season", "game_type", "week", "player_id"]


def test_infer_key_cols_falls_back_to_game_id_if_no_week():
    cols = ["season", "season_type", "game_id", "player_id", "x"]
    assert _infer_key_cols(cols) == ["season", "season_type", "game_id", "player_id"]
