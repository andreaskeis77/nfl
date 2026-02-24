from nfl_rag_db.ingest.pbp import pbp_url_for_season


def test_pbp_url_for_season():
    assert pbp_url_for_season(2023).endswith("/pbp/play_by_play_2023.parquet")