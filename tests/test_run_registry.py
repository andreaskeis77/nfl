import json

import duckdb

from nfl_rag_db.run_registry import finish_run, start_run


def test_run_registry_roundtrip():
    con = duckdb.connect(":memory:")

    run_id = start_run(con, component="unit_test", source="dummy", params={"season": 2021})
    finish_run(con, run_id=run_id, outcome="ok", counts={"ingested": 3, "quarantined": 0})

    row = con.execute(
        "SELECT outcome, params_json, counts_json FROM audit.ingest_run WHERE run_id = ?",
        [run_id],
    ).fetchone()

    assert row is not None
    outcome, params_json, counts_json = row
    assert outcome == "ok"
    assert json.loads(params_json)["season"] == 2021
    assert json.loads(counts_json)["ingested"] == 3