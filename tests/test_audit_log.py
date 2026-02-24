import duckdb

from nfl_rag_db.audit_log import record_source_file, record_table_stat


def test_audit_log_roundtrip():
    con = duckdb.connect(":memory:")

    file_id = record_source_file(
        con,
        run_id="run-1",
        source="nfldata",
        dataset="games",
        url="https://example.invalid/games.csv",
        local_path="data/raw/nfldata/games/x.csv",
        sha256="abc",
        size_bytes=123,
    )
    stat_id = record_table_stat(
        con,
        run_id="run-1",
        table_fqn="core.game",
        row_count=10,
        previous_row_count=7,
        note="unit test",
    )

    row = con.execute(
        "SELECT dataset, sha256, size_bytes FROM audit.ingest_source_file WHERE file_id = ?",
        [file_id],
    ).fetchone()
    assert row == ("games", "abc", 123)

    row2 = con.execute(
        (
    "SELECT row_count, previous_row_count, delta_row_count "
    "FROM audit.ingest_table_stat WHERE stat_id = ?"
),
        [stat_id],
    ).fetchone()
    assert row2 == (10, 7, 3)