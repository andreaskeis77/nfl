import duckdb

from nfl_rag_db.change_detection import compute_change_counts


def test_change_detection_insert_update_delete():
    con = duckdb.connect(":memory:")

    con.execute("CREATE TABLE existing(id INTEGER, a INTEGER, b TEXT);")
    con.execute("INSERT INTO existing VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z');")

    con.execute("CREATE TABLE incoming(id INTEGER, a INTEGER, b TEXT);")
    # id=1 unchanged, id=2 updated, id=4 inserted, id=3 deleted
    con.execute("INSERT INTO incoming VALUES (1, 10, 'x'), (2, 99, 'y'), (4, 40, 'n');")

    c = compute_change_counts(
        con,
        existing_fqn="existing",
        incoming_fqn="incoming",
        key_cols=["id"],
        hash_cols=["id", "a", "b"],
    )

    assert c["inserted"] == 1
    assert c["updated"] == 1
    assert c["deleted"] == 1