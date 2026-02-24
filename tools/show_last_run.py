from __future__ import annotations

import json

from nfl_rag_db.db import connect


def _pretty_json(s: str | None) -> str:
    if not s:
        return "{}"
    try:
        return json.dumps(json.loads(s), indent=2, ensure_ascii=False)
    except Exception:
        return s


def main() -> None:
    con = connect()

    last = con.execute(
        """
        SELECT
          run_id, started_at, component, source, outcome, duration_ms, counts_json
        FROM audit.ingest_run
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    if not last:
        print("No runs found in audit.ingest_run")
        return

    run_id, started_at, component, source, outcome, duration_ms, counts_json = last
    print("=== LAST RUN ===")
    print(f"run_id     : {run_id}")
    print(f"started_at : {started_at}")
    print(f"component  : {component}")
    print(f"source     : {source}")
    print(f"outcome    : {outcome}")
    print(f"duration_ms: {duration_ms}")
    print("counts_json:")
    print(_pretty_json(counts_json))

    files = con.execute(
        """
        SELECT dataset, size_bytes, sha256, local_path
        FROM audit.ingest_source_file
        WHERE run_id = ?
        ORDER BY dataset
        """,
        [run_id],
    ).fetchall()

    print("\n=== SOURCE FILES ===")
    if not files:
        print("(none)")
    else:
        for r in files:
            print(r)

    stats = con.execute(
        """
        SELECT table_fqn, row_count, previous_row_count, delta_row_count, note
        FROM audit.ingest_table_stat
        WHERE run_id = ?
        ORDER BY table_fqn
        """,
        [run_id],
    ).fetchall()

    print("\n=== TABLE STATS ===")
    if not stats:
        print("(none)")
    else:
        for r in stats:
            print(r)


if __name__ == "__main__":
    main()