from __future__ import annotations

import argparse

from nfl_rag_db.db import connect
from nfl_rag_db.ingest.nfldata import ingest_nfldata_core


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest nfldata teams+games into DuckDB")
    parser.add_argument("--db", default=None, help="DuckDB path (default: data/nfl.duckdb)")
    parser.add_argument(
    "--season-min",
    type=int,
    default=2011,
    help="Minimum season to keep in core tables",
)
    args = parser.parse_args()

    con = connect(args.db)
    run_id = ingest_nfldata_core(con, season_min=args.season_min)
    print(f"OK run_id={run_id}")


if __name__ == "__main__":
    main()