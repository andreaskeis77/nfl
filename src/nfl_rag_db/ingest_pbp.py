from __future__ import annotations

import argparse

from nfl_rag_db.db import connect
from nfl_rag_db.ingest.pbp import ingest_pbp_and_scoring


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest nflverse PBP + scoring timeline")
    parser.add_argument("--db", default=None, help="DuckDB path (default: data/nfl.duckdb)")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g. 2023")
    parser.add_argument("--url", default=None, help="Override pbp parquet URL")
    args = parser.parse_args()

    con = connect(args.db)
    run_id = ingest_pbp_and_scoring(con, season=args.season, url=args.url)
    print(f"OK run_id={run_id}")


if __name__ == "__main__":
    main()