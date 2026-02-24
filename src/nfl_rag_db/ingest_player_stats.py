from __future__ import annotations

import argparse

from nfl_rag_db.db import connect
from nfl_rag_db.ingest.player_stats import PLAYER_STATS_URL, ingest_player_stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest nflverse player_stats into DuckDB")
    parser.add_argument("--db", default=None, help="DuckDB path (default: data/nfl.duckdb)")
    parser.add_argument("--season-min", type=int, default=2011, help="Minimum season to keep")
    parser.add_argument("--url", default=PLAYER_STATS_URL, help="Override parquet URL")
    args = parser.parse_args()

    con = connect(args.db)
    run_id = ingest_player_stats(con, season_min=args.season_min, url=args.url)
    print(f"OK run_id={run_id}")


if __name__ == "__main__":
    main()