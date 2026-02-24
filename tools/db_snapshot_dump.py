#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _truncate(s: str | None, n: int = 200) -> str:
    if not s:
        return ""
    return s if len(s) <= n else (s[: n - 3] + "...")


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return int(n) > 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump DuckDB schema + table counts + ingest audit summary.")
    ap.add_argument("--db", default="data/nfl.duckdb", help="DuckDB path")
    ap.add_argument("--out", required=True, help="Output markdown path")
    ap.add_argument("--max-runs", type=int, default=25, help="How many ingest runs to include")
    ap.add_argument("--max-season-rows", type=int, default=25, help="Max seasons in pbp breakdown")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# DB Snapshot (DuckDB)\n\n")
    lines.append(f"- generated_utc: {utc_now_iso()}\n")
    lines.append(f"- db_path: `{db_path}`\n\n")

    if not db_path.exists():
        lines.append("_DB file does not exist._\n")
        out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
        print(f"DB snapshot written to: {out_path}")
        return 0

    con = duckdb.connect(str(db_path))
    try:
        schemas = ["core", "stg", "audit"]
        lines.append("## Tables\n\n")
        lines.append("| schema | table | rows |\n|---|---|---:|\n")
        tables: list[tuple[str, str]] = []
        for sch in schemas:
            rows = con.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
                """,
                [sch],
            ).fetchall()
            tables.extend([(r[0], r[1]) for r in rows])

        for sch, tbl in tables:
            try:
                cnt = int(con.execute(f"SELECT COUNT(*) FROM {sch}.{tbl};").fetchone()[0])
            except Exception:
                cnt = -1
            lines.append(f"| {sch} | `{tbl}` | {cnt} |\n")

        lines.append("\n## Schema details\n\n")
        for sch, tbl in tables:
            lines.append(f"### `{sch}.{tbl}`\n\n")
            cols = con.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
                """,
                [sch, tbl],
            ).fetchall()
            lines.append("| column | type | nullable |\n|---|---|:---:|\n")
            for c, t, n in cols:
                lines.append(f"| `{c}` | `{t}` | {n} |\n")
            lines.append("\n")

        if _table_exists(con, "audit", "ingest_run"):
            lines.append("## Ingest runs (latest)\n\n")
            runs = con.execute(
                """
                SELECT started_at, component, source, outcome, duration_ms, counts_json,
                       error_class, error_message
                FROM audit.ingest_run
                ORDER BY started_at DESC
                LIMIT ?
                """,
                [int(args.max_runs)],
            ).fetchall()

            lines.append("| started_at | component | source | outcome | ms | counts | error |\n")
            lines.append("|---|---|---|---|---:|---|---|\n")
            for st, comp, src, outc, ms, counts, ecls, emsg in runs:
                c = _truncate(counts, 200)
                err = _truncate(f"{ecls or ''} {emsg or ''}".strip(), 200)
                lines.append(f"| {st} | `{comp}` | `{src}` | {outc} | {ms} | `{c}` | `{err}` |\n")
            lines.append("\n")

        if _table_exists(con, "core", "pbp"):
            lines.append("## core.pbp rows by season\n\n")
            by_season = con.execute(
                """
                SELECT season, COUNT(*) AS rows
                FROM core.pbp
                GROUP BY season
                ORDER BY season
                """
            ).fetchall()
            lines.append("| season | rows |\n|---:|---:|\n")
            for season, rows in by_season[: int(args.max_season_rows)]:
                lines.append(f"| {season} | {rows} |\n")
            if len(by_season) > int(args.max_season_rows):
                lines.append("\n_note: truncated seasons table_\n")
            lines.append("\n")

    finally:
        con.close()

    out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
    print(f"DB snapshot written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())