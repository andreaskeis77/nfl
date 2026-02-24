#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def dir_size_bytes(p: Path) -> int:
    total = 0
    for root, _, files in os.walk(p):
        for fn in files:
            fp = Path(root) / fn
            try:
                total += fp.stat().st_size
            except OSError:
                continue
    return total


def count_files(p: Path) -> int:
    n = 0
    for _, _, files in os.walk(p):
        n += len(files)
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description="Snapshot data folder (sizes, counts) without copying data.")
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--out", required=True, help="Output markdown path")
    ap.add_argument("--db", default="data/nfl.duckdb", help="DuckDB path (relative to root)")
    args = ap.parse_args()

    repo_root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data_dir = repo_root / "data"
    raw_dir = data_dir / "raw"
    db_path = (repo_root / args.db).resolve()

    lines: list[str] = []
    lines.append("# Data Snapshot\n\n")
    lines.append(f"- generated_utc: {utc_now_iso()}\n")
    lines.append(f"- repo_root: `{repo_root}`\n\n")

    if not data_dir.exists():
        lines.append("_No `data/` directory found._\n")
        out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
        print(f"Data snapshot written to: {out_path}")
        return 0

    lines.append("## DB file\n\n")
    if db_path.exists():
        lines.append(f"- path: `{db_path}`\n")
        lines.append(f"- size_bytes: {db_path.stat().st_size}\n")
    else:
        lines.append(f"- path: `{db_path}` (missing)\n")
    lines.append("\n")

    lines.append("## Raw data (folder-level)\n\n")
    if raw_dir.exists():
        lines.append(f"- raw_dir: `{raw_dir}`\n")
        lines.append(f"- total_files: {count_files(raw_dir)}\n")
        lines.append(f"- total_size_bytes: {dir_size_bytes(raw_dir)}\n\n")

        lines.append("| dataset | files | size_bytes |\n|---|---:|---:|\n")
        for child in sorted([p for p in raw_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            lines.append(f"| `{child.name}` | {count_files(child)} | {dir_size_bytes(child)} |\n")
        lines.append("\n")
    else:
        lines.append("_No `data/raw` directory found._\n\n")

    out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
    print(f"Data snapshot written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())