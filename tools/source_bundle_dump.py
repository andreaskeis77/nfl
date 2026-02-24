#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import subprocess
from pathlib import Path

DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "data",
    "docs/_snapshot",
    "node_modules",
    "dist",
    "build",
}

DEFAULT_EXCLUDE_GLOBS = [
    "*.pyc",
    "*.pyo",
    "*.db",
    "*.duckdb",
    "*.sqlite",
    "*.sqlite3",
    "*.parquet",
    "*.csv",
    "*.zip",
    "*.7z",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.webp",
    "*.pdf",
]


def run_cmd(cmd: list[str], cwd: Path) -> tuple[int, str]:
    r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)
    out = (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")
    return r.returncode, out.strip()


def git_ls_files(repo_root: Path) -> list[str]:
    rc, out = run_cmd(["git", "ls-files"], cwd=repo_root)
    if rc != 0 or not out.strip():
        return []
    return [line.strip().replace("\\", "/") for line in out.splitlines() if line.strip()]


def should_exclude(rel: str) -> bool:
    parts = set(Path(rel).parts)
    if "docs" in parts and "_snapshot" in parts:
        return True
    if parts & DEFAULT_EXCLUDE_DIRS:
        return True

    rel_norm = rel.replace("\\", "/")
    name = Path(rel_norm).name
    for g in DEFAULT_EXCLUDE_GLOBS:
        if fnmatch.fnmatch(name, g) or fnmatch.fnmatch(rel_norm, g):
            return True
    return False


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def is_probably_binary(data: bytes) -> bool:
    if not data:
        return False
    if b"\x00" in data:
        return True
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)))
    nontext = data.translate(None, text_chars)
    return len(nontext) / max(1, len(data)) > 0.30


def language_tag(path: Path) -> str:
    ext = path.suffix.lower().lstrip(".")
    return {
        "py": "python",
        "ps1": "powershell",
        "md": "markdown",
        "toml": "toml",
        "yml": "yaml",
        "yaml": "yaml",
        "json": "json",
        "txt": "text",
    }.get(ext, "text")


def main() -> int:
    ap = argparse.ArgumentParser(description="Create a single-file source bundle (tracked files).")
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--out", required=True, help="Output markdown path")
    ap.add_argument("--max-bytes", type=int, default=500_000, help="Max bytes per file")
    args = ap.parse_args()

    repo_root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rels = [r for r in git_ls_files(repo_root) if not should_exclude(r)]
    rels = sorted(rels, key=lambda s: s.lower())

    lines: list[str] = []
    lines.append("# Source Bundle (tracked files)\n\n")
    lines.append(f"- repo_root: `{repo_root}`\n")
    lines.append(f"- files_included: {len(rels)}\n")
    lines.append(f"- max_bytes_per_file: {int(args.max_bytes)}\n\n")
    lines.append("---\n\n")

    for rel in rels:
        p = repo_root / rel
        try:
            raw_full = p.read_bytes()
        except Exception as e:
            lines.append(f"## `{rel}`\n\n")
            lines.append(f"_ERROR reading file: {e}_\n\n---\n\n")
            continue

        size = len(raw_full)
        if is_probably_binary(raw_full[:4096]):
            lines.append(f"## `{rel}`\n\n")
            lines.append(f"- size: {size}\n")
            lines.append(f"- sha256: `{sha256_bytes(raw_full)}`\n")
            lines.append("_binary omitted_\n\n---\n\n")
            continue

        raw = raw_full
        note = ""
        if size > int(args.max_bytes):
            raw = raw[: int(args.max_bytes)]
            note = f"TRUNCATED to {int(args.max_bytes)} bytes"

        text = raw.decode("utf-8", errors="replace")
        lines.append(f"## `{rel}`\n\n")
        lines.append(f"- size: {size}\n")
        lines.append(f"- sha256: `{sha256_bytes(raw_full)}`\n")
        if note:
            lines.append(f"- note: {note}\n")
        lines.append("\n")
        lines.append(f"```{language_tag(p)}\n")
        lines.append(text.rstrip("\n"))
        lines.append("\n```\n\n---\n\n")

    out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
    print(f"Source bundle written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())