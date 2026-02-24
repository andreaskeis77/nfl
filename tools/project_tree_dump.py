#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from typing import List


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


def run_cmd(cmd: List[str], cwd: Path) -> tuple[int, str]:
    r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)
    out = (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")
    return r.returncode, out.strip()


def git_ls_files(repo_root: Path) -> list[str]:
    rc, out = run_cmd(["git", "ls-files"], cwd=repo_root)
    if rc != 0 or not out.strip():
        return []
    return [line.strip().replace("\\", "/") for line in out.splitlines() if line.strip()]


def walk_all_files(repo_root: Path) -> list[str]:
    out: list[str] = []
    for root, _, files in os.walk(repo_root):
        for fn in files:
            p = Path(root) / fn
            rel = str(p.relative_to(repo_root)).replace("\\", "/")
            out.append(rel)
    return sorted(out, key=lambda s: s.lower())


def should_exclude(rel: str, exclude_dirs: set[str]) -> bool:
    parts = set(Path(rel).parts)
    if "docs" in parts and "_snapshot" in parts:
        return True
    return bool(parts & exclude_dirs)


def tree_view(paths: list[str]) -> str:
    tree: dict = {}
    for p in paths:
        cur = tree
        parts = Path(p).parts
        for part in parts[:-1]:
            cur = cur.setdefault(part, {})
        cur.setdefault(parts[-1], None)

    def render(node: dict, prefix: str = "") -> list[str]:
        lines: list[str] = []
        keys = sorted(node.keys(), key=lambda s: s.lower())
        for i, k in enumerate(keys):
            last = i == len(keys) - 1
            connector = "└── " if last else "├── "
            lines.append(prefix + connector + k)
            child = node[k]
            if isinstance(child, dict):
                extension = "    " if last else "│   "
                lines.extend(render(child, prefix + extension))
        return lines

    return "\n".join(render(tree))


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump project tree (tracked files by default).")
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--out", required=True, help="Output file (txt)")
    ap.add_argument("--include-untracked", action="store_true", help="Use os.walk instead of git ls-files")
    args = ap.parse_args()

    repo_root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rels = walk_all_files(repo_root) if args.include_untracked else git_ls_files(repo_root)
    rels = [r for r in rels if not should_exclude(r, DEFAULT_EXCLUDE_DIRS)]

    text = []
    text.append(f"repo_root: {repo_root}\n")
    text.append(f"mode: {'os.walk' if args.include_untracked else 'git ls-files'}\n")
    text.append(f"files: {len(rels)}\n\n")
    text.append(tree_view(rels) if rels else "(no files)\n")

    out_path.write_text("".join(text), encoding="utf-8", newline="\n")
    print(f"Tree written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())