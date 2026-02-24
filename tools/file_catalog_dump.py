#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import re
import subprocess
from pathlib import Path


def run_cmd(cmd: list[str], cwd: Path) -> tuple[int, str]:
    r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)
    out = (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")
    return r.returncode, out.strip()


def git_ls_files(repo_root: Path) -> list[str]:
    rc, out = run_cmd(["git", "ls-files"], cwd=repo_root)
    if rc != 0 or not out.strip():
        return []
    return [line.strip().replace("\\", "/") for line in out.splitlines() if line.strip()]


_RULES: list[tuple[re.Pattern[str], str, str]] = [
    (
        re.compile(r"^src/nfl_rag_db/ingest/"),
        "Ingestion",
        "Dataset ingestion pipelines",
    ),
    (
        re.compile(r"^src/nfl_rag_db/ingest_.*\\.py$"),
        "Ingestion",
        "CLI entrypoints for ingestion",
    ),
    (
        re.compile(r"^src/nfl_rag_db/"),
        "Core",
        "Core library code",
    ),
    (
        re.compile(r"^tools/"),
        "Tools",
        "Developer tools (handoff, diagnostics)",
    ),
    (
        re.compile(r"^tests/"),
        "Tests",
        "Unit/integration tests",
    ),
    (
        re.compile(r"^docs/adr/"),
        "Docs",
        "Architecture Decision Records (ADRs)",
    ),
    (
        re.compile(r"^docs/concepts/"),
        "Docs",
        "Design / concept documents",
    ),
    (
        re.compile(r"^docs/reference/"),
        "Docs",
        "Reference docs (e.g., manifest)",
    ),
    (
        re.compile(r"^docs/"),
        "Docs",
        "Project documentation",
    ),
    (
        re.compile(r"^pyproject\\.toml$"),
        "Config",
        "Python project configuration",
    ),
    (
        re.compile(r"^\\.gitignore$"),
        "Config",
        "Git ignore rules",
    ),
    (
        re.compile(r"^\\.gitattributes$"),
        "Config",
        "Git attributes / line endings",
    ),
    (
        re.compile(r"^README\\.md$"),
        "Docs",
        "Project README",
    ),
]


def classify(path: str) -> tuple[str, str]:
    for pat, cat, purpose in _RULES:
        if pat.search(path):
            return cat, purpose
    return "Other", "Unclassified"


def first_docstring_line(py_path: Path) -> str:
    try:
        tree = ast.parse(py_path.read_text(encoding="utf-8"))
        doc = ast.get_docstring(tree)
        if not doc:
            return ""
        return doc.strip().splitlines()[0].strip()
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate file catalog with purpose/category.")
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--out", required=True, help="Output markdown path")
    args = ap.parse_args()

    repo_root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(git_ls_files(repo_root), key=lambda s: s.lower())

    lines: list[str] = []
    lines.append("# File Catalog\n\n")
    lines.append("| path | category | purpose | docstring (py) |\n")
    lines.append("|---|---|---|---|\n")

    for rel in files:
        cat, purpose = classify(rel)
        doc = first_docstring_line(repo_root / rel) if rel.endswith(".py") else ""
        lines.append(f"| `{rel}` | {cat} | {purpose} | {doc} |\n")

    out_path.write_text("".join(lines), encoding="utf-8", newline="\n")
    print(f"File catalog written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())