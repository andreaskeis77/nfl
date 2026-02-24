#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
from pathlib import Path


def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def run(cmd: list[str], cwd: Path) -> tuple[int, str]:
    r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)
    out = (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")
    return r.returncode, out.strip()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def copy_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def copy_dir_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    if dst.exists():
        shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)


def main() -> int:
    ap = argparse.ArgumentParser(description="Create a handoff bundle under docs/_snapshot.")
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--db", default="data/nfl.duckdb", help="DuckDB path (relative to root)")
    ap.add_argument(
        "--max-bytes",
        type=int,
        default=1_000_000,
        help="Max bytes per file in dumps",
    )
    ap.add_argument(
        "--include-untracked",
        action="store_true",
        help="Include untracked files in audit/tree",
    )
    ap.add_argument("--zip", action="store_true", help="Create a zip of the snapshot folder")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    snap_root = root / "docs" / "_snapshot"
    stamp = now_stamp()
    out_dir = snap_root / f"handoff_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    steps: list[tuple[str, int, str]] = []
    failed = False

    rc, head = run(["git", "rev-parse", "HEAD"], cwd=root)
    git_head = head.splitlines()[-1].strip() if rc == 0 and head else "unknown"
    rc, br = run(["git", "branch", "--show-current"], cwd=root)
    git_branch = br.strip() if rc == 0 and br else "unknown"
    rc, st = run(["git", "status", "--porcelain=v1"], cwd=root)
    git_dirty = bool(st.strip()) if rc == 0 else False

    # project tree
    tree_out = out_dir / "project_tree.txt"
    cmd = [sys.executable, "tools/project_tree_dump.py", "--root", ".", "--out", str(tree_out)]
    if args.include_untracked:
        cmd.append("--include-untracked")
    rc, out = run(cmd, cwd=root)
    steps.append(("project_tree_dump", rc, str(tree_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "project_tree_dump.stderr.txt", out + "\n")

    # file catalog
    cat_out = out_dir / "file_catalog.md"
    rc, out = run(
        [sys.executable, "tools/file_catalog_dump.py", "--root", ".", "--out", str(cat_out)],
        cwd=root,
    )
    steps.append(("file_catalog_dump", rc, str(cat_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "file_catalog_dump.stderr.txt", out + "\n")

    # source bundle
    src_out = out_dir / "source_bundle.md"
    rc, out = run(
        [
            sys.executable,
            "tools/source_bundle_dump.py",
            "--root",
            ".",
            "--out",
            str(src_out),
            "--max-bytes",
            str(int(args.max_bytes)),
        ],
        cwd=root,
    )
    steps.append(("source_bundle_dump", rc, str(src_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "source_bundle_dump.stderr.txt", out + "\n")

    # project audit
    audit_out = out_dir / "project_audit_dump.md"
    audit_cmd = [
        sys.executable,
        "tools/project_audit_dump.py",
        "--root",
        ".",
        "--out",
        str(audit_out),
        "--max-bytes",
        str(int(args.max_bytes)),
    ]
    if args.include_untracked:
        audit_cmd.append("--include-untracked")
    rc, out = run(audit_cmd, cwd=root)
    steps.append(("project_audit_dump", rc, str(audit_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "project_audit_dump.stderr.txt", out + "\n")

    # data snapshot
    data_out = out_dir / "data_snapshot.md"
    rc, out = run(
        [
            sys.executable,
            "tools/project_data_snapshot.py",
            "--root",
            ".",
            "--out",
            str(data_out),
            "--db",
            args.db,
        ],
        cwd=root,
    )
    steps.append(("project_data_snapshot", rc, str(data_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "project_data_snapshot.stderr.txt", out + "\n")

    # db snapshot
    db_out = out_dir / "db_snapshot.md"
    rc, out = run(
        [sys.executable, "tools/db_snapshot_dump.py", "--db", args.db, "--out", str(db_out)],
        cwd=root,
    )
    steps.append(("db_snapshot_dump", rc, str(db_out)))
    if rc != 0:
        failed = True
        write_text(out_dir / "db_snapshot_dump.stderr.txt", out + "\n")

    # runtime state
    runtime_out = out_dir / "runtime_state.md"
    rc2, pf = run([sys.executable, "-m", "pip", "freeze"], cwd=root)
    pip_freeze = pf if rc2 == 0 else "(pip freeze failed)"
    runtime: list[str] = []
    runtime.append("# Runtime State\n\n")
    runtime.append(f"- generated_local: {dt.datetime.now().isoformat(timespec='seconds')}\n")
    runtime.append(f"- git_branch: {git_branch}\n")
    runtime.append(f"- git_head: {git_head}\n")
    runtime.append(f"- git_dirty: {git_dirty}\n")
    runtime.append(f"- python: {sys.version.split()[0]}\n")
    runtime.append(f"- python_executable: {sys.executable}\n")
    runtime.append(f"- db_path: {args.db}\n\n")
    runtime.append("## Environment (presence only)\n\n")
    for k in ["OPENAI_API_KEY", "HTTP_PROXY", "HTTPS_PROXY"]:
        runtime.append(f"- {k}: {'SET' if os.getenv(k) else 'NOT_SET'}\n")
    runtime.append("\n## pip freeze\n\n```text\n")
    runtime.append(pip_freeze)
    runtime.append("\n```\n")
    write_text(runtime_out, "".join(runtime))
    steps.append(("runtime_state", 0, str(runtime_out)))

    # copy docs
    copy_dir_if_exists(root / "docs" / "adr", out_dir / "docs" / "adr")
    copy_dir_if_exists(root / "docs" / "concepts", out_dir / "docs" / "concepts")
    copy_dir_if_exists(root / "docs" / "reference", out_dir / "docs" / "reference")
    copy_if_exists(root / "README.md", out_dir / "README.md")
    copy_if_exists(root / "pyproject.toml", out_dir / "pyproject.toml")
    if (root / "docs" / "HANDOFF_MANIFEST.md").exists():
        copy_if_exists(root / "docs" / "HANDOFF_MANIFEST.md", out_dir / "HANDOFF_MANIFEST.md")

    # summary
    summary_out = out_dir / "handoff_summary.md"
    summ: list[str] = []
    summ.append("# Handoff Summary\n\n")
    summ.append(f"- timestamp: {stamp}\n")
    summ.append(f"- git_branch: {git_branch}\n")
    summ.append(f"- git_head: {git_head}\n")
    summ.append(f"- git_dirty: {git_dirty}\n")
    summ.append(f"- db_path: {args.db}\n")
    summ.append(f"- status: {'FAILED' if failed else 'OK'}\n\n")
    summ.append("## Steps\n\n| step | rc | output |\n|---|---:|---|\n")
    for name, rc, p in steps:
        summ.append(f"| `{name}` | {rc} | `{p}` |\n")
    summ.append("\n")
    write_text(summary_out, "".join(summ))

    # latest
    latest_dir = snap_root / "latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir, ignore_errors=True)
    shutil.copytree(out_dir, latest_dir)

    # zip
    if args.zip:
        import zipfile

        zip_path = snap_root / f"handoff_{stamp}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in out_dir.rglob("*"):
                if p.is_file():
                    zf.write(p, arcname=str(p.relative_to(out_dir)))
        steps.append(("zip", 0, str(zip_path)))
        print(f"Zip written to: {zip_path}")

    print(f"Handoff written to: {out_dir}")
    print(f"Latest updated: {latest_dir}")
    if failed:
        print("Handoff FAILED (see handoff_summary.md).")
        return 2
    print("Handoff OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())