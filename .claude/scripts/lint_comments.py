#!/usr/bin/env python3
"""Block multi-line comment blocks in code. One line max, per the standing rule.

Usage:
    lint_comments.py <file> [<file> ...]
    lint_comments.py --staged
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

EXTS = (".py", ".sh", ".bash", ".yaml", ".yml", ".tf", ".hcl", ".sql", ".js", ".ts")
HEADER_LINES = 12  # a block ending by this line is the allowed usage header
MAX_RUN = 1  # consecutive full-line comments outside the header


def _is_comment(line: str, ext: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if ext == ".sql":
        return s.startswith("--")
    if ext in (".js", ".ts"):
        return s.startswith("//")
    if ext in (".tf", ".hcl"):
        return s.startswith(("#", "//"))
    return s.startswith("#") and not s.startswith("#!")


def _staged_content(path: str) -> str:
    r = subprocess.run(["git", "show", f":{path}"], capture_output=True, check=False)
    return r.stdout.decode("utf-8", errors="replace") if r.returncode == 0 else ""


def check(path: str, text: str | None = None) -> list[str]:
    """Return one message per over-long comment block."""
    ext = os.path.splitext(path)[1]
    if text is None:
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            return []
    out, run, start = [], 0, 0
    for i, line in enumerate([*text.splitlines(), ""], 1):
        if _is_comment(line, ext):
            if run == 0:
                start = i
            run += 1
            continue
        if run > MAX_RUN and start + run - 1 > HEADER_LINES:
            out.append(
                f"{path}:{start}-{start + run - 1}  {run}-line comment block (cap {MAX_RUN})"
            )
        run = 0
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="One-line comments only, outside a short header.")
    ap.add_argument("files", nargs="*")
    ap.add_argument("--staged", action="store_true")
    args = ap.parse_args()

    files = args.files
    if args.staged:
        r = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
            capture_output=True,
            text=True,
            check=False,
        )
        files = r.stdout.split()
    files = [f for f in files if f.endswith(EXTS)]
    if not files:
        print("[comments] nothing to check")
        return 0

    hits = [m for f in files for m in check(f, _staged_content(f) if args.staged else None)]
    for m in hits:
        print(f"VIOLATION {m}")
    if hits:
        print(
            f"[comments] {len(hits)} block(s) over {MAX_RUN} line. Put the why in the PR "
            "description or commit message, not the file."
        )
        return 1
    print(f"[comments] {len(files)} file(s) OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
