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

EXTS = (".py", ".sh", ".yaml", ".yml", ".sql", ".js")
DURABLE_PREFIXES = ("lib/", ".claude/scripts/")
HEADER_LINES = 12
MAX_RUN = 1  # consecutive full-line comments outside the header


def _is_comment(line: str, ext: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if ext == ".sql":
        return s.startswith("--")
    if ext == ".js":
        return s.startswith("//")
    return s.startswith("#") and not s.startswith("#!")


def _header_span(lines: list[str], ext: str) -> tuple[int, int]:
    """Return the 1-based span of a compliant top-of-file usage header, else (0, 0)."""
    i = 0
    while i < len(lines) and (not lines[i].strip() or lines[i].startswith("#!")):
        i += 1
    start = i
    while i < len(lines) and _is_comment(lines[i], ext):
        i += 1
    if 0 < i - start <= HEADER_LINES:
        return (start + 1, i)
    return (0, 0)


def _git_show(spec: str) -> str | None:
    r = subprocess.run(["git", "show", spec], capture_output=True, check=False)
    return r.stdout.decode("utf-8", errors="replace") if r.returncode == 0 else None


def check(path: str, text: str | None = None) -> list[str]:
    """Return one message per over-long comment block."""
    ext = os.path.splitext(path)[1]
    if text is None:
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            return []
    lines = text.splitlines()
    header_start, _ = _header_span(lines, ext)
    out, run, start = [], 0, 0
    for i, line in enumerate([*lines, ""], 1):
        if _is_comment(line, ext):
            if run == 0:
                start = i
            run += 1
            continue
        if run > MAX_RUN and start != header_start:
            out.append(
                f"{path}:{start}-{start + run - 1}  {run}-line comment block (cap {MAX_RUN})"
            )
        run = 0
    return out


def _staged_files() -> list[str]:
    r = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z"],
        capture_output=True,
        text=True,
        check=False,
    )
    return [f for f in r.stdout.split("\0") if f.startswith(DURABLE_PREFIXES)]


def main() -> int:
    ap = argparse.ArgumentParser(description="One-line comments only, outside a short header.")
    ap.add_argument("files", nargs="*")
    ap.add_argument("--staged", action="store_true")
    args = ap.parse_args()

    files = [f for f in (_staged_files() if args.staged else args.files) if f.endswith(EXTS)]
    if not files:
        print("[comments] nothing to check")
        return 0

    hits = []
    for f in files:
        if args.staged:
            staged = _git_show(f":{f}")
            if staged is None:
                sys.exit(f"[comments] cannot read staged content of {f}")
            viols = check(f, staged)
            # ratchet: pre-existing debt passes; a new block raises the count and fails
            if viols and len(viols) <= len(check(f, _git_show(f"HEAD:{f}") or "")):
                continue
            hits += viols
        else:
            hits += check(f)
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
