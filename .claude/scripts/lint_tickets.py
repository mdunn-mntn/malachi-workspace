#!/usr/bin/env python3
"""lint_tickets.py — ticket front-matter linter (the work-side mirror of lint_coverage.py).

Every ticket card (tickets/<name>/summary.md, and epic children one level down) must carry the
machine-readable front-matter tickets/INDEX.md is generated from. The rule that gives the index teeth:
a ticket marked `status: done` must state a real `result` (no buried answers, no "done" with nothing shown).

Checks (per card):
  · front-matter present with doc_type in {ticket, epic}
  · title / date / summary / result all present and non-placeholder
  · status in {backlog, in_progress, blocked, done}
  · status: done  =>  result is real (not empty / '—' / a {template} stub)

Usage: lint_tickets.py [--check]   # default; non-zero exit on any violation (hook/CI friendly)
"""
import argparse, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TDIR = os.path.normpath(os.path.join(HERE, "..", "..", "tickets"))
STATUSES = {"backlog", "in_progress", "blocked", "done"}
PLACEHOLDER = re.compile(r"^\s*$|^[—\-]$|\{.*\}|<=?\s*90|fill|TODO", re.IGNORECASE)


def front_matter(path):
    try:
        lines = open(path, encoding="utf-8").read().split("\n")
    except Exception:
        return None
    if not lines or lines[0].strip() != "---":
        return None
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        return None
    fm = {}
    for l in lines[1:end]:
        m = re.match(r"^(\w+):\s*(.*)$", l)
        if m:
            fm[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return fm


def check(path, rel):
    fm = front_matter(path)
    v = []
    if fm is None:
        return [f"{rel}: no YAML front-matter (add the ticket card block — see folder_definitions.md)"]
    dt = fm.get("doc_type")
    if dt not in ("ticket", "epic"):
        v.append(f"{rel}: doc_type={dt!r} (must be ticket|epic)")
    st = fm.get("status")
    if st not in STATUSES:
        v.append(f"{rel}: status={st!r} (must be {sorted(STATUSES)})")
    for field in ("title", "date", "summary", "result"):
        val = fm.get(field, "")
        if not val or PLACEHOLDER.match(val):
            v.append(f"{rel}: {field} missing/placeholder ({val!r})")
    if st == "done":
        r = fm.get("result", "")
        if not r or PLACEHOLDER.match(r):
            v.append(f"{rel}: status=done but result is empty/placeholder — a done ticket must show its answer")
    return v


def cards():
    out = []
    if not os.path.isdir(TDIR):
        return out
    for name in sorted(os.listdir(TDIR)):
        d = os.path.join(TDIR, name)
        if not os.path.isdir(d) or name.startswith(("_", ".")):
            continue
        if os.path.exists(os.path.join(d, "summary.md")):
            out.append((os.path.join(d, "summary.md"), f"tickets/{name}/summary.md"))
        for c in sorted(os.listdir(d)):
            cd = os.path.join(d, c)
            if os.path.isdir(cd) and not c.startswith(("_", ".")) and os.path.exists(os.path.join(cd, "summary.md")):
                out.append((os.path.join(cd, "summary.md"), f"tickets/{name}/{c}/summary.md"))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.parse_args()
    all_cards = cards()
    violations = []
    for path, rel in all_cards:
        violations += check(path, rel)
    for msg in violations:
        print(f"VIOLATION {msg}", file=sys.stderr)
    print(f"lint_tickets --check: {len(all_cards)} cards, {len(violations)} violation(s).")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
