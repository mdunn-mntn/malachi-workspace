#!/usr/bin/env python3
"""lint_tickets.py — ticket front-matter linter (the work-side mirror of lint_coverage.py).

Every ticket card (tickets/<name>/summary.md, and epic children one level down) must carry the
machine-readable front-matter tickets/INDEX.md is generated from. The rule that gives the index teeth:
a ticket marked `status: done` must state a real `result` (no buried answers, no "done" with nothing shown).

Checks (per card):
  · front-matter present with doc_type in {ticket, epic}
  · title / date / summary all present and non-placeholder
  · status in {backlog, in_progress, blocked, done}
  · status: done  =>  result is real (not empty / '—' / a {template} stub)  [result is required only when done]

Framing gate (the start-of-ticket mirror of the result-when-done rule):
  · framing_state (when present) must be draft | locked | skip: <reason>
  · framing_state: skip  =>  a non-placeholder reason follows the colon
  · status in {in_progress, done}  =>  framing_state is locked or skip (draft is a VIOLATION —
    a ticket can't be "in progress" on an un-agreed question; run /frame, or skip a trivial one)
  · framing_state: locked  =>  the `question` field is real (the head of §0 Framing was filled)
  · a legacy card (no framing_state at all) that is in_progress/done only WARNS — never blocks —
    so adoption is opt-in per ticket, not a retroactive break of the existing corpus.

Usage: lint_tickets.py [--check]   # default; non-zero exit on any VIOLATION (hook/CI friendly). WARNs never fail.
"""

import argparse
import os
import re
import sys
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
TDIR = os.path.normpath(os.path.join(HERE, "..", "..", "tickets"))
STATUSES = {"backlog", "in_progress", "blocked", "done"}
PLACEHOLDER = re.compile(r"^\s*$|^[—\-]$|\{.*\}|<=?\s*90|fill|TODO", re.IGNORECASE)


def front_matter(path):
    try:
        lines = Path(path).read_text(encoding="utf-8").split("\n")
    except Exception:
        return None
    if not lines or lines[0].strip() != "---":
        return None
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        return None
    fm = {}
    for line in lines[1:end]:
        m = re.match(r"^(\w+):\s*(.*)$", line)
        if m:
            fm[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return fm


FRAMING_STATES = {"draft", "locked", "skip"}

# The framing gate was built 2026-07-24. Cards dated before it never had a framing_state to omit,
# and they will not be backfilled — warning on each of them every run produced ~84 permanent lines
# that nobody could ever clear, which trains you to skip the linter's output. They are counted, not
# listed (use --all for the list). A card dated on or after the gate with no framing_state is a real
# miss and still warns by name.
FRAMING_GATE_DATE = "2026-07-24"


def _is_grandfathered(fm):
    d = str(fm.get("date") or "")
    return bool(re.match(r"\d{4}-\d{2}-\d{2}", d)) and d < FRAMING_GATE_DATE


def check(path, rel):
    """Return (violations, warnings, grandfathered) — lists of message strings."""
    fm = front_matter(path)
    v, w, g = [], [], []
    if fm is None:
        return (
            [
                f"{rel}: no YAML front-matter (add the ticket card block — see folder_definitions.md)"
            ],
            w,
            g,
        )
    dt = fm.get("doc_type")
    if dt not in ("ticket", "epic"):
        v.append(f"{rel}: doc_type={dt!r} (must be ticket|epic)")
    st = fm.get("status")
    if st not in STATUSES:
        v.append(f"{rel}: status={st!r} (must be {sorted(STATUSES)})")
    for field in ("title", "date", "summary"):
        val = fm.get(field, "")
        if not val or PLACEHOLDER.match(val):
            v.append(f"{rel}: {field} missing/placeholder ({val!r})")
    # `result` is the blessed final answer — required ONLY when done (an in-progress ticket has no
    # result yet; requiring it everywhere forces fake answers and would trip the commit gate on every
    # touch of a live ticket). This matches the docstring's "status: done => result is real" intent.
    if st == "done":
        r = fm.get("result", "")
        if not r or PLACEHOLDER.match(r):
            v.append(
                f"{rel}: status=done but result is empty/placeholder — a done ticket must show its answer"
            )

    # --- Framing gate ---
    fs_raw = fm.get("framing_state")
    if fs_raw is None:
        # legacy card — never block; nudge only when it's actually being worked
        if st in ("in_progress", "done"):
            msg = f"{rel}: no framing_state (legacy card) — run /frame to add §0 Framing (Question/Goal/Objective/Approach)"
            if _is_grandfathered(fm):
                g.append(msg)
            else:
                w.append(msg)
    else:
        state, _, reason = fs_raw.partition(":")
        state, reason = state.strip(), reason.strip()
        if state not in FRAMING_STATES:
            v.append(f"{rel}: framing_state={fs_raw!r} (must be draft | locked | 'skip: <reason>')")
        else:
            if state == "skip" and (not reason or PLACEHOLDER.match(reason)):
                v.append(
                    f"{rel}: framing_state=skip needs a reason — 'skip: <one-line why this ticket needs no framing>'"
                )
            if st in ("in_progress", "done") and state == "draft":
                v.append(
                    f"{rel}: status={st} but framing_state=draft — run /frame to lock §0, or set framing_state: 'skip: <reason>' for a trivial ticket"
                )
            if state == "locked":
                q = fm.get("question", "")
                if not q or PLACEHOLDER.match(q):
                    v.append(
                        f"{rel}: framing_state=locked but question missing/placeholder — a locked frame must state its question"
                    )
    return v, w, g


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
            if (
                os.path.isdir(cd)
                and not c.startswith(("_", "."))
                and os.path.exists(os.path.join(cd, "summary.md"))
            ):
                out.append((os.path.join(cd, "summary.md"), f"tickets/{name}/{c}/summary.md"))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument(
        "--all",
        action="store_true",
        help=f"also list the pre-{FRAMING_GATE_DATE} legacy cards (normally counted, not listed)",
    )
    args = ap.parse_args()
    all_cards = cards()
    violations, warnings, grandfathered = [], [], []
    for path, rel in all_cards:
        v, w, g = check(path, rel)
        violations += v
        warnings += w
        grandfathered += g
    for msg in warnings:
        print(f"WARN {msg}", file=sys.stderr)
    if grandfathered:
        if args.all:
            for msg in grandfathered:
                print(f"LEGACY {msg}", file=sys.stderr)
        else:
            print(
                f"LEGACY {len(grandfathered)} card(s) predate the framing gate ({FRAMING_GATE_DATE}) "
                f"— not backfilled by design; --all to list",
                file=sys.stderr,
            )
    for msg in violations:
        print(f"VIOLATION {msg}", file=sys.stderr)
    print(
        f"lint_tickets --check: {len(all_cards)} cards, {len(violations)} violation(s), "
        f"{len(warnings)} warning(s), {len(grandfathered)} legacy."
    )
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
