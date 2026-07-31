#!/usr/bin/env python3
"""request_digest.py — mine the request log for recurring work shapes; PROPOSE (never create) a skill.

Reads knowledge/.request_log.jsonl (keyword-only records written by the log_request UserPromptSubmit
hook) and surfaces what the user keeps asking for: top verbs, top domain nouns, and verb+noun pairs that
recur enough to be worth a /skill or a runbook. It PROPOSES only — turning a proposal into a skill is a
human call (autonomous skill creation is a named anti-goal).

Read-only. Run it on demand (e.g. during a weekly review or /capture): `request_digest.py [--min N]`.
"""

import argparse
import contextlib
import json
import os
from collections import Counter

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
LOG = os.path.join(ROOT, "knowledge", ".request_log.jsonl")


def load():
    if not os.path.exists(LOG):
        return []
    out = []
    with open(LOG, encoding="utf-8") as _fh:
        for ln in _fh:
            ln = ln.strip()
            if not ln:
                continue
            with contextlib.suppress(Exception):
                out.append(json.loads(ln))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min", type=int, default=4, help="recurrence threshold to propose a skill")
    ap.add_argument("--top", type=int, default=12)
    args = ap.parse_args()

    recs = load()
    if not recs:
        print(
            "request_digest: no requests logged yet (knowledge/.request_log.jsonl is empty or absent)."
        )
        print("  The UserPromptSubmit hook populates it as you work; re-run after a few sessions.")
        return 0

    verbs = Counter(r.get("verb") for r in recs if r.get("verb"))
    nouns = Counter(n for r in recs for n in r.get("nouns", []))
    # A "shape" = the request's verb + its single most salient (globally-rare-but-here-frequent) noun.
    pairs = Counter()
    for r in recs:
        v = r.get("verb")
        if not v:
            continue
        for n in r.get("nouns", []):
            pairs[(v, n)] += 1

    span = f"{recs[0].get('ts', '?')[:10]} → {recs[-1].get('ts', '?')[:10]}"
    print(f"request_digest: {len(recs)} requests logged ({span})\n")

    print("Top verbs (what you ask FOR):")
    for v, c in verbs.most_common(args.top):
        print(f"  {c:>3}×  {v}")
    print("\nTop domain nouns (what you ask ABOUT):")
    for n, c in nouns.most_common(args.top):
        print(f"  {c:>3}×  {n}")

    proposals = [(vn, c) for vn, c in pairs.most_common() if c >= args.min]
    print(f"\nRecurring shapes (verb+noun, ≥ {args.min}× → skill candidates):")
    if proposals:
        for (v, n), c in proposals[: args.top]:
            print(f"  {c:>3}×  {v} … {n}   → consider a /skill or runbook for '{v} {n}'")
        print(
            "\n→ PROPOSAL ONLY. A human decides whether any of these becomes a skill (skills are not auto-created)."
        )
    else:
        print(f"  (nothing recurs ≥ {args.min}× yet — keep working; re-run later.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
