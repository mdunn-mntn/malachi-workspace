#!/usr/bin/env python3
"""lint_comms.py — the terse-comms linter for outward-facing writing.

Sibling of lint_tickets.py / lint_coverage.py, but for the *prose* that other people read:
Jira comments, Jira ticket descriptions, and .xlsx read-me / notes cells. Enforces the MNTN
Terse Comms Standard (CLAUDE.md §9): lead with the answer, facts only, hard character caps,
no hedging / throat-clearing / editorializing.

Two failure classes:
  · VIOLATION — over a hard cap, or contains an em-dash (a standing MNTN rule). Exit 1.
  · TRIM      — hedge / throat-clearing / editorial filler word. Advisory; does not fail the CLI.

Caps (chars / words / bullets):
  comment      500 / 75  / 5    progress or blocker update
  completion   800 / 120 / 8    ticket-completion comment (needs room for findings)
  description  400 / 60  / 4    Jira ticket description body
  xlsx         200-per-line, 12 lines max    read-me / notes cell
  pr           900 / 130 / 10   PR description (lead line then What / Why / Validation)
  pr_comment   500 / 75  / 5    PR review comment or reply
  commit       500 / 75  / 6    commit message; subject (first line) also capped at 72 chars

Usage:
  lint_comms.py --kind comment --file draft.txt
  echo "text"           | lint_comms.py --kind comment
  lint_comms.py --kind completion --body "..."
  lint_comms.py --from-json --file payload.json      # parse a Jira REST v2 payload
  lint_comms.py --hook                               # PreToolUse hook: lints a Jira curl, never blocks
"""

import argparse
import json
import re
import sys
from pathlib import Path

CAPS = {
    "comment": {"chars": 500, "words": 75, "bullets": 5},
    "completion": {"chars": 800, "words": 120, "bullets": 8},
    "description": {"chars": 400, "words": 60, "bullets": 4},
    "xlsx": {"chars": 200, "words": 30, "lines": 12},  # terse notes cell; chars = per-line cap
    "xlsx_explainer": {
        "chars": 340,
        "words": 60,
        "lines": 10,
    },  # narrative Read-me / Method sheet; chars = per-section cap, lines = sections.
    # Caps track AUDI-1172 (the reference workbook): 9 sections, longest body 330 chars.
    "pr": {
        "chars": 900,
        "words": 130,
        "bullets": 10,
    },  # PR description: lead line (what+why) → What / Why / Validation
    "pr_comment": {
        "chars": 500,
        "words": 75,
        "bullets": 5,
    },  # PR review comment / reply (same bar as a Jira comment)
    "commit": {
        "chars": 500,
        "words": 75,
        "bullets": 6,
    },  # commit message (subject + terse body); subject also capped below
}
LINE_KINDS = {"xlsx", "xlsx_explainer"}  # measured per line/section, not as one blob
TITLE_CAP = 120  # Jira summary/title (hard Jira limit is 255; our guidance is far tighter)
COMMIT_SUBJECT_CAP = 72  # commit subject line (first line) — git convention, hard cap

HEDGES = [
    "i think",
    "i believe",
    "i feel",
    "in my opinion",
    "imo",
    "seems",
    "appears",
    "sort of",
    "kind of",
    "probably",
    "maybe",
    "might be",
    "may be",
    "could be",
    "should be",
    "arguably",
    "presumably",
    "fairly",
    "somewhat",
    "i guess",
    "i suppose",
]
THROAT = [
    "in order to",
    "it is worth noting",
    "it's worth noting",
    "it should be noted",
    "as mentioned",
    "as you know",
    "as you may know",
    "needless to say",
    "it is important to note",
    "it's important to note",
    "just wanted to",
    "wanted to let you know",
    "for what it's worth",
    "at the end of the day",
    "that being said",
    "with that being said",
    "please note that",
    "quick note",
]
EDITORIAL = [
    "significant",
    "significantly",
    "interesting",
    "interestingly",
    "robust",
    "huge",
    "massive",
    "very",
    "really",
    "extremely",
    "incredibly",
    "notably",
    "remarkable",
    "remarkably",
    "clearly",
    "obviously",
    "basically",
    "essentially",
    "actually",
    "simply",
]
DASHES = {"—": "em-dash", "–": "en-dash"}


def _phrase_hits(text, phrases):
    low = text.lower()
    hits = []
    for p in phrases:
        # word-boundary match so "may be" doesn't fire inside "maybe", etc.
        if re.search(r"(?<![a-z])" + re.escape(p) + r"(?![a-z])", low):
            hits.append(p)
    return hits


def lint_text(text, kind):
    """Return (violations, warnings, stats) for one block of prose."""
    cap = CAPS[kind]
    violations, warnings = [], []
    chars = len(text)
    words = len(text.split())
    lines = [line for line in text.splitlines() if line.strip()]
    bullets = sum(1 for line in lines if line.lstrip().startswith(("*", "-", "•")))

    if kind in LINE_KINDS:
        unit = "section" if kind == "xlsx_explainer" else "line"
        over = [(i + 1, len(line)) for i, line in enumerate(lines) if len(line) > cap["chars"]]
        for ln, n in over:
            violations.append(f"{unit} {ln} is {n} chars (cap {cap['chars']}/{unit})")
        if len(lines) > cap["lines"]:
            violations.append(f"{len(lines)} {unit}s (cap {cap['lines']})")
        stats = (
            f"{len(lines)} {unit}s, longest {max((len(line) for line in lines), default=0)} chars"
        )
    else:
        if chars > cap["chars"]:
            violations.append(
                f"{chars} chars (cap {cap['chars']}) — over by {chars - cap['chars']}"
            )
        if words > cap["words"]:
            violations.append(
                f"{words} words (cap {cap['words']}) — over by {words - cap['words']}"
            )
        if bullets > cap["bullets"]:
            violations.append(f"{bullets} bullets (cap {cap['bullets']})")
        stats = f"{chars} chars / {words} words / {bullets} bullets  (cap {cap['chars']}/{cap['words']}/{cap['bullets']})"

    if kind == "commit" and lines and len(lines[0]) > COMMIT_SUBJECT_CAP:
        violations.append(
            f"commit subject {len(lines[0])} chars (cap {COMMIT_SUBJECT_CAP}) — tighten the first line"
        )

    for d, name in DASHES.items():
        if d in text:
            violations.append(f"contains {name} '{d}' — use a period or a comma (standing rule)")

    h = _phrase_hits(text, HEDGES)
    t = _phrase_hits(text, THROAT)
    e = _phrase_hits(text, EDITORIAL)
    if h:
        warnings.append("hedging (state it or cut it): " + ", ".join(h))
    if t:
        warnings.append("throat-clearing (delete): " + ", ".join(t))
    if e:
        warnings.append("editorializing (facts, not adjectives): " + ", ".join(e))
    return violations, warnings, stats


def _report(label, text, kind):
    violations, warnings, stats = lint_text(text, kind)
    status = "OVER" if violations else "OK"
    print(f"[{kind}{('/' + label) if label else ''}] {stats}  {status}")
    for v in violations:
        print(f"VIOLATION {label or kind}: {v}", file=sys.stderr)
    for w in warnings:
        print(f"TRIM {label or kind}: {w}", file=sys.stderr)
    return bool(violations)


def _jobs_from_payload(payload):
    """Turn a Jira REST v2 body into (label, text, kind) lint jobs."""
    jobs = []
    if isinstance(payload, dict) and "fields" in payload:  # issue-create
        f = payload["fields"]
        if f.get("summary"):
            jobs.append(
                ("summary/title", f["summary"], "description")
            )  # title uses TITLE_CAP below
        if f.get("description"):
            jobs.append(("description", f["description"], "description"))
    elif isinstance(payload, dict) and "body" in payload:  # comment
        body = payload["body"]
        kind = "completion" if re.search(r"\bcompleted\b", body, re.IGNORECASE) else "comment"
        jobs.append(("", body, kind))
    return jobs


def _lint_title(text):
    n = len(text)
    if n > TITLE_CAP:
        print(
            f"VIOLATION summary/title: {n} chars (cap {TITLE_CAP}) — tighten the title",
            file=sys.stderr,
        )
        print(f"[title] {n} chars (cap {TITLE_CAP})  OVER")
        return True
    print(f"[title] {n} chars (cap {TITLE_CAP})  OK")
    return False


def _extract_curl_payload(command):
    """Best-effort: pull the -d / --data JSON out of a Jira REST v2 curl. None if not a Jira write."""
    if "atlassian.net/rest/api/" not in command:
        return None
    m = re.search(r"(?:-d|--data(?:-raw)?)\s+'(.*)'\s*$", command, re.DOTALL)
    if not m:
        m = re.search(r'(?:-d|--data(?:-raw)?)\s+"(.*)"\s*$', command, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1), strict=False)  # tolerate raw newlines in a hand-built body
    except Exception:
        return None


def run_hook():
    """PreToolUse(Bash) hook. Lints a Jira comment/issue curl before it posts. NEVER blocks (exit 0)."""
    try:
        data = json.load(sys.stdin)
    except Exception:
        return 0
    if data.get("tool_name") != "Bash":
        return 0
    command = (data.get("tool_input") or {}).get("command", "")
    payload = _extract_curl_payload(command)
    if payload is None:
        return 0
    jobs = _jobs_from_payload(payload)
    if not jobs:
        return 0
    print(
        "[comms-lint] checking Jira text against the Terse Comms Standard (advisory):",
        file=sys.stderr,
    )
    for label, text, kind in jobs:
        if label == "summary/title":
            _lint_title(text)
        else:
            violations, warnings, stats = lint_text(text, kind)
            tag = "OVER" if violations else "OK"
            print(f"[comms-lint] {label or kind}: {stats}  {tag}", file=sys.stderr)
            for v in violations:
                print(f"  VIOLATION {v}", file=sys.stderr)
            for w in warnings:
                print(f"  TRIM {w}", file=sys.stderr)
    print("  → over cap? cut to the answer line + Done/Next. See CLAUDE.md §9.", file=sys.stderr)
    return 0  # advisory: never block a post. Change to `return 2` to make it a hard gate.


def main():
    ap = argparse.ArgumentParser(
        description="Lint outward-facing prose against the Terse Comms Standard."
    )
    ap.add_argument("--kind", choices=list(CAPS), default="comment")
    ap.add_argument("--file")
    ap.add_argument("--body")
    ap.add_argument(
        "--from-json",
        action="store_true",
        help="input is a Jira REST v2 payload; extract + lint each field",
    )
    ap.add_argument(
        "--hook",
        action="store_true",
        help="PreToolUse hook mode: lint a Jira curl on stdin, never block",
    )
    args = ap.parse_args()

    if args.hook:
        return run_hook()

    if args.body is not None:
        raw = args.body
    elif args.file:
        raw = Path(args.file).read_text(encoding="utf-8")
    else:
        raw = sys.stdin.read()

    if args.from_json:
        payload = json.loads(raw, strict=False)
        failed = False
        for label, text, kind in _jobs_from_payload(payload):
            failed |= _lint_title(text) if label == "summary/title" else _report(label, text, kind)
        return 1 if failed else 0

    return 1 if _report("", raw, args.kind) else 0


if __name__ == "__main__":
    sys.exit(main())
