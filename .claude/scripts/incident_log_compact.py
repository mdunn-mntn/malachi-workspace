#!/usr/bin/env python3
"""Render the on-call incident log as 4-line entries, and lint every one.

The repo runbook §3 keeps the full narrative (mechanism, dead ends, exact numbers) because
that is what the debugger's incident matcher and a future re-diagnosis read. This renders the
INDEX of that record: BLUF / Incident / Solve / PR, one line each, hard-capped, for the
Confluence playbook where nobody reads 8000 characters.

    python3 .claude/scripts/incident_log_compact.py            # markdown table (paste target)
    python3 .claude/scripts/incident_log_compact.py --lint     # cap violations only, exit 1 on any
    python3 .claude/scripts/incident_log_compact.py --entries  # the 4-line blocks
    python3 .claude/scripts/incident_log_compact.py --inject   # refresh the playbook's log block
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
JSONL = os.path.join(ROOT, "on-call", "incident_log.jsonl")
PLAYBOOK = os.path.join(ROOT, "on-call", "ti_on_call_playbook.md")
_BEGIN, _END = "<!-- INCIDENT_LOG:BEGIN -->", "<!-- INCIDENT_LOG:END -->"
RUNBOOK = os.path.join(ROOT, "on-call", "oncall_runbook.md")
CAP_CHARS, CAP_LINES = 120, 4

# '### INC-023 — `dag` `task` — read a source table while its producer was rebuilding it'
_HDR = re.compile(r"^### (INC-\d+)\s*[—-]\s*(.*)$", re.MULTILINE)


def _blufs() -> dict[str, str]:
    """The §3 heading tail is already a one-line BLUF; reuse it rather than write a second one."""
    with open(RUNBOOK, encoding="utf-8") as f:
        text = f.read()
    out = {}
    for inc, tail in _HDR.findall(text):
        parts = [p.strip() for p in re.split(r"\s+[—-]\s+", tail) if p.strip()]
        # drop the leading `dag` `task` fragments; the last segment is the summary
        summary = parts[-1] if parts else tail
        out[inc] = re.sub(r"`", "", summary).strip()
    return out


def _rows() -> list[dict]:
    rows = []
    with open(JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


# Older rows recorded the action as a snake_case token. A token the reader has to decode is a
# variable, not a word, so render it as plain English.
_ACTION_WORDS = {
    "ack": "acknowledged",
    "no": "no",
    "rerun": "re-run",
    "self": "self",
    "heals": "heals",
    "healed": "healed",
    "heal": "heal",
    "ok": "OK",
    "pr": "PR",
    "ds": "ds",
    "id": "id",
    "1198": "#1198",
    "300": "#300",
}


def _humanise(action: str) -> str:
    """A snake_case action token becomes a readable phrase; free text passes through."""
    if not action:
        return ""
    if " " in action:  # already written as prose
        return action
    words = [_ACTION_WORDS.get(w, w) for w in action.split("_")]
    phrase = " ".join(words)
    return phrase[0].upper() + phrase[1:] if phrase else phrase


def _clip(s: str, cap: int = CAP_CHARS) -> str:
    s = " ".join((s or "").split())
    return s if len(s) <= cap else s[: cap - 1].rstrip() + "…"


def entries() -> list[tuple[str, list[str]]]:
    """(incident id, the 4 capped lines)."""
    blufs = _blufs()
    out = []
    for r in _rows():
        inc = r.get("inc") or "INC-???"
        who = "/".join(filter(None, [r.get("dag"), r.get("task")]))
        bluf = blufs.get(inc) or r.get("verdict") or "unclassified"
        verdict = r.get("verdict") or "unclassified"
        lines = [
            _clip(f"BLUF: {bluf[0].upper() + bluf[1:]}. Verdict {verdict}."),
            _clip(f"Incident: {who}. {r.get('signature') or 'no signature recorded'}"),
            _clip(f"Solve: {_humanise(r.get('action') or '') or r.get('note') or 'not recorded'}"),
            _clip(f"PR: {r.get('fix_pr') or 'none'}"),
        ]
        out.append((inc, lines))
    return out


def markdown() -> str:
    rows = ["| Incident | Date | DAG / task | BLUF | Solve | PR |", "|---|---|---|---|---|---|"]
    data = {r.get("inc"): r for r in _rows()}
    for inc, lines in entries():
        r = data.get(inc, {})
        who = "/".join(filter(None, [r.get("dag"), r.get("task")]))
        pr = r.get("fix_pr") or ""
        pr_cell = f"[{pr.rsplit('/', 1)[-1]}]({pr})" if pr else "—"
        rows.append(
            f"| {inc} | {r.get('date', '')} | `{who}` | "
            f"{lines[0][6:]} | {lines[2][7:]} | {pr_cell} |"
        )
    return "\n".join(rows)


def inject() -> int:
    """Rewrite the playbook's generated log block so the page cannot go stale."""
    with open(PLAYBOOK, encoding="utf-8") as f:
        page = f.read()
    if _BEGIN not in page or _END not in page:
        print(f"markers missing in {PLAYBOOK}")
        return 1
    head, rest = page.split(_BEGIN, 1)
    _, tail = rest.split(_END, 1)
    block = (
        f"{_BEGIN}\n"
        "<!-- generated by .claude/scripts/incident_log_compact.py --inject; do not hand-edit -->\n"
        f"{markdown()}\n{_END}"
    )
    with open(PLAYBOOK, "w", encoding="utf-8") as f:
        f.write(head + block + tail)
    print(f"injected {len(entries())} incidents into {os.path.relpath(PLAYBOOK, ROOT)}")
    return 0


def lint() -> int:
    bad = 0
    for inc, lines in entries():
        for i, line in enumerate(lines, 1):
            if len(line) > CAP_CHARS:
                print(f"{inc} line {i}: {len(line)} chars (cap {CAP_CHARS})")
                bad += 1
        if len(lines) > CAP_LINES:
            print(f"{inc}: {len(lines)} lines (cap {CAP_LINES})")
            bad += 1
    print(f"incident log: {len(entries())} entries, {bad} violation(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lint", action="store_true")
    ap.add_argument("--entries", action="store_true")
    ap.add_argument("--inject", action="store_true")
    a = ap.parse_args()
    if a.lint:
        sys.exit(lint())
    if a.inject:
        sys.exit(inject())
    if a.entries:
        for inc, lines in entries():
            print(f"--- {inc}")
            print("\n".join(lines))
            print()
    else:
        print(markdown())
