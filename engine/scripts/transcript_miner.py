#!/usr/bin/env python3
"""transcript_miner.py — Claude Code session transcripts -> Tier-2 eval-case skeletons + signals.

Claude Code transcripts (~/.claude/projects/<slug>/*.jsonl) cannot drive dsh's llm-replay (different
event vocabulary), so each becomes a Tier-2 (fresh-run) case skeleton: the first real user prompt +
derived checks. Also mines user-correction turns ("no, actually...") as friction signals. Keyless.

Output: engine/corpus/cases/<case-id>/case.json (gitignored, Mac-local) + a manifest line.
Writes nothing outside engine/. Halts if engine/STOP exists.
"""

import argparse
import hashlib
import json
import pathlib
import re
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
PROJECTS = (
    pathlib.Path.home() / ".claude" / "projects" / "-Users-malachi-Developer-work-mntn-workspace"
)
STOP = ENGINE / "STOP"

CORRECTION_RE = re.compile(
    r"^\s*(no,|not quite|actually|that'?s wrong|too long|wrong|undo|revert|that isn'?t)", re.I
)


def text_of(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return ""


def first_real_prompt(path):
    corrections = 0
    prompt = None
    for line in path.read_text(errors="ignore").splitlines():
        try:  # noqa: SIM105
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        if d.get("type") != "user":
            continue
        t = text_of(d.get("message", {}).get("content")).strip()
        if not t or t.startswith("<") or "command-name" in t or "command-message" in t:
            continue
        if prompt is None and len(t) > 25:
            prompt = t
        if CORRECTION_RE.match(t):
            corrections += 1
    return prompt, corrections


def make_case(session_id, prompt):
    case_id = "cc-" + hashlib.sha256(session_id.encode()).hexdigest()[:12]
    # Tier-2-only skeleton: deterministic checks are left for a human/curator to fill from the
    # ticket Objective; we seed a minimal non-empty-answer check so the schema validates.
    return {
        "id": case_id,
        "source": "claude-code",
        "session_id": session_id,
        "task_prompt": prompt[:2000],
        "workspace_sha": None,
        "checks": [{"type": "output_nonempty"}],
        "baseline": None,
        "tags": ["mined", "tier2"],
        "tier": [2],
        "holdout": False,
        "added": None,
        "last_green": None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="cap number of transcripts (0 = all)")
    ap.add_argument(
        "--emit-cases",
        action="store_true",
        help="write case.json skeletons (default: signals only)",
    )
    args = ap.parse_args()

    if STOP.exists():
        print("engine/STOP present — miner halted", file=sys.stderr)
        return 3
    if not PROJECTS.exists():
        print(f"no transcripts dir at {PROJECTS}", file=sys.stderr)
        return 1

    files = sorted(PROJECTS.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if args.limit:
        files = files[: args.limit]

    cases = 0
    total_corrections = 0
    manifest = ENGINE / "corpus" / "manifest.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            try:  # noqa: SIM105
                seen.add(json.loads(line)["id"])
            except (json.JSONDecodeError, KeyError):
                pass

    new_lines = []
    for path in files:
        prompt, corrections = first_real_prompt(path)
        total_corrections += corrections
        if not prompt:
            continue
        case = make_case(path.stem, prompt)
        if case["id"] in seen:
            continue
        if args.emit_cases:
            cdir = ENGINE / "corpus" / "cases" / case["id"]
            cdir.mkdir(parents=True, exist_ok=True)
            (cdir / "case.json").write_text(json.dumps(case, indent=2) + "\n")
            new_lines.append(
                json.dumps({k: case[k] for k in ("id", "source", "tier", "holdout", "tags")})
            )
            cases += 1
        else:
            cases += 1

    if new_lines:
        with manifest.open("a") as f:
            for ln in new_lines:
                f.write(ln + "\n")

    print(
        f"transcript_miner: {len(files)} transcripts, {cases} case skeletons"
        f"{' written' if args.emit_cases else ' (dry-run)'}, {total_corrections} correction-turns as friction signal"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
