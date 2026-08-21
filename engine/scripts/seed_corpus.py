#!/usr/bin/env python3
"""seed_corpus.py — seed the eval corpus from the retrieval probes.

Reads the `## PROBES` JSON block in knowledge/eval_probes.md and writes one tier-2 case per probe
(checks = probe_reached per must_reach target). Idempotent: skips ids already in the manifest.
"""

import json
import pathlib
import re
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
PROBES = WS / "knowledge" / "eval_probes.md"


def load_probes():
    text = PROBES.read_text()
    m = re.search(r"## PROBES\s*```json\s*(\[.*?\])\s*```", text, re.S)
    if not m:
        return []
    return json.loads(m.group(1))


def main():
    probes = load_probes()
    manifest = ENGINE / "corpus" / "manifest.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            try:  # noqa: SIM105
                seen.add(json.loads(line)["id"])
            except (json.JSONDecodeError, KeyError):
                pass

    written = 0
    new_lines = []
    for i, p in enumerate(probes):
        cid = f"probe-{p['id']}"
        if cid in seen:
            continue
        case = {
            "id": cid,
            "source": "probe",
            "task_prompt": p["question"],
            "workspace_sha": None,
            "checks": [{"type": "probe_reached", "target": t} for t in p.get("must_reach", [])],
            "baseline": {"pass": True, "tokens": 0, "usd": 0, "latency_s": 0},
            "tags": ["retrieval", "seed"],
            "tier": [2],
            "holdout": (i % 5 == 0),  # ~20% holdout
            "added": None,
            "last_green": None,
        }
        cdir = ENGINE / "corpus" / "cases" / cid
        cdir.mkdir(parents=True, exist_ok=True)
        (cdir / "case.json").write_text(json.dumps(case, indent=2) + "\n")
        new_lines.append(
            json.dumps({k: case[k] for k in ("id", "source", "tier", "holdout", "tags")})
        )
        written += 1

    if new_lines:
        with manifest.open("a") as f:
            for ln in new_lines:
                f.write(ln + "\n")
    total_checks = sum(len(p.get("must_reach", [])) for p in probes)
    print(
        f"seed_corpus: {len(probes)} probes -> {written} new cases, "
        f"{total_checks} probe_reached checks total"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
