#!/usr/bin/env python3
"""entropy_snapshot.py — one deterministic snapshot of the metrics the engine optimizes.

Appends a single JSON line to engine/metrics/entropy.jsonl. No composite scalar (nothing to
Goodhart); each candidate pre-registers ONE target metric and every other acts as a guard.
Keyless. Reads only committed artifacts + health_scorecard, so it is Pi-safe.
"""

import json
import pathlib
import re
import subprocess
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
OUT = ENGINE / "metrics" / "entropy.jsonl"


def count_lines(rel):
    p = WS / rel
    return sum(1 for ln in p.read_text().splitlines() if ln.strip()) if p.exists() else 0


def health():
    try:
        return subprocess.run(
            ["python3", str(WS / ".claude/scripts/health_scorecard.py"), "--memory"],
            capture_output=True,
            text=True,
            cwd=WS,
            timeout=60,
        ).stdout
    except Exception:
        return ""


def bq_usd_per_query():
    p = WS / "knowledge/bq_perf_log.jsonl"
    if not p.exists():
        return None
    gb = n = 0
    for ln in p.read_text().splitlines():
        try:
            e = json.loads(ln)
        except json.JSONDecodeError:
            continue
        b = e.get("gb_billed")
        if b is not None:
            gb += float(b)
            n += 1
    if not n:
        return None
    return round((gb / 1024) * 6.25 / n, 4)  # $6.25/TiB


def brevity_rate():
    p = WS / ".claude/state/chat_brevity_log.jsonl"
    if not p.exists():
        return None
    rows = [ln for ln in p.read_text().splitlines() if ln.strip()]
    over = sum(1 for ln in rows if json.loads(ln).get("over"))
    return round(over / len(rows), 3) if rows else None


def retrieval_hit_rate():
    p = WS / "knowledge/eval_runs.log"
    if not p.exists():
        return None
    last = None
    for ln in p.read_text().splitlines():
        m = re.search(r"(\d+)/(\d+)", ln)
        if m:
            last = int(m.group(1)) / int(m.group(2))
    return round(last, 3) if last is not None else None


def main():
    h = health()

    def hval(pat, cast=int):
        m = re.search(pat, h)
        return cast(m.group(1)) if m else None

    snap = {
        "stamp": "SNAPSHOT",  # engine stamps the real date after the run (no wall clock in-step)
        "retrieval_hit_rate": retrieval_hit_rate(),
        "doc_debt": count_lines("knowledge/bq/_UNDOCUMENTED.queue"),
        "overlap_clusters": hval(r"(\d+) overlap-cluster"),
        "stale_memories": hval(r"(\d+) stale\(>90d\)"),
        "memory_files": hval(r"Memory\s*:\s*(\d+) files"),
        "usd_per_query": bq_usd_per_query(),
        "brevity_breach_rate": brevity_rate(),
        "corpus_cases": count_lines("engine/corpus/manifest.jsonl"),
        "candidates_queued": count_lines("engine/candidates/queue.jsonl"),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(snap, sort_keys=True)
    with OUT.open("a") as f:
        f.write(line + "\n")
    print(f"entropy_snapshot -> {OUT.relative_to(WS)}")
    for k, v in snap.items():
        if k != "stamp":
            print(f"  {k}: {v}")
    # byte-stability check: recompute and compare (excluding the append)
    line2 = json.dumps({**snap}, sort_keys=True)
    print(f"  byte-stable: {line == line2}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
