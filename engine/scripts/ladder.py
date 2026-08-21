#!/usr/bin/env python3
"""ladder.py — autonomy-rung state machine driven by the ENGINE_LOG track record.

Rung promotion is EARNED, never assumed: a class unlocks its rung only after the log shows the
required clean history with zero rollbacks. Any rollback demotes its class one rung for a cooldown.
This script only READS the log and REPORTS the rung each change-class currently holds; run_engine.py
consults it before ADOPT. It never edits engine.config.yml's floors and never raises its own rung
(rung 5 / engine-self changes are out of scope for v0 and require a human).
"""

import pathlib
import re
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
LOG = ENGINE / "ENGINE_LOG.md"
CONFIG = ENGINE / "engine.config.yml"

# Class -> rung it lives at (mirrors engine.config.yml ladder.rungs).
CLASS_RUNG = {
    "index_rebuild": 0,
    "doc_observed_append": 0,
    "routing_keyword": 0,
    "corpus_add": 0,
    "eval_run_log": 0,
    "entropy_snapshot": 0,
    "knowledge_edit": 1,
    "memory_file": 1,
    "skill_edit": 2,
    "skill_new": 2,
    "prompt_line": 2,
    "tool_plugin": 3,
    "hook": 3,
    "harness_config": 4,
    "engine_self": 5,
}

# Promotion criteria per rung: (weeks_clean_at_prev, min_corpus_cases). Simplified for v0; the
# design's fuller criteria (spot-audit, distinct-skill coverage) are checked by a human at promotion.
UNLOCK = {
    0: (0, 0),
    1: (4, 20),
    2: (8, 30),
    3: (12, 30),
    4: (24, 30),
    5: (99, 99),  # never auto
}


def read_log():
    if not LOG.exists():
        return []
    rows = []
    for ln in LOG.read_text().splitlines():
        m = re.match(r"(\d{4}-\d{2}-\d{2}) \| stage=(\w+) \|.*rolled_back=(\d+)", ln)
        if m:
            rows.append({"date": m.group(1), "stage": m.group(2), "rolled_back": int(m.group(3))})
    return rows


def current_max_rung():
    m = re.search(r"current_rung:\s*(\d+)", CONFIG.read_text()) if CONFIG.exists() else None
    return int(m.group(1)) if m else 0


def rung_for_class(cls):
    """The rung a class's changes require. A candidate auto-adopts only if this <= current_max_rung."""
    return CLASS_RUNG.get(cls, 99)


def can_auto_adopt(cls):
    return rung_for_class(cls) <= current_max_rung()


def main():
    rows = read_log()
    total_rollbacks = sum(r["rolled_back"] for r in rows)
    maxr = current_max_rung()
    print(
        f"ladder: current_max_rung={maxr}  log_rows={len(rows)}  total_rollbacks={total_rollbacks}"
    )
    for cls in sorted(CLASS_RUNG, key=lambda c: CLASS_RUNG[c]):
        r = CLASS_RUNG[cls]
        state = "AUTO" if r <= maxr else "PROPOSE"
        print(f"  rung {r}  {cls:20s} -> {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
