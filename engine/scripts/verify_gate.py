#!/usr/bin/env python3
"""verify_gate.py — the machine gate. Computes a quantitative PASS for a built candidate from
four evidence components, per engine.config.yml thresholds. This is the deterministic PASS/FAIL
computation; the components (statics, replay, reviews, cost) are gathered by run_engine.py and
passed in as a verdict-input JSON. Keeping the decision here (pure, testable) is the point — the
gate can be unit-tested and never silently loosened.

Input JSON (stdin or --file):
{
  "candidate": "c-...",
  "statics_pass": true,                 # verify.sh full + lints + vitest all green in the worktree
  "replay": {"total": N, "clean": M},   # Tier-1 corpus replays
  "reviews": [{"verdict": "CLEAN"|"FINDINGS"}, ...],   # adversarial reviewers
  "metric": {"name": "...", "direction": "up"|"down", "baseline": x, "candidate": y, "min_delta": d},
  "guards": {"tokens_to_answer": {"baseline": .., "candidate": ..}, "usd_per_case": {...}, ...}
}
Output: {"pass": bool, "reasons": [...]} and exit 0 if pass else 1.
"""

import argparse
import json
import pathlib
import re
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
CONFIG = WS / "engine" / "engine.config.yml"


def thresholds():
    t = CONFIG.read_text() if CONFIG.exists() else ""

    def num(key, default):
        m = re.search(rf"{key}:\s*([\d.]+)", t)
        return float(m.group(1)) if m else default

    return {
        "tier1_replay_pass": num("tier1_replay_pass", 1.0),
        "tokens_to_answer_max_increase": num("tokens_to_answer_max_increase", 0.10),
        "usd_per_case_max_increase": num("usd_per_case_max_increase", 0.10),
        "wall_latency_max_increase": num("wall_latency_max_increase", 0.20),
        "reviewers_required_clean": int(num("reviewers_required_clean", 2)),
    }


GUARD_LIMITS = {
    "tokens_to_answer": "tokens_to_answer_max_increase",
    "usd_per_case": "usd_per_case_max_increase",
    "wall_latency": "wall_latency_max_increase",
}


def evaluate(v, th):
    reasons = []
    ok = True

    if not v.get("statics_pass", False):
        ok = False
        reasons.append("statics FAILED (verify.sh/lints/vitest not all green)")

    rp = v.get("replay", {"total": 0, "clean": 0})
    rate = (rp["clean"] / rp["total"]) if rp["total"] else 1.0
    if rate < th["tier1_replay_pass"]:
        ok = False
        reasons.append(
            f"replay {rp['clean']}/{rp['total']} < required {th['tier1_replay_pass']:.0%}"
        )

    reviews = v.get("reviews", [])
    clean = sum(1 for r in reviews if r.get("verdict") == "CLEAN")
    if clean < th["reviewers_required_clean"]:
        ok = False
        reasons.append(
            f"only {clean}/{len(reviews)} reviewers CLEAN (need {th['reviewers_required_clean']})"
        )

    m = v.get("metric")
    if not m or not m.get("name"):
        ok = False
        reasons.append(
            "no pre-registered metric (anti-reward-hacking: a spec without a metric is invalid)"
        )
    elif m.get("baseline") is not None and m.get("candidate") is not None:
        delta = m["candidate"] - m["baseline"]
        if m.get("direction") == "down":
            delta = -delta
        if delta < m.get("min_delta", 0):
            ok = False
            reasons.append(
                f"metric '{m['name']}' moved {delta:+.4g} < min_delta {m.get('min_delta')}"
            )

    for name, g in (v.get("guards") or {}).items():
        limit_key = GUARD_LIMITS.get(name)
        if not limit_key:
            continue
        base, cand = g.get("baseline"), g.get("candidate")
        if base and cand is not None and base > 0:
            inc = (cand - base) / base
            if inc > th[limit_key]:
                ok = False
                reasons.append(f"guard '{name}' regressed +{inc:.0%} > {th[limit_key]:.0%}")

    if ok:
        reasons.append(
            "all components green: statics, replay 100%, metric >= min_delta, reviewers clean, no guard regressed"
        )
    return ok, reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file")
    args = ap.parse_args()
    raw = pathlib.Path(args.file).read_text() if args.file else sys.stdin.read()
    v = json.loads(raw)
    ok, reasons = evaluate(v, thresholds())
    print(json.dumps({"pass": ok, "candidate": v.get("candidate"), "reasons": reasons}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
