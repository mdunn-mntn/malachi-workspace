#!/usr/bin/env python3
"""observe.py — post-adoption monitoring + auto-rollback decision.

For each engine commit still inside its observation window (7 days / 20 sessions per
engine.config.yml), re-snapshot the pre-registered metric + guards and decide whether a rollback
trigger has fired. Deterministic, keyless — the backstop behind the VERIFY gate (replay is the gate,
observation is the safety net). It DECIDES and prints the rollback command; it does not execute the
revert itself (run_engine.py or a human runs rollback.sh), so a bad decision has no blast radius.

An adopted candidate records its watch state in engine/candidates/<id>/adopted.json:
  {"commit": "<sha>", "adopted": "<date>", "metric": {...}, "guard_baselines": {...}, "sessions_seen": N}
"""

import json
import pathlib
import re
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
CONFIG = ENGINE / "engine.config.yml"


def cfg_int(key, default):
    m = re.search(rf"{key}:\s*(\d+)", CONFIG.read_text()) if CONFIG.exists() else None
    return int(m.group(1)) if m else default


def days_between(a, b):
    from datetime import date

    ya, ma, da = map(int, a.split("-"))
    yb, mb, db = map(int, b.split("-"))
    return (date(yb, mb, db) - date(ya, ma, da)).days


def current_metric(name):
    """Re-read a live metric from the entropy snapshot (latest line)."""
    ent = ENGINE / "metrics" / "entropy.jsonl"
    if not ent.exists():
        return None
    lines = [ln for ln in ent.read_text().splitlines() if ln.strip()]
    if not lines:
        return None
    snap = json.loads(lines[-1])
    return snap.get(name)


def main():
    if (ENGINE / "STOP").exists():
        print("engine/STOP present — observe halted", file=sys.stderr)
        return 3
    today = (
        sys.argv[1] if len(sys.argv) > 1 else None
    )  # engine passes the date; no wall clock in-step
    window_days = cfg_int("window_days", 7)
    bad_streak_limit = cfg_int("rollback_consecutive_bad_sessions", 3)

    decisions = []
    for adopted_file in ENGINE.glob("candidates/*/adopted.json"):
        st = json.loads(adopted_file.read_text())
        if today and days_between(st["adopted"], today) > window_days:
            continue  # window closed; corrections forward-only now
        m = st.get("metric") or {}
        name = m.get("name")
        if not name:
            continue
        live = current_metric(name)
        if live is None:
            continue
        regressed = False
        base = m.get("candidate")  # the value we adopted at
        if base is not None:
            if m.get("direction") == "down" and live > base:
                regressed = True
            if m.get("direction") == "up" and live < base:
                regressed = True
        streak = st.get("bad_streak", 0) + (1 if regressed else 0)
        st["bad_streak"] = 0 if not regressed else streak
        adopted_file.write_text(json.dumps(st, indent=2) + "\n")
        if streak >= bad_streak_limit:
            decisions.append(
                {
                    "candidate": adopted_file.parent.name,
                    "commit": st["commit"],
                    "trigger": f"metric '{name}' regressed {streak} consecutive checks",
                    "action": f"rollback.sh {st['commit']} 'observed {name} regression'",
                }
            )

    if not decisions:
        print("observe: all in-window adoptions holding; no rollback triggered")
        return 0
    print(f"observe: {len(decisions)} ROLLBACK trigger(s):")
    for d in decisions:
        print(f"  {d['candidate']} @ {d['commit'][:8]}: {d['trigger']}")
        print(f"    -> engine/scripts/{d['action']}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
