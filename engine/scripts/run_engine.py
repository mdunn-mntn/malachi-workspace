#!/usr/bin/env python3
"""run_engine.py — the engine orchestrator: HARVEST -> HYPOTHESIZE -> BUILD -> VERIFY -> ADOPT/PROPOSE -> OBSERVE.

Ties the stage scripts into one loop over the candidate queue. LLM-requiring stages (HYPOTHESIZE,
adversarial review) shell out to `dsh-lab/bin/dsh-mntn --profile mntn-automation` (Keychain-keyed,
Mac only). Deterministic stages (statics, replay, adopt, observe) are the Python/bash scripts.

Modes:
  --dry            : HARVEST + classify each candidate AUTO vs PROPOSE by rung; no LLM, no changes.
  --candidate <id> : run the full pipeline for one queued candidate.
  --propose-all    : write every non-rung-0 candidate to improvements_backlog.md as a PROPOSE row.

Floors: halts at engine/STOP; spend cap checked before any LLM call; ADOPT only if the gate PASSes
AND ladder.can_auto_adopt(class); otherwise the candidate becomes a PROPOSE row (never silently applied).
"""

import argparse
import json
import pathlib
import subprocess
import sys

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
LAB = WS.parent / "dsh-lab"
DSH = LAB / "bin" / "dsh-mntn"
STOP = ENGINE / "STOP"
sys.path.insert(0, str(ENGINE / "scripts"))
import ladder  # noqa: E402


def load_queue():
    q = ENGINE / "candidates" / "queue.jsonl"
    if not q.exists():
        return []
    return [json.loads(ln) for ln in q.read_text().splitlines() if ln.strip()]


def dsh_headless(task, timeout=300):
    """One-shot headless dsh call; returns stdout text (empty on failure)."""
    try:
        r = subprocess.run(
            [str(DSH), "--profile", "mntn-automation", task],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(WS),
        )
        return r.stdout if r.returncode == 0 else ""
    except Exception:
        return ""


def extract_json(text):
    import re

    m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.S) or re.search(r"(\{.*\})", text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def propose_row(cand, spec=None):
    """Append a PROPOSE row to improvements_backlog.md (never auto-applied)."""
    bl = WS / "improvements_backlog.md"
    metric = (spec or {}).get("preregistered", {}).get("metric", "unset")
    line = (
        f"\n- ENGINE-PROPOSE `{cand['id']}` ({cand['class']}/{cand['signal']}): {cand['summary']}"
        f" [metric: {metric}] — AWAITING APPROVAL (rung above current auto tier)"
    )
    with bl.open("a") as f:
        f.write(line + "\n")
    print(f"  PROPOSE -> improvements_backlog.md ({cand['id']})")


def run_candidate(cand, do_llm):
    print(f"\n=== candidate {cand['id']} [{cand['class']}/{cand['signal']}] ===")
    print(f"  {cand['summary']}")
    auto = ladder.can_auto_adopt(cand["class"])
    print(
        f"  rung {ladder.rung_for_class(cand['class'])} -> {'AUTO-eligible' if auto else 'PROPOSE (rung locked)'}"
    )

    # Rung-0 deterministic classes bypass HYPOTHESIZE (the change is mechanical).
    if cand["class"] in ("index_rebuild", "corpus_add", "entropy_snapshot"):
        print("  rung-0 mechanical class -> adopt.sh")
        r = subprocess.run(
            ["bash", str(ENGINE / "scripts" / "adopt.sh"), cand["class"], cand["id"]],
            cwd=str(WS),
            capture_output=True,
            text=True,
        )
        print("  " + (r.stdout or r.stderr).strip().replace("\n", "\n  "))
        return

    if not do_llm:
        propose_row(cand)
        return

    # HYPOTHESIZE (LLM): produce a spec with a pre-registered metric.
    prompt = (
        (ENGINE / "prompts" / "hypothesize.md")
        .read_text()
        .replace("{{CANDIDATE_JSON}}", json.dumps(cand, indent=2))
    )
    spec = extract_json(dsh_headless(prompt))
    specdir = ENGINE / "candidates" / cand["id"]
    specdir.mkdir(parents=True, exist_ok=True)
    if not spec or not spec.get("preregistered", {}).get("metric"):
        print(
            "  HYPOTHESIZE produced no valid metric-bearing spec -> REFUSED (candidate stays queued)"
        )
        (specdir / "spec_rejected.json").write_text(
            json.dumps({"reason": "no pre-registered metric", "raw": spec}, indent=2) + "\n"
        )
        return
    (specdir / "spec.json").write_text(json.dumps(spec, indent=2) + "\n")
    print(
        f"  HYPOTHESIZE ok: class={spec['change_class']} metric={spec['preregistered']['metric']} "
        f"paths={spec['target_paths']}"
    )

    # In v0 the rung for LLM-authored classes is above 0, so the pipeline stops at PROPOSE with the
    # full spec attached (BUILD/VERIFY/ADOPT for these classes unlock as the ladder promotes).
    if not auto:
        propose_row(cand, spec)
        return
    print(
        "  (rung unlocked: BUILD/VERIFY/ADOPT would run here — deferred until a class is promoted)"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--candidate")
    ap.add_argument("--propose-all", action="store_true")
    ap.add_argument(
        "--llm", action="store_true", help="allow LLM (HYPOTHESIZE) — Mac only, Keychain key"
    )
    ap.add_argument(
        "--auto",
        action="store_true",
        help="full unattended loop over the queue: rung-0 -> adopt, others -> hypothesize+propose (spend-bounded)",
    )
    ap.add_argument(
        "--max-llm",
        type=int,
        default=3,
        help="max NEW candidates to hypothesize per --auto run (spend cap; already-specced are skipped)",
    )
    args = ap.parse_args()

    if STOP.exists():
        print("engine/STOP present — engine halted", file=sys.stderr)
        return 3

    queue = load_queue()

    if args.auto:
        llm_used = 0
        rung0 = 0
        proposed = 0
        for c in queue:
            if c["class"] in ("index_rebuild", "corpus_add", "entropy_snapshot"):
                run_candidate(c, do_llm=False)  # mechanical rung-0
                rung0 += 1
                continue
            specdir = ENGINE / "candidates" / c["id"]
            if (specdir / "spec.json").exists() or (specdir / "spec_rejected.json").exists():
                continue  # already processed on a prior run; don't re-spend
            if llm_used >= args.max_llm:
                continue  # spend cap reached for this run
            run_candidate(c, do_llm=True)
            llm_used += 1
            proposed += 1
        print(
            f"\nauto: rung-0 adopted={rung0}, hypothesized+proposed={proposed} (llm cap {args.max_llm})"
        )
        return 0
    if args.dry:
        print(f"HARVEST queue: {len(queue)} candidates")
        for c in queue:
            auto = ladder.can_auto_adopt(c["class"])
            print(
                f"  {'AUTO   ' if auto else 'PROPOSE'} rung{ladder.rung_for_class(c['class'])} {c['id']} {c['class']}/{c['signal']}"
            )
        return 0

    if args.candidate:
        c = next((c for c in queue if c["id"] == args.candidate), None)
        if not c:
            print(f"no such candidate {args.candidate}", file=sys.stderr)
            return 1
        run_candidate(c, do_llm=args.llm)
        return 0

    if args.propose_all:
        for c in queue:
            if not ladder.can_auto_adopt(c["class"]):
                propose_row(c)
        return 0

    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
