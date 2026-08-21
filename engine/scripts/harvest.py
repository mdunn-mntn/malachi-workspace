#!/usr/bin/env python3
"""harvest.py — deterministic, keyless candidate miner for the self-improvement engine.

Reads the workspace friction signals (perf log, request log, brevity log, doc-debt queue,
eval runs, incidents, backlog) and emits one candidate row per genuine, evidenced friction
signal to engine/candidates/queue.jsonl. No LLM. No writes outside engine/. Halts at entry if
engine/STOP exists.

A candidate is only the raw signal + its evidence; HYPOTHESIZE (Phase 5, dsh) turns it into a
change spec. v0 auto-applies nothing from here — rung-0 classes are applied by adopt.sh, and
everything else lands as a PROPOSE row.
"""

import json
import pathlib
import sys
from collections import Counter
from datetime import UTC, datetime

WS = pathlib.Path(__file__).resolve().parents[2]
ENGINE = WS / "engine"
QUEUE = ENGINE / "candidates" / "queue.jsonl"
STOP = ENGINE / "STOP"


def today():
    # Deterministic per-run stamp; the engine passes no wall clock to sub-steps.
    return datetime.now(UTC).strftime("%Y-%m-%d")


def read_jsonl(path):
    out = []
    p = WS / path
    if not p.exists():
        return out
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:  # noqa: SIM105
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out


def candidates():
    stamp = today()
    seq = 0
    cands = []

    def emit(cls, signal, evidence, summary):
        nonlocal seq
        seq += 1
        cands.append(
            {
                "id": f"c-{stamp}-{seq:03d}",
                "class": cls,
                "signal": signal,
                "summary": summary,
                "evidence": evidence,
                "status": "queued",
                "created": stamp,
            }
        )

    # 1. Costly / repeated BQ queries (perf log). Group by sql_sha256; a query shape run many
    # times or billing many GB is a caching/materialization candidate.
    perf = read_jsonl("knowledge/bq_perf_log.jsonl")
    by_sha = {}
    for e in perf:
        sha = e.get("sql_sha256")
        if not sha:
            continue
        b = by_sha.setdefault(
            sha,
            {
                "n": 0,
                "gb": 0.0,
                "preview": e.get("sql_preview", ""),
                "tables": e.get("sql_tables", []),
            },
        )
        b["n"] += 1
        b["gb"] += float(e.get("gb_billed") or 0)
    for sha, b in sorted(by_sha.items(), key=lambda kv: kv[1]["gb"], reverse=True)[:5]:
        if b["n"] >= 5 or b["gb"] >= 50:
            emit(
                "knowledge_edit",
                "costly_query",
                [
                    {
                        "source": "knowledge/bq_perf_log.jsonl",
                        "count": b["n"],
                        "gb_billed_total": round(b["gb"], 1),
                        "sql_sha256": sha,
                        "tables": b["tables"],
                        "examples": [b["preview"][:120]],
                    }
                ],
                f"query shape run {b['n']}x billing {round(b['gb'], 1)} GB total — document a cheaper path or a materialized source",
            )

    # 2. Doc debt: undocumented tables queued.
    q = WS / "knowledge/bq/_UNDOCUMENTED.queue"
    if q.exists():
        tables = [ln.strip() for ln in q.read_text().splitlines() if ln.strip()]
        if tables:
            emit(
                "index_rebuild",
                "doc_debt",
                [
                    {
                        "source": "knowledge/bq/_UNDOCUMENTED.queue",
                        "count": len(tables),
                        "examples": tables[:5],
                    }
                ],
                f"{len(tables)} undocumented tables queued — run bq_introspect + catalog docs",
            )

    # 3. Recurring request shapes (request log): a verb+noun n-gram repeated >= threshold that no
    # skill serves is an automation candidate. Keyword-only, never the raw prompt.
    reqs = read_jsonl("knowledge/.request_log.jsonl")
    noun_counts = Counter()
    for e in reqs:
        for n in e.get("nouns", [])[:6]:
            if len(n) >= 5:
                noun_counts[n] += 1
    for noun, c in noun_counts.most_common(8):
        if c >= 8:
            emit(
                "skill_new",
                "repeated_sequence",
                [{"source": "knowledge/.request_log.jsonl", "count": c, "examples": [noun]}],
                f"'{noun}' recurs in {c} requests — candidate for a skill or a routing keyword",
            )

    # 4. Brevity breaches (chat_brevity_log): a high over-rate is a prompt/behavior candidate.
    brev = read_jsonl(".claude/state/chat_brevity_log.jsonl")
    if brev:
        over = sum(1 for e in brev if e.get("over"))
        rate = over / len(brev)
        if rate > 0.25:
            emit(
                "prompt_line",
                "recurring_friction",
                [
                    {
                        "source": ".claude/state/chat_brevity_log.jsonl",
                        "count": over,
                        "total": len(brev),
                        "over_rate": round(rate, 3),
                    }
                ],
                f"{round(rate * 100)}% of replies breach the brevity cap — tighten the reply-shape guidance",
            )

    # 5. Repeat incidents not obviously in the catalog: same dag+signature seen more than once.
    inc = read_jsonl("on-call/incident_log.jsonl")
    sig_counts = Counter()
    for e in inc:
        key = (e.get("dag", ""), (e.get("signature", "") or "")[:40])
        sig_counts[key] += 1
    for (dag, sig), c in sig_counts.items():
        if c >= 2 and dag:
            emit(
                "knowledge_edit",
                "repeat_incident",
                [
                    {
                        "source": "on-call/incident_log.jsonl",
                        "count": c,
                        "examples": [f"{dag}: {sig}"],
                    }
                ],
                f"incident '{dag}' recurred {c}x with the same signature — add/strengthen a runbook catalog entry",
            )

    return cands


def main():
    if STOP.exists():
        print("engine/STOP present — harvest halted", file=sys.stderr)
        return 3
    cands = candidates()
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with QUEUE.open("w") as f:
        for c in cands:
            f.write(json.dumps(c) + "\n")
    print(f"harvest: {len(cands)} candidates -> {QUEUE.relative_to(WS)}")
    for c in cands:
        print(f"  [{c['class']}/{c['signal']}] {c['summary']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
