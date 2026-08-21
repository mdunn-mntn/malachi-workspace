# Eval corpus

The regression suite the machine gate replays a candidate against. `manifest.jsonl` (committed) holds
one line of metadata per case; `cases/<id>/case.json` (gitignored — raw prompts/data are sensitive,
Mac-local) holds the full case.

## case.json schema
```
{
  "id": "<source>-<hash>",
  "source": "dsh" | "claude-code" | "probe" | "incident",
  "task_prompt": "<the task, verbatim>",
  "workspace_sha": "<git sha the recording ran at, or null>",
  "checks": [                       # deterministic assertions graded at VERIFY
    {"type": "output_nonempty"},
    {"type": "output_contains", "value": "..."},
    {"type": "file_exists", "path": "..."},
    {"type": "probe_reached", "target": "..."},
    {"type": "tokens_max", "value": 50000},
    {"type": "cost_max_usd", "value": 1.0}
  ],
  "baseline": {"pass": true, "tokens": 0, "usd": 0, "latency_s": 0} | null,
  "tags": ["..."],
  "tier": [1] | [2] | [1,2],        # 1 = replayable (dsh log), 2 = fresh-run eval
  "holdout": false,                 # true = invisible to HYPOTHESIZE/BUILD, checked only in VERIFY
  "added": "<date>",
  "last_green": "<date>"
}
```

## What becomes a case
1. The retrieval probes in `knowledge/eval_probes.md` (source `probe`, tier 2, checks = `probe_reached` per `must_reach`).
2. Every closed ticket's golden session — the `/frame` binary Objective compiles into the checks (source `dsh`, tier 1+2).
3. Every incident in `on-call/incident_log.jsonl` (source `incident`).
4. Every VERIFY true-positive failure and OBSERVE rollback — misses become permanent regression tests.

## Growth / rotation
≤1 new case per closed ticket; Tier-2 active set capped at 40 / $3 per full run; Tier-1 unbounded (keyless).
20% flagged `holdout: true`. Cases untouched by any candidate diff for 6 months archive monthly.
