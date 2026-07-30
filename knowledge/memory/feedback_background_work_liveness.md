---
name: background_work_liveness
description: Never passively wait on background/async work; arm a stall-detector and actively verify liveness — a hung task sends no notification
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2f8d4ec8-78d6-419a-9c3f-4da329f3c216
doc_type: memory
keywords: [background work liveness, stall detector, Monitor poll, hung task no notification, TaskOutput no task found, Workflow fan-out, AUDI-1173, mtime stale]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-28
---
Whenever ANY background/async task is outstanding — `Agent(run_in_background:true)`, a `Workflow`, or background `Bash` — pair it with **active liveness monitoring**. Never wait solely on the "you'll be notified on completion" contract: a task that HANGS (vs completes or cleanly errors) sends no notification, so passive waiting can stall silently for hours.

**Why:** In the AUDI-1173 orchestration (2026-07-28) two background implementer agents hung ~2 hours with no completion notification; it was only caught when the user pinged "still waiting?". Signature of the hang: `TaskOutput(block:false)` → "No task found", no completion notification ever arrived, and transcript/output mtimes + the BQ perf log went stale ~2h ago while the tasks were nominally "running."

**How to apply:**
- **On dispatch, arm a stall-detector.** Fire a `Monitor` poll loop that every ~5 min checks the newest mtime of the task's transcript dir + its output files and emits a line ONLY when idle > ~15 min (`STALL-SUSPECT ...`). Silence = healthy; the task's own completion notification covers the happy path. Recipe (macOS/BSD stat):
  ```
  DIR=<workflow-transcript-dir>; OUT=<ticket outputs/artifacts>
  echo "monitor armed"; while true; do sleep 300; now=$(date +%s)
    f=$(ls -t "$DIR"/* "$OUT"/* 2>/dev/null | head -1); [ -z "$f" ] && continue
    age=$((now-$(stat -f %m "$f"))); [ "$age" -gt 900 ] && echo "STALL-SUSPECT: newest change ${age}s ago — check /workflows or TaskOutput"; done
  ```
- **Declare HUNG and act (stop + re-dispatch the unfinished unit) when:** `TaskOutput(block:false)` returns "No task found" with no completion notification, OR transcript/output mtimes are stale > ~15 min while nominally running, OR the perf/activity log shows no new entries for the task.
- **Prefer the `Workflow` tool for multi-unit fan-out** (one tracked task + `/workflows` progress + single completion) over many loose background `Agent`s — fewer independent things that can silently hang.
- This is **stall detection (idle / no forward progress), NOT impatience.** Do not preempt a task that is long but actively progressing (a legitimately slow BQ query) — see [[feedback_bq_workflow]]. Judge by idle time, not elapsed time.
- Relates to [[feedback_adversarial_workflow_authoring]] (the multi-agent verify pattern this monitoring wraps).
- **A Workflow's FINAL synth step can hang after the parallel agents finish (seen AUDI-1172, 2026-07-29).** The verify agents all completed and journaled their verdicts, but the `Synthesize` agent never ran and no completion notification fired — the stall detector caught it. **Recovery: don't re-run the whole workflow.** `Read <transcriptDir>/journal.jsonl` — it holds every completed agent's return value — extract the verdicts and synthesize yourself; `TaskStop` the hung run. Same lesson as the resume note: journal first, don't assume you lost the parallel work.
- **Batch large fan-outs (established 2026-07-29).** For a multi-unit Workflow over many items (e.g. verify 208 table docs, catalog 70), do NOT spawn one agent per item — batch ~7-9 items/agent (208→24 agents, 70→10), so each agent loops its batch and runs the shared lint once. Same coverage, far less startup overhead. **Generate the script FILE with the item array baked in** (Bash writes the `.js` via `json.dumps`) — this also dodges the args-as-JSON-string footgun (`args.map is not a function`).
- **Reconcile a Workflow agent's success claim against actual file state (2026-07-29).** A verify agent reported a table `→ verified` whose doc was never written (optimistic/lost write); a coverage+queue reconcile caught it and a follow-up agent cataloged it. After any doc-producing fan-out, confirm the files exist / the doc-debt queue drained / coverage moved — don't trust the returned summaries alone. Same spirit as "journal first, don't assume."
