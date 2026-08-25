---
name: feedback-adversarial-workflow-authoring
description: "Lessons for authoring multi-agent adversarial-verify Workflows — blocking-only gate, fixer loop, parse args, no silent drops"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 45f5f508-2584-4aba-8aa8-efdac5abeb02
doc_type: memory
keywords: [adversarial workflow authoring, multi-agent verify, blocking gate, fixer loop, JSON.parse args, silent drops, try catch stages, token waste, scratchpad file race, unique payload filenames, concurrent subagents, parallel write collision]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-24
---
When authoring Workflow-tool multi-agent passes with an adversarial verify gate:

**Why:** a mis-calibrated gate wastes tokens and yields stochastically; silent agent failures under-report coverage and read as "done" when they aren't.

**How to apply:**
- **Blocking-only gate.** A dataset/project qualifier added to a routing/Tables field is normalization, not fabrication — don't reject on it. Reserve rejection for: an unsupported fact/number/outcome, a hedge upgraded to certainty, a plan step reported as done, inflated scope, or a table the source never names.
- **Fixer converge loop.** Binary 2-of-2 "assume wrong" reject is stochastic (same card flips pass→fail on identical input). Pair it with a fixer that repairs only the flagged claims against source and re-verifies once, so failures converge instead of dropping. Recalibrating this lifted yield 3/5 → 5/5.
- **Parse args.** Workflow `args` can arrive as a JSON string — `JSON.parse` it before use, or `args.tickets` is undefined and the run silently falls back to the full default list (this cost one ~12M-token full re-run).
- **No silent caps.** A thrown pipeline stage drops the item to `null`, vanishing from BOTH pass and fail lists. Wrap stages in try/catch and record `extract_error`/`converge_error` so a retry-cap failure is a tracked row, never a silent gap.
- **Hand-finish the tail.** For a few persistent schema-cap failures, hand-author those cards from source rather than re-running the whole pass.
- **Parallel subagents sharing the session scratchpad must write uniquely named payload/draft files (2026-08-24).** Two concurrent Jira-write agents both used a generic `desc_payload.json`; the race briefly PUT AUDI-1061's description onto AUDI-882 (caught and corrected the same minute; both re-verified correct). Suffix every scratch file with the work-unit key (`desc_payload_audi_882.json`), and after any parallel write wave re-read the targets to confirm each landed on its own unit.

Four more, from building the pr_gauntlet loop (2026-08-24, [[reference_pr_gauntlet]]):
- **`Workflow({name})` snapshots the script at its first per-session resolution** and keeps serving that copy — an edit + re-invoke by name reran the STALE script. Iterating? Dispatch by `scriptPath`.
- **`agentType` resolves against a registry snapshotted at session start.** Agent files written mid-session are invisible to workflows until a new session; catch the "not found" throw and fall back to a default agent prompted to Read the role file.
- **A line-bucket dedupe key is not a thrash detector.** Two DIFFERENT defects at lines 55 and 58 collided into one `file:class:bucket10` key and false-aborted the loop as THRASH. Any "fixed finding recurred" guard needs a semantic check — one arbiter agent ruling same-defect vs neighbor — before aborting.
- **Fixers diverge by ADDING surface.** Told only "fix the confirmed findings," fixers grew a 124-line linter to 350 lines over 3 rounds; every addition spawned next-round findings and the run hit FAIL_MAX_ROUNDS. The fixer prompt must mandate the smallest correct fix, forbid new features/flags, and prefer deleting a prior round's addition over patching it.

Links: [[project_self_optimizing_context]], [[reference_ticket_context_eval_tooling]], [[feedback_bq_workflow]].
