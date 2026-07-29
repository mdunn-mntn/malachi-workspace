---
name: reference_improvements_backlog
description: "Improvement/durable-fix/tech-debt ideas go in improvements_backlog.md (a file), NOT Jira — keeps the board clean; promote a row to Jira only when prioritized"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 98511e71-bad1-4bb7-9ed1-393b4675a39b
doc_type: memory
keywords: [improvements_backlog, improvements, backlog, improvement, durable, tech, debt, ideas]
domain: [reference]
lifecycle: active
last_verified: 2026-07-28
---
`improvements_backlog.md` (workspace root) is the single tracker for improvement ideas / durable fixes / tech-debt we want to remember but **not** put on the Jira board yet. Malachi's call (2026-07-28): keep these in a file so a good idea isn't lost and the board stays uncluttered.

**How it works:** add one row (`Status: idea`, `Ref:` the source e.g. `on-call INC-NNN`) whenever an improvement is spotted — on-call durable-fix, workflow gap, code cleanup, "we should automate X." Promote to Jira only when actually prioritized (`Status: promoted:TI-XXX`, keep the row). Never delete rows — flip status to `done`/`wontfix`.

**Wired in:** the [[reference_oncall_runbook]] `/oncall` skill's prod-safety gate now says "log durable fixes to improvements_backlog.md, NOT Jira by reflex"; runbook INC durable-fix notes link their IMP row. Seed rows: IMP-001 (ipdsc_monitor optional-partner `soft_fail`, from INC-001) · IMP-002 (Fangorn Dataproc ~94% collision staggering/quota, from INC-002).
