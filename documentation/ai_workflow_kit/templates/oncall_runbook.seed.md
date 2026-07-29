---
doc_type: runbook
title: On-Call Runbook — Master
summary: "Read FIRST on any alert. Triage protocol, alert catalog (signature→verdict→protocol), incident log, producer→consumer maps. Every resolution appends back here."
last_verified: 2025-01-01
keywords: [on-call, oncall, on call, incident, pager, alert triage, pipeline failure, job failure, task failed, escalation, runbook, prod safety]
tags: [on-call, incident-response]
---

# On-Call Runbook — Master

**Read this FIRST on any on-call alert. Append an incident entry after every resolution.**
The more incidents we log, the faster the next one closes. If an alert matches a row in the
§2 Known-Alert Catalog, jump straight to its protocol.

## 0. Is this on-call? — classify FIRST, then pick the surface to write to
- **Alert / pager fired AND a pipeline is degraded** → on-call. Use `/oncall`, write to THIS runbook.
- **A question or a change with no pager** → a ticket. Use `/frame`, write to `tickets/`.
- An alert that exposes a recurring defect **spawns a ticket** for the durable fix — but log the incident
  here first.

## 1. General triage protocol (any alert)
1. **Capture the signature** — the failing job/task, the exact error line, the timestamp.
2. **Check empirical state** — did the expected output land? Is the upstream source present and on time?
3. **Classify the verdict** — benign/expected (late data, optional-partner skip) · transient (re-run) ·
   real defect (route to owner + spawn a durable-fix ticket).
4. **Act** — clear/re-run the consumer task, or route to the owning team. **Never hot-patch prod** to
   silence an alert.
5. **Write back** — §3 incident narrative + §2 one-line signature + one `incident_log.jsonl` record, then
   rebuild the index (`.claude/scripts/build_index.sh`).

## 2. Known-Alert Catalog (signature → verdict → protocol)
Grep this table (and `_ROUTING.md`) for an alert symptom to reach the right protocol.

| # | job / task key | signature | verdict | protocol |
|---|---|---|---|---|
| _(seed a row per recurring alert as you resolve it — link to its INC below)_ | | | | |

## 3. Incident log
Append one entry per incident, newest at the bottom. Template:

```
### INC-000 — `<job>` `<task>` — <one-line title>
- Date: YYYY-MM-DD
- Signature: <the exact error line / symptom>
- Root cause: <what actually happened>
- Verdict: benign | transient | real-defect
- Action: <clear/re-run | routed to <owner> | durable-fix ticket <id>>
- Prevent: <what would stop a recurrence; log it in improvements_backlog if not fixed now>
```

## 4. Producer → consumer system maps
Sketch the pipelines that page you: who produces each dataset a consumer waits on, and the expected
landing time. A late/absent producer is the most common root cause. _(Fill per your systems.)_

## 5. Structured incident log
Every resolution also appends one JSON record to `on-call/incident_log.jsonl` (machine-readable, one
object per line: `{date, job, task, signature, verdict, action}`). The `/oncall` skill enforces all three
write-backs (§3 + §2 + jsonl).
