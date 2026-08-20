---
name: project_insight_spelunking_session
description: "Bryce's Insight Spelunking fieldwork session 2026-08-21 2-3pm MT — claim FICO campaign group 81053, diagnose the CPA miss in the reporting UI only, submit via /insight spelunking; prep is done and committed"
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [insight spelunking, bryce wagg, h2 optimizer, peak performance, bad cpa, fico 81053, claim brief, audi-1083, reporting ui, lauren gregg sheet]
domain: [project, business]
lifecycle: active
last_verified: 2026-08-19
---

**Session: Friday 2026-08-21, 2-3pm MT** (Bryce Wagg moved it from the original slot for the
east-coasters; Sean Yang may join late). Squad fieldwork for the H2 optimizer: claim a live campaign
missing its CPA goal off the "bad cpa" tab of Lauren Gregg's sheet, diagnose it **in the reporting UI
as the customer** (no backend, no DB, no public API), log every confusion and rage-click on the shared
board, and submit one finding with `/insight spelunking` in
`#dev-reporting-insights-audience-segment-builder`. Peak Performance campaigns get claimed first.

**The friction is the deliverable, not the diagnosis.** Awards for best insight, most rage-clicks, and
the biggest "I built this?!" moment.

**Prep is DONE and committed (2026-08-19), do not redo it:**
- Claim: **FICO, campaign group 81053** — $220,763 spend, $41.65 CPA against a $25 goal, the
  largest-spend unclaimed Peak Performance miss on the list.
- Answer key: `tickets/audi_1083_mm_classifying_view/artifacts/audi_1083_spelunk_claim_brief.md`
  (three mechanisms: v2 vertical-only so it cannot reach the high-intent band, `hhst_current = 0` so no
  score gate bites at all, a 3P segment AND-narrowing the universe). Fallbacks listed there.
  **Do not open the brief during the hour** — it is BigQuery ground truth and the point is measuring
  what the UI does and does not let a customer conclude.
- Claim list: `My Drive/Tickets/AUDI-1083/AUDI-1083 Peak Performance Bad CPA Claim List.xlsx`.
- Record + method: `tickets/audi_1083_mm_classifying_view/summary.md` §6f.

**Open on the user, not on me:** confirm the reporting UI login and a report pull, and confirm
`/insight spelunking` runs (there was a permissions hiccup; Bryce said ping him if still blocked).
Neither the UI URL nor the Slack shortcut is documented anywhere in this workspace.

**Also unposted:** the thread reply at
`tickets/audi_1083_mm_classifying_view/artifacts/audi_1083_slack_pp_filter_reply.txt` (answers Alex
Knorr's "how do we tell which are Peak Performance" — the sheet's own `peak perf` column agrees with
the classifier on 250 of 251 rows; LifeVac cgid 123213 is the one to fix).

Related: [[project_audi_1083_mm_classifier]] [[reference_mm_component_taxonomy]]
[[reference_mntn_google_drive_access]]
