---
name: project_backlog_gate_pings
description: 7 gate-unblock pings sent 2026-08-24 for Malachi's backlog tickets; what each reply decides
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [backlog pings, gate, AUDI-1213, AUDI-1173, AUDI-802, AUDI-1176, AUDI-1145, AUDI-1061, AUDI-1016, Nick Scialli, Matt Brorby, Zach Schoenberger, Sean Yang, Maya Triman, Edgar von Trotha, Eric Salinger]
domain: [project, jira-process]
lifecycle: active
last_verified: 2026-08-24
---

All 7 gate pings for the open backlog tickets were sent on Slack 2026-08-24 (per the backlog audit ranking). On any reply, act per the branch and update this file; when all are resolved, archive it.

- AUDI-1213 → Nick Scialli: RESOLVED 2026-08-25, UI covers delivering only. Scope shrunk in Jira (description + comment): lapsed-cohort-only build (2,546 advertisers), spend-basis + arm-split fixes stay, no Mode port of the delivering half.
- AUDI-1173 → Matt Brorby: RESOLVED 2026-08-24, Matt said yes ("AMOS is basically a bandit-like approach... makes sense to merge"). Merge comments posted on both tickets; Malachi co-owns randomization (apply_flag) + measurement design in AUDI-1216; 1173 keeps only the bidder-side cap feature if AMOS lacks it.
- AUDI-802 → Zach Schoenberger: RESOLVED 2026-08-24. Zach never investigated the missing list but agreed to close; root cause of the ~30 all-time untraceable rows stays unknown. Closed Done 2026-08-24 with completion comment; Zach flags when there's bandwidth to finish; residual promoted to AUDI-1222 (2026-08-25, Zach asked for the trace tables + DDM monitors; IMP-069 closed out).
- AUDI-1176 → Sean Yang: REPLIED 2026-08-24, status unknown to Sean; he thinks all experiments are held off for the MNTN-ID work and will raise it in the Kirsa meeting 2026-08-25. CLOSED Won't Do 2026-08-25 by owner decision after the hold proved void (the experiment never existed); reopen path preserved in the close comment ([[project_audi_1175_ds14_scoring_cost]]).
- AUDI-1145 → Maya Triman: REPLIED 2026-08-24, gate changed. AP went over the DS63-update crediting script that morning with Jack and Wei; Jack agrees with AP's proposal; Maya was unaware of AUDI-1145's extra logic and asked for a concrete over-credited ad_served_id to walk the logic on. Ownership settled later that day: no recording exists; from September 2026 Mike Dolt + Jaime own the crediting logic, AP only helped with August updates. Maya shared her DS4/DS63 crediting-examples sheet (link in [[reference_graph_vendor_crediting]]). Boundary-confirm sent to Mike Dolt 2026-08-24; his reply CONTRADICTS Maya (he thought his side owns only the script and Maya's team the graph part) and he asked Malachi to start a clarifying thread with him/Jaime + ID + Maya. Ownership questions posted 2026-08-24 into Alyson's original crediting thread (assembly of Lists 1/2/3; per-list logic ownership), tagging Mike/Jack/Maya/Jaime. Org-level answer landed 08-24 via Kristen Colley's timeline: logic changes = Kale's org (AUDI + AP), audit/reporting = BAE, Jaime runs. Malachi put the transfer call to Mike Dolt in-thread (recommends AP/Jaime take it whole, offered a vendor-crediting walkthrough, fine either way). Alyson Lefkowitz took the ownership question to Mike (2026-08-25); her read: AP handles it NOW because AUDI is at capacity, AUDI takes it back eventually. Awaiting their clarity; on transfer, reassign AUDI-1145 to Jaime + hand over the AUDI-694 evidence ([[reference_graph_vendor_crediting]]).
- AUDI-1061 → Edgar von Trotha: RESOLVED 2026-08-24. Edgar does not recall the ask ("you can probably kill it"); closed Won't Do with completion comment. Re-file only if a concrete block-coverage need returns.
- AUDI-1016 → Eric Salinger: CLOSED DONE 2026-08-25 (completion comment 611443). Both tracks resolved by in-flight work: AP waterfall score migration (bidder stops reading AUDI advertiser/campaign scores), MemDB dedup work + MNTN-ID migration shrink the empties. No AUDI build. Residual outside the ticket: [[project_ap_score_feed_migration]] (confirm with Alyson) + DPlat informal incremental-intent-drop ask. All 7 gate pings now resolved except AUDI-1145 awaiting ownership clarity (1176 closed Won't Do same day).

Meanwhile: AUDI-1060 (size DS19 OpenAI spend) is the one ungated start; AUDI-882/1203/1202 hold by design.
