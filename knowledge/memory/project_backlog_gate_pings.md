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

- AUDI-1213 → Nick Scialli: UI MDE covers lapsed advertisers? yes = close ticket, no = shrink scope to lapsed cohort.
- AUDI-1173 → Matt Brorby: RESOLVED 2026-08-24, Matt said yes ("AMOS is basically a bandit-like approach... makes sense to merge"). Merge comments posted on both tickets; Malachi co-owns randomization (apply_flag) + measurement design in AUDI-1216; 1173 keeps only the bidder-side cap feature if AMOS lacks it.
- AUDI-802 → Zach Schoenberger: RESOLVED 2026-08-24. Zach never investigated the missing list but agreed to close; root cause of the ~30 all-time untraceable rows stays unknown. Closed Done 2026-08-24 with completion comment; Zach flags when there's bandwidth to finish; residual + reopen triggers logged as IMP-069 in improvements_backlog.md.
- AUDI-1176 → Sean Yang: REPLIED 2026-08-24, status unknown to Sean; he thinks all experiments are held off for the MNTN-ID work and will raise it in the Kirsa meeting 2026-08-25. If that confirms the experiment is parked, the sequencing premise is gone: ship the output-only gate variant (keeps full scoring) per [[project_audi_1175_ds14_scoring_cost]] §3. Await tomorrow's answer.
- AUDI-1145 → Maya Triman: REPLIED 2026-08-24, gate changed. AP went over the DS63-update crediting script that morning with Jack and Wei; Jack agrees with AP's proposal; Maya was unaware of AUDI-1145's extra logic and asked for a concrete over-credited ad_served_id to walk the logic on. Ownership settled later that day: no recording exists; from September 2026 Mike Dolt + Jaime own the crediting logic, AP only helped with August updates. Maya shared her DS4/DS63 crediting-examples sheet (link in [[reference_graph_vendor_crediting]]). Next: decide with Mike whether AUDI-1145 transfers to him/Jaime or narrows to supplying the AUDI-694 over-crediting examples.
- AUDI-1061 → Edgar von Trotha: still wants block-coverage + lookback model? no = close.
- AUDI-1016 → Eric Salinger: ScyllaDB migration absorbs conditional-write question? yes = close free.

Meanwhile: AUDI-1060 (size DS19 OpenAI spend) is the one ungated start; AUDI-882/1203/1202 hold by design.
