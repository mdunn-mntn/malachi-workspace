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
- AUDI-1173 → Matt Brorby: fold bandit RFD into AUDI-1216 AMOS? yes = co-own randomization/measurement there, no = wait on RFD + [[AUDI-1179]] unpause.
- AUDI-802 → Zach Schoenberger: 30 no_bid_ip visits explained? yes = close, no = fresh-cohort retest inside 90d bid-log TTL.
- AUDI-1176 → Sean Yang: Kirsa remove-DS14 experiment status? delayed = ship output-only variant ([[project_audi_1175_ds14_scoring_cost]]).
- AUDI-1145 → Maya Triman: DDP monthly-script walkthrough invite.
- AUDI-1061 → Edgar von Trotha: still wants block-coverage + lookback model? no = close.
- AUDI-1016 → Eric Salinger: ScyllaDB migration absorbs conditional-write question? yes = close free.

Meanwhile: AUDI-1060 (size DS19 OpenAI spend) is the one ungated start; AUDI-882/1203/1202 hold by design.
