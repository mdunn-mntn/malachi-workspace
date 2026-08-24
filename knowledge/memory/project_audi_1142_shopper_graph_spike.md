---
name: project_audi_1142_shopper_graph_spike
description: "AUDI-1142 spike state: evidence complete 2026-08-24, 7 SP core / 9 SP with DQ guard drafted for AUDI-1086; posting + spike close gated on Bryce Wagg's scope reply."
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [AUDI-1142, AUDI-1086, AUDI-1087, shopper graph vertical spike, vertical endpoint, vertical_from_url, select recommendations, story point estimate, Bryce Wagg, Mike Allen, Brian McAdams, mm_domain_map, precache DAG, shopper_graph clone]
domain: [project, repos]
lifecycle: active
last_verified: 2026-08-24
---
Spike AUDI-1142 (estimate AUDI-1086, epic AUDI-1087 Support Select Recommendations): **evidence
complete 2026-08-24**, one day. Estimate: **7 SP core / 9 SP with the DQ guard**, drafted at
`tickets/audi_1142_shopper_graph_vertical_spike/outputs/audi_1142_jira_comment_draft.txt`.
**Posting the comment + closing the spike are gated on Bryce Wagg's reply** about scope
(estimate-only vs design input).

Key evidence (full record: the ticket's `summary.md`): /vertical never consults fpa.mm_domain_map
while /autopilot does; POST /vertical always scrapes+GPT-classifies when no vertical_id is sent;
97% of prod POSTs fail 400 with a 563-AID recurring population (IMP-067); the ticket's DLQ premise
is structurally absent today (errors acked on the embedding path, recommendations on Temporal);
mm_domain_map is NOT mirrored to BQ.

`SteelHouse/shopper_graph` is cloned at `/Users/malachi/Developer/work/mntn/shopper_graph`
(@6626756, the evidence commit).

**Why:** the next session on AUDI-1142/1086 needs the gate state, the SP numbers, and the clone
path without re-deriving them.

**How to apply:** on Bryce's reply — estimate-only: lint + post the draft on AUDI-1086, close
AUDI-1142, set this memory `lifecycle: archived`; design input: expand the frame per summary.md §0.
[[reference_shopper_graph_deploy]]


Update 2026-08-24: spike CLOSED by Malachi. Estimate (7 SP core / 9 with DQ guard) is on AUDI-1086 comment 610650. Brian McAdams will assess it later; any design decisions land on AUDI-1086, not the spike.
