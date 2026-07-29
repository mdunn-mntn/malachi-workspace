---
name: project_fangorn_on_mntn_id
description: "AUDI-1049 Fangorn-on-MNTN-ID epic — re-key Fangorn feature store IP→household (Option 1); Malachi's lane = FS build (1166-1170) + validation (1105); folder + full context at tickets/audi_1049_*"
metadata: 
  node_type: memory
  type: project
  originSessionId: 1e31df63-2e33-4ee1-ad1b-7fc2395f8bb7
doc_type: memory
keywords: [fangorn on mntn id, audi-1049, household re-key, feature store, household_id, audi-1166, audi-1105, sean yang, airflow-ti, graph_translation_signal]
domain: [project, identity, audience-scoring]
lifecycle: active
last_verified: 2026-07-29
---
**AUDI-1049 "Fangorn on MNTN ID"** (epic owner Matt Brorby; ⚠ AUDI-1057 is a *Done* modeling spike, NOT the
epic). Re-keys the Fangorn feature store + intent scoring **IP→MNTN ID (household)**, running parallel to the
IP flow until the Q1-2027 cutover (Identity initiative ID-327). Chosen path = **Option 1 hybrid translation**:
as-of join the identity graph inside the existing airflow-ti pipeline, re-key (advertiser_id, ip)→(advertiser_id,
household_id), emit a 2nd household-keyed output. **Sept-4 MVP.**

**Malachi's lane (chosen 2026-07-28): feature-store IMPLEMENTATION, open to validation** — "moving more toward
feature-store stuff, actual implementation; can do the validation." Wanted context ready, not executing yet.
- Primary: FS build chain **AUDI-1166** (L1 graph-mirror) → **1167** (household_resolution) → **1168** (L2
  derived) → **1169** (L3 ~900-col pivot) → **1170** (orchestration/backfill/shadow), then **1100** (feature-eng).
  All Unassigned/Backlog; **Sean Yang is de-facto FS lead** — coordinate scope. (1134 = the build umbrella that
  decomposed into 1166-1170.)
- Secondary: **AUDI-1105** validate MID-vs-IP (champion/challenger invalid — grain mismatch — needs custom design).

**Full context lives in git:** `tickets/audi_1049_fangorn_on_mntn_id/summary.md` (architecture, Option-1
commitments, ownership map, gating open questions, on-ramp) + per-child cards + the 2026-07-28 meeting
transcript in `meetings/`. Durable design facts (household_graph_parquet as-of join, max-confidence resolution,
HHID ~85% stable/30d, guid_log-only/DS46 scope, bidder-alignment risk) are in `knowledge/data_knowledge.md`
§"MNTN ID (household) re-keying of the feature store". Repo = `SteelHouse/airflow-ti`.

**Post-sync updates (Slack, 2026-07-29):** Sept-4 scope narrowing to **simplest end-to-end** (Matt's
anti-scope-creep) — PUNT DS13/19/46 replication, bidder-resolution alignment, full IPv6, non-IPv4 households.
Identifier scope = IPv4 (`id_type=30`) + maybe **GUID (`id_type=42`)**; `guid_log` has NO IPv6 (IPv6 only for
augmentor_log, excluded), so IPv6 is moot for v1. **NEW requirement lands on the FS work:** log every
ID→household translation → `dw-main-silver.identity.graph_translation_signal` (Weiang Li dev, modeled on
`hashed_email_signal`) for **graph-vendor crediting** (required even though FS sources only internal logs); ID
team ships a pyspark graph interface ~early Aug, Sean wires it into AUDI-1167. DDP crediting: none for Fangorn
(no DDP), but DS13/19 use DDP → needed ~mid-Oct ([[reference_ddp_billing_logic]]).

**IPv4-only v1 implementation (Slack #dev-audi-mntn-id, 2026-07-29):** map IPv4→HHID and build household L2/L3
on top of the **existing IP-keyed L1 — leave L1 alone**; the keyset-struct L1 rebuild is a fast-follow (graph
snapshot mirror still built). Deferring multi-identifier avoids the **multiple-membership intent shift** (adding
IPv6/GUID later moves a household's score). Bidder resolves via **id-service** (single ID → 1 max-confidence
HHID; `SteelHouse/id-service/src/bigtable.rs#L1084`); IPv4-only leaves IPv6/MAID-only households unscored (bidder
may not bid) — accepted. Bidder ~a few weeks off (id-service latency standup) → not near-term-blocking.

**Gating open questions before building:** 60-vs-90d graph retention (AUDI-1101); daily-vs-monthly L3 training
table; multi-IP→household collapse function (Identity chose random-pick "for code simplicity" — AUDI feature-
quality call). Adjacent north-star thread = the Uplift/incrementality model RFD B (AUDI-1052, Matt) — trains on
the same MID-keyed L3 tables. Related: [[reference_audience_intent_scoring_dag]] [[reference_airflow_ti]]
[[reference_fangorn_two_model_passes]] [[incrementality_experiment]] [[reference_bidder_serving_stores]].
