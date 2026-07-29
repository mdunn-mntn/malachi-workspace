---
name: project_fangorn_on_mntn_id
description: "AUDI-1049 Fangorn-on-MNTN-ID epic — re-key Fangorn feature store IP→household (Option 1, IPv4-only Sept-4); Malachi + Brian co-own L2/L3 build (1168/1169), Sean→DS13/19, 1166 optional/1167 placeholder; validation 1105; folder tickets/audi_1049_*"
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

**Feature-aggregation membership = THE core FS-build decision (Brian McAdams/Ryan/Sean, 2026-07-29):** an
identifier can belong to multiple HHIDs at different confidence (IPv6 → HHID1@0.3 AND HHID2@0.7); features
derive at the raw-row grain then attribute to household(s). Choice: resolve each row to **1 HHID by
max(confidence)** (like id-service, no overlap) vs let features **flow to multiple HHIDs (duplicates
visits/conversions → "LiveRamp segments" = HHID store unusable for analytics)**. Ryan: don't duplicate visit
counts; Sean leans resolve-to-1; Ryan: **test both ways**. Throw out shared IPs; more IDs in a lookup → higher
confidence; the graph resolves the ID-combo (IP+device_id disambiguates ISP rotations). Same call as the collapse
function — lands in AUDI-1168/1100, gates AUDI-1105 analytics. Loop in Matt.

**Locked 2026-07-29 PM (dev-mntn-id / #dev-audi-mntn-id):** (1) Naming = **`household_id`** (Jack; = mountain_id
= MNTN ID). (2) Sept-4 = **daily IPv4 conversion** (Jack), other id types bonus. (3) **Resolution in Layer 2**
for IPv4-only → L1 tables optional. (4) Membership: **Matt/Brian lean resolve-to-1-household-first** (single
max-confidence HHID); multi-HHID alt = store all ids as columns in `site_visit_signal`, drop confidence <0.5,
let visits duplicate. (5) **Coverage OK iff the bidder never targets an unscored HHID** (Brian). (6) **DS13 =
`site_visit_signal` = guid_log ∪ DDP ∪ augmentor_log**; re-key DS13 in the `tpa_ipdsc_export` job (Ryan) —
DS13/19/46 share the same re-key problem, all under AUDI. DS13 upstream = `ip_vertical_associations`, which
drops IPv6 (`.filter("ip NOT LIKE '%:%'")`); IPv4-only = convert right before `tpa_ipdsc_export`. Bidder-coverage
nuance (Ryan): bidder unchanged — if HHST threshold set it looks up the score, if no score AND threshold=0 it
bids anyway (unscored HHIDs NOT auto-skipped; watch when gate=0).

**Does IPv4-only re-keying even change the features? (2026-07-29 analysis — Malachi's first empirical step,
AUDI-1105/1168.)** IPv4-only convert-at-L2 resembles Nivas's convert-at-end (ID-359) but differs via (a) the
**historical as-of graph lookup** and (b) **IPv4→HHID being multiple:multiple, not 1:1** (pick max-confidence).
Brian's 4 cases: 1:1=no change; 1 HHID←2 IPv4=pick-one-IPv4; 1 IPv4→2 HHID=pick-one-HHID (no double-count);
no-IPv4 HHID=no score. **Aggregation diverges from IP-L2 only via (i) a HHID inheriting MULTIPLE IPv4s' features
— where the household VALUE is — or (ii) orphaned IPs.** Pick-one keeps it ~IP-level. Run the
household-vs-IP feature-distribution comparison early (Ryan: "code it and compare") to size whether the MID model
is worth it.

**Score-translation ≠ feature-aggregation (validated "100% right" by Ryan, 2026-07-29):** score translation
(`intent_score_household_map`, convert-at-end) keeps the single highest-confidence IP's score per household;
**feature aggregation (FS build, AUDI-1168) resolves per IP (max-conf, no fan-out) then GROUP BY mntn_id** so a
household aggregates ALL its IPs' features (pick-one discards signal). Cardinality: max-conf + fixed asOfDate/day
→ IPv4→HHID ~1:1/day, HHID→IPv4 1:many; graph maps each current IPv4 to one household (case 3 rare). Aggregation
= SUM, but **distinct/HLL features don't sum** (need HLL-merge) — open: does guid_log L2 have distincts? Two more
divergence sources: graph churn over the lookback (features follow day's vs snapshot's household — undecided);
orphaned/shared IPs vanish (**~9.5% of current IPv4 rows flagged shared**).

**RESOLVED (Ryan, 2026-07-29):** insertion point = **right before L158 of
`feature_group_2_derived/guid_log_derived_ip_vertical_id.py`**; distincts → **hll_merge** the sketches, else
sum/min/max. Works because of the **FS Layer-1 invariant** (every L1 feature aggregatable over 30d via
sum/min/max/hll_merge) → household GROUP BY mntn_id reuses the temporal-rollup primitives (**temporal rollup ≡
household rollup**; see [[reference_airflow_ti]]). Re-key confirmed a real change (improves multi-IP case + lookback).

**Work split + convention (2026-07-29):** design consensus reached. **Ryan Kleck owns the GUID fast-follow in
PARALLEL** (add GUID to L1 + ip/guid-combo lookup in L2, once IPv4 works) → the L1 keyset/multi-identifier work
is off Malachi's IPv4 critical path. **DAG convention (Sean, confirmed): reuse existing DAGs, add HHID work as
new models with an `hh_` prefix** (not a separate `feature_store_hhid_*` DAG set) — the concrete form of
AUDI-1170's additive-task-group / no-forked-DAG.

**OWNERSHIP SHIFT (2026-07-29, reshapes the lane):** **Sean Yang moved to DS13/DS19** (Alyson: DS13/19 is
AUDI's initial-rollout responsibility too); Sean asked **Brian McAdams to build the L2/L3 feature store WITH
Malachi** → **Malachi + Brian now CO-OWN the core L2/L3 build (AUDI-1168/1169)** — Malachi is a co-lead, no
longer a builder under Sean. **AUDI-1166 (graph mirror) is now OPTIONAL** (join the graph directly by default;
mirror = perf fallback; Sean stubbing it). **AUDI-1167 (`household_resolution.py`) is a PLACEHOLDER** (resolution
lives inline in the L2 model before L158 for v1; Sean stubbing the util). Sean's draft PR airflow-ti #1156 adds
a Layer-1 `(ip, guid)` grain table (for Ryan's GUID fast-follow; not needed for IPv4-only v1).

**Gating open questions before building:** 60-vs-90d graph retention (AUDI-1101); daily-vs-monthly L3 training
table; multi-IP→household collapse function (Identity chose random-pick "for code simplicity" — AUDI feature-
quality call). Adjacent north-star thread = the Uplift/incrementality model RFD B (AUDI-1052, Matt) — trains on
the same MID-keyed L3 tables. Related: [[reference_audience_intent_scoring_dag]] [[reference_airflow_ti]]
[[reference_fangorn_two_model_passes]] [[project_incrementality_experiment]] [[reference_bidder_serving_stores]].
