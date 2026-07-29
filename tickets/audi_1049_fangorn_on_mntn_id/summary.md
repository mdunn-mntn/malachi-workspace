---
doc_type: epic
title: "AUDI-1049: Fangorn on MNTN ID — parallel household feature store + model"
status: backlog
date: 2026-07-28
summary: "Re-key Fangorn feature store + intent scoring IP→MNTN ID (household); parallel MID-keyed store + retrained model. Sept-4 MVP."
result: "not started"
question: "Can a Fangorn-like model trained on a MNTN-ID-keyed household feature store match/beat the current IP model rolled up to household, and ship for the Sept-4 MVP?"
framing_state: "skip: epic — per-child framing (1166-1170 build, 1100 tuning, 1103 train, 1105 validate, 1108 experiment)"
---

# AUDI-1049: Fangorn on MNTN ID — parallel household feature store + model

**Jira:** https://mntn.atlassian.net/browse/AUDI-1049
**Epic owner:** Matt Brorby · **RFD decider:** Alyson Lefkowitz · **FS-build lead:** Sean Yang
**Malachi's lane:** feature-store build (AUDI-1166→1170, 1100) primary · validation (AUDI-1105) secondary
**Status:** backlog · **Date:** 2026-07-28

> **This folder is the home for all of Malachi's work on this epic.** Child folders below hold per-ticket
> work; epic-level `queries/ outputs/ meetings/ artifacts/` hold cross-cutting work. Meeting transcripts +
> chat land in `meetings/`. Full assembled context (RFDs, PRD, Slack, ticket comments) is reconciled here.

---
## 0. Framing
Epic — framing lives per-child (`/frame` each build/validate ticket when you begin it). The one-line epic
question is in front-matter. The single falsifiable epic question: *does household-keying the Fangorn feature
store actually produce a better model than rolling up IP scores to the household, in time for Sept-4?*

---
## 1. What this is, and why (the whole initiative)
MNTN is swapping its primary identity key from **IPv4 → "MNTN ID," a household-resolution UUID** (Identity
team; Experian backbone + MNTN 1P signals — HEM, GUID, IP). MNTN ID is being threaded through **every**
platform system — targeting, bidding, frequency-capping, attribution, reporting — running **in parallel with
the IP flow** until a full cutover in **Q1 2027** (PRD, `ID-327`). **AUDI-1049 is the targeting leg:** re-key
Fangorn's feature store + intent scoring from IP → household so the model **trains and scores at household
grain** instead of transforming IP scores at the end.

**Why:** one IP can be one household or hundreds. That noise costs MNTN in reach counts, freq-capping,
attribution, and — the part AUDI owns — **models train on noisy signal.** Proof already in production:
- **EX-46** (DS47 household CRM exclusions): **−24% cost-per-new-customer, +30% conversion lift** (caveat: NTB
  *CPA* improved, NTB *rate* flat — distinct metrics).
- **World Cup HHID pilot**: HHID Verified Visits live; ~**12% of raw VVs / 19% of conversions** already on
  HHID models (Jun 1–15).

**North-star tie:** MM-AI Theme 3 (graph-based identity + continuous scoring) and Kale's incrementality
mandate — household grain is the substrate that makes a **clean per-advertiser RCT** possible (the always-on
~10% ghost-bid holdout + the bidder freq cap both move to household grain). See the uplift model, §8.

---
## 2. Architecture — the data path (reconciled: PDF roadmap + RFDs + tickets)
Parallel, **additive** re-keying — a second household-keyed output next to the IP one; no cutover before Run
(PRD NFR-4).

```
FS LAYER 1 (IP-keyed, raw)            MNTN IDENTITY GRAPH (idg repo; Experian + 1P/3P
  — stays IP-keyed & SHARED             edges + shared-IP disambiguation; daily/weekly/monthly)
    (no duplicate raw-log scans)                  │
          │        published to DW as:
          │        • dw-main-bronze.raw.identity_graph_history
          │            (daily as-of history; id_type=30 = IPv4 → household_id; is_shared,
          │             confidence_score, start/end_time, as_of_date, graph_version; ~60d)
          │            ← point-in-time / backfill / training
          │        • dw-main-silver.public.identity_graph (view, latest version) ← current translate
          ▼                                        ▼
   ┌──────────────────────────────────────────────────────┐
   │  "MNTN ID <> IP MAPPING & AGG"                        │
   │  • L1 graph-mirror (AUDI-1166): daily IP→household map │
   │  • household_resolution (AUDI-1167): +mntn_id,         │
   │    resolution_status (clean/shared/unresolved); as-of  │
   │    interval join                                       │
   └──────────────────────────────────────────────────────┘
          ▼
FS LAYER 2 (BOTH IP- and MID-keyed; daily + monthly)  — AUDI-1168
  • household derived: per (mntn_id, vertical_id), 7/14/30d lookbacks
          ▼
FS LAYER 3 (pivoted; BOTH IP- and MID-keyed)          — AUDI-1169
  • wide ~900-col model-ready pivot at mntn_id grain
          ▼
FANGORN train (1103·Brian) → daily household scoring (1106·Brian) → staging (1138·Sean) →
  audience_intent keyed to mntn_id (1136·Sean) → HHDSC = DS13/DS19 via graph end-join (1156·Sean) →
  tpa_hhdsc_export (1157·Sean) → MembershipDB HHID instance (AP-5385) → BIDDER reads household_score
```

**Repo:** `SteelHouse/airflow-ti` (local `~/Developer/work/mntn/airflow-ti`). The four new FS models are an
**additive task group inside the existing `feature_store_setup_model.py`** — one schedule, **no forked DAG**
(AUDI-1170). Cadence: graph lands ~20:00 UTC; setup DAG runs 01:03 UTC on the **d-1 `as_of_date`** (1-day
mapping lag, accepted). Layer dirs: `models/feature_store/feature_group_{1,2,3}_*`;
`feature_store_snapshot.py` builds forward-window labels; `model_run.py` submits to Dataproc Serverless.

**Two live design choices baked into the build (own these):**
- **Keyset vs household_id for L2 (Ryan Kleck, Slack).** Keying L2 by the *keyset* insulates you from
  graph-algorithm changes — "different graph model in future → don't backfill L1, just keep L2 going." The
  hedge against HHID churn (§6.4). Decide early.
- **id-service already returns the highest-confidence household_id** (`id-service/src/bigtable.rs#L1084`) —
  how the bidder consumes it. You don't build single-household disambiguation yourself.

---
## 3. Option 1 is confirmed — and the commitment
**RFD A "Feature Store Data Path & IPDSC" (Sean Yang, decider Alyson) recommends Option 1 (hybrid translation
layer).** IPDSC = the daily IP-keyed data-source-category export; Option 1 builds a parallel **HHDSC**
mirroring it + a new `hh_tpa_export`.
- **Opt 1 (chosen):** keep per-IP daily aggregation; add an **as-of join** to `identity_graph_history`
  (id_type=30); re-key `(advertiser_id, ip) → (advertiser_id, household_id)`; emit a 2nd household output.
- **Opt 2 (resolve-upfront):** cleanest long-term, event-level HH features — but **blocked** (auction logs
  carry no HHID). Not credible for Sept 4.
- **Opt 3 (translate-at-end, ID-358/359 "Simple Path"):** cheapest, already in flight for the Aug-3 baseline,
  is the **fallback** — "model never sees household-grain signal," doesn't satisfy the epic. Matt wants it run
  in parallel as insurance.

**Commitment:** AUDI owns the re-keying seams (`fangorn_14day_lookback.py`
max-score-per-`(vertical_id,ip)`→household_id; six `audience_intent` jobs), the **household-grain label
re-derivation** (biggest unknown), the HHDSC datasets, and `hh_tpa_export`. ~33 dev-days single-dev.
Producer-side cost ≈ **$15.5K/mo landing + $300/mo storage** (PDF). **RFD A still open — 8 decisions pending,
due 2026-08-03, outcome table empty.**

**Modeling decisions locked in the AUDI-1057 spike** (constrain the build): keep **148 verticals** for MVP;
**exclude out-of-graph households** from train + serve; label = **`guid_hh_log`**; rule-based **DS13/DS19 move
to household by a simple graph END-JOIN** (household carries a category if any member IP does — no DS-builder
change, reuse DS IDs; AUDI-1156); true Fangorn categories need the native retrain (1103); DS46 deferred until
HH content exists; validation is **offline-first, not champion/challenger** (grain mismatch).

---
## 4. Child map & sequencing (Malachi's lane in **bold**; ⚠ epic = AUDI-1049, not 1057)
Critical path: **L1 mirror → resolution → L2 → L3 → orchestration → train → validate → experiment.**

| WS | Ticket | Owner | Role | Folder |
|---|---|---|---|---|
| A | AUDI-1055/1056 spikes | Sean | ✅ Done — Option 1 chosen | — |
| A | **AUDI-1166** L1 graph-mirror | **Malachi** | daily IP→household_id mirror model | `audi_1166_l1_graph_mirror/` |
| A | **AUDI-1167** household_resolution | **Malachi** | `resolve_households()` util + as-of join + tests | `audi_1167_household_resolution/` |
| A | **AUDI-1168** L2 derived | **Malachi** | household derived models (7/14/30d) | `audi_1168_l2_derived/` |
| A | **AUDI-1169** L3 pivot | **Malachi** | ~900-col mntn_id-grain pivot + parity report | `audi_1169_l3_pivot/` |
| A | **AUDI-1170** orchestration/backfill/shadow | **Malachi** | additive task group + shadow parity | `audi_1170_orchestration_shadow/` |
| A | **AUDI-1100** household feature-eng | **Malachi** | tuning (sum/mean/recency) + enrichment; follow-up | `audi_1100_household_feature_engineering/` |
| — | AUDI-1134 build umbrella | Malachi | decomposed → 1166-1170 (holds shared build-frame) | `audi_1134_feature_store_build/` |
| B | AUDI-1057 spike | Matt | ✅ Done — modeling scope | — |
| B | AUDI-1101 backtest depth | Matt | 60-vs-90d retention (gates backfill) | — |
| B | AUDI-1102 `guid_hh_log` visit label | Matt | household-grain training labels | — |
| B | AUDI-1103 train MNTN ID Fangorn | Brian | retrain XGBoost/vertical on household store | — |
| B | AUDI-1106 daily household scoring job | Brian | productionize predictions | — |
| C | AUDI-1104 tune HH thresholds | Alex | re-calibrate hi/mid cutoffs | — |
| C | **AUDI-1105** validate MID vs IP model | **Malachi (candidate)** | offline AUC/PR-AUC vs rolled-up IP | `audi_1105_validate_mid_vs_ip/` |
| C | AUDI-1138 staging job | Sean | `fangorn_14day_lookback` equiv | — |
| D | AUDI-1108 design HH experiment | *unassigned* | custom online design (Exp team) — Malachi candidate | — |
| E | AUDI-1136 audience_intent→mntn_id | Sean | household scoring job | — |
| E | AUDI-1140 new datasource | Sean | ✅ Done | — |
| E | AUDI-1156 DS13/DS19→HHDSC | Sean | graph end-join | — |
| E | AUDI-1157 tpa_hhdsc_export POC | Sean | → MembershipDB | — |
| E | AUDI-1107 HH scoring monitors | *unassigned* | mirror IP Fangorn monitors | — |
| F | AUDI-1164 cost to FinOps | Brian | In Progress | — |

**Status:** 4 Done (1055/1056/1057 spikes + 1140); 1164 In Progress; **~18 build/model/validate/experiment/
monitor tickets Backlog, most Unassigned** — including the whole L1/L2/L3 build (yours).

---
## 5. Ownership map (who to talk to)
- **Identity / ID team** — the graph (idg), `identity_graph` tables, Identity Service (3–5ms), MNTN ID def.
  **Jack Barbey** (TL/EM, endpoint spec), **Elena Donnelly** (PRD), **Luis Chelala** (TPM), **Ryan Kleck**
  (bridges Identity↔audience_intent; owns audience_intent DAG; ID-358), **Nivas** (ID-359 IPDSC).
- **AUDI (yours)** — **Matt Brorby** (epic + modeling + labels + uplift RFD), **Sean Yang** (FS spikes/RFD
  author, **de-facto FS build lead** — coordinate scope with him), **Brian McAdams** (Vertex train + scoring +
  FinOps), **Alex Knorr** (thresholds), **Bryce Wagg** (PM), **Alyson Lefkowitz** (dept lead / FS-RFD decider).
- **Audience Platform** — consumers + audience-service re-key; MembershipDB HHID. **Jaime Mutale** (Aud-Svc
  RFD), **Daniel Hartnett** (AP-5385), **Zach Schoenberger** (SoT resolver + MID MemDB), **Mike Dolt** (budget).
- **Bidder** — Identity-Service call, dual cache lookup (HH+IP) + blend, f-cap reset (BID-3356).
- **Pacing** — HHST DAG deprecation / dual path (Swapnil Patil, PER-6688).
- **Measurement** — VV→GA lift, VV→Conversion + offline HHID match (Nate Gardner; models 31/33 live).

---
## 6. Open questions / contradictions that gate the build (resolve early)
1. **Graph-history retention 60 vs 90 days.** RFD A + 1170 say 60d (caps backtest); 1057 says 90d "sufficient."
   Gates the backfill window; **AUDI-1101 must resolve before 1170's backfill.** Fallback = `household_graph_parquet`.
2. **Daily vs monthly L3.** PDF roadmap says **train on L3 *monthly***, infer daily; the build tickets encode
   **daily only** — monthly training table not yet specced. Reconcile before AUDI-1168/1169.
3. **Multi-IP→household collapse function.** Identity's quick path (ID-358/359) picked **random/first for the
   intent score + union-dedupe for categories, "for code simplicity," revisit "if it becomes a performance
   baseline"** — it now is. Random pick can **dilute the HI/PP two-pass signal.** AUDI feature-quality call
   (your AUDI-1100/1168; your own open AUDI-1057 comment asks exactly this).
4. **HHID stability across graph runs** (Ryan Kleck ×2): if household IDs churn run-to-run, **audience counts
   inflate + MID feature/score semantics drift.** The keyset-vs-household_id L2 choice (§2) is the hedge.
5. **Household label source** (Decision 7 / AUDI-1102): roll IP-grain VVs up through the graph vs HHID-native
   models 31/33. v1 = roll-up **with a label-sensitivity analysis first** (not done).
6. **Reconciliation band** during parallel run (audience-size delta + rank-correlation thresholds) undefined.
7. **Coverage join:** ID-358 intent feed ≈ **111M households**; ID-359 IPDSC subset ≈ **11.5M mntn_id** (one
   campaign) — do they join cleanly for the L2/L3 build?

---
## 7. Malachi's on-ramp when you begin
1. **`/transcribe`** today's `MNTN ID → Fangorn` meeting (running now → `meetings/`); reconcile vs §4 (who-takes-what).
2. **Claim FS-build tickets** at planning; coordinate scope with Sean. Optionally AUDI-1105.
3. **First BQ profiling** (before writing L2/L3): (a) `identity_graph_history` id_type=30 — resolution rate,
   clean/shared/out-of-graph split, `confidence_score` distribution, IPv4 coverage vs delivery IPs;
   (b) prototype the **as-of interval join** + measure cost (federated dry-runs under-count ~30× — sample 1 day);
   (c) settle the **collapse function** (§6.3) with a quick study; (d) pin **60-v-90d** + **daily-v-monthly L3**.
4. **Resolve §6 gating questions** with Sean / Matt / Ryan before building.

---
## 7b. Meeting notes — 2026-07-28 "MNTN ID → Fangorn" design sync (Ryan Kleck, Sean Yang, Matt Brorby, Alex)
Transcript: `meetings/audi_1049_01_mntn_id_to_fangorn_2026_07_28.txt`. This is the freshest design detail and
**refines the build** — read it before writing L1/L2.

**The core problem = the lookback join.** Converting an IP→household *today* is trivial (look it up). The hard
part is going **back 30 days** in the feature store: L1 stores IP daily, L2 aggregates 30d back per IP — so
you must join to the **historical graph as-of each day**, not the current graph.

**Ryan's recommended architecture (his "option c", confirmed direction):**
1. **Materialize a daily graph snapshot** partitioned by `as_of_date` (the graph already ships this way) into
   AUDI's own GCS/feature-store — the snapshot model is a **~7-min job** (Ryan tried it). Source table:
   **`household_graph_parquet`** (~600 GB), partitioned by **`as_of_date` + `as_of_date_revision_number`**
   (usually 0, sometimes 1 on a revise), with a non-partition `graph_version` column. As-of pattern: for one
   day of guid_log (e.g. Jun 2), take `max(as_of_date) < Jun 2` then `max(as_of_date_revision_number)` inside
   → partition elimination. **TTL unknown — Ryan action item to not dump it (needed for historical training).**
2. **Keep L1 IP-keyed** but as a **STRUCT keyset** of every identifier (IPv4, IPv6, GUID, hashed_email,
   hashed_phone — null if absent), grouping by the struct instead of `ip`. (This is the "keyset vs
   household_id" choice from Ryan's Slack — the meeting resolves it toward **keyset in L1**.)
3. **Do the graph join at L2/L3** against the historical snapshot. Keeps L1 insulated from graph-algorithm
   changes (don't backfill L1 when the graph model changes).

**Multi-identifier resolution = MAX CONFIDENCE.** One row can carry IPv4 + IPv6 (+ GUID + HEM), each mapping
to a *different* household with a different `confidence_score`. **Join once per identifier, take the highest
confidence → one household per row** (never let it fan out / double-count visits). This matches the
**id-service `resolveHouseholdId` endpoint**, which "just does max confidence" (Ryan, just took it over — to
confirm). id-service today takes **one** id (IPv4 **or** GUID, **not IPv6**) and doesn't always return a
household (misses). Possible new "resolve best household ID" endpoint taking multiple id types (TBD).

**BIGGEST RISK — bidder/scoring resolution must MATCH.** The bidder resolves auction IP→household at bid time;
the feature-store scoring **must resolve identically**, or "we give you a score for a household but matched it
differently, so they aren't actually related." **Bidder team has no resolution strategy yet** — need their
criteria (require a match? drop ~20% unmappable?). Cross-team, unsequenced. (Ryan → Jack: who resolves IPv6
for the bidder, bidding-side or identity-side?)

**HHID stability (Alex's analysis): 85% of household_ids stay constant over a 30-day window (~15% churn).**
Ryan confirms the household_id key itself is **not** semantically reassigned to a different physical house
("doesn't happen from what they tell me") — it's the IP→household *mapping* that moves. Partially answers §6.4.

**Scope narrowing (what actually feeds Fangorn):** Fangorn consumes **only `guid_log` (IP, advertiser_id)** at
L1 → L2 aggregates to day/IP/vertical + a **30-day snapshot** (monthly version on the 1st of month, else
daily) → L3 pivots wide (column per vertical). So **~1 L2 + related L3** — not the whole store.
**`augmentor_log` is built into the feature store but NOT consumed by the ML model → leave it out of the MNTN
ID version.** The **Challenger model** (the "model in waiting") already runs on Feature Store — the MNTN-ID
Fangorn **is** the Challenger path. **L1 is currently IPv4-only** — adding IPv6 means **rebuilding L1** (more
scope → flag to Bryce); IPv4-only is the baby-step.

**Sept-4 scope debate (unresolved — ask Alyson):** Matt/Sean = **Fangorn end-to-end, feature store → IPDSC
output, DS46 only** (DS13/DS19 a bonus). Ryan's caution: whatever you do for Fangorn you'll need for
**DS13/DS19/DS46** too, because the audience expression uses DS13 — convert only Fangorn and "why aren't we
serving certain households, because DS13 isn't converted." **DS13/DS19/DS46 fall under AUDI**; everything else
is Nevis's direct IP→mountain_id re-key (union-aggregate). DS13 today is **rule-based (group-by-IP verticals),
not feature-store**. Smoke test now = quickest IP→household conversion of the output (Nevis IPDSC + Ryan's
intent-score-household job).

**Naming (open):** column = `mountain_id` (= household_id = MNTN ID, all the same). Sean uses
`mountain_id`; id-service returns `household_id`. Ryan to standardize with his team (mountain_id vs hh_id vs
household_id). Note: if left as-is, `household_score` will actually mean the **prospecting** score.

**Ryan's 3 action items:** (1) study id-service resolution + bidder/IPv6 ownership (→ Jack); (2) confirm
`household_graph_parquet` TTL (don't dump — need historical training depth); (3) standardize the id column name.

## 7c. Slack updates — post-sync scope + crediting (2026-07-28/29)
Threads: #dev / idg-tgt-workspace (Matt Brorby, Sean Yang, Brian McAdams, Jack Barbey, Luis Chelala, Alyson,
Weiang Li). Decisions still forming — flag as such.

**Sept-4 scope is narrowing to "simplest end-to-end" (Matt's push against scope creep, team aligning):** a
working end-to-end Fangorn pipeline re-keyed to MNTN ID, using **the simplest logic Ryan uses for his smoke
tests**. **PUNT to later:** DS13/DS19/DS46 replication to MNTN ID (Ryan said these are on AUDI for Fangorn to
*fully* work), the **bidder-resolution alignment** (our IP→HHID logic must match the bidder's or scores are
unreliable — IP_1→HHID_1 vs HHID_2), full **IPv6**, and **non-IPv4 households**. Initial version covers only
households that have an **IPv4**.

**Identifier scope — GUID (id_type=42), not IPv6 (Sean):** `guid_log` has **no IPv6 data** at all, so IPv6 is
only relevant once `augmentor_log` is added to training (excluded from v1). But `guid_log` carries **`guid`,
which IS an identifier in the graph — `id_type=42`** — and the current FS design scoped only IPv4. **Open for
Sept-4: whether to bake GUID into L1 as a 2nd identifier.** (This corrects §7b's "IPv6 = L1 rebuild" framing —
for the guid_log-only scope, IPv6 is moot; GUID is the real question.)

**NEW REQUIREMENT — graph-translation-signal logging (graph-vendor crediting), lands on the FS work (Jack
Barbey/Luis Chelala):** AUDI must **log every event where an ID is translated into a household_id** in the
feature store and pipe it to **`dw-main-silver.identity.graph_translation_signal`** (dev version by Weiang Li;
modeled on today's `hashed_email_signal` table). **Required even though the FS sources only internal logs
(guid/augmentor)** — the graph itself contains licensed-vendor data, so graph vendors must be credited. The
**ID team is building a little pyspark interface to the graph** (handles current-graph selection + translation
logging) — target ~end of next week (~Aug 8); **Sean drops it into the FS code.** Weiang Li → Sean to spec the
event data. This is a resolution-step concern → touches AUDI-1167 (where translation happens).

**DDP crediting under MNTN ID (Alyson/Jack, open):** **Fangorn uses NO DDP data → no DDP crediting for
Fangorn.** But **DS13/DS19 DO use DDP** → DDP-vendor crediting still needs to change under MNTN ID for those;
Alyson to bring the team's thoughts. Needed by **~mid-October** for real-campaign testing. Ties to
`reference_ddp_billing_logic`.

## 7d. IPv4-only implementation approach + bidder resolution (Slack #dev-audi-mntn-id, 2026-07-29)
Refines §7b's "keyset struct in L1" — for the **IPv4-only v1 the existing L1 stays untouched.**

**IPv4-only v1 = edit L2/L3 only, LEAVE L1 ALONE (Ryan/Sean/Brian).** Map IPv4→HHID and build the parallel
household L2/L3 **on top of the existing IP-keyed L1** — "just edit the level-2 and level-3 feature store jobs
and leave level 1 alone" (Ryan). So the **keyset-struct rebuild of L1 (§7b) is the FAST-FOLLOW**, not v1; v1's
graph join lands in AUDI-1168/1169, not a rebuilt AUDI-1166 guid_log L1. (The graph-snapshot mirror itself is
still built.) Sean is fine with IPv4-only for a quicker turnaround, pending alignment with the bigger team's
deliverables.

**Multiple-membership risk (Brian McAdams) — the reason to defer, not rush, multi-identifier.** If a household
has both an IPv4 and an IPv6 (or GUID/MAID), then **adding those identifiers later will shift household intent
noticeably** — more IPs/signal roll into the household, so a household's score can change drastically when the
logic expands. Ship IPv4-only cleanly first; treat the identifier expansion as a deliberate, measured change
(this is also why the collapse-function §6.3 matters).

**How the bidder resolves (Ryan) — single-ID → 1 HHID via id-service.** The bidder will soon call **id-service**
with whatever identifier is in the bid stream (IPv4, IPv6-with-no-IPv4, MAID, …) and get back the **single
highest-confidence household_id** (`SteelHouse/id-service/src/bigtable.rs#L1084`). A single-ID lookup resolves
to exactly one HHID — simpler than the multi-ID max-confidence join AUDI does in the FS. **Coverage gap
(accepted for v1):** if AUDI only scores IPv4-resolved households, a household the bidder resolves via
IPv6/MAID only **won't have a score → the bidder may not bid on it.** **Timeline:** the bidder is "a few weeks"
off — Ryan is still standing up id-service to hit their latency SLA — so bidder-alignment is **not near-term
blocking**; AUDI can proceed IPv4-only. The ID team's **pyspark "SDK-type" interface** (the graph-selection +
translation-logging helper, §7c) is what AUDI will consume for the resolution + crediting logic.

## 8. Adjacent north-star thread — the Uplift model (RFD B), for awareness
**RFD B "Fangorn-Like Incrementality (Uplift) Model" (Matt Brorby, DRAFT, recommends Option 2 — additive
persuadables audience).** Fangorn ranks propensity (ROC-AUC 0.96) but the High band (~78% of volume) returns
only ~+1% lift while Mid/PP carry 5–10×. Uses the always-on ghost-bid holdout as both training label and eval;
X-learner prototype (Qini 1.86), distinct from Fangorn (Spearman 0.25). **It wants to train on the *same*
MID-keyed L3 tables you're building** — keep your L3 schema uplift-friendly. Malachi has two live comments
(0–100 blend range; 120-day IP blocks + HHID). Parent tickets AUDI-1052 (epic) / AUDI-1077 (spike, done).

---
## Appendix — sources & data locations
- **Identity graph:** `dw-main-bronze.raw.identity_graph_history` (as-of, id_type=30, is_shared,
  confidence_score, ~60d); `dw-main-silver.public.identity_graph` (latest view).
- **Scores GCS:** `gs://household-scoring-prod/output/scoring/{prospecting_intent,advertiser_intent,fangorn_prospecting_scoring}/`;
  HH dev sample `gs://household-scoring-dev/output/scoring/intent_score_household_dev/` (~111M households, dt=2026-07-20).
- **HH log tables live:** `ads_clickpass_hh_log, click_hh_log, guid_hh_log, conversion_hh_log`.
- **RFDs (TAR space):** A Data-Path/IPDSC `3704094762` (Opt 1) · B Uplift `3699310667` (Opt 2 draft) ·
  C Audience-Services `3689218249` (Opt 1) · D AP-5385 MembershipDB `3689938973`. PRD `3668377601` (ID-327).
  Touchpoints Tracker `3638362166`. Prospecting-vs-Advertiser scores `3487891474`.
- **Jira:** epic **AUDI-1049**; Identity **ID-327/357/358/359**; uplift **AUDI-1052/1077**; airflow-ti PR #1142
  (intent-score HH re-key).
- **Readiness brief (source):** `~/.claude/plans/we-have-a-huge-dazzling-crayon.md` (full ingest synthesis).

---
## 9. Data Documentation Updates
_(record here what gets added to `knowledge/data_catalog.md` / `data_knowledge.md` as the build progresses —
Identity Graph tables schema, as-of join cost, resolution-rate findings, collapse-function study.)_
