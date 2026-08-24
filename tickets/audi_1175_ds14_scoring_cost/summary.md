---
doc_type: ticket
title: "AUDI-1175: Quantify $ cost of scoring non-addressable MM/vertical IPs (DS14 gate)"
status: done
date: 2026-07-28
summary: "Cost the MM/vertical IPs DS14's 8-day gate makes non-biddable; go/no-go on gating scoring"
result: "Gate is safe across all consumers (HHST auction-scoped); worth ~$2-11k/mo; impl = AUDI-1176"
question: "How much compute $ is spent scoring MM/vertical IPs DS14 makes non-biddable, and can we gate it with no coverage loss?"
framing_state: locked
---

# AUDI-1175: Quantify $ cost of scoring non-addressable MM/vertical IPs (DS14 gate)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1175
**Status:** Done (Jira transitioned 2026-08-24)
**Date Started:** 2026-07-28
**Assignee:** Malachi
**Blocks:** [AUDI-1176](https://mntn.atlassian.net/browse/AUDI-1176) (gate scoring input to DS14-addressable set)

---

## 0. Framing (locked 2026-07-28)

- **Question:** How much daily/annual Dataproc compute (and IPDSC storage) is spent scoring the MM/vertical IP universe that DS14's ~8-day availability gate makes non-biddable, and can scoring be gated to the addressable set with zero biddable-coverage loss?
- **Goal (why):** Cost reduction (Kale focus area). The answer decides whether to fund AUDI-1176 (gate the scoring input). North-star tie: Tier 3 infra/cost that also de-risks MembershipDB resilience.
- **Objective (done-when):** A dollar figure (daily + annual) for the wasted scoring compute/storage, a list of any non-bidding consumers of the full scored universe, and a binary go/no-go on AUDI-1176.
- **Approach:** (1) sizing — DONE (HLL distinct-IP on `ipdsc__v1`). (2) Dataproc cost — estimate from the `audience_intent` cluster config x runtime. (3) consumer audit — grep every reader of the scored outputs (intent_score tables, GCS prospecting-scores, IPDSC DS13/19/46) beyond the bidder. (4) validate the no-loss argument with the bidder owners.
- **What would change the answer:** if a non-bidding consumer (AUD-5221 deciles / LiftLab / lookalike seeds / Fangorn training) legitimately needs the full universe, gate only the serving-bound path (savings shrink toward storage-only). If the Dataproc cost of the scoring stage is trivial (<~$1k/mo), AUDI-1176 is not worth it.

## 1. Introduction

Origin: user hypothesis (2026-07-28) that DS14 ("MNTN Global Data") restricts bidding to recently-seen IPs, so MNTN may be scoring/storing far more IPs on MM than it can ever bid on. Investigated DS14's mechanics, effective window, the scoring pipeline's input universe, and sized the gap.

## 2. The Problem

MM/vertical intent scoring materializes scores for the full ~31-day IP universe, but DS14 (ANDed onto ~every audience expression at serving time) makes only the recently-seen subset biddable. The scored-but-non-biddable remainder is recomputed daily = wasted Dataproc compute + IPDSC storage.

## 3. Plan of Action

1. Confirm what DS14 is and its effective serving window. **DONE.**
2. Confirm whether MM/vertical scoring is gated to the addressable set. **DONE (it is not).**
3. Size the scored-vs-addressable gap. **DONE.**
4. Estimate the Dataproc $ of the scoring stage. **IN PROGRESS.**
5. Audit non-bidding consumers of the full scored universe. **TODO.**
6. Validate the no-loss gating argument with Ryan Kleck / Sean Yang / Zach Schoenberger. **TODO.**
7. Go/no-go recommendation on AUDI-1176.

## 4. Investigation & Findings

Three parallel code investigations (airflow-ti, sqlmesh, membership-db) + one BQ sizing. Full write-back in `knowledge/data_knowledge.md` "DS14-opt spike" block and `knowledge/ds_catalog.md`.

**Fact 1 — DS14 origin (not a pre-MM recency filter).** Born 2025-08-13 (Sean Yang prototype), productionized 2025-12-11 under AUDI-369. POST-MM. Positive addressability set from `augmentor_log` (1d) ∪ `bidder_auction_events` (1d) ∪ `guid_log` (4d); `category_id`=1 (first-party) / `exchange_id` (auction-only) / 1000 (`.0` quarantine, unread downstream).

**Fact 2 — effective window = ~8 days, not 1-4.** membership-db applies per-source TTL `data_source_ttls['14']=8` via per-IP epoch decay (not archive overwrite). Matchable 8 days past last build inclusion. (30d if DS14 is Portal audience-type registered; 8 is the code-intended path.)

**Fact 3 — DS14 IS materialized in IPDSC** (`ipdsc__v1`, `data_source_id=14`, ~149M IPs/day). Corrects the prior `data_knowledge.md` "zero ipdsc rows / computed at bid time" note.

**Fact 4 — scoring is UNGATED by DS14.** `spark/audience_intent/vertical_high.py` / `vertical_mid.py` score every (ip, vertical_id) over a 31-day DS13 IPDSC window; `vertical_mid` BUCKET 2 deliberately scores IPs with no recent activity. DS14 is a separate DAG, ANDed only at bid time.

**Sizing (HLL distinct-IP on `ipdsc__v1`, ~1.5% err; query in `queries/`):**

| Source | Scored (31d) | Addressable ∩ (DS14 8d) | Not addressable | % waste |
|---|---:|---:|---:|---:|
| DS19 (MM Core) | 499.4M | 156.6M | 342.8M | **69%** |
| DS13 (verticals / PP-v1) | 269.8M | 164.7M | 105.1M | **39%** |

DS14 8d set = 259M IPs; 1d = 149M. At the strict 1-day window (original thesis) DS13 waste = 56%. Addressable count uses ALL DS14 categories; the standard `cats:[1]` gate is a subset, so true waste is >= these figures (conservative).

**Window note (2026-07-30):** DS14's effective in-market window is ~9-12 days from an IP's *last log sighting* (1-4d build lookback + 8d serving TTL), but the sizing above is on the **8-day union of materialized DS14 membership** — which IS the biddable snapshot (TTL=8d ⇒ biddable = the last 8 daily builds), so the 9-12-day figure is narrative and does NOT change these numbers. Empirical DS14-window behavior (no hard cliff; display is a same-day augmentor echo; CTV has only a soft edge) is in the companion ticket **AUDI-1117** (`tickets/audi_1111_vendor_quality/audi_1117_ds14_svs_overlap/`). (The old "~7-day augmentor" reading was a decode error.)

### Consumer audit — is the gate safe? (2026-07-28)

Swept airflow-ti, sqlmesh, olympus, membership-db, mode-assets, airflow-camperbid, DDM/Redshift, targeting-infra-ml. **Verdict: safe for every serving/bidding path, but NOT safe to apply globally without scoping** — one live control-plane consumer reads the full scored universe.

**SAFE-TO-GATE (serving-bound, already addressable):** camperbid `intent_score` DAG (scores→Aerospike), `intent_score_household_map`, membership-db, `tpa_mntn_id_export`, the LIVE holdout-membership model (`tpa_membership_updates_log_insegments.sql`), Mode reporting (reads delivered CIL scores, not the raw set), Olympus (catalog-only, no query consumer).

**SEPARATE PIPELINE (unaffected):** Fangorn training/inference read a monthly **1%-sampled feature store** (`run_target_engineering.py` / `run_inference.py`), NOT `prospecting_intent`. Caveat: do NOT gate feature-store generation itself (needs full-population negatives).

**HHST threshold recommender — TWO systems (deeper read + file-verified 2026-07-28):**
- **(A) airflow-camperbid `intent_score_threshold_v4`** (`campaign_bucket_population_runner`) — the **auction-scoped** recommender. Builds its bucket population from **bid-side logs** (`bid_price_log` ∪ `bidder_bid_events`) over a ≤24h window, filtered to `has_price` OR score-failure reasons. Non-biddable scored IPs never generate a bid row, so **they are already absent from its denominator** → the scoring gate does NOT starve it. Buckets on a bid-time coalesced `hh_score` (conquest/segment → household → advertiser → 0), NOT on `prospecting_intent` directly; keyword info is baked into the score BANDS upstream in `prospecting_join` (Max Reach 1-3332 keyword-only-random, Mid 3333-6665, Peak 6666-8000 high-no-kw, High 8001-10000 high-with-kw). **Gate-SAFE.**
- **(B) DDM/Redshift `ddm.hhst_bucket_collections` → `hhst_generate_recommendation`** (`ETL-DCO-Automated-Threshold-Adjustment.py`, owner **Devon Rogers**; all three files verified to exist via `gh api`) — a DCO automation that DOES read the FULL scored `ext_tpa.prospecting_intent` as `ip_population`, but scoped to the **`ddm.test_hhst_campaigns` pilot** set. This is the only scored-universe coupling, small-scope. Gate would bias it (`<100→6666` guardrail). NB the 30-day window in this system is the **freq-cap exhaustion** lookback, not the population window (population is ≤24h, already tighter than DS14's 8d) — so the earlier "30d-vs-8d" flag is pre-existing, not gate-created.

**Open question (Devon):** which system sets production HHST for the campaigns we'd gate, and is the DDM pilot (B) expanding or can its denominator move to an addressable/auction basis?

**UNKNOWN (confirm with owners, not resolvable from code):** AUD-5221 population deciles (no implementation in any repo; if it splits the full US IP population it needs the full set — Alex/Zach) and any LiftLab/DS52 full-scored-universe incrementality export (none found; DS52 is an IPDSC *input* only, Liftlab is outbound Orca sync — #dev-incremental-lift owner).

**MUST-KEEP-FULL — a planned (non-code) consumer, surfaced 2026-07-30:** the incrementality team's proposed **"remove DS14 as an experiment treatment"** (Kirsa; Q2 incrementality OKR) NEEDS the full scored universe. Removing DS14 from a campaign's bid expression opens bidding to scored-but-not-recently-seen IPs (~1.6× vertical / ~3.2× MM-Core pools = `1/(1−0.39)` and `1/(1−0.69)`, i.e. derived from our own waste figures). This gate removes exactly those scores → the two conflict if both go global. This is the concrete instance the audit's "what would change the answer" anticipated. Resolution is a priority/sequencing call (incrementality is Q2 #1): run the experiment first, gate output-only (keeps full scoring), or hold AUDI-1176 while the experiment is active. Tracked in AUDI-1176 §4.

**Design implication (updated 2026-07-28):** the ~$9.6k/mo prize most likely **SURVIVES**. The primary auction-scoped recommender (A) already excludes non-biddable IPs from its denominator, so gating scoring doesn't touch it. The only scored-universe coupling is the DDM pilot (B) on `test_hhst_campaigns` — small-scope, and Devon owns it. De-risk with two shadow queries before any rollout:
1. **Starvation:** per active funnel-1 prospecting campaign, count how many would fall below `next_population_required` if bid-log population were restricted to DS14-addressable IPs (and how many already hit "HHST set to max reach").
2. **Max-Reach inflation:** fraction of funnel-1 bid rows with a score-failure reason (`missingIntentScore`/`invalidHouseholdScore`) that coalesce to `hh_score=0` and land in bucket 0 (bounded to the intra-day DS14 timing gap already covered by RTC).

Confirm system authority with Devon; clear AUD-5221 + LiftLab with their owners regardless.

### Shadow query 1 — Max-Reach inflation (run 2026-07-28, `queries/audi_1175_maxreach_inflation.sql`)

1-hour prospecting sample of `bid_price_log` (Beeswax, `objective_id IN (1,5,6)`):

| Arm (recommender denominator) | rows | distinct IPs |
|---|---:|---:|
| has_price (valid score, real bid) | 29.5M | 3.15M |
| intent-score failure → Max Reach (`invalidCampaignIntentScore` 1.77B, `missingIntentScore` 532M, `invalidAdvertiserIntentScore` 7.6M) | ~2.31B | 5–6M+ (overlap) |
| household-score failure (`invalidHouseholdScore`/`invalidAdvertiserHouseholdScoreFailure`) | ~0 (not in top 40) | ~0 |
| pacing/floor/geo/ghost (NOT in denominator) | ~740M | — |

**Finding: Max-Reach is already the dominant bucket by construction (~99% of the v4 denominator rows are already unscored → `hh_score=0`), gate or no gate.** Our gate can't push a currently-priced IP into Max Reach: the IPs we'd stop scoring are non-DS14, which fail the availability AND and never reach the score check. The priced arm (3.15M IPs/hr) is preserved because we keep scoring all DS14-addressable IPs. Residual = the scoring-snapshot vs 8-day-serving-window alignment gap (gate on the 8-day DS14 union; let RTC cover intra-run new IPs). **Net: the gate adds ~nothing to Max-Reach inflation.**

Note: the two HHST implementations use DIFFERENT failure sets. Redshift/DDM retains only household-score failures (`invalidHouseholdScore`/`invalidAdvertiserHouseholdScoreFailure`) — **~0 here → its denominator is mostly has_price (robust)**. v4 retains the intent-score failures (which dominate). (Authority resolved below: the v3/v4 → idso chain is the auction-scoped prod writer; no Devon confirmation needed.)

### HHST write path + applied-threshold distribution (empirical, 2026-07-28, `queries/audi_1175_hhst_writer_and_thresholds.sql`)

Compass located the tables but was denied row access to `dw-main-bronze`; I ran the queries it couldn't via the CDC mirrors (I have access; the `camperbid_prod__hhst_v4__*` GCS externals are still blocked for me too — `gs://camperbid-prod` list denied, same wall).

**Two write paths, cleanly separated:**
- `dso.configuration_service_campaign_presets.household_score_threshold_preset` = **manual ops overrides only** — ~26 campaigns, every `reason` human (`PER-####` Jira, "WC Setup", "SF Program", "TOF MANAGE", "Special Help"). The automated recommender does NOT write here. Gate-irrelevant.
- `dso.household_score_thresholds` (CDC: `dso_household_score_thresholds`) = **the applied threshold**, one row/campaign, written by a **live** automated writer (newest update = today; ~2,082 campaigns updated in the last day).

**Applied-threshold distribution (32,550 campaigns):**

| Band | threshold | campaigns | % |
|---|---|---:|---:|
| Max Reach (ungated) | 0 | 21,144 | 65.0% |
| Max Reach band | 1–3332 | 1,410 | 4.3% |
| Mid | 3333–6665 | 2,308 | 7.1% |
| Guardrail default | 6666 | 2,285 | 7.0% |
| Peak | 6667–8000 | 618 | 1.9% |
| High | ≥8001 | 4,103 | 12.6% |

**Starvation baseline (answers the metric the GCS-blocked v4 table would have given): ~69% of campaigns are ALREADY at Max Reach (threshold ≤ 3332), 65% exactly at 0 (ungated).** Max Reach is the system's dominant steady state, not a failure mode. The gate can't worsen the 69% already ungated; the gated ~31% hold high thresholds because they have ample addressable population, which the gate preserves (we keep scoring all DS14-addressable IPs). **Starvation risk: low.**

**Authority — RESOLVED, gate-SAFE (code-confirmed 2026-07-28).** The residual is closed, and the "different system" correction above was itself a misread now traced: **camperbid v3/v4 ARE the PTV writers.** v4's `campaign_runner` does `.join(df_mntn_select_campaings, how="left_anti")` where that set is built `WHERE product_id=2` — a Select-**exclusion** anti-join, so v4 KEEPS PTV (Fangorn advertisers) and v3 handles non-Fangorn PTV; both funnel-1 prospecting. (Compass read the `product_id=2` clause as a Select-only filter; it is the opposite.) Compute DAG `intent_score_threshold_v4` (airflow-camperbid, midnight-UTC daily, PagerDuty-critical) + v3 write `performance.optimized_intent_thresholds`; the **sole** physical writer of `dso.household_score_thresholds` in the org is `SteelHouse/idso` (BOS) `HouseholdScoreThresholdRepository.kt` (`ON CONFLICT (campaign_id) DO UPDATE`), pulling `COALESCE(manual_preset, oit.threshold)`. **Population is auction-scoped:** `campaign_bucket_population_runner` = `COUNT(DISTINCT ip)` over `bid_price_log` + `bidder_bid_events` only (has_price OR threshold-failure), no scored-universe join. **→ the DS14 scoring gate does not couple to the prod HHST population. Gate-SAFE.**

Caveat (monitoring only, no delivery impact): the one DDM routine `cache_hhst_population_filters.sql` DOES read the full scored `prospecting_intent`/`advertiser_intent` as a `pct_visible/pct_active` denominator — but it `INSERT`s to `ddm.hhst_population_filters` (an analytics cache), NOT `dso.household_score_thresholds`. A DS14 gate would shift that monitoring metric, not any threshold. The full chain is 3 hops: **v3/v4 compute** (per-campaign threshold) → `performance.optimized_intent_thresholds` **sync** (`sync_optimized_intent_thresholds.sql`, v3 non-Fangorn UNION v4 Fangorn) → **idso BOS apply** (hourly cron `5 * * * *`, ns prod-optimization; the sole `INSERT INTO dso.household_score_thresholds` org-wide). The EMR `dags/camperbid/intent_score_threshold/main` + legacy Redshift `dags/performance/intent_score_threshold` are **superseded predecessors** of the same chain (Redshift→EMR→BQ migration; even the EMR's scored-universe read `tmul_unnested_intent_scores_7day` is commented out → auction-only there too). Not-confirmable-from-code (runtime — close via Airflow UI): which single compute DAG is un-paused (code points to BQ v3/v4 — commit #386 "Rewrite campaign_bucket_population to use BigQuery"; v3 row-gate `BETWEEN 1000 AND 10000` matches ~2,078/day) and idso-cron liveness. **Verdict SAFE regardless — every candidate in the lineage is auction-only.** (Independently re-confirmed 2026-07-28.)

### AUD-5221 — RESOLVED (Jira text, 2026-07-28)

AUD-5221 is a **Closed** Epic under BER-2250, owned by Malachi. Per its text it deciles the **intent-score distribution** (even/odd control/treatment for the Intent-Score-Shuffling experiment), NOT a US-population census → it operates on the scored/addressable set → **gate-safe**. Discrepancy to reconcile: `strategic_north_star.md` frames AUD-5221 as a "US Population 1-10 random split" (TTD-style, full universe); the built/closed ticket is score-distribution deciles. If a true random US-population split is later built, it's a net-new full-universe pipeline and this gate assumption is revisited then.

## 5. Solution / Recommendation (draft)

**Lever (→ AUDI-1176):** intersect the 31-day DS13/DS19 scoring input with the current DS14 (8d) set before `vertical_high/mid` + `populate_data_source`. Est. daily scoring-compute cut ~39% (verticals) / ~69% (MM Core). Zero biddable-coverage loss: any IP that becomes addressable is scored that day from still-retained DS13 history; intra-day-new IPs handled by RTC.

**Not the lever:** cutting raw-visit / DS13 retention. 30d = the model's scoring lookback, not slack.

### Cost estimate (Dataproc Serverless; order-of-magnitude, 2026-07-28)

Pipeline runs on **Dataproc Serverless** (DCU-billed, autoscaled per job), not provisioned clusters. Whole `audience_intent` scoring DAG + `populate_data_source` ≈ **$1,300/day ≈ $39k/mo** (point estimate; band ~$13k-$59k/mo). Cost concentrates in `prospecting_keywords` ($396/day, 34% — the DS19 keyword job over ~33.8B rows), `prospecting_join` + `advertiser_join` (~22% each). The vertical high/mid jobs are only ~$100/day combined.

**The prize is where the DS19 volume lands.** DS19 MM Core is consumed by `prospecting_keywords`, NOT the vertical jobs. Mapping input-cut levers to the jobs they drive:

| Lever | Job(s) | Savings |
|---|---|---|
| DS13 −39% | vertical high/mid + ½ populate | ~$1.3k/mo |
| **DS19 −69%** | **`prospecting_keywords` + ½ populate** | **~$9.6k/mo** (the real prize) |
| Both | — | **~$11k/mo (~$130k/yr)** |

Confidence: LOW / order-of-magnitude. Executor sizes/tiers/ceilings are exact from source; runtime + avg concurrent executors are assumed (jobs rarely sit at ceiling). Assumes the 69% distinct-IP cut shrinks `prospecting_keywords`' exploded keyword×IP rows ~linearly. **Firm up to a point estimate** via the GCP Billing BQ export (Dataproc Serverless SKUs, grouped by batch labels `team=ti`, `application=tpa-export`) or `gcloud dataproc batches describe` (`runtimeInfo.approximateUsage.milliDcuSeconds` per `aud-int-*` batch).

## 6. Questions Answered

- **Q:** Is DS14 a pre-MM recency filter? **A:** No. Born Aug 2025, prod Dec 2025 (AUDI-369), post-MM.
- **Q:** Is the effective bid-eligibility window 24h? **A:** No, ~8 days (serving TTL, per-IP decay).
- **Q:** Is scoring already gated to addressable IPs? **A:** No. Full 31-day universe scored.
- **Q:** How big is the waste? **A:** 39% (DS13) / 69% (DS19) of scored IPs are non-biddable at 8d.

## 7. Data Documentation Updates

- `knowledge/data_knowledge.md` — added "DS14-opt spike" block; corrected the "not materialized in IPDSC" line.
- `knowledge/ds_catalog.md` — DS14 resolved bullet updated (8d TTL, materialized, ungated scoring, origin).

## 8. Open Items / Follow-ups

- **2026-08-24: Jira closed Done (backlog audit)** — deliverable existed since 2026-07-28 and the RFD is published; the 2026-07-28 keep-in-Backlog directive is superseded. Billing firm-up (GCP billing IAM) + owner validation carry on AUDI-1176 as sprint-start steps.
- **RFD published to Confluence (TAR / Targeting) 2026-07-29** → [TAR page](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3722346650) (tiny https://mntn.atlassian.net/x/moDe3Q). Source `artifacts/audi_1175_rfd_draft.md`, adversarially hardened against 2 reviews (`artifacts/audi_1175_rfd_adversarial_review.md`). Socialize with the `audience_intent` owner (likely Ryan Kleck / AUDI team) + heads-up DDM/Devon before beginning AUDI-1176. The ask: approve the work, name a co-owner, agree the shadow-run delivery-parity gate.

- **$ figure — DONE (order-of-magnitude):** gate optimization saves ~$1.3k/mo (DS13) to ~$11k/mo (~$130k/yr, if DS19 cut is applied to `prospecting_keywords` where the volume lands). Whole scoring DAG ≈ $39k/mo. Firm up to a point estimate via GCP Billing BQ export / `gcloud dataproc batches describe` (DCU-seconds per batch).
- **Consumer audit — DONE (see §4).** Safe for all serving paths + Fangorn. Primary HHST recommender (camperbid v4) auction-scoped → gate-safe; DDM DCO pilot (Devon) small-scope → prize most likely survives. **AUD-5221 RESOLVED** (score-distribution deciles, gate-safe). **Max-Reach shadow query DONE** (baseline ~99% unscored, gate adds ~nothing). **LiftLab confirmed safe** (Compass: served-only MAPI export, no scored-universe path). **Starvation baseline DONE** (69% of campaigns already Max Reach; risk low). **Presets = manual overrides only.** **Residual CLOSED (code-confirmed 2026-07-28):** the PTV HHST writer = camperbid **v3/v4** (the `product_id=2` clause is a Select-exclusion anti-join, not a filter — v4=Fangorn PTV, v3=non-Fangorn PTV); population is auction-scoped (`bid_price_log`/`bidder_bid_events`, no scored-universe join); sole physical writer of `dso.household_score_thresholds` = `SteelHouse/idso` (BOS). **Gate-SAFE — the HHST recommender does not couple to the scored universe.** Only remaining = AUDI-1176 build-time delivery-parity shadow run.
- **Owner validation** — confirm no-loss with Ryan Kleck / Sean Yang / Zach Schoenberger before AUDI-1176.
- **Exact intersection** — optional exact DISTINCT+JOIN to confirm the HLL estimate.
- **`cats:[1]` refinement** — recompute addressable set restricted to category 1 (unnest `data_source_category_ids`) for the true gate.
