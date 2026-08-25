---
doc_type: ticket
title: "AUDI-1176: Gate audience_intent scoring input to DS14-addressable IP set"
status: backlog
date: 2026-07-28
summary: "Gate scoring input to DS14 (8d) IPs; ~$1.3k-11k/mo compute cut, zero coverage loss (safety proven in AUDI-1175)"
result: ""
question: "Can we gate the scoring input to DS14-addressable IPs and cut ~39-69% of daily scoring compute with no coverage loss?"
framing_state: locked
---

# AUDI-1176: Gate audience_intent scoring input to DS14-addressable IP set

**Jira:** https://mntn.atlassian.net/browse/AUDI-1176
**Status:** Backlog (sprint-ready — diligence complete in AUDI-1175)
**Assignee:** Malachi
**Blocked by:** [AUDI-1175](https://mntn.atlassian.net/browse/AUDI-1175) — sizing, cost, and consumer/safety audit (DONE; verdict: safe)

---

## BLUF

**Stop scoring IPs we can never bid on.** `audience_intent` scores a full 31-day IP universe every day, but DS14 (the freshness gate ANDed onto every audience expression) means only IPs seen in the last ~8 days are biddable. So **~69% of MM Core (DS19) and ~39% of vertical (DS13) scored IPs are recomputed daily for nothing.** Intersect the scoring input with the current DS14 (8-day) set before the expensive jobs → **~$1.3k–11k/mo compute saved (~up to $130k/yr), zero biddable-coverage loss.** All safety diligence is done (AUDI-1175): the HHST recommender is auction-scoped (no coupling), and every other consumer is unaffected. This ticket is the implementation only.

---

## 0. Framing (locked 2026-07-28)

- **Question:** Can the `audience_intent` scoring input be gated to the DS14-addressable (8-day) IP set, cutting ~39% (verticals) / ~69% (MM Core) of daily scoring compute, without losing any biddable coverage?
- **Goal (why):** Realize the cost reduction AUDI-1175 sized. Also shrinks IPDSC volume (MembershipDB resilience). Cost-reduction (Kale focus area).
- **Objective (done-when):** Scoring input intersected with the current DS14 8-day set before the expensive scoring jobs; a shadow run shows the compute drop AND identical biddable delivery on an advertiser holdout (no lost impressions/reach/visit-rate). Shipped behind a flag with rollback.
- **Approach:** Pre-filter the scoring input against DS14-recent (details in §3). **Largest $ lever = the DS19 `prospecting_keywords` job (34% of DAG cost, ~$9.6k/mo)**, not just the vertical jobs (~$1.3k/mo). Validate delivery parity before cutover.
- **What would change the answer:** if a shadow run shows any delivery drop, revert and diagnose (the expected mechanism is the DS14-snapshot vs 8-day-serving-window alignment — see §3 guardrails). **Sequencing decision (2026-07-30):** the incrementality DS14-removal experiment runs FIRST (it needs today's ungated scoring, which exists now); this gate sequences after it, or ships as the §3 output-only variant to preserve full scoring. Endorsed by Malachi; final go is the incrementality team's (see §4 Interaction).

## 1. The Problem

- Scoring (`vertical_high/mid`, `prospecting_keywords`, `populate_data_source`) reads a **31-day** DS13/DS19 IPDSC window and emits a score for **every** IP in it — including IPs with no recent activity (`vertical_mid` BUCKET 2 scores them deliberately).
- Bidding requires the IP to pass **DS14** (ANDed on every expression; ~8-day effective serving TTL, per-IP epoch decay). IPs outside that window can't be bid on.
- **Sizing (AUDI-1175, `ipdsc__v1` HLL, 2026-07-28):**

  | Scored universe (31d) | Addressable ∩ (DS14 8d) | Non-biddable | Waste |
  |---|---:|---:|---:|
  | DS19 (MM Core) 499.4M | 156.6M | 342.8M | **69%** |
  | DS13 (verticals) 269.8M | 164.7M | 105.1M | **39%** |

- That non-biddable remainder is scored and stored **daily**, then discarded and recomputed the next day.

## 2. The Solution

Intersect the scoring **input** with the **current DS14 (8-day union)** set before the expensive scoring, so we only score IPs that can actually be bid on. Keep raw-visit / DS13 retention at 30 days (that is the model's scoring lookback, NOT slack) — the lever is the *scoring output universe*, not input retention. No model or targeting-quality change; the same IPs that get bid on today still get scored.

## 3. Implementation (repo: `SteelHouse/airflow-ti`)

> **Prod-safety:** airflow-ti is the prod feature-store repo. Feature branch + PR + review; NO direct main / DAG edits. Ship behind a flag, staged rollout, rollback ready. (See memory `airflow_prod_safety`.)

**The DS14 set to gate against** = union of the last **8** daily DS14 builds, `data_source_id=14` (matches the 8-day serving TTL; a single day under-covers). Source options: `dw-main-bronze.external.ipdsc__v1 WHERE data_source_id=14` (materialized, ~149M/day; 8d union ≈ 259M), or the upstream `create_mntn_global_data_pyspark.py` output. Use `category_id=1` (the standard gate; note auction-only IPs carry only an exchange category and would be excluded — acceptable, matches the bidder gate).

**Insertion points, in $ priority order:**

1. **PRIMARY — `prospecting_keywords` (DS19 keyword job).** The ~33.8B-row, ~$396/day job (34% of DAG). Pre-filter its input IP set against DS14-8d before the keyword×IP explosion. → the ~$9.6k/mo lever.
2. **`vertical_high.py` / `vertical_mid.py`** — pre-filter the exploded 31-day DS13 IPDSC load (`for i in range(0,31)`) against DS14-8d before scoring. → ~$1.3k/mo (verticals).
3. **`spark/data_source/populate_data_source.py`** — the DS13/DS19/DS46 IPDSC build; gate the emitted output universe (storage-side).
4. **Fangorn path** (`models/audience_intent/fangorn_prospecting_scoring.py`, `fangorn_14day_lookback.ipdsc_inclusion_flag`) — check whether it's already partially gated; mirror if not.

**Alternative (lower prize, simpler/safer):** intersect `intent_score_map` **output** against DS14-8d before the serving-store load. This gates storage + downstream but NOT the expensive per-IP compute — captures the storage win only. Use as a fallback if input-gating a specific job is risky.

**Guardrails (why coverage loss is zero):**
- An IP that re-enters DS14 later is scored **that day** from its still-retained 30-day DS13 history — so gating loses nothing that would have been biddable.
- Intra-day brand-new IPs are handled by **RTC** (`realtime_conquest_score`), not the batch MM score — already the case today.
- Gate on the **8-day union**, not one day, or you clip IPs that are addressable-but-not-in-today's-build.

**DAG:** `dags/audience_intent/audience_intent.py` (`export_intent('prospecting'/'advertiser')`; preconditions already wait on IPDSC DS13/DS19). Add the DS14-8d dependency for the gated jobs.

## 4. Impact

**Changes:**
- Daily Dataproc-serverless scoring compute ↓ **~39% (DS13) / ~69% (DS19)** on the gated jobs.
- IPDSC output volume ↓ (DS19 ~499M→~157M, DS13 ~270M→~165M addressable rows/day) → storage + downstream-scan savings, and smaller MembershipDB load.

**Unchanged — proven safe in AUDI-1175 (so no re-litigation needed):**
- **Biddable delivery** — zero coverage loss (guardrails above).
- **HHST recommender** — its population is **auction-scoped** (`bid_price_log`+`bidder_bid_events`, via the v3/v4 → `optimized_intent_thresholds` → idso chain); it never reads the scored universe. No coupling.
- **Serving/bidding** (Aerospike/membership-db), **Fangorn** (separate 1%-sampled feature store), **LiftLab** (served-only), **AUD-5221** (intent-score deciles on the addressable set), **totals/sizing** (already DS14-gated).

**Monitoring caveat (no delivery impact, but give a heads-up):** `ddm.cache_hhst_population_filters` reads the full `prospecting_intent` as a `pct_visible/pct_active` denominator (it writes an analytics cache, never thresholds). Gating scoring will **shift that monitoring metric**. Flag the DDM/Devon owner before cutover so a metric shift isn't misread as an incident.

**Interaction — conflicts with the incrementality DS14-removal experiment (surfaced 2026-07-30):** the incrementality team (Kirsa) plans to test *removing* DS14 from a campaign's bid expression as a treatment, which opens bidding to **scored-but-not-recently-seen IPs** (~1.6× the vertical pool, ~3.2× the MM-Core pool — those multipliers are `1/(1−0.39)` and `1/(1−0.69)`, i.e. derived from our own 39%/69% waste). Those IPs only become biddable *because they are scored*. **This gate removes exactly those scores**, so if both go global they conflict: the gate forecloses the experiment. It's the concrete case AUDI-1175's audit anticipated ("a non-bidding consumer needs the full universe"). Since incrementality is the Q2 #1: **recorded resolution (2026-07-30) — run the experiment first, then this gate, OR ship the §3 output-only variant (keeps full scoring). Endorsed by Malachi; the incrementality team holds the final go** (experiment sign-off owners: Sean Yang / Zach Schoenberger / Ryan Kleck). This ticket stays in Backlog until then.

## 5. Expected Improvement (quantified)

- **~$1.3k/mo (DS13 verticals) to ~$11k/mo (~$130k/yr)** if the DS19 cut is applied to `prospecting_keywords` where the MM Core volume lands. Whole scoring DAG ≈ **$39k/mo**, so this is ~3–28% of it.
- Plus IPDSC storage / downstream-scan reduction (not separately $-sized).
- **Confidence: order-of-magnitude** — $ is a Dataproc-serverless config estimate (executor sizes exact; runtime assumed). Firm to a point via the **GCP Billing BQ export** (Dataproc Serverless SKUs by batch label) or `gcloud dataproc batches describe` (`milliDcuSeconds`) — a ~10-min step to do at sprint start.

## 6. Validation & Acceptance Criteria

- [ ] DS14-8d intersection added to the gated job(s), behind a flag.
- [ ] **Shadow run** on a recent day: gated scored-set size + Dataproc cost vs current (proves the compute drop).
- [ ] **Delivery parity** on an advertiser holdout: no lost impressions / reach / visit-rate vs ungated. (This is the go/no-go for cutover.)
- [ ] DDM/Devon notified of the `pct_visible` monitoring-metric shift.
- [ ] Cutover flagged with rollback; realized compute reduction measured post-launch.

## 7. Risks Cleared (from AUDI-1175 — diligence is done)

| Risk | Status |
|---|---|
| HHST recommender starved/biased | ✅ auction-scoped, no coupling (code-confirmed ×2) |
| Serving / bidder / Aerospike | ✅ addressable-bound already |
| Fangorn training/inference | ✅ separate 1%-sampled feature store |
| LiftLab / incrementality export | ✅ served-only (MAPI), no scored-universe read |
| AUD-5221 deciles | ✅ intent-score deciles on the addressable set |
| Starvation → Max Reach | ✅ 65% already at Max Reach; gate-neutral |

## 8. Open Design Questions (resolve at sprint start, not blockers)

- Input pre-filter (max compute win) vs output intersection (simpler) per job — pick per risk tolerance.
- Whether the Fangorn 14-day path is already partially gated by `ipdsc_inclusion_flag`.
- Firm the $ with the GCP billing export before/at kickoff.

**2026-08-25 — the blocking premise collapsed.** Kirsa does not recognize the experiment attributed to her (direct DM), Sean Yang didn't know its status, and the 7/30 claim has no recorded provenance. The hold is void unless someone at the 2:30 Kirsa meeting (Sean/Zach/Ryan are the recorded sign-off names) claims an actual remove-DS14 experiment. If nobody does: unpark, run the §3 shadow-run plan (output-only variant remains the belt-and-suspenders option).

**2026-08-25 provenance found — hold formally void.** The 7/30 'Kirsa experiment' was her floating the DS14 block as a candidate treatment in a Slack thread; Malachi and Kirsa both downgraded it as low-impact in that same thread, and she confirmed today she doesn't recognize any such experiment. No experiment exists to sequence behind. Also answered (Zach Schoenberger, same thread): DS14's PURPOSE is audience-size sanity — it deflates inflated audience-size numbers, pacing historically used audience size, and it reduces cost. That purpose is consistent with this gate. Ticket unparked; next step remains the §3 shadow run.
