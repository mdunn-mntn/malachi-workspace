# RFD — AUDI-1176: Gate MM/vertical scoring to DS14-addressable IPs (cut daily Dataproc compute)

*Status: DRAFT for review. Decision doc / Request-for-Decision. Confluence-ready. Sources: AUDI-1175 `summary.md` (sizing, cost, consumer/safety audit + saved queries), AUDI-1176 `summary.md` (implementation plan). Safety core is code-confirmed twice (an internal code sweep + an independent Compass sweep).*

---

## Decision requested (BLUF)

**Approve gating the `audience_intent` scoring input to the DS14-addressable (~8-day) IP set, contingent on a shadow-run delivery-parity check.** We score a full 31-day IP universe every day, but DS14 (the freshness gate ANDed onto every audience expression) means only IPs seen in the last ~8 days are biddable — so **~69% of MM Core (DS19) and ~39% of vertical (DS13) scored IPs are recomputed daily and never used.** Gating the input cuts **~$1.3k–11k/mo (~up to $130k/yr)** of Dataproc-serverless compute with **zero biddable-coverage loss**. All consumer-safety diligence is done: the HHST threshold recommender is auction-scoped (no coupling to the scored universe), and every other consumer is unaffected. The ask is to approve the work, name a pipeline co-owner, and agree the shadow-validation gate.

---

## Why this is worth doing

- **It is real waste, not an uncertain causal claim.** DS14 gates bidding to ~8 days; scoring runs over 31 days, ungated (`vertical_mid` even deliberately scores IPs with no recent activity). The delta is computed daily, emitted, and discarded.
- **The savings are genuine compute we stop paying for** — not a spend-redirect. (Contrast the freq-cap RFD, where capped spend is redirectable-not-saved.)
- **Cost reduction** (leadership focus) plus a smaller IPDSC footprint → MembershipDB resilience.

**Sizing (AUDI-1175, `ipdsc__v1` HLL distinct-IP, 2026-07-28):**

| Scored universe (31d) | Addressable ∩ (DS14 8d) | Non-biddable | Waste |
|---|---:|---:|---:|
| DS19 (MM Core) 499.4M | 156.6M | 342.8M | **69%** |
| DS13 (verticals) 269.8M | 164.7M | 105.1M | **39%** |

---

## The honest core (do not soften)

1. **The dollar figure is order-of-magnitude, not a locked number.** It is a Dataproc-serverless config estimate — executor sizes/tiers are exact from source, but runtime and average concurrency are assumed. Whole scoring DAG ≈ **$39k/mo**; the gate saves **~$1.3k/mo (DS13 verticals) to ~$11k/mo (if the DS19 cut lands at `prospecting_keywords`, the 34%-of-DAG job).** Firm to a point via the GCP Billing export (Dataproc SKUs by batch label) before cutover — a ~10-min step.
2. **"Zero coverage loss" is the design claim; it is PROVEN only by the shadow run.** The mechanism is sound (an IP that re-enters DS14 is scored that day from its still-retained 30-day history; RTC covers intra-day new IPs), but the go/no-go for cutover is **delivery parity on an advertiser holdout**, not this analysis.
3. **The one real risk is window-alignment:** gate against the **8-day DS14 union**, not a single daily build, or you clip IPs that are addressable-but-not-in-today's-snapshot. Any residual shows up as delivery drop in the shadow run.
4. **Do NOT cut raw-visit / DS13 retention.** The 30-day window is the model's scoring lookback (its memory), not slack. The lever is the scoring *output* universe, not input retention.

---

## What we're changing (airflow-ti — feature branch, PR, flagged, staged; no direct main)

Intersect the scoring **input** with the current DS14 8-day set before the expensive jobs. In $ priority:
1. **`prospecting_keywords` (DS19)** — the ~33.8B-row, ~$396/day job. The ~$9.6k/mo lever.
2. **`vertical_high.py` / `vertical_mid.py`** — pre-filter the 31-day DS13 load. ~$1.3k/mo.
3. **`populate_data_source.py`** DS13/19/46 IPDSC build — gate the emitted universe (storage-side).
4. **Fangorn 14-day path** — check whether already partially gated by `ipdsc_inclusion_flag`; mirror if not.

Fallback (lower prize, simpler): intersect `intent_score_map` **output** against DS14 before the serving-store load — captures the storage win only, not the compute win. Full step-by-step in AUDI-1176 §3.

---

## Why it's safe (consumer audit — complete, AUDI-1175)

| Risk | Status |
|---|---|
| **HHST threshold recommender** starved/biased | ✅ **auction-scoped, no coupling.** Chain = camperbid v3/v4 → `performance.optimized_intent_thresholds` → `idso` BOS upsert (sole writer of `dso.household_score_thresholds`); population = `COUNT(DISTINCT ip)` over `bid_price_log`+`bidder_bid_events`. Code-confirmed ×2. |
| Serving / bidder / Aerospike / membership-db | ✅ addressable-bound already |
| Fangorn training / inference | ✅ reads a separate monthly 1%-sampled feature store, not `prospecting_intent` |
| LiftLab / incrementality export (DS52) | ✅ served-only (MAPI), no scored-universe read |
| AUD-5221 audience deciles | ✅ intent-score-distribution deciles on the addressable set |
| Starvation → Max Reach | ✅ 65% of 32,550 campaigns already at Max Reach (threshold=0); gate-neutral |

**One monitoring heads-up (no delivery impact):** the DDM analytics cache `cache_hhst_population_filters` reads the full `prospecting_intent` as a `pct_visible/pct_active` denominator (it writes a cache, never thresholds). Gating scoring will **shift that monitoring metric** — flag DDM/Devon before cutover so a metric shift isn't misread as an incident.

---

## Validation & rollout

1. **Shadow run** on a recent day: gated scored-set size + Dataproc cost vs current (proves the compute drop).
2. **Delivery parity** on an advertiser holdout: no lost impressions / reach / visit-rate vs ungated — **this is the cutover gate.**
3. Notify DDM/Devon of the monitoring-metric shift.
4. Flagged cutover with rollback; measure realized compute reduction post-launch.

---

## The ask

1. **Approve beginning AUDI-1176** (gate scoring input to DS14-addressable), contingent on the shadow-run delivery-parity gate.
2. **Name a co-owner for the `audience_intent` / airflow-ti change** (likely Ryan Kleck / the Audience Intelligence team) — 30 min to confirm the smallest insertion point at `prospecting_keywords`.
3. **Agree the shadow-run delivery-parity check as the cutover criterion.**
4. **Heads-up to DDM/Devon** on the `pct_visible` monitoring-metric shift.

---

## Appendix — what would change the answer

- **Shadow run shows any delivery drop** → revert, diagnose (expected cause: DS14-snapshot vs 8-day-window alignment — widen the union / lean on RTC).
- **The DS19 cut can't cleanly land at `prospecting_keywords`** → savings shrink to ~$1.3k/mo (DS13) + storage; reassess whether it clears the effort bar.
- **GCP billing shows the DAG is materially cheaper than the estimate** → re-weigh against implementation effort (a 5-pt change).
- **A non-bidding consumer we didn't find needs the full universe** → gate only the serving-bound output path, not the model inputs (the audit found none, but the shadow run is the backstop).
