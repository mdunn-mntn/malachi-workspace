# RFD — AUDI-1176: Gate MM/vertical scoring to DS14-addressable IPs (reduce daily Dataproc compute)

*Status: DRAFT for review. Decision doc / Request-for-Decision. Confluence-ready. Sources: AUDI-1175 `summary.md` (sizing, cost, consumer/safety audit + saved queries), AUDI-1176 `summary.md` (implementation plan). This draft has been adversarially reviewed (technical + business lenses); the caveats below are the surviving open items, stated plainly.*

---

## Decision requested (BLUF)

**Approve a scoped effort to gate `audience_intent` scoring to the DS14-addressable IP set — gated on three confirmations, not a blank check.** We score a full 31-day IP universe every day, but DS14 (the freshness gate on every audience expression) means only IPs seen in a short recent window are biddable, so a large fraction of MM Core (DS19) and vertical (DS13) scored IPs *appear* non-biddable by design and are recomputed daily. Gating the input should cut daily Dataproc-Serverless compute. **Firm floor is modest (~$1.3k/mo, DS13); the larger DS19 prize (up to ~$9.6k/mo) is an upper bound that depends on unverified assumptions (below).** The HHST threshold recommender is auction-scoped and does not couple to the scored universe, so the core safety concern is retired. The ask: approve the work **contingent on** (1) a billing-export pull confirming the marginal invoice actually drops, (2) confirming the bidder's exact DS14 predicate, and (3) a shadow run proving delivery + score-quality parity before cutover.

---

## Why this is worth investigating

- **The structural waste is code-solid.** Scoring genuinely reads a 31-day DS13/DS19 universe *ungated* (`vertical_mid.py` even deliberately scores no-recent-activity IPs); DS14 is a separate short-window DAG ANDed at bid time. Confirmed in `vertical_mid.py`, `prospecting_keywords.py`, `create_mntn_global_data_pyspark.py`, membership-db `config.yml`.
- **Cost reduction** (leadership focus). Secondary, unsized: a smaller IPDSC footprint (MembershipDB load) — a qualitative benefit, not a headline.

**Sizing (AUDI-1175, `ipdsc__v1` HLL distinct-IP, 2026-07-28) — caveat: sized on DS14 *all-categories*, not the `cat=1` predicate the gate would use (see Open Risk A):**

| Scored universe (31d) | Addressable ∩ (DS14 8d, all cats) | Apparently non-biddable | Share |
|---|---:|---:|---:|
| DS19 (MM Core) 499.4M | 156.6M | 342.8M | ~69% |
| DS13 (verticals) 269.8M | 164.7M | 105.1M | ~39% |

These shares size the *opportunity envelope*, not the realized compute cut (see Open Risk C).

---

## The honest core (do not soften)

1. **We have not confirmed the bill actually drops.** The jobs run on Dataproc **Serverless** (DCU pay-per-use — code-confirmed: `DataprocCreateBatchOperator`, no provisioned cluster). *If* there is no committed-use discount or minimum-spend floor, cut DCU-seconds leave the invoice. That "if" is **unverified.** The shadow run measures DCU-seconds, not the invoice. This is **Gate B** — whoever has GCP billing / Dataproc IAM runs the ready query (`queries/audi_1175_dataproc_billing_probe.sql`, ~10 min); the analyst is IAM-walled from both the billing export and `dataproc.batches` (confirmed 2026-07-28). The whole magnitude rests on this number. (Downstream BQ scans run on the reserved slot pool, where freed slots are *not* a direct dollar saving — the compute prize is Dataproc DCU + GCS storage only.)
2. **The realized savings are almost certainly sub-linear, not the raw waste %.** `prospecting_keywords` (the ~$9.6k lever) is CPU-bound RDD `combineByKey` with a **broadcast** campaign dict; its dominant cost is the **33.8B-row parquet write**, and gating against the ~259M-IP DS14 set would introduce a **new large shuffle** the job is engineered to avoid — output rows (campaign × IP) may not fall proportionally to the IP cut. `vertical_high/mid` read **all 31 DS13 partitions regardless** (DS14 is not a partition key), so read-I/O is unchanged and only post-read stages shrink. **Treat 69%/39% as an upper bound on compute; realized savings are unquantified until the shadow run.**
3. **"Zero coverage loss" is wrong as stated; the real claim is ≤24h score staleness on a re-entering cohort.** RTC (`realtime_conquest_score`, 1-day TTL) fills only *conquest* matches, not the batch MM/vertical household score. An IP outside the gated set that re-enters DS14 mid-day and is non-conquest has **no fresh household score until the next daily batch** — a ≤24h staleness, not zero. (Truly brand-new IPs are unscored today gate-or-no-gate, so that part is unchanged.) The shadow run must check **score quality** on the re-entering cohort, not just impression/reach counts.
4. **The DS14 window is 8 *or* 30 days — unconfirmed, and it swings the case.** `config.yml` has two TTL layers (`audience_type_ttls.default_ttl: 30`, `data_source_ttls['14']: 8`); 8 is the code-intended path, 30 if DS14 is Portal audience-type-registered. At 30 days the waste shrinks sharply **and** an 8-day gate would clip biddable IPs. Pin the true serving window (owner + shadow run) before gating. (Also: the build reads 1-4 days back, so the effective in-market window is ~9-12 days from an IP's *last log sighting*; the sizing uses the 8-day materialized-membership union — the biddable snapshot — either way.)
5. **Do NOT cut raw-visit / DS13 retention** — the 30-day window is the model's scoring lookback, not slack.

---

## Open risks to close (the three go/no-go gates)

- **A. Gate predicate = bidder predicate.** The sizing counted DS14 under *all* categories; the gate would use `category_id=1`. Auction-only IPs carry an exchange category, not 1 (`create_mntn_global_data_pyspark.py`), so a `cat=1` gate keeps a *smaller* set than sized. Production audience expressions show `data_source_id:14, category_ids:[1]`, but we did **not** prove that is the bidder's DS14 predicate universally. **Gate scoring on the exact predicate the bidder enforces, and re-size the prize on that predicate.** If any campaign accepts DS14 under other categories, a `cat=1` scoring gate is real coverage loss.
- **B. Billing reality (owner / data-platform task — analyst is IAM-walled).** Confirm the marginal invoice drops: no Dataproc committed-use discount / minimum-spend floor absorbing the cut. Ready query in `queries/audi_1175_dataproc_billing_probe.sql` (~10 min) — sum Cloud Dataproc SKUs **net of credits** for the `aud-int-*` batches; a large negative `credits` = a CUD that zeroes the saving.
- **C. Realized savings + delivery/quality parity.** Shadow run: gated compute cost vs current on a recent day, AND delivery + score-quality parity on an advertiser holdout (impressions/reach/visit-rate *and* the re-entering-cohort score check). This is the cutover gate.

---

## What we're changing (airflow-ti — feature branch, PR, flagged, staged; no direct main)

Intersect the scoring **input** with the current DS14 set (matching predicate + window per Risk A/#4) before the expensive jobs. Priority: `prospecting_keywords` (DS19) → `vertical_high/mid` (DS13) → `populate_data_source` output → Fangorn 14-day path. Fallback: intersect `intent_score_map` **output** before serving-store load (captures storage only, avoids reshaping the compute-heavy jobs). Step-by-step in AUDI-1176 §3. **Netting the downside:** this adds a new cross-DAG dependency to `prospecting_keywords`, a PagerDuty-critical daily job — a new failure surface and ongoing coupling to weigh against the (uncertain) savings.

---

## Why the HHST-safety concern is retired (the one that could have blocked this)

The prod household-score-threshold recommender does **not** read the scored universe. **Every writer in its lineage is auction-scoped** — population = `COUNT(DISTINCT ip)` over `bid_price_log` ∪ `bidder_bid_events`, no scored-universe join (v4 `campaign_bucket_population_runner`, confirmed). Chain: camperbid v3/v4 compute → `performance.optimized_intent_thresholds` → `idso` BOS upsert into `dso.household_score_thresholds` (plain `INSERT … ON CONFLICT DO UPDATE`; the preset/threshold coalesce is upstream in v4). **So the gate is safe regardless of which compute DAG is live** — which we could not confirm at runtime (un-paused-DAG + idso-cron liveness are not confirmable from code; the safety conclusion does not depend on it).

**Other consumers (AUDI-1175 audit):** serving/Aerospike/membership-db (addressable-bound); Fangorn (separate 1%-sampled feature store, not `prospecting_intent`); LiftLab/DS52 (served-only MAPI, no scored read). **Two ✅s are inference, disclosed:** starvation — the direct counterfactual query needs a `camperbid-prod` GCS table we can't access; as a proxy, 65% of campaigns already bid at Max Reach and the gate preserves addressable population, so risk *appears* low (confirm on the holdout). AUD-5221 — resolved as intent-score deciles per the Jira ticket, though `strategic_north_star.md` still describes a full-US-population split; reconcile with the owner. **One monitoring heads-up:** DDM `cache_hhst_population_filters` (mid-band `hhst 3334-6665` campaigns) reads `prospecting_intent` for a `pct_visible` analytics cache — never a threshold; gating will shift that metric. Flag DDM/Devon so it isn't misread as an incident.

**New consumer (surfaced 2026-07-30) — MUST-KEEP-FULL:** the incrementality team's proposed *remove-DS14* experiment treatment (Kirsa; Q2 incrementality OKR) needs the full scored universe — it bids on scored-but-not-recently-seen IPs (~1.6×/3.2× the vertical/MM-Core pools, derived from our own 39%/69% waste). This gate removes exactly those scores, so the two conflict if both go global — the concrete instance of the appendix's "a non-bidding consumer needs the full universe." Since incrementality is the Q2 #1, sequence the gate after the experiment, use the output-only gate (keeps full scoring), or hold. Clear with the incrementality owner.

---

## The ask

1. **Approve beginning AUDI-1176**, contingent on the three go/no-go gates (A: gate=bidder predicate; B: billing confirms the invoice drops; C: shadow-run delivery + score-quality parity).
2. **Confirm the `audience_intent` DAG owner** (AUDI team) and name a co-owner for the change — 30 min to agree the smallest insertion point.
3. **Decider:** the pipeline owner (technical sign-off) + cost-focused leadership (is the tempered $ worth a change to a PagerDuty-critical DAG?).
4. **Heads-up to DDM/Devon** on the `pct_visible` monitoring shift.

---

## Appendix — what would change the answer

- **Billing shows a Dataproc CUD / minimum-spend floor** → the bill doesn't drop; the headline is wrong, not just imprecise. This is the single biggest risk.
- **Shadow run shows delivery drop or score-quality degradation** on the re-entering cohort → revert; the gate predicate/window is mis-set.
- **The DS19 cut can't cleanly land at `prospecting_keywords`** (shuffle cost ≥ savings, or output doesn't shrink) → prize collapses to ~$1.3k/mo (DS13) + storage.
- **Serving window is 30 days, not 8** → waste shrinks and an 8-day gate clips coverage.
- **DS13 floor (~$1.3k/mo ≈ $16k/yr) alone** may not clear the cost of a prod change + holdout validation + new DAG coupling — an honest possibility the decider should weigh.
- **The incrementality DS14-removal experiment gets prioritized** → this gate forecloses it (removes the scores it wants to bid on). Sequence after it, gate output-only, or hold. Concrete now (surfaced 2026-07-30), not hypothetical.
