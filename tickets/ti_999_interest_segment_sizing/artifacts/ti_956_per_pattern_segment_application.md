# TI-956 — Segment-ranking data product + per-pattern application reference

**Status:** input to TI-956 build (2026-06-01) — derived from Pass 27-33 findings.
**Audience:** Alex Knorr (TI-956 build owner), Alyson Lefkowitz, TI team, UI team (informational).

---

## Scope split

| Scope | Owner | Deliverable |
|---|---|---|
| **Data product — ranked segments table in GCS / BQ** | TI team (TI-956, Alex) | Daily refresh of per-(advertiser × segment) scores, ranks, and sizes. |
| **UI logic — how to surface those ranks in the audience composer** | UI team | Per-pattern application of the ranking (see "Per-pattern application reference" below — informational only). |

We (TI / TI-956) ship the data. UI team consumes the ranks/sizes and applies the per-pattern UI rules in their own scope.

---

## What we add to Alex's existing scoring framework

Alex's existing scoring (in [`targeting-infra-ml#57`](https://github.com/SteelHouse/targeting-infra-ml/pull/57) or his notebook) already produces a composite per-segment quality score. TI-999 contributes three **input refinements** and one **output addition**:

### Input refinements (correctness)

1. **Restrict the quality computation to the OPERATIVE-TARGETING impression subset.** Per Pass 26-33 + Ryan Kleck (2026-06-01), MM+3P-OR-incl is bidder-inert under HHST > 0 — the 3P segment doesn't affect who gets bid on. Quality KPIs (CVR, IVR) from those impressions are MM-only KPIs with a 3P label slapped on. Including theater impressions in segment-quality scoring inflates the segment's apparent quality with MM-only signal.
   - **Subset to keep:** impressions from campaigns matching `3P-only`, `3P-only + GEO`, `MM + 3P-AND-incl` (intersect with MM), or `3P-only + CRM-AND-excl` patterns.
   - **Subset to drop:** impressions from `MM + 3P-OR-incl` (theater).
   - **Classifier:** Pass 26 / Pass 32 JS UDFs (`queries/ti_999_pass26_or_vs_and_include.sql`, `queries/ti_999_pass32_perm_matrix_geo_narrow_incl_only.sql`) walk the audience expression tree to detect OR vs AND-include semantics via LCA. Port this logic into the TI-956 pipeline as an impression-filter.

2. **Per-advertiser scoring with global fallback for sample-size.** Per-advertiser quality is more accurate but suffers on long-tail segments. Recommended structure: per-advertiser score where impressions ≥ threshold (e.g. 10K), global category-average fallback otherwise. Score table should include both columns so the UI team can pick which to apply.

3. **Segment universe locked.** Active 3P set = `{DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP}` per TI-999 Finding 1. DS49 Publisher Network still borderline — confirm with Zach S. before including. DS1 Oracle flagged as legacy (553 campaigns reference it but zero IPDSC volume — likely dead weight; safe to exclude).

### Output addition (UI team requirement)

4. **Include `size` (distinct IP count) as an output column.** UI team needs both quality-rank AND size-rank to drive the per-pattern surfaces. Without size, the UI can't implement the OR-include "rank by size descending" flow (see reference below). Add to the daily output schema.

### Suggested output schema for the GCS / BQ table

| column | type | description |
|---|---|---|
| `day` | DATE | partition key |
| `dscid` (or `data_source_id` + `category_id`) | INT64 | segment identifier |
| `advertiser_id` | INT64 | NULL for global rows |
| `quality_score` | FLOAT64 | Alex's composite — higher = better |
| `size_distinct_ips` | INT64 | reach proxy — used for size-rank |
| `quality_rank` | INT64 | dense rank, 1 = best, per advertiser_id (or global if advertiser_id IS NULL) |
| `anti_quality_rank` | INT64 | dense rank, 1 = worst — used for AND-exclude UI |
| `size_rank` | INT64 | dense rank, 1 = largest — used for OR-include UI |
| `n_impressions` | INT64 | sample-size diagnostic |
| `n_conversions` | INT64 | sample-size diagnostic |
| `coverage_flag` | STRING | "per_advertiser" or "global_fallback" |
| `freshness_days` | INT64 | days since segment's source data refreshed (LiveRamp/Dstillery/ShareThis) |

GCS path TBD with Macie. Likely `gs://household-scoring-prod/output/scoring/segment_quality/year=YYYY/month=MM/day=DD/`. BQ surface table `bronze.household_scoring.segment_quality_daily` or similar.

---

## Per-pattern application reference (UI team scope — informational)

The UI team can use the data product above to drive different surfaces depending on the buyer's campaign pattern. These rules are derived from TI-999 Pass 27-33 + Ryan Kleck's locked HHST mechanic. They are NOT in TI-956 scope but documented here so the UI team has the conceptual map.

The rule of thumb:

| Campaign pattern | Per-segment scoring application |
|---|---|
| **3P-only** (no MM, may have GEO/CRM) | **Apply scoring fully.** 3P drives delivery — segment quality determines KPIs. |
| **MM + 3P-OR-incl** (theater) | **Don't change delivery — UI hygiene only.** Bidder is bidder-inert here; rank top 3P audiences by SIZE descending so buyers picking these don't believe they're targeting low-quality interests. |
| **MM + 3P-AND-incl** (real narrowing) | **Show BEST audiences only.** 3P narrows MM to (MM ∩ 3P); buyer wants the highest-quality slice. Surface top-quality segments first. |
| **MM + 3P-AND-excl** (carve-out) | **Show WORST audiences only.** Buyer is removing IPs from MM; surface anti-quality segments (the ones to suppress). |

Apply to CRM similarly when present. GEO doesn't affect this ordering (it's a separate filter).

---

## Why per-pattern

From TI-999 Passes 27-33, we now have the empirical mapping from expression structure → bidder behavior, locked with Ryan Kleck / Matt Brorby (2026-06-01).

**Three structural patterns matter for whether the 3P segment changes what gets bid on:**

1. **3P alone** — the 3P clause IS the targeting. The bidder bids on (3P set) ∩ (geo) ∩ (campaign filters). Segment quality directly determines who gets bid on, what they convert at, and what the buyer pays.
2. **3P OR'd with MM** (`{op:or, value:[MM_clause, 3P_clause]}`) — bidder receives membership = (MM ∪ 3P) but only scores the MM IPs. 3P-only IPs get score = 0/-1, fail HHST, never get bid on. **The 3P clause is bidder-inert.** Buyer thinks they're broadening; mechanically they get MM-only delivery.
3. **3P AND'd with MM** (`{op:and, value:[MM_clause, 3P_clause]}`) — bidder bids on (MM ∩ 3P). The 3P segment quality determines which slice of MM-scored IPs the bidder sees. **Real narrowing.**

For exclusions (`op:not` wrapping a 3P clause): the buyer removes that 3P set from the bid pool. Best practice: exclude *bad* segments (low-quality interests), keep *good* segments in.

**Pass 30 + Pass 32 spend distribution (30d ending 2026-05-28):**

- MM + 3P-OR-incl + GEO-NARROW-incl: $3.54M / 11.0% (theater on the biggest non-pure-MM bucket)
- MM + 3P-OR-incl: $1.57M / 4.9% (theater, US-broad)
- MM + 3P-AND-incl + 3P-OR-incl (mixed, treat as AND-incl for delivery): $0.36M / 1.1%
- 3P-AND-excl + 3P-OR-incl + CRM-AND-excl (3P-only AND-excl): $1.23M / 3.8%

So the **theater bucket is the largest** by spend within MM-touching, and **AND-incl is rare** — meaning today, most "MM + 3P" spend is delivering as if 3P wasn't there.

---

## Implementation rules per pattern

### Rule 1 — Pure 3P (no MM): apply scoring fully

**Detection:** campaign expression contains DS17/18/35 positive clauses but NO positive DS13/19/38/46 clause. May or may not have CRM, GEO, exclusions.

**Application:** rank segments by per-segment quality score (CVR, IVR, or composite). Surface ranked list in UI. Buyer chooses from the top.

**Why:** delivery is segment-driven; better segments → better KPIs. This is the TI-956 baseline use case.

### Rule 2 — MM + 3P-OR-incl: UI hygiene, no delivery effect

**Detection:** MM positive clause present, AND a 3P positive clause is OR-connected with MM at any LCA. From Pass 26: ~80% of MM+3P-incl spend is OR semantics.

**Application:** rank top N (e.g. 20) audiences by **size descending** (largest reach). Surface those. Buyers who add 3P-OR don't shift delivery, but they DO see a UI indication of "audience size" that's miscommunicated today.

**Why:** under HHST > 0, OR-incl 3P clauses are bidder-inert (3P-only IPs are unscored, fail HHST). Delivery = MM-only regardless of which 3P segment the buyer picks. So quality ranking is pointless. Size ranking serves the UI honesty goal: buyer sees the "if I were to target this 3P interest, this is its size" framing without implying it changes who gets bid on.

**Caveat:** if buyer has HHST = 0 (or unset), OR-incl 3P DOES affect delivery — 3P-only IPs get pulled in. In that minority case (frequency unknown, pending HHST-distribution probe), Rule 2 should fall through to Rule 1 logic. Practical implementation: until we have per-campaign HHST visibility, assume HHST > 0 is dominant.

### Rule 3 — MM + 3P-AND-incl: real narrowing, surface BEST

**Detection:** MM positive clause present, AND a 3P positive clause is AND-connected with MM at the LCA.

**Application:** rank segments by per-segment quality score (CVR / lift). Surface only the TOP K (e.g. 10-20) — the BEST narrowing options. Hide low-quality segments from this UI surface.

**Why:** buyer is narrowing MM to (MM ∩ 3P). If they narrow to a low-quality 3P segment, they shrink their bid pool to a worse slice of MM. They want the best slice. Surfacing bad segments here is harmful — buyer picks one thinking "more is more" and gets worse delivery.

### Rule 4 — MM + 3P-AND-excl: carve-out, surface WORST

**Detection:** MM positive clause present, AND a 3P clause is inside `op:not` (always AND-wrapped).

**Application:** rank segments by *anti-quality* (lowest CVR, lowest IVR). Surface the BOTTOM K — the segments most worth excluding. Hide high-quality segments from this surface.

**Why:** buyer is removing a 3P set from the bid pool. The right things to remove are bad-performing interests (low-quality intent). Surfacing good segments in the exclude UI invites buyers to suppress audiences that would have been valuable.

### Rule 5 — CRM: same logic applies

**Detection:** CRM positive (DS4/8/47) or CRM negative (`op:not` over CRM).

**Application:**
- CRM-AND-incl with MM → narrow MM to MM ∩ CRM. Surface BEST customer-list slice (highest LTV / highest converting subset). Rare today (~5% of spend).
- CRM-AND-excl with MM → standard customer suppression (78% of CRM-touching today). Surface ALL of advertiser's CRM (they want to suppress all known customers). No ranking needed; this is "exclude my customer list" hygiene.
- CRM-OR-incl with MM → theater, same as Rule 2 (rank by size descending).

### Rule 6 — GEO: independent dimension, doesn't change segment ranking

GEO operates at a separate filter layer (`geos.where`). It narrows the geographical scope but doesn't change which 3P/CRM segments are relevant. Apply Rules 1-5 within the geo-scoped subset of IPs.

---

## Open questions to confirm with Alex before TI-956 build finalizes

1. **Sample-size floor for per-advertiser scoring.** Below what `n_impressions` does the per-advertiser score fall back to the global category average? (Working assumption: 10K imp + ≥20 conversions for CVR; adjustable.)
2. **CTV vs display split.** Channel-id matters for KPI levels but doesn't change the structural rules. Should the ranking be channel-specific (separate quality_score per channel) or pooled?
3. **Freshness exposure.** Should `freshness_days` filter the output (drop stale segments) or only annotate them so the UI can de-rank? TI-999 Finding 4 shows 18.3% of prospecting spend touches stale 3P (~$55M/yr); the UI team will need a freshness signal regardless.
4. **DS49 Publisher Network inclusion** — borderline classification pending Zach S. (TI-999 Finding 1 open item).
5. **HHST visibility** — for proper per-campaign theater filtering, we need to know whether HHST is set. Per `data_knowledge.md` § "Bidder-side score logging" the HHST field in `bidder_bid_events` is always 0 (not the configured value). Need a separate source. Until probed, the conservative move is to assume HHST > 0 dominates and filter all MM+3P-OR impressions out of the scoring subset.

---

## Where this comes from in TI-999

- **Pass 26** (`queries/ti_999_pass26_or_vs_and_include.sql`) — JS UDF that classifies MM+3P-incl as OR (theater) vs AND-include (real narrowing) via expression-tree LCA. **Port to TI-956 pipeline as impression filter.**
- **Pass 32** (`queries/ti_999_pass32_perm_matrix_geo_narrow_incl_only.sql`) — canonical 8-axis permutation taxonomy.
- **Pass 33** (`queries/ti_999_pass33_within_advertiser_collapsed_taxonomy.sql`) — within-advertiser comparison showing MM + 3P-OR theater hurts within-CVR by -8.08e-4 even though theory says bidder-inert. Either HHST sometimes unset or selection of weak campaigns. Reinforces the rule: filter theater impressions out of segment-quality computation.
- **Knowledge:** `knowledge/data_knowledge.md` § "MM + 3P intersection mechanics — LOCKED LOGIC", § "HHST — what it is and what gates it", § "Bidder-side score logging — empirical finding".
