# TI-956 — Per-pattern segment scoring & ranking application

**Status:** design spec (2026-06-01) — derived from Pass 27-33 empirical findings on how buyer expression patterns actually map to bidder behavior.
**Audience:** Alex Knorr (TI-956 framework owner), Alyson Lefkowitz (incrementality / product), TI team review.

---

## TL;DR

Per-segment scoring/ranking applies **differently depending on what else is in the campaign**. The rule of thumb:

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

## Implementation plan / handoff to Alex

### What TI-956 needs to deliver

1. **Per-segment quality score** at the `(advertiser_id, dscid)` grain — daily refresh, GCS-landed (per Macie path TBD). Quality metric: CVR or lift over baseline, computed on the subset of impressions where the segment was the OPERATIVE targeting (filter out theater impressions).
2. **Per-segment size** at the `(advertiser_id, dscid)` grain — distinct IP count, daily refresh.
3. **Segment ranking API** that takes `(advertiser_id, campaign_pattern_type, mode)` where mode ∈ `{full-rank, size-rank, top-quality, top-anti-quality}` and returns ordered list of dscids.
4. **Pattern-type classifier** — given an audience expression at compose time, classify it into one of: 3P-only / MM+OR / MM+AND-incl / MM+AND-excl / CRM-incl / CRM-excl. The Pass 27-32 SQL/JS UDFs are reference implementations.

### Open implementation questions

1. **Compute quality metric on the right subset of impressions.** Today's CVR on MM+3P-OR (theater) is the MM-only CVR with a 3P label slapped on — using it to rank 3P segments would just propagate selection bias. The quality score should be computed on (a) pure-3P delivery only, OR (b) MM+3P-AND-incl delivery only. Both have low spend → low sample size for many segments. Need to specify the right baseline.
2. **Sample-size floor.** Many segments are low-volume; we should set a minimum impression count before a segment gets a per-advertiser quality score. Default global score for sub-threshold segments (e.g. category average).
3. **Cold-start for new segments.** When LiveRamp ships a new segment, no quality data yet. Surface only as size-rank (Rule 2 UI), withhold from Rule 3 (top-quality) until threshold met.
4. **HHST visibility.** For Rule 2's caveat (HHST = 0 means OR-incl DOES affect delivery), we need per-campaign HHST. Where it lives: unconfirmed (per `data_knowledge.md` HHST section). Pending probe.
5. **Storage / freshness.** Daily DAG; GCS partitioned `year=YYYY/month=MM/day=DD`. BQ surface table `bronze.household_scoring.advertiser_segment_quality_daily` or similar. Coordinate with Macie.

### Empirical findings that pin this design

The rules above are not opinion — they fall out of:

- **Pass 21 / Pass 26 bucket math** — the spend distribution across patterns says where this matters.
- **Ryan Kleck (2026-06-01)** — locked the mechanic: HHST > 0 → OR-include is bidder-inert; HHST = 0 → OR-include broadens to unscored IPs.
- **Matt Brorby (2026-06-01)** — RTC is first check in the bidder scoring waterfall; doesn't affect this design but worth knowing for impression attribution.
- **Pass 33 within-advertiser comparison** — MM + 3P-OR-incl shows within-advertiser CVR delta of -8.08e-4 even though theory says delivery is unchanged. Either HHST is sometimes unset, or the theater clause changes some downstream behavior. Either way, surfacing 3P quality info in Rule 2's UI is still correct — buyers should know what they're picking even if it doesn't change delivery.

### Where this comes from in TI-999

- Pass 26 SQL: `queries/ti_999_pass26_or_vs_and_include.sql` (OR vs AND classification UDF)
- Pass 32 SQL: `queries/ti_999_pass32_perm_matrix_geo_narrow_incl_only.sql` (canonical permutation taxonomy)
- Pass 33 SQL: `queries/ti_999_pass33_within_advertiser_collapsed_taxonomy.sql` (within-advertiser deltas)
- Knowledge: `knowledge/data_knowledge.md` § "MM + 3P intersection mechanics — LOCKED LOGIC", § "HHST — what it is and what gates it", § "Bidder-side score logging — empirical finding"

---

## Open questions to confirm with Alex / Alyson before building

1. **Is the per-segment quality metric advertiser-specific (per-advertiser CVR / lift) or global?** Per-advertiser is more accurate but suffers sample-size problems for long-tail segments. Hybrid (advertiser-specific where N ≥ threshold, fall back to category average otherwise) is the likely shape.
2. **What about CTV vs display split?** Channel-id matters for KPIs but doesn't change the structural rules above. Should the ranking be channel-specific?
3. **Buyer UI integration.** Where does the ranked list surface? Audience composer? Recommendation prompt? Default audience preset? Affects whether we need real-time API or daily snapshot.
4. **Override behavior.** If a buyer explicitly types in a low-quality segment for Rule 3 (MM+AND-incl), do we warn / hide / allow? Product call.
