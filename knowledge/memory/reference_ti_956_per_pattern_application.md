---
name: reference-ti-956-per-pattern-application
description: TI-956 design — per-segment scoring/ranking applies differently per campaign pattern. 3P-only=full scoring; MM+3P-OR=UI-size-rank (theater); MM+3P-AND-incl=top-quality; MM+3P-AND-excl=top-anti-quality. Locked 2026-06-01 from Pass 27-33 + Ryan/Matt/Sean.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [TI-956, per-segment scoring, campaign pattern, MM+3P AND OR, HHST bidder-inert, segment ranking, theater impressions, 3P-only, exclude include]
domain: [audience-scoring, bidding, project]
lifecycle: active
last_verified: 2026-06-01
---
**The locked design rule for TI-956 (2026-06-01):** per-segment scoring/ranking is NOT one-size-fits-all. It depends on what other clauses are in the campaign and the connector semantics (AND vs OR, include vs exclude).

| Campaign pattern | Per-segment scoring application |
|---|---|
| **3P-only** (no MM, may have GEO/CRM) | **Apply scoring fully** — 3P drives delivery, segment quality directly maps to KPIs |
| **MM + 3P-OR-incl** (theater, ~80% of MM+3P-incl spend) | **UI hygiene — rank top by SIZE descending**, no delivery effect because bidder is bidder-inert under HHST > 0 |
| **MM + 3P-AND-incl** (real narrowing, ~5% of MM+3P-incl spend) | **Show BEST audiences only** — buyer picks the highest-quality slice |
| **MM + 3P-AND-excl** (carve-out, exclusion) | **Show WORST audiences only** — buyer excludes the lowest-quality slice |
| **MM + CRM-AND-incl** | Same as 3P-AND-incl logic — narrow to best customer slice |
| **MM + CRM-AND-excl** | Standard customer suppression — surface full CRM, no ranking needed |
| **MM + CRM-OR-incl** | Same as 3P-OR-incl theater — UI size-rank only |
| **GEO** | Independent dimension — doesn't change segment ranking; applies as a filter |

**Why these rules:**

- **3P-only** — bidder bids on (3P ∩ geo ∩ campaign filters). Segment quality is causal on KPIs.
- **MM + 3P-OR-incl** — under HHST > 0, 3P-only IPs get score 0/-1, fail HHST, don't get bid on. The 3P clause is **bidder-inert** despite being in the expression. Buyer believes they're combining; mechanically they get MM-only delivery. Pass 26 says 80% of MM+3P-incl spend is OR. Surfacing quality-rank here misleads — surfacing size-rank prevents buyers from thinking they're targeting low-quality interests.
- **MM + 3P-AND-incl** — bidder bids on (MM ∩ 3P). Segment quality determines the slice of MM-scored IPs that get bid on. Buyer wants the best slice.
- **MM + 3P-AND-excl** — buyer removes a 3P set from the bid pool. They want to suppress *low-quality* interests, not high-quality ones.

**Empirical anchors locking this design:**

1. **Pass 21 / Pass 26 bucket math** — distribution of buyer expression patterns.
2. **Ryan Kleck (2026-06-01)** — HHST mechanic: HHST > 0 means OR-include is bidder-inert; HHST = 0 means OR-include broadens to unscored IPs.
3. **Pass 32** — canonical 8-axis permutation taxonomy (`queries/ti_999_pass32_perm_matrix_geo_narrow_incl_only.sql`).
4. **Pass 33** — within-advertiser CVR delta for MM + 3P-OR-incl is -8.08e-4 even though theory says bidder-inert. Either HHST is sometimes unset, or the OR clause changes some downstream behavior. Either way, segment quality info still belongs in the UI (just framed differently).

**Implementation requirements for TI-956:**

1. **Per-segment quality score** at `(advertiser_id, dscid)` grain, daily refresh. Computed on the OPERATIVE-TARGETING subset (pure-3P or MM+3P-AND-incl impressions only) — NOT on theater impressions where segment quality is decoupled from delivery.
2. **Per-segment size** at `(advertiser_id, dscid)` grain — distinct IP count.
3. **Segment ranking API**: takes `(advertiser_id, pattern_type, mode)` where mode ∈ `{full-rank, size-rank, top-quality, top-anti-quality}`.
4. **Pattern-type classifier** at compose time: detect 3P-only / MM+OR / MM+AND-incl / MM+AND-excl / CRM-incl / CRM-excl. Reference implementation: Pass 26/27/32 JS UDFs.

**Open implementation questions:**

- Per-advertiser quality metric vs global vs hybrid? (Sample-size constraint on long-tail segments.)
- Sample-size floor for per-advertiser per-segment scoring?
- Cold-start handling for new segments (no history yet)?
- Where does the ranked list surface in UI?
- HHST visibility — per-campaign HHST isn't yet locatable in BQ (it's NOT in bidder_bid_events per [[reference-bidder-score-fields-empirically-zero]]). Until probed, assume HHST > 0 dominates and Rule 2 (theater UI hygiene) applies.

**See also:** [[reference-mm-3p-intersection-mechanics]] (the load-bearing mechanic); [[reference-rtc-hhst-gating]] (HHST gate); [[feedback-us-only-no-geo-broad-axis]] + [[feedback-geo-narrow-excl-not-meaningful-axis]] (geo axes dropped); [[project-ti-999-strategic-goal]] (overall framing); `tickets/ti_999_interest_segment_sizing/artifacts/ti_956_per_pattern_segment_application.md` for the full design doc shareable to Alex/Alyson.
