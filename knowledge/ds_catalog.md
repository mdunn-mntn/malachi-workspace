# MNTN Data Source (DS) Catalog — Canonical Reference

**Source of truth:** `bronze.integrationprod.data_sources` (62 canonical type=1 DSes plus per-advertiser type=2 instances) + `bronze.tpa.categories` for per-DS category semantics.
**Last audited:** 2026-05-29 (TI-999 Finding 15 / Passes 16-17 DS audit + tpa.categories cross-check).
**Empirical usage:** 30-day window 2026-04-29 → 2026-05-28, prospecting only (objective_id IN 1,5,6), 11,909 campaigns / $32.10M.

## Family taxonomy (CORRECTED after tpa.categories cross-check)

The 17 actively-used DSes group into four families:

| Family | DSes | Buyer-selectable? | Functional role |
|---|---|:-:|---|
| **MM** (MNTN-derived audience targeting) | DS13, DS19, DS38\*, DS46 | Yes | Prospecting via MNTN audience signals |
| **List Retargeting** (a.k.a. "1P" in tables) | DS4, DS8, DS47 | Yes (positive = retargeting; negative = CRM suppression in prospecting) | Re-engage / suppress known customers |
| **3P interest segments** (bought) | DS1\*\*, DS17, DS18, DS35 | Yes | Prospecting via described interests |
| **Pixel-derived / auto-attached** | DS2, DS21, DS34, DS43 | No (used as auto-exclusion clauses) | Past-visitor / converter suppression |
| **Bid routing + internal taxonomy** (NOT a targeting family) | DS14, DS16 | Required by bidder mechanics, not buyer audience picks | Bid routing (DS14) + per-advertiser/internal event tags (DS16) |

\* DS38 (BUK) is in the MM family conceptually but is empirically UNUSED in active prospecting (0 positive, 0 negative).
\*\* DS1 (Oracle) included in 3P per user direction 2026-05-29 despite "zero IPDSC volume" memory note. Used positively in 553 prospecting campaigns / $5.97M / 30d. Open question whether the bidder delivers against Oracle clauses — pending Pass-7-style ceiling check.

## DS14 / DS16 reclassification (correction from earlier passes)

Earlier passes (10-15) called DS14 "MNTN Global Data" and DS16 "MNTN Taxonomy Data" and treated them as part of MM because their names suggested audience targeting. After querying `tpa.categories`:

- **DS14 categories** are bid routing destinations (id=1 "Beeswax Bidder", id=150 "Magnite", id=152 "Index Exchange", id=1000 "IP Ends In .0"). Not audience targeting.
- **DS16 categories** are per-advertiser identifiers (id=1 "AdvertiserID - Eat Clean Bro") plus MNTN-internal event taxonomy (id=2 "PageViews", id=3 "Conversions", id=5 "Prospecting", id=6 "Retargeting", id=7 "MultiTouch", id=8 "VV"). Not audience targeting.

Both DS14 and DS16 appear in almost every campaign because they encode bid-side plumbing, not buyer-selected audience choices. They are NOT MM.

## Per-DS detail (canonical 0-61, ordered by ID)

### Out-of-scope / unused / deprecated (44 DSes)

| DS | Name | Notes |
|---:|---|---|
| -1 | MNTN Pixel | Probably a sentinel value; not in main analysis |
| 3 | MNTN Third Party | Deprecated — replaced by named providers (DS17/18/35) |
| 5 | Oracle Custom Audience | Unused in active prospecting |
| 6 | MNTN Control Group | Internal experimentation control — out of scope |
| 7 | MNTN Audience Ext | Internal |
| 10 | MNTN Geo File | Geo (separate from categories tree) |
| 11 | LiveRamp (legacy) | Deprecated — replaced by DS35 LiveRamp IP |
| 12 | MNTN Product Groups | Internal product config |
| 15 | MNTN Testing | Test |
| 20, 22, 24–33, 36, 37, 39–42, 44, 45, 48–61 | Various named providers + ingestion sources (Experian, OnAudience, Klaviyo, Hubspot, Tealium, S3/GCS buckets, etc.) | None actively used in prospecting expressions. Many are CRM ingestion sources that feed DS4 CRM rather than being directly selectable. |

### MM — MNTN-derived audience targeting

| DS | Name | +camps | +spend (30d) | −camps | Notes |
|---:|---|---:|---:|---:|---|
| 13 | MNTN Vertical Categorization | 1,525 | $6.94M | 1 | Vertical-based targeting. Buyer picks "Finance" or similar. |
| **14** | **MNTN Global Data** | **11,888** | **$32.10M** | 0 | **Default MNTN audience signal — appears in nearly every prospecting campaign.** Often used with category_ids `[1]` or `[1, 150]`. |
| 16 | MNTN Taxonomy Data | 7,669 | $5.47M | 108 | Taxonomy-based filtering layered on top of DS14. |
| 19 | MNTN Matched | 2,914 | $19.71M | 0 | "MNTN Matched" — what we'd previously called RTC. Buyer selects per-vertical IDs. Major MM signal. |
| 38 | MNTN UI Audience Keywords | 0 | 0 | 0 | **BUK — currently unused in prospecting.** Memory had this as active; it isn't. |
| 46 | ML Audience Intent Scoring Model | 241 | $1.70M | 0 | Fangorn ML-driven intent scoring. Newer, lower adoption. |

**Functional read:** DS14 is the workhorse — almost every prospecting campaign references it. DS13/16/19 are layered selectively. DS46 is opt-in. DS38 is exists but isn't yet used.

### List Retargeting (advertiser-uploaded lists)

| DS | Name | +camps | +spend (30d) | −camps | Notes |
|---:|---|---:|---:|---:|---|
| 4 | CRM | 318 | $1.58M | 754 | Buyer-uploaded customer list. **2.4x more campaigns use it for exclusion** (suppression in prospecting) than for positive retargeting. |
| 8 | IP List | 0 | 0 | 492 | **Exclusion-only in prospecting** — buyers don't use IP lists as positive prospecting input. |
| 47 | CRM Identity Graph Generated | 0 | 0 | 2 | Essentially unused (2 negative-only references). |

**Functional read:** In prospecting, "1P / List Retargeting" is almost entirely about EXCLUDING known customers from MM-driven prospecting. Positive 1P clauses in a prospecting expression are rare (318 camps total).

### 3P interest segments (bought)

| DS | Name | +camps | +spend (30d) | −camps | Notes |
|---:|---|---:|---:|---:|---|
| 17 | ShareThis | 686 | $5.95M | 35 | Bought interest segments. Catalog 100% &gt;2yr stale at metadata level. |
| 18 | Dstillery | 512 | $3.16M | 33 | Bought interest segments. Catalog 100% &gt;2yr stale. |
| 35 | LiveRamp IP | 1,873 | $13.57M | 264 | The dominant 3P. ~213k active categories (97% of 3P by count). 99.6% fresh metadata. |
| 1 | Oracle | 553 | $5.97M | 161 | **Open question.** Used positively in 553 prospecting campaigns ($5.97M) but memory says zero IPDSC volume. Verify whether this DS drives actual delivery or is dead-weight like below-ceiling 3P. |

**Functional read:** LiveRamp dominates by usage AND fresh metadata. ShareThis + Dstillery are widely used but their catalogs are stale. Oracle is a wild card — needs delivery-side verification.

### Pixel-derived / auto-attached (not buyer-selectable in the family sense)

| DS | Name | +camps | −camps | Notes |
|---:|---|---:|---:|---|
| 2 | MNTN First Party | 21 | 482 | Memory name "MNTN First Party" is overloaded — this is pixel-derived first-party data. Mostly used as exclusion (482 camps), rarely positive (21). |
| 21 | MNTN Conversion | 0 | 3,842 | **Pure exclusion** — past converters suppressed from prospecting. |
| 34 | MNTN Pageview | 0 | 3,818 | **Pure exclusion** — past pageview visitors suppressed from prospecting. |
| 43 | MNTN ISP Type | 0 | 17 | Internal ISP-based exclusion. Niche. |

**Functional read:** These are pixel-fired data points used reflexively for "exclude past visitors from new prospecting." Not buyer-selectable in the "which segments should I pick?" sense. The TI-956 question doesn't apply to them.

### Marginal / internal

| DS | Name | +camps | +spend | Notes |
|---:|---|---:|---:|---|
| 9 | MNTN Campaigns | 12 | $0.20M | Internal campaign-cross-reference. Negligible usage. |

## Open questions for further investigation

1. **DS1 Oracle delivery test:** does the bidder actually deliver against Oracle clauses (despite zero IPDSC volume in catalog)? Or are these 553 campaigns paying for unreachable clauses? Similar pattern to the below-ceiling 3P cohort?
2. **DS38 (BUK) zero usage:** memory had BUK as an active MM signal. Is BUK queued for rollout? Is the UI not surfacing it? Worth a UI/PM check.
3. **DS14 universal use:** 11,888 of 11,864 prospecting campaigns reference DS14 (essentially 100%). Is DS14 auto-attached at campaign creation, or is the buyer explicitly picking it? Behaves like a default.
4. **DS2 (MNTN First Party) vs DS21/34 (pixel) overlap:** all three are MNTN-pixel-derived. What's the functional difference?

## Naming convention (CORRECTED — for deck / future analyses)

- **MM** = `{DS13, DS19, DS38, DS46}` (DS38 kept conceptually despite zero current usage)
- **List Retargeting (1P)** = `{DS4, DS8, DS47}` (positive use only DS4; DS8/DS47 are exclusion-only in prospecting)
- **3P interest** = `{DS1, DS17, DS18, DS35}` (DS1 Oracle included pending delivery verification)
- **Pixel exclusions** = `{DS2, DS21, DS34, DS43}` (auto-attached, used only in negative clauses)
- **Bid routing + internal taxonomy** = `{DS14, DS16}` (NOT in family taxonomy — these are bid-side mechanics, not buyer audience picks)

## Pass 17 bucket results (CORRECTED MM, prospecting only)

| Bucket | n_camps | % | Spend (30d) | % spend | Annualized |
|---|---:|---:|---:|---:|---:|
| nothing (no MM/1P/3P targeting clauses) | 7,619 | 64.1% | $5.06M | 15.8% | $61M |
| MM only | 1,119 | 9.4% | $5.51M | 17.2% | $66M |
| 1P only | 486 | 4.1% | $1.22M | 3.8% | $15M |
| 3P only | 488 | 4.1% | $1.63M | 5.1% | $20M |
| **MM + 3P** | **1,208** | **10.2%** | **$7.75M** | **24.1%** | **$93M** |
| MM + 1P | 511 | 4.3% | $4.51M | 14.0% | $54M |
| 1P + 3P (no MM) | 97 | 0.8% | $2.53M | 7.9% | $30M |
| MM + 1P + 3P | 361 | 3.0% | $3.89M | 12.1% | $47M |
| **Total prospecting** | **11,909** | 100% | **$32.10M** | 100% | **$385M** |

**Read:**
- MM + 3P is the biggest single bucket by spend ($7.75M / 24.1% / ~$93M annualized) — the canonical prospecting-with-interest-segments cohort.
- 3P-touching: 2,154 camps / 18.1% / $15.81M / 49.2% of prospecting spend (~$190M annualized).
- MM-touching: 3,199 camps / 26.9% / $21.66M / 67.5%.
- "Nothing" bucket (64% camps / 16% spend) = small prospecting campaigns using DS19 RTC score block + geo + pixel exclusions only. Avg $664/camp.

## Related references

- `knowledge/data_knowledge.md` §8 "Bidder Scoring Reality" — score-field details
- `tickets/ti_999_interest_segment_sizing/queries/ti_999_ds_catalog_usage.sql` — query that produced this audit
- `tickets/ti_999_interest_segment_sizing/outputs/ti_999_ds_catalog_usage_2026_05_29.csv` — raw output
