# MNTN Data Source (DS) Catalog — Canonical Reference

**Source of truth:** `bronze.integrationprod.data_sources`. **68 canonical type=1 DSes total**: 62 in the contiguous low-ID range (-1 to 61, no DS0), plus 6 high-ID outliers (DS328493-355420) that are mobile-attribution + a newer pixel DS. **79,587 per-advertiser type=2 instances** start at ID 1072 (the 2-digit → 4-digit jump). Companion table: `bronze.tpa.categories` for per-DS category semantics.
**Last audited:** 2026-05-29 (TI-999 Finding 15 / Passes 16-17 DS audit + tpa.categories cross-check + taxonomy lock + visible-flag + type=2 cross-check).
**Empirical usage:** 30-day window 2026-04-29 → 2026-05-28, prospecting only (objective_id IN 1,5,6), 11,909 campaigns / $32.10M.

> ⚠️ **NAMING-COLLISION WARNING — read before using "1P" anywhere**
>
> "1P" is overloaded and this catalog DOES NOT USE THE LABEL. Two distinct senses in active circulation:
>
> - **Victor Savitskiy canonical (2026-05-28):** "1P" = advertiser-uploaded data (DS4 CRM, DS8 IP List, DS47 CRM IDG). NOT MNTN-scored.
> - **MNTN-pixel sense:** "1P" = data MNTN owns via its pixel (DS2, DS21, DS34, DS43).
>
> Both senses are defensible; both have shown up in prior TI-999 passes and Slack threads. This catalog avoids the collision by using **Advertiser CRM** for `{DS4, DS8, DS47}` and **MNTN Pixel** for `{DS2, DS21, DS34, DS43}`. Any cross-doc reference to "1P" should specify which sense.

## Locked taxonomy (6 product groups + 1 bid-mechanics + 1 dormant)

| # | Group | DSes | One-line definition |
|---|---|---|---|
| 1 | **MM** (Mountain Match 2.0) | DS13, DS19, DS38, DS46 | MNTN's proprietary scoring product. DS13 = bucket/vertical, DS19 = keywords (MNTN Matched V2, LLM-derived), DS38 = BUK keywords (queued — augments DS19, doesn't replace; V2 stays for cold-start). HI / PP / MI / Max Reach are scoring **tiers** inside this system. RTC and MM are the same scoring system — RTC is the real-time variant that fires within an hour, the main scorer catches up after (Sean Yang, TI team, 2026-05-29). |
| 2 | **PP** (Peak Performance tier) | DS13 + DS19 (shared with MM) | Scoring tier inside MM where the IP matches the vertical (DS13) but has no keyword match (DS19). Score = 8000. Not a standalone DS family — surfaced separately because PP appears as a product in the UI. |
| 3 | **MNTN Pixel** | DS2, DS21, DS34, DS43 | First-party data derived from the MNTN pixel. DS2 = OPM segment pointer, DS21 = conversion, DS34 = pageview, DS43 = ISP type. Used almost entirely as negative/exclusion clauses in prospecting. |
| 4 | **MNTN Select** | DS9, DS42 | MNTN's premium-video product — its own audience layer. DS9 = "Households Reached in [Deal Name]" audiences sourced from Select impression exposure; DS42 = Select deal/order registry (metadata only, no expression use). DS9 used by **3 of 22 active Select advertisers** (narrow retargeting feature, not Select-wide). The canonical product identifier is `campaign_groups.product_id=2` — a campaign-group attribute, not a DS. |
| 5 | **3P** (bought interest) | DS1, DS17, DS18, DS35 | Externally-purchased interest segments. DS35 LiveRamp dominates by volume and freshness. DS1 Oracle carried in this group pending delivery verification. |
| 6 | **Advertiser CRM** | DS4, DS8, DS47 | Advertiser-uploaded customer/IP lists. In prospecting these are almost entirely exclusion clauses (suppress known customers); positive use is retargeting territory. Victor canonically calls this "1P". |
| 7 | **Bid mechanics / internal taxonomy** | DS14, DS16 | NOT a targeting family. DS14 carries bid-routing (Beeswax / Magnite / Index Exchange / IP filters); DS16 carries per-advertiser identifiers + MNTN-internal event taxonomy. Appear in nearly every expression because they encode plumbing. |
| 8 | **Dormant / out-of-scope** | DS-1 sentinel, DS3, 5, 6, 7, 10, 11, 12, 15, 20, 22–33, 36, 37, 39–41, 44, 45, 48–61, plus 6 high-ID outliers (328493 Adjust, 328494 AppsFlyer, 328495 Branch, 328496 Kochava, 328497 Singular, 355420 MNTN PageView) | Registered DSes with zero or negligible use. **DS11 LiveRamp legacy** retained here per Sean Yang (TI team, 2026-05-29): deprecated old LiveRamp that used device_id→IP mapping; kept in `tpa.categories` only because reporting still needs the historical category references. Other sub-types: CRM-ingestion sources feeding DS4, deprecated providers, internal control/test, mobile-attribution partners. Do not affect Pass 17 buckets. |

**Coverage check:** all 68 canonical type=1 DSes (IDs -1 through 61, plus 6 high-ID outliers 328493-355420) are accounted for in exactly one group — 19 active (across MM / MNTN Pixel / MNTN Select / 3P / Advertiser CRM / Bid mechanics) + 49 dormant. PP's DSes intentionally double-list because PP is a tier inside MM, not a parallel family.

## What the `data_sources` table tells you — `visible` flag and the ID-jump boundary

Two structural columns in `data_sources` carry classification info that the raw `name` field doesn't:

**`visible` flag = buyer-selectable in the UI.** Only **11 of 68 canonical type=1 DSes** have `visible=true`. Of those 11, only **5 are actively used** in 30d prospecting:

| DS | Name | Group | 30d use |
|---:|---|---|---|
| 1 | Oracle | 3P | 553 +camps / $5.97M |
| 4 | CRM | Advertiser CRM | 318 +camps / $1.58M |
| 17 | ShareThis | 3P | 686 +camps / $5.95M |
| 18 | Dstillery | 3P | 512 +camps / $3.16M |
| 35 | LiveRamp IP | 3P | 1,873 +camps / $13.57M |

The other 6 visible DSes (DS-1 MNTN Pixel sentinel, DS5 Oracle Custom Audience, DS11 LiveRamp legacy, DS20 OnAudience, DS22 Experian, DS50 shopify) are buyer-pickable but dormant.

**Implication:** every actively-used MM, PP, MNTN Pixel, and Bid-mechanics DS has `visible=false`. They are NOT directly buyer-selected. They are attached by platform code:
- **MM / PP** — when the buyer picks "Mountain Match" / "Peak Performance" template in the UI, the platform builds an expression referencing visible=false DS13 / DS19 / DS38 / DS46.
- **MNTN Pixel** — DS2 / DS21 / DS34 / DS43 are auto-attached for first-party exclusion (`blockFirstParty` and friends).
- **Bid mechanics** — DS14 / DS16 are auto-attached for bid routing and per-advertiser/event tagging.

Only the **3P providers + CRM upload** are surfaced as raw direct picks in the UI.

**ID-jump boundary at 1072 = canonical → per-advertiser.** The contiguous low-ID range (-1 to 61) is canonical / shared DSes. The next type=1 IDs are the 6 high-ID outliers (DS328493-355420 — mobile-attribution + a duplicate pixel). All type=2 (per-advertiser) instances start at **DS1072**. Type=2 follows a `{advertiser_id} - {container name}` naming pattern with 6 named container types:

| Container | Per-advertiser rows |
|---|---:|
| First Party Audience | 14,036 |
| Third Party Audience | 14,036 |
| Control Group Audience | 14,036 |
| Extension Audience | 14,036 |
| Prospecting Campaign | 11,721 |
| Retargeting Campaign | 11,721 |

Total type=2 rows: 79,587. **None are visible=true.** Mostly absent from TPA expression JSON, **but not universally** — empirical correction 2026-05-29 (TI-999): at least one advertiser (AID 36678) references their own per-advertiser DS `36678 - Prospecting Campaign` (DS69734) in 6 active expressions. The earlier "0 references" claim in `data_knowledge.md` was scoped to the four audience container types (First/Third Party / Control / Extension); the two campaign container types (Prospecting Campaign / Retargeting Campaign) DO appear in some expressions. Implication: audience-bucket detectors should enumerate referenced DS IDs from a sample of expressions rather than assuming only canonical type=1 IDs appear.

## MM 2.0 scoring state table (user-provided, 2026-05-29)

The MM 2.0 product evaluates each IP against DS13 (bucket = industry, vertical = subindustry) and DS19 (keywords) and assigns a tier and score:

| State | In bucket (DS13 industry) | In vertical (DS13 subindustry) | Keywords (DS19) | Tier | Score | MM 2.0 bid-eligible? |
|:-:|:-:|:-:|:-:|---|---|---|
| 1 | ✗ | ✗ | ✗ | — | NULL | No — fails on keywords AND vertical |
| 2 | ✗ | ✗ | ✓ | **Max Reach** | NULL | Yes — succeeds on keywords only |
| 3 | ✓ | ✗ | ✗ | Mid Intent (not bid on) | 3333-6665 | No — fails on keywords |
| 4 | ✓ | ✗ | ✓ | **Mid Intent** | 3333-6665 | Yes — succeeds on keywords AND DS13 (bucket) |
| 5 | ✓ | ✓ | ✗ | **Peak Performance** | **8000** | Yes — fails on keywords, succeeds on DS13 (vertical) |
| 6 | ✓ | ✓ | ✓ | **High Intent** | 10000 | Yes — succeeds on keywords AND DS13 (vertical) |

**Read:** PP = "in vertical, no keyword match" → score 8000. It's MNTN's fallback tier when keywords don't fire but the vertical does. PP is what gets targeted "when we don't have other things" (user, 2026-05-29).

The older TI-896 detector `score_type=rtc + DS13 + DS19` was a v1 expression pattern proxy; the current product definition is the scoring-tier construction above.

## DS14 / DS16 reclassification (correction from earlier passes)

Earlier passes (10-15) called DS14 "MNTN Global Data" and DS16 "MNTN Taxonomy Data" and treated them as part of MM because their names suggested audience targeting. After querying `tpa.categories`:

- **DS14 categories** are bid routing destinations (id=1 "Beeswax Bidder", id=150 "Magnite", id=152 "Index Exchange", id=1000 "IP Ends In .0"). Not audience targeting.
- **DS16 categories** are per-advertiser identifiers (id=1 "AdvertiserID - Eat Clean Bro") plus MNTN-internal event taxonomy (id=2 "PageViews", id=3 "Conversions", id=5 "Prospecting", id=6 "Retargeting", id=7 "MultiTouch", id=8 "VV"). Not audience targeting.

Both DS14 and DS16 appear in almost every campaign because they encode bid-side plumbing, not buyer-selected audience choices. They sit in the **Bid mechanics / internal taxonomy** group, not in any targeting family.

## Per-DS detail (ordered by group, then ID)

### MM — Mountain Match 2.0

All MM DSes are `visible=false` — surfaced via UI templates ("Mountain Match", "Peak Performance"), not as raw buyer picks.

| DS | Name | Vis | +camps | +spend (30d) | −camps | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 13 | MNTN Vertical Categorization | ✗ | 1,525 | $6.94M | 1 | Bucket (industry) + vertical (subindustry). **Also the underlying DS for the PP tier** — state 5/6 in the MM 2.0 state table both evaluate against DS13's vertical match. Product-named "Peak Performance" by Bryce Wagg (2026-04-22); canonical `data_sources.name` is what's shown here. |
| 19 | MNTN Matched | ✗ | 2,914 | $19.71M | 0 | **Keyword half of the MM 2.0 state table.** Buyer selects per-vertical keyword IDs through a UI template; the underlying DS is not visible. Largest MM signal by spend. **RTC = MM real-time variant** — per Sean Yang (TI team, 2026-05-29) RTC and MM are literally the same scoring system; RTC fires within an hour, the main scorer catches up after. No separate "RTC scoring system" exists — `realtime_conquest_score` is the hot-path output of the same MM logic. |
| 38 | MNTN UI Audience Keywords | ✗ | 0 | 0 | 0 | **BUK — feature being rolled out, not active yet** (per Sean Yang, TI team, 2026-05-29). 52.7M categories already loaded in `tpa.categories`. **BUK augments DS19, does not replace it** (per Alex Knorr, 2026-05-29): BUK leverages DS19 as an input source and replaces the LLM-generated keyword pipeline, but DS19 (MNTN Matched V2) stays in production to handle cold-start cases (new advertisers / new keywords don't get BUK recommendations). Steady-state MM = DS13 + DS19 (V2) + DS38 (BUK) combined. |
| 46 | ML Audience Intent Scoring Model | ✗ | 241 | $1.70M | 0 | Fangorn ML-driven intent scoring. DS13→DS46 swap rolled out to first three advertisers week of 2026-04-30 (`mntn_business.md`). Lower adoption today; ramping. |

**Functional read:** DS13 + DS19 do the heavy lifting today (4,439 +camps combined, ~80% of MM-positive spend). DS46 is the Fangorn replacement path. DS38 (BUK) is queued.

### PP — Peak Performance tier (subset of MM)

| DS | Name | Membership | Notes |
|---:|---|---|---|
| 13 | MNTN Vertical Categorization | shared with MM | PP fires when the IP matches the vertical (DS13) and the bidder assigns score=8000 in `household_score`. |
| 19 | MNTN Matched | shared with MM | PP specifically requires NO keyword match. If DS19 keywords ALSO fire, the IP escalates to High Intent (state 6, score=10000), not PP. |

**Not a standalone DS family.** PP has no DSes that uniquely belong to it. The catalog surfaces it as a top-level row because PP is the product the UI exposes — but at the DS layer it's identical to MM, distinguished only by which state in the MM 2.0 state table fires. No new query, expression filter, or bucket logic for PP — it is a scoring tier inside MM.

### MNTN Pixel — first-party data from MNTN's pixel

All MNTN Pixel DSes are `visible=false` — auto-attached by platform code, not buyer-selected.

| DS | Name | Vis | +camps | +spend (30d) | −camps | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 2 | MNTN First Party | ✗ | 21 | $0.13M | 482 | **OPM segments — buyer-selectable first-party targeting** (Zach Schoenberger, 2026-06-01): "DS 2 is our OPM segments. they are just another way to create a category based on our first party data. they get targeted in the expression like any other category id." So DS2 isn't only a backing pointer for `blockFirstParty` suppression — buyers actively target first-party categories via DS2 in expressions just like 3P interest segments. The 21 positive uses are real first-party audience targeting; the 482 negative uses are exclusion-style suppression. Architecturally: each DS2 category_id resolves to an OPM (`expression_type_id=1`) audience. |
| 21 | MNTN Conversion | ✗ | 0 | 0 | 3,842 | **Pure exclusion** — past converters suppressed from prospecting. |
| 34 | MNTN Pageview | ✗ | 0 | 0 | 3,818 | **Pure exclusion** — past pageview visitors suppressed from prospecting. Note a near-duplicate DS355420 "MNTN PageView" (different ID, capital V, distinct `data_source_key`) exists in the high-ID range; never used in prospecting expressions. Unknown which is canonical going forward — flag. |
| 43 | MNTN ISP Type | ✗ | 0 | 0 | 17 | Internal ISP-based exclusion. Niche. |

**Functional read:** All four are auto-attached by MNTN platform code (not buyer-selected). DS2 is the OPM-pointer flag; DS21/34 are the standard "exclude past visitors" suppression; DS43 is an ISP filter. Almost zero positive use in prospecting (21 +camps total, all DS2).

### 3P — bought third-party interest segments

All 4 actively-used 3P DSes are `visible=true` — these ARE the raw buyer picks in the UI.

| DS | Name | Vis | +camps | +spend (30d) | −camps | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 1 | Oracle | ✓ | 553 | $5.97M | 161 | **Legacy 3P from Oracle. Buyer-selectable but no longer in IPDSC** — per Sean Yang (TI team, 2026-05-29) Oracle is a legacy data source still available in MNTN's taxonomy (so buyers can pick it in the UI), but Oracle data is no longer in IPDSC. May have already been disabled by the AUD team (Sean unsure). The 553 prospecting campaigns positively referencing Oracle are paying for **dead-weight clauses** — eligibility doesn't fire because the IPs aren't in IPDSC. **TI-999 deck v7 carves Oracle OUT of the 3P bucket in Pass 17** (user decision 2026-05-29). Open follow-up: confirm with AUD team whether Oracle is currently disabled in the buyer UI or still selectable. |
| 17 | ShareThis | ✓ | 686 | $5.95M | 35 | Bought interest segments. Catalog metadata 100% >2yr stale. |
| 18 | Dstillery | ✓ | 512 | $3.16M | 33 | Bought interest segments. Catalog metadata 100% >2yr stale. |
| 35 | LiveRamp IP | ✓ | 1,873 | $13.57M | 264 | The dominant 3P. ~213k active categories (97% of 3P by count). 99.6% fresh metadata. |

**Functional read:** LiveRamp dominates. ShareThis + Dstillery are widely used but stale. Oracle is a wild card pending delivery verification.

### MNTN Select — Select-product audience + registry

MNTN Select is MNTN's premium-video product (see `mntn_business.md`); these are its dedicated DSes. The canonical product identifier is **`campaign_groups.product_id=2`** (per Ray, 2026-05-05 — a campaign-group attribute, NOT a DS). DS9 + DS42 are the Select-product DS layer.

| DS | Name | Vis | +camps (30d active) | +spend (30d) | Expression refs (all objectives) | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 9 | MNTN Campaigns | ✗ | 12 | $0.20M | 86 (50 prospecting + 36 multi-touch) | **Currently Select household-exposed audiences, but NOT Select-only by design** (Zach Schoenberger, 2026-06-01, definitive): "it is not select only. it was on its way to being removed before it was repurposed to be used with select." So DS9 was slated for deprecation, then **repurposed as the substrate for Select household audiences**. Categories today: "MNTN Select: Households Reached in [Deal Name]" — Andor, NFL Live + Playoffs, BowFlex: March Madness, Renovation Nation, etc. 213 categories in `tpa.categories` (208 Select households + 4 block lists + 1 ROOT); 56 distinct names; 1 partner_id (centrally created). Earliest category 2023-10-23, latest 2026-05-29 (still actively being added). **Used by 3 of 22 active Select advertisers** (~14% — heaviest is AID 36678 at 29/98 of their campaigns). |
| 42 | MNTN Select | ✗ | 0 | 0 | 0 | **Select deal/order registry — metadata only, not used for audience targeting.** 908 categories of deal/order IDs (UUIDs, timestamped compound IDs like `100444-456-1738701684948`). Likely a cross-reference table used by Select platform code, not a buyer pick. |

### Advertiser CRM — advertiser-uploaded customer lists

Only DS4 (the buyer-facing CRM upload) is `visible=true`. The other two are platform-internal.

| DS | Name | Vis | +camps | +spend (30d) | −camps | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 4 | CRM | ✓ | 318 | $1.58M | 754 | Buyer-uploaded customer list. **2.4× more campaigns use it for exclusion** (suppression in prospecting) than for positive retargeting. Victor canonically labels this group "1P". |
| 8 | IP List | ✗ | 0 | 0 | 492 | **Exclusion-only in prospecting** — buyers don't use IP lists as positive prospecting input. Not buyer-visible; the IP list comes from a different upload path. |
| 47 | CRM Identity Graph Generated | ✗ | 0 | 0 | 2 | Essentially unused (2 negative-only references). Platform-internal output of CRM identity graph resolution. |

**Functional read:** In prospecting, Advertiser CRM is almost entirely about EXCLUDING known customers from MM-driven prospecting. Positive use is retargeting territory (`objective_id=4`), which is out of scope for the prospecting cut.

### Bid mechanics / internal taxonomy

Both `visible=false` — pure platform plumbing, never buyer-selected.

| DS | Name | Vis | +camps | +spend (30d) | −camps | Notes |
|---:|---|:-:|---:|---:|---:|---|
| 14 | MNTN Global Data | ✗ | 11,888 | $32.10M | 0 | **Freshness / eligibility filter, NOT bid routing** (per Sean Yang, TI team, 2026-05-29). Auto-attached to all expressions; narrows eligibility to IPs MNTN has seen recently — specifically in `guid_log` (4-day window) and `augmentor_log` (1-day window). The 5 categories (Beeswax Bidder, Magnite, Index Exchange, IP Ends In .0, ROOT) represent the source channels that count as "recent." Every campaign implicitly only bids on IPs MNTN has recently observed via these streams. |
| 16 | MNTN Taxonomy Data | ✗ | 7,669 | $5.47M | 108 | **Per-advertiser identifiers + internal event taxonomy** (PageViews, Conversions, Prospecting, Retargeting, MultiTouch, VV). NOT audience targeting. |

### Dormant / out-of-scope

Registered DSes with negligible or zero use in the 30d prospecting window. Listed for completeness; do not affect Pass 17 buckets. **`Vis` column flags the 6 visible-but-dormant DSes** — buyer-pickable in the UI but no advertisers used them in the last 30d.

| DS | Name | Vis | +camps | +spend | −camps | Sub-type |
|---:|---|:-:|---:|---:|---:|---|
| -1 | MNTN Pixel | ✓ | — | — | — | Sentinel value; not used in TPA expressions (`visible=true` but the ID -1 is reserved) |
| 3 | MNTN Third Party | ✗ | 0 | 0 | 0 | Deprecated — replaced by named providers (DS17/18/35) |
| 5 | Oracle Custom Audience | ✓ | 0 | 0 | 0 | Buyer-visible 3P variant, but no current uptake |
| 6 | MNTN Control Group | ✗ | 0 | 0 | 0 | Internal experimentation control |
| 7 | MNTN Audience Ext | ✗ | 0 | 0 | 0 | Internal |
| 10 | MNTN Geo File | ✗ | 0 | 0 | 0 | Geo (separate from category tree) |
| 11 | LiveRamp (legacy) | ✓ | 0 | 0 | 0 | **Deprecated old LiveRamp** (Sean Yang, TI team, 2026-05-29). Used device_id→IP mapping for targeting; DS35 replaced it by having LiveRamp send IPs directly. Retained in `tpa.categories` because reporting still needs the historical category references. 5 inactive prospecting campaigns still reference it positively (zero impressions in 30d). |
| 12 | MNTN Product Groups | ✗ | 0 | 0 | 0 | Internal product config |
| 15 | MNTN Testing | ✗ | 0 | 0 | 0 | Test DS |
| 20 | OnAudience | ✓ | 0 | 0 | 0 | 3P provider, buyer-visible but no current uptake |
| 22 | Experian | ✓ | 0 | 0 | 0 | 3P provider, buyer-visible but no current uptake |
| 23 | guid_log | ✗ | 0 | 0 | 0 | Internal log reference. `display_name="MNTN Pixel"` — duplicate display name with DS-1. |
| 24 | Justuno | ✗ | 0 | 0 | 0 | CRM ingestion source (feeds DS4) |
| 25 | 5x5 | ✗ | 0 | 0 | 0 | Provider, no current use |
| 26 | sharethis_predactiv | ✗ | 0 | 0 | 0 | ShareThis variant, no current use |
| 27 | LaunchLabs | ✗ | 0 | 0 | 0 | Provider, no current use |
| 28 | 33Across | ✗ | 0 | 0 | 0 | 3P provider, no current use |
| 29 | deepsync | ✗ | 0 | 0 | 0 | Provider, no current use |
| 30 | MNTN augmentor_log | ✗ | 0 | 0 | 0 | Internal log reference |
| 31 | CRM Upload | ✗ | 0 | 0 | 0 | CRM ingestion source (feeds DS4) |
| 32 | CDK | ✗ | 0 | 0 | 0 | Provider, no current use |
| 33 | Sovrn | ✗ | 0 | 0 | 0 | 3P provider, no current use |
| 36 | Cybba | ✗ | 0 | 0 | 0 | 3P provider, no current use |
| 37 | CallRail | ✗ | 0 | 0 | 0 | Provider, no current use |
| 39 | Klickly | ✗ | 0 | 0 | 0 | Provider, no current use |
| 40 | 33Across API | ✗ | 0 | 0 | 0 | 3P provider variant, no current use |
| 41 | Freshpaint | ✗ | 0 | 0 | 0 | CRM ingestion source |
| 44 | Captify | ✗ | 0 | 0 | 0 | 3P provider, no current use |
| 45 | Hubspot | ✗ | 0 | 0 | 0 | CRM ingestion source. Shares `data_source_key='ojLY3uGYtq'` AND `create_time=2025-11-25 21:21:41.453456` with DS48 Tealium (Sean Yang, 2026-05-29). Likely unintentional batch artifact — `data_source_key` is auto-generated from timestamp, so simultaneous creation produced identical keys. |
| 48 | Tealium | ✗ | 0 | 0 | 0 | CRM ingestion source. Shares `data_source_key='ojLY3uGYtq'` AND `create_time=2025-11-25 21:21:41.453456` with DS45 Hubspot — same batch artifact (see DS45 note). |
| 49 | Publisher Network | ✗ | 0 | 0 | 0 | Internal |
| 50 | shopify | ✓ | 0 | 0 | 0 | CRM ingestion source, buyer-visible but no current uptake |
| 51 | Bombora | ✗ | 0 | 0 | 0 | 3P provider, no current use |
| 52 | Liftlab | ✗ | 0 | 0 | 0 | Incrementality vendor, no current use |
| 53 | GCS Bucket | ✗ | 0 | 0 | 0 | Generic ingestion source |
| 54 | S3 Bucket | ✗ | 0 | 0 | 0 | Generic ingestion source |
| 55 | Klaviyo | ✗ | 0 | 0 | 0 | CRM ingestion source |
| 56 | Segment | ✗ | 0 | 0 | 0 | CRM ingestion source |
| 57 | Ours Privacy | ✗ | 0 | 0 | 0 | Provider, no current use |
| 58 | Audience Acuity | ✗ | 0 | 0 | 0 | Provider, no current use |
| 59 | Storage Buckets | ✗ | 0 | 0 | 0 | Generic ingestion source |
| 60 | mParticle | ✗ | 0 | 0 | 0 | CRM ingestion source |
| 61 | AppsFlyer v2 | ✗ | 0 | 0 | 0 | Mobile attribution, no current use |
| **328493** | Adjust - Mobile Partner | ✗ | 0 | 0 | 0 | **High-ID outlier.** Mobile-attribution partner. Outside the contiguous 0-61 range; never referenced in TPA expressions. |
| **328494** | AppsFlyer | ✗ | 0 | 0 | 0 | High-ID outlier. Mobile-attribution partner (display_name="AppsFlyer", distinct from DS61 "AppsFlyer v2"). |
| **328495** | Branch - Mobile Partner | ✗ | 0 | 0 | 0 | High-ID outlier. Mobile-attribution partner. |
| **328496** | Kochava | ✗ | 0 | 0 | 0 | High-ID outlier. Mobile-attribution / measurement partner. |
| **328497** | Singular | ✗ | 0 | 0 | 0 | High-ID outlier. Mobile-attribution / measurement partner. |
| **355420** | MNTN PageView | ✗ | 0 | 0 | 0 | **Unused** (Sean Yang, TI team, 2026-05-29: "I don't think we ever use DS355420"). DS34 remains the canonical MNTN Pageview DS for production exclusion logic. DS355420 is shelf-warmer / experimental, no live traffic. |

## Open questions

### Resolved (Sean Yang, TI team, 2026-05-29 Slack thread)

- **DS1 Oracle delivery:** ✅ Oracle is a legacy 3P, no longer in IPDSC, still in MNTN's taxonomy so buyers can pick it on UI (may have been disabled by AUD — Sean unsure). Jordan Piepkow confirms (2026-05-29): "deprecated but some audiences still use." The 553 positive-Oracle prospecting campaigns are almost certainly paying for dead-weight clauses.
- **DS11 LiveRamp legacy:** ✅ Deprecated old LiveRamp (device_id→IP mapping); replaced by DS35 (IPs delivered directly). Retained in `tpa.categories` because reporting still needs them.
- **DS14 mechanics:** ✅ Freshness / eligibility filter — confirmed by both Sean Yang (2026-05-29) and Zach Schoenberger (2026-06-01). Sean: "MNTN global data, which gets automatically added to all expressions to filter down to only IPs seen in guid_log (4-day window) and augmentor_log (1-day window)." Zach: "ds14 is if we have seen the ip in our logs. it limits our segments to recently seen ip's." Auto-attached. Not bid routing as previously framed.
- **`score` block in audience expression — what it does:** ✅ Jordan Piepkow (TI team, 2026-05-29): "That's the score used by bidder to decide if we will bid (high mid low)." The `select.score.types` array tells the bidder to evaluate IPs against the scoring pipeline (RTC or other) and decide bid-or-skip based on the resulting tier. **The `id` is a `vertical_id`** — per Ryan Kleck (TI team, 2026-06-01): the RTC score gives 10K to IPs that match that vertical in real-time.
- **HHST gates whether RTC is effective:** ⚠️ Per Ryan Kleck (TI team, 2026-06-01): "the score doesn't matter if the HHST is not set." So even though `score_type=rtc` appears in 99.9% of prospecting expressions, **RTC only actually affects bidding when the campaign has a Household Score Threshold (HHST) set**. For campaigns without HHST configured, RTC is a no-op even though the flag is in the expression JSON. **This invalidates "RTC-touching = 99.9%" as a measure of effective RTC.** To find truly RTC-active campaigns: lookup bids for the campaign_id in `bid_events` and check whether the threshold is set on the bidder side. Ryan also notes he doesn't fully understand the bidder mechanism — AUD team needed for definitive answer. **TI-999 implication:** the 3P-only baseline (404 camps / $1.23M) may still be RTC-confounded if HHST is set on those campaigns; need to segment by HHST-set vs HHST-not-set for a truly clean 3P performance signal.
- **DS9 is not Select-only by design:** ✅ Zach Schoenberger (TI team, 2026-06-01): "it is not select only. it was on its way to being removed before it was repurposed to be used with select." So earlier Jordan + Sean framing ("DS9 = just select") was simplified — historically DS9 was slated for deprecation, then repurposed as the substrate for MNTN Select household audiences. Updates the catalog framing: DS9 isn't a Select-specific allocation, it's a general-purpose DS currently used for Select.
- **DS2 is buyer-selectable, not just a backing pointer:** ✅ Zach Schoenberger (TI team, 2026-06-01): "DS 2 is our OPM segments. they are just another way to create a category based on our first party data. they get targeted in the expression like any other category id." Updates earlier framing — DS2 isn't only a `blockFirstParty` suppression pointer, it's actively used as a positive targeting layer where buyers target first-party (OPM) categories like any other interest segment. The 21 positive prospecting refs are intentional first-party targeting.
- **DS45/DS48 duplicate key — confirmed bug:** ✅ Zach Schoenberger (TI team, 2026-06-01): "someone incorrectly copied the row. they should not be the same." Operator copy-paste error during registration; should be fixed.
- **DS34 vs DS355420 — different things:** ✅ Zach Schoenberger (TI team, 2026-06-01): "they do different things." Earlier "successor / deprecated" framing was wrong; DS34 is the production pageview exclusion DS, DS355420 represents something else entirely (still open what exactly).
- **DS14 — Zach confirms freshness filter framing:** ✅ Zach Schoenberger (TI team, 2026-06-01): "ds14 is if we have seen the ip in our logs. it limits our segments to recently seen ip's." Matches Sean's earlier framing (guid_log 4d + augmentor_log 1d).
- **Pixel DSes (DS2/21/34/43) — each represents different data:** ✅ Zach Schoenberger (TI team, 2026-06-01): "they all represent different data." The "one DS per pixel-event-type" mental model is roughly right — each is a distinct signal source with a distinct function (DS2 = OPM segment targeting; DS21 = conversion exclusion; DS34 = pageview exclusion; DS43 = ISP filter).
- **DSes are not a bidder concept (Ryan Kleck, 2026-06-01):** "the bidder doesn't know about DSs.. that's a MemDB concept." MemDB translates audience expressions (with DS references) into IP × campaign membership; the bidder receives ALL scores for those IPs and operates on scores + HHST, not on DSes directly. **Implication:** bid-side data (e.g., `bid_events`) does not show DS references. Analyses that need DS-level audience composition must work upstream from the bidder, via the expression or MemDB membership.
- **DS34 vs DS355420:** ✅ **Two separate DSes that do different things** (Zach Schoenberger, 2026-06-01, definitive). Earlier framing of "DS34 being deprecated in favor of DS355420" is wrong — they're not a successor pair, they represent different data. Sean Yang (2026-05-29) said DS355420 was unused; combined with Zach's "different things," the picture is: DS34 = production pageview exclusion DS (3,818 negative refs in 30d prospecting); DS355420 = a different thing that's not in active use today. What exactly DS355420 represents is still open — but it's not the successor to DS34.
- **DS38 BUK rollout:** ✅ Feature being rolled out, not yet active (Sean Yang). **BUK augments DS19 — does not replace it** (Alex Knorr, 2026-05-29): BUK leverages DS19 as an input and replaces the LLM-generated keyword pipeline, but DS19 (MNTN Matched V2) stays in production to handle cold-start cases (new advertisers / new keywords don't get BUK recommendations). Steady-state MM = DS13 + DS19 (V2) + DS38 (BUK) combined.
- **DS19 in MM (RTC vs separate scoring):** ⚠️ Sean had two readings: (a) "RTC is literally the same as MM, but it gets done in real time within an hour" (early in thread); (b) "RTC is a pipeline running independently to MM, so it automatically assigns a 10k score in real time if found a match. MM is a batch process that goes through IPDSC" (later in thread). Reconciled read: RTC and MM are **independent pipelines** producing the same kind of output (10k scores) — RTC is a real-time match-and-tag pipeline filling `realtime_conquest_score`; MM is a batch IPDSC scoring pipeline filling `household_score`. They are NOT the same scoring system. **AUD team to confirm definitively.**
- **DS45/DS48 duplicate `data_source_key`:** ✅ Confirmed bug — Zach Schoenberger (2026-06-01): "someone incorrectly copied the row. they should not be the same." Sean Yang (2026-05-29) noted both rows share `data_source_key='ojLY3uGYtq'` AND `create_time=2025-11-25 21:21:41.453456` — auto-generated from timestamp, simultaneous creation, not the auto-gen mechanism's fault. Operator copy-paste error during registration. Should be fixed.

### Still open

1. **RTC vs MM pipeline relationship (AUD squad):** Sean's two statements differ — first read "RTC = MM real-time variant"; revised read "RTC is independent of MM, MM is batch via IPDSC, RTC is real-time match-and-tag." Matt Brorby (2026-06-01) adds: RTC is the first check in the bidder scoring waterfall, takes precedence. AUD team to confirm whether RTC and MM share the same scoring engine or are genuinely separate pipelines.
2. **HHST gates effective RTC (Ryan Kleck flagged 2026-06-01):** RTC only affects bidding when HHST is set. **Where to find HHST settings — campaigns table? campaign_configs? bidder runtime?** TI-999 3P-only baseline needs HHST segmentation OR per-impression `realtime_conquest_score != 10000` filter to be clean.
3. **Does Fangorn (DS46) bake in RTC?** User question 2026-06-01. Matt Brorby partially answered for Fangorn measurement (filter to Fangorn-score > HHST for heterogeneous-effect analysis), but whether Fangorn-scored IPs ALSO get RTC-tagged for the same campaign is still open.
4. **What is DS355420 actually for?** Zach (2026-06-01): DS34 and DS355420 "do different things" — but DS355420 isn't in production use per Sean. So what is it for, and is it dormant on purpose? Pixel-platform team for the specific purpose.
5. **Per-advertiser type=2 in expressions (Prospecting/Retargeting Campaign vs First/Third Party/Control/Extension):** Zach's response addressed DS2 instead of the type=2 question. Still need to ask why the two Campaign container types appear in expressions while the four Audience container types don't.
2. **How RTC-only audience expressions get created in the UI:** the 7,659 RTC-only campaigns (geo + `score_type=rtc` + DS14 + holdout, no buyer-picked DSes) — what UI flow produces this minimal expression? Default-when-no-template-picked? Specific product (e.g., "Geo only" prospecting)? AUD team to clarify.
3. **How buyers OPT OUT of RTC:** 16 campaigns across 3 advertisers (AID 36678, 37336, 42097) have no `score_type=rtc` flag. Is there a UI toggle to disable RTC? Are these constructed via API / direct platform tooling? Why are 36678 and 37336 (both heavy MNTN Select users) the dominant non-RTC advertisers? Campaign IDs at `outputs/ti_999_pass20_anomalies_2026_05_29.csv`.
3. **Confirm Oracle (DS1) disabled in buyer UI:** Sean unsure whether AUD team has already disabled Oracle as a selectable option in the buyer UI. Ask AUD directly. If still selectable, advocate for disabling (553 active prospecting campaigns are paying for clauses that never deliver).
4. **DS2 vs DS21/DS34/DS43 functional split inside MNTN Pixel:** DS2 = OPM-segment pointer for retargeting; DS21/34 = pure exclusion suppression; DS43 = ISP filter. Kept grouped; flag if any downstream pass needs to disambiguate.
5. **DS9 MNTN Select scope going forward:** Jordan + Sean both confirmed DS9 is Select-only today. Is it intended to broaden beyond the 6 advertisers currently using it? Tied to Select product roadmap.
6. **Mobile-attribution outliers (DS328493 Adjust / DS328494 AppsFlyer / DS328495 Branch / DS328496 Kochava / DS328497 Singular):** all six high-ID type=1 DSes have zero TPA-expression use. Do they show up anywhere else (e.g., MMP integration via core tables, attribution reports)? Out-of-scope for prospecting but worth confirming they're not silently feeding something downstream.

## Pass 21 bucket results — current locked taxonomy (RTC dropped from axes)

Supersedes Pass 18, 19, and 20. RTC is in 99.9% of prospecting expressions and isn't a buyer-pickable axis — it belongs with the platform plumbing (geo, DS14 freshness filter, 10% holdout) rather than as a bucket category. Pass 21 drops it. **4 buyer-pickable binary axes:**
- **MM** = `{DS13, DS19, DS38, DS46}`
- **MNTN Select** = `{DS9, DS42}`
- **3P** = `{DS17, DS18, DS35}` (Oracle DS1 carved out)
- **Advertiser CRM** = `{DS4, DS8, DS47}` (any polarity)

Universal platform plumbing (not bucket axes — present on essentially every prospecting campaign):
- **Geo clause** — 100% of expressions
- **`score_type=rtc`** — 99.9% (16 anomalies, see callout below)
- **DS14** (freshness filter, IPs in `guid_log` 4d / `augmentor_log` 1d) — 100%
- **10% holdout via MD5 hash bucket** — 100%
- **MNTN Pixel exclusions** (DS21/34/43) — auto-attached suppression for past converters / pageview visitors / ISP filter

Math is identical to Pass 18; only the labels and framing are improved.

Query: `tickets/ti_999_interest_segment_sizing/queries/ti_999_finding15_pass18_select_axis_oracle_carved.sql` (reused)
Output: `tickets/ti_999_interest_segment_sizing/outputs/ti_999_pass21_buckets_2026_05_29.csv`

### Bucket breakdown

### Bucket breakdown

Buckets are mutually exclusive — each name implicitly excludes the axes not listed (e.g., "MM + 3P" means "no CRM, no Select").

| Bucket | n_camps | % | Spend (30d) | % spend | Annualized |
|---|---:|---:|---:|---:|---:|
| **Geo-only** (no buyer audience layer) | **7,663** | **64.5%** | **$5.25M** | **16.4%** | **$63M** |
| MM only | 1,194 | 10.0% | $6.02M | 18.7% | $72M |
| MM + 3P | 1,133 | 9.5% | $7.25M | 22.6% | $87M |
| MM + CRM | 566 | 4.8% | $5.08M | 15.8% | $61M |
| CRM only | 480 | 4.0% | $1.07M | 3.3% | $13M |
| 3P only | 439 | 3.7% | $1.41M | 4.4% | $17M |
| MM + 3P + CRM | 306 | 2.6% | $3.32M | 10.3% | $40M |
| 3P + CRM | 96 | 0.8% | $2.51M | 7.8% | $30M |
| Select + CRM | 7 | 0.1% | $0.17M | 0.5% | $2M |
| Select only | 5 | 0.0% | $0.03M | 0.1% | $0M |
| **Total prospecting** | **11,889** | 100% | **$32.10M** | 100% | **$385M** |

> ⚠️ **CRM polarity matters when combined with MM (or any positive targeting layer).** The bucket table above lumps CRM-include and CRM-exclude together for headline simplicity, but they mean fundamentally different things at the scoring layer:
>
> - **CRM-exclude** = hygiene (suppress known customers from prospecting). Doesn't change MM scoring; eligible-IP pool is just narrowed to "MM-scored IPs that aren't already customers." Standard prospecting practice.
> - **CRM-include** = positive targeting layer. The eligibility intersection becomes "MM-scored IPs ∩ CRM-list IPs" — MM scoring is now ranked over the customer-list cohort only. Effectively a customer-list-seeded MM prospecting motion (per Zach: this is the intended use of CRM in prospecting, not retargeting).
>
> Empirical split inside CRM-touching: **78% are pure exclusion** (hygiene), **16% are include-only**, **5% are both**. So most of the $12.15M CRM-touching spend is hygiene; only ~$1.57M (4.9% of all prospecting) involves CRM as a positive scoring constraint. Worth carving out for any downstream analysis where MM-scoring eligibility actually matters.

Plus the **16-campaign no-RTC anomaly cohort** (see callout below) — these 16 still count in the table above (they fall into Geo-only / MM-only / various combos depending on their DSes), but are flagged separately because they bypass the universal RTC default.

### Anomaly cohort: 16 campaigns with no RTC (concentrated in 3 advertisers)

99.9% of prospecting expressions have `score_type=rtc` auto-attached. The 16 that don't are concentrated in **just 3 advertisers** (AID 36678, AID 37336, AID 42097) — all sophisticated MNTN Select customers with custom audience setups. Full list at `tickets/ti_999_interest_segment_sizing/outputs/ti_999_pass20_anomalies_2026_05_29.csv`.

**6 MM-without-RTC** — buyer attached explicit DS19 keywords (MM batch) but the expression has no `score_type=rtc` flag. Maybe intentional opt-out from the real-time scoring pipeline:

| Campaign | Audience Seg | Advertiser | Spend (30d) | DS refs |
|---:|---:|---:|---:|---|
| 487499 | 621633 | 36678 | $36,970 | DS1(-), DS13(+), DS14(+), DS17(+), DS19(+), DS2(-), DS4(-) |
| 608696 | 731603 | 36678 | $16,250 | DS1(-), DS14(+), DS17(+), DS19(+), DS2(-), DS35(+), DS4(-) |
| 621934 | 748302 | 36678 | $1,250 | DS1(-), DS14(+), DS17(+), DS19(+), DS2(-), DS35(+), DS4(-) |
| 247343 | 317894 | 37336 | $12,120 | DS14(+), DS19(+), DS2(-), DS4(-), DS8(-) |
| 483155 | 583796 | 37336 | $19,910 | DS14(+), DS19(+), DS2(-), DS4(-), DS8(-) |
| 620506 | 746338 | 42097 | $8,550 | DS13(-), DS14(+), DS2(-), DS21(-), DS34(-), DS35(+), DS4(-) |

**10 no-RTC no-MM** — neither RTC nor MM batch, just custom DS combos (DS9 Select households, DS2 OPM pointer, DS8 IP list):

| Campaign | Audience Seg | Advertiser | Spend (30d) | DS refs |
|---:|---:|---:|---:|---|
| 487500 | 621641 | 36678 | $2,320 | DS14(+), DS2(+) |
| 487501 | 621637 | 36678 | $4,780 | DS14(+), DS9(+) |
| 608694 | 731610 | 36678 | $60 | DS14(+), DS2(+) |
| 608697 | 731612 | 36678 | $2,830 | DS14(+), DS9(+) |
| 621932 | 748309 | 36678 | $0 | DS14(+), DS2(+) |
| 621935 | 748311 | 36678 | $210 | DS14(+), DS9(+) |
| 247341 | 317897 | 37336 | $1,860 | DS14(+), DS8(-), DS9(+) |
| 247342 | 317896 | 37336 | $450 | DS14(+), DS2(+), DS8(-) |
| 483150 | 583799 | 37336 | $1,240 | DS14(+), DS2(+), DS8(-) |
| 483151 | 583801 | 37336 | $2,590 | DS14(+), DS8(-), DS9(+) |

**Read:** AID 36678 alone accounts for 9 of 16 anomalies (the same advertiser that's the heaviest DS9 / MNTN Select household user). AID 37336 has 6 anomalies (also a DS9 user). These advertisers appear to be deliberately bypassing the RTC platform default — possibly because they want pure batch MM scoring (the 6 MM-without-RTC cases) or pure custom audience targeting via DS2 / DS9 (the 10 no-RTC no-MM cases). Worth asking AUD whether there's a UI flow that opts out of RTC, or whether these expressions are constructed via API / direct platform tooling.

### Axis-touching headline rollups

| Axis | Campaigns | Spend | % spend | Read |
|---|---:|---:|---:|---|
| **MM-touching** (buyer-picked DS13/19/38/46) | 3,199 | $21.66M | **67.5%** | Buyer attached an MM batch DS clause. |
| **3P-touching** (DS17/18/35) | 1,974 | $14.49M | **45.1%** | Buyer added a 3P interest segment (LiveRamp / ShareThis / Dstillery). |
| **CRM-touching** (DS4/8/47, any polarity) | 1,455 | $12.15M | **37.9%** | 78% pure exclusion (hygiene). Polarity matters when combined with MM — see callout above. |
| **CRM-include-touching only** (DS4/8/47 used positively) | 318 | $1.57M | **4.9%** | The cohort that's a positive scoring constraint on top of MM, not hygiene. |
| **MNTN Select-touching** (DS9/42) | 12 | $0.20M | **0.6%** | Narrow Select-customer cohort. Never co-occurs with MM or 3P. |

### Read

> **🔑 Load-bearing deck finding (Ryan Kleck + Venn analysis, 2026-06-01):** when buyers combine MM with 3P-include (the majority of prospecting spend — buckets `MM + 3P`, `MM + 3P + CRM`, and `MM + CRM` together = ~49% of spend), the bidder mechanics are **NOT audience expansion** as buyers usually assume. With HHST > 0, 3P functions as a **narrowing filter** that intersects MM scoring to (MM ∩ 3P). Only the IPs that fall in BOTH the MM-scored set AND the 3P segment get bid on; 3P-only IPs without MM scores fail the HHST threshold. **3P segment quality therefore directly determines which slice of MM-scored IPs the bidder bids on** — which means 3P quality directly determines MM delivery quality. This is the strongest argument for TI-956's per-segment scoring framework. Full mechanism in `data_knowledge.md` § "MM + 3P intersection mechanics — LOCKED LOGIC".

- **The single biggest cohort by campaign count is "Geo-only (no buyer audience layer)"** — 7,663 campaigns / 64.5% / $5.25M / 16.4% of spend. These buyers attached **no MM batch DS**, **no 3P interest segment**, **no CRM**, **no MNTN Select audience**. Their intentional input was just a geo. The platform handled everything else (RTC scoring via the `score_type=rtc` flag, DS14 freshness filter, 10% holdout, pixel exclusions).
- **MM-touching = 67.5% of spend** — 3,199 campaigns / $21.66M. Buyer attached an explicit MM batch DS clause (DS13 vertical, DS19 keywords, DS38 BUK queued, or DS46 Fangorn).
- **3P-touching = 45.1% of spend** — 1,974 campaigns / $14.49M. Buyer attached a bought interest segment.
- **MM + 3P is the biggest single audience-driven bucket** — 1,133 camps / $7.25M / 22.6% — the canonical "MM prospecting narrowed by 3P interest" cohort.
- **CRM-touching = 37.9% of spend, but 78% of that is exclusion-only** (hygiene). Only ~5% of all prospecting spend involves CRM-include / look-alike-style.
- **MNTN Select is microscopic** — 12 camps / $0.20M / 0.6%. Concentrated in 6 advertisers total. Never co-occurs with MM or 3P.
- **RTC and geo are not bucket axes — they're platform plumbing.** RTC is in 99.9% of expressions; geo is in 100%. Treating either as a bucket axis adds no information; the meaningful axes are the buyer-pickable layers above.

### Pass 20 historical (superseded — kept for reference)

Pass 20 kept RTC as its own bucket axis. Headlines under Pass 20: RTC-only (geo-only) = 7,659 / 64.4% / $5.24M / 16.3%; MM + RTC = 1,194 / $6.02M; MM + RTC + 3P = 1,134 / $7.25M. Superseded by Pass 21 once we recognized RTC is universal (99.9%) and belongs in platform plumbing, not a bucket axis.

### Pass 19 historical (superseded — kept for reference)

Pass 19 folded RTC into MM-touching based on Sean's first reading "RTC is literally the same as MM, but real-time." Headline under that framing: MM-touching = 99.9% of spend. Pass 19 is superseded by Pass 20 (and then Pass 21) once Sean revised to "RTC is an independent pipeline."

Axes: MM `{13,19,38,46}` ∪ `{score_type=rtc}` · MNTN Select `{9,42}` · 3P `{17,18,35}` · Advertiser CRM `{4,8,47}`. "Any signal" = positive OR negative DS reference, or (for MM) the RTC flag at the expression top level.

Query: `tickets/ti_999_interest_segment_sizing/queries/ti_999_finding15_pass19_rtc_in_mm.sql`
Output: `tickets/ti_999_interest_segment_sizing/outputs/ti_999_pass19_buckets_2026_05_29.csv`

| Bucket | n_camps | % | Spend (30d) | % spend | Annualized |
|---|---:|---:|---:|---:|---:|
| **MM only** (incl. RTC-only) | **8,853** | **74.5%** | **$11.26M** | **35.1%** | **$135M** |
| **MM + 3P** | **1,572** | **13.2%** | **$8.66M** | **27.0%** | **$104M** |
| MM + Advertiser CRM | 1,044 | 8.8% | $6.15M | 19.1% | $74M |
| MM + 3P + Advertiser CRM | 402 | 3.4% | $5.83M | 18.2% | $70M |
| MM + MNTN Select + CRM | 5 | 0.0% | $0.17M | 0.5% | $2M |
| MM + MNTN Select | 2 | 0.0% | $0.02M | 0.1% | $0M |
| MNTN Select only | 3 | 0.0% | $0.01M | 0.0% | $0M |
| MNTN Select + CRM | 2 | 0.0% | $0.00M | 0.0% | $0M |
| Advertiser CRM only | 2 | 0.0% | $0.00M | 0.0% | $0M |
| nothing (truly bare: no MM, no RTC, no 3P, no CRM, no Select) | 4 | 0.0% | $0.01M | 0.0% | $0M |
| **Total prospecting** | **11,889** | 100% | **$32.10M** | 100% | **$385M** |

**Read:**
- **MM-touching now covers ~99.9% of prospecting spend** (11,878 camps / $32.09M). Once RTC is correctly bucketed as MM-real-time, essentially every MNTN prospecting campaign uses MM in some form. Only 4 campaigns / $14K are truly non-MM.
- **3P-touching: 1,974 camps / $14.49M / 45.1%** (unchanged vs Pass 18).
- **CRM-touching: 1,453 camps / $12.15M / 37.9%** — mostly hygiene exclusions (78% of CRM-touching campaigns reference CRM only as a negative clause; only 16% are CRM-include / look-alike-style). Per Zach Schoenberger (2026-04-30), CRM lists are usable only in prospecting (not retargeting), so CRM-include is intentional design, not a retargeting workaround.
- **MNTN Select: 12 camps / $0.20M / 0.6%** (unchanged in totals, slight reshuffling: 7 of the 12 also use RTC, so they sit inside MM-touching buckets now).
- **RTC-only is the dominant MM pattern.** Within the 11,878 MM-touching campaigns:
  - 8,679 (73%) are RTC-only — no batch MM DS reference (DS13/19/38/46), just `score_type=rtc`
  - 3,193 (27%) use both batch MM DSes and RTC
  - 6 use batch MM DSes without RTC (rare)
  - **Takeaway: MNTN prospecting overwhelmingly leans on the MM real-time / recent-visitor pathway. Buyer-picked DS13 verticals and DS19 keywords are present in ~27% of MM-touching campaigns, but the dominant signal is RTC.**

### Pass 18 historical (superseded — kept for reference)

Pass 18 treated RTC as outside MM. Under that framing the "nothing" bucket (RTC-only campaigns) accounted for 7,663 campaigns / $5.25M / 16.4% of spend, and MM-touching was 3,199 camps / $21.67M / 67.5%. Pass 19 corrects this by folding RTC into MM per Sean Yang's clarification.

### Pass 17 historical (superseded — kept for reference)

Pass 17 treated Oracle as 3P and had no Select axis. Headline numbers were: 3P-touching = 2,154 camps / $15.81M (49.2%); MM-touching = 3,199 camps / $21.66M (67.5%); MM+3P = 1,208 camps / $7.75M.

## Related references

- `knowledge/data_knowledge.md` §"Intent Scoring Architecture" — HHST score ranges, current production distribution
- `knowledge/data_knowledge.md` §"Audience System Architecture" — DS2 = OPM-segment pointer, expression_type_id semantics
- `knowledge/data_knowledge.md` §"Bidder Scoring Reality" — three score fields (household_score / advertiser_household_score / realtime_conquest_score)
- `knowledge/mntn_business.md` §"Peak Performance / Mountain Matched relationship" — TI-896 segment-expression detection, 24% PP/MM overlap
- `tickets/ti_999_interest_segment_sizing/queries/ti_999_ds_catalog_usage.sql` — query that produced the per-DS usage counts
- `tickets/ti_999_interest_segment_sizing/queries/ti_999_finding15_pass17_corrected_mm.sql` — Pass 17 bucket query (MM = {13,19,38,46})
- `tickets/ti_999_interest_segment_sizing/outputs/ti_999_ds_catalog_usage_2026_05_29.csv` — raw output
