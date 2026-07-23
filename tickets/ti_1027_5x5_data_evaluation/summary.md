---
doc_type: ticket
title: "TI-1027: 5x5 Data Evaluation"
status: in_progress
date: 2026-07-09
summary: "Renew/drop/renegotiate eval of 5x5 (DS25) MNTN Matched site-visit data vendor"
result: "KEEP — 5x5 outsized ~3.4x its scale, #2 unique MM-domain contributor; fee pending billing"
keywords: [5x5, ds25, site_visit_signal, mntn matched, direct_data_partners, flat_fee, ddp, vendor evaluation, willingness to pay, website_crawl_verticals, domain overlap, leverage ratio, predactiv, 33across, vertical classification, ti-1027]
---

## TL;DR

**Q:** Renew/drop/renegotiate evaluation of the 5x5 (DS25) MNTN Matched site-visit data vendor: quantify its value and impact on Fangorn/MNTN Matched relative to its scale.

**A:** KEEP (renew). 5x5 (DS25) is a flat-fee DDP feeding site_visit_signal into MNTN Matched via domain-to-vertical coverage, not reach. Value is unique DOMAINS: 68.5% of its domains are provided by no other vendor; 47,069 of them classify to a vertical (~12% of the MM-usable classified-domain universe, B2B-weighted 25-34%). It is outsized ~3.4-3.85x its 3.6% data scale and is the #2 unique MM-domain contributor (behind Predactiv, also flat-fee). Reach is NOT the value: 73.8% of its IPs are already seen internally (DS23/30). The "domain-only, no extended URL" claim is CONFIRMED (only 3.8% of 5x5 URLs carry a path vs 67-100% for other vendors) but moot for MM because the vertical classifier strips every URL to domain. Delivered IP score-tier quality is ~uniform across high-volume vendors (5x5 39.4% High-Intent, highest among high-volume). MM is estimated worth tens of $M/yr, so 5x5's slice clears a typical DDP flat fee (tens-to-low-hundreds of $K/yr) with margin; it would take a low-$M/yr fee to fail break-even. Recommendation KEEP; final sign-off pending the flat-fee amount from billing (Sherwin), never obtained. If the fee is surprisingly large, fall back to renegotiate (demand URL paths). WTP: floor ~$40K/yr, fair ~$150-600K/yr, walk-away ~$6.3M/yr (monthly fair ~$15-50K/mo). Status 2026-07-09: all 8 external MM DDPs still enabled; 5x5 still delivering ~9 days past end-of-June contract end, unclear if renewal signed. The redundant $0.50-CPM DDPs (33Across API 3.2% unique, Sovrn 1.6%, Cybba 5.7%) are the real cost-review targets (AUDI-1051 backlog).

**How:** Confirmed 5x5 lineage from SteelHouse/airflow-ti (raw gs://mntn-data-partners/partners/5x5/ip_to_url parquet, positional _COL_0/1/2 = ip/url/epoch, ~2-hr batches; dsid25_5x5_processing.py; DS25 in ENABLED_DSIDS=[23,25,26,28,30,36]; Stage1 fpa_vendor_log; Stage2 unified site_visit_signal separable by data_source_id; consumer distinct_site_visit_signal_domains.py excludes DS23, strips url to domain; feature store then MNTN Matched). Queried GCS parquet via BQ temp external tables (site_visit_signal ~250 GiB/day, raw 5x5 ~1.48 GiB/day; zzz_temp.site_visit_signal is manual, not auto-populated). Verified cost structure in tpa.direct_data_partners (is_current=true): DS25 billing_type=flat_fee, fixed_cpm=null, used_in_mntn_match=true; peer MM-DDP rate $0.50 CPM. Phase 1 SCALE: per-vendor rows/IPs/domains for 2026-06-15 (2.57B rows; 5x5=93M rows, 20.8M IPs, 93K domains, 3.8% URLs w/ path). Phase 3 CONTRIBUTION: 7-day domain-overlap (997,963 domain universe; 5x5-unique 138,496=13.88%) and 1-day IP overlap (5x5-only IPs 4.1M=19.8%). Phase 2x3 QUALITY: joined website_crawl_verticals (1,415,814 classified domains) giving 47,069 unique classified domains. Score-tier check joined 5x5 IPs to delivered household_score in cost_impression_log (the cheap realized-score source; full scoring universe household_scoring.prospecting_intent_daily is 19.4 TB/day, not scanned). 30-day recency: 69.8% of 5x5 pairs sole, 95.4% sole-or-freshest. Domain via NET.REG_DOMAIN(url). Findings map to the Findings/Results sections; the flat-fee amount and causal ablation remain not done.

**Tables:** site_visit_signal, fpa_vendor_log, tpa.direct_data_partners, website_crawl_verticals, cost_impression_log, household_scoring.prospecting_intent_daily, zzz_temp.site_visit_signal, agg__daily_sum_by_campaign

**Learned:**
- 5x5 (DS25) value to MNTN Matched is domain-to-vertical coverage (unique domains), NOT reach or URL-level; 73.8% of its IPs already seen internally and only 3.8% of its URLs carry a path (domain-only confirmed, but moot because the vertical classifier strips to domain).
- 5x5 is outsized ~3.4-3.85x its 3.6% data scale, #2 unique MM-domain contributor (68.5% unique) behind Predactiv (both flat-fee); the per-use $0.50-CPM DDPs (33Across API 3.2%, Sovrn 1.6%, Cybba 5.7% unique) are largely redundant and the real cost-review targets.
- IP household-score tier mix is ~uniform across high-volume vendors (household score is a property of the household, not the vendor) — vendor differentiation is unique domains, not IP quality.
- Recommendation KEEP is not fully closed: the flat-fee $ amount was never obtained from billing (BAE-2358 billing-usage-reporting ticket was Canceled 2025-01-15), and as of 2026-07-09 5x5 was still delivering ~9 days past the end-of-June contract end with no recorded renewal decision.
- household_scoring.prospecting_intent_daily (the full all-IP scoring universe) is ~19.4 TB/day to scan; cost_impression_log's delivered household_score is the cheap realized-score substitute.

**Reuse when:**
- evaluating any 3P/DDP data vendor for renew/drop/renegotiate or willingness-to-pay
- questions about 5x5 / DS25 or which vendors feed site_visit_signal and MNTN Matched
- assessing vendor uniqueness/redundancy or the $0.50-CPM DDP cost-review (AUDI-1051)
- estimating the value of MNTN Matched / Fangorn or a vendor's attributable slice
- needing per-vendor scale, domain overlap, or IP score-tier methodology over site_visit_signal

---

# TI-1027: [SPIKE] 5x5 Data Evaluation

**Jira:** https://mntn.atlassian.net/browse/TI-1027
**Status:** In Progress — analysis complete, recommendation KEEP; pending flat-fee from billing for final sign-off
**Date Started:** 2026-06-16
**Date Completed:** —
**Assignee:** Malachi

---

## 1. Introduction
Our contract with the data provider **5x5 (DS 25)** expires **end of June 2026**. Leadership (Kale McNaney; ticket by
Alyson Lefkowitz) wants a **renew / drop / renegotiate decision** backed by an **estimate of value**. 5x5 is a DDP
(direct data partner) that delivers **IP → URL** site-visit signals feeding **MNTN Matched** (vertical classification
→ feature store → MM scoring).

**Kale's framing (the spine):** *"Quantify/estimate just the measurable bits to start. 5x5 accounts for a certain %
of raw data — is its impact on Fangorn/MNTN Matched **outsized relative to its scale, or in line**? How do we estimate
the value of Fangorn/MNTN Matched? It's an estimation exercise."* → Headline metric = **leverage ratio**
(contribution_share ÷ data_share); dollars come from valuing MM, then attributing 5x5's slice.

**Sean Yang (pipeline owner) decision bar + action loop:** *"5x5 is just one of several sources into
site_visit_signal. If their data is unique with minimal overlap with other vendors we should keep them… let me know
either way so I will adjust the DAG."* → Our rec drives Sean toggling `25` in/out of `ENABLED_DSIDS`.

## 2. The Problem
- Is 5x5's flat-fee data worth renewing? What breaks if we drop it?
- Ticket observations to verify: (a) "only sending domain, not extended URL — can't tell what person is looking at";
  (b) "some data looks like quality websites, rest are total garbage."
- Hard deadline: contract ends end of June 2026.

## 3. Plan of Action
Per approved plan (`~/.claude/plans/read-ti-1027-…md`), Kale's estimation chain:
0. **Phase 0** — clear blockers (cost structure/amount; delivery recency) + confirm lineage.
1. **Phase 1 SCALE** — each vendor's share of site_visit_signal (records / IPs / domains) → 5x5 `data_share`.
2. **Phase 2 QUALITY** — ecommerce/vertical classifier on 5x5 domains → % whitelist vs garbage, % classifiable.
3. **Phase 3 CONTRIBUTION vs SCALE** — overlap DS25 vs internal DS30∪DS23 + other DDPs; net-new; **leverage ratio**.
4. **Phase 4 VALUE OF MM** — MM-touched revenue + Fangorn lift band (the denominator).
5. **Phase 5 SYNTHESIS** — attribute 5x5 slice vs flat fee + break-even; verticals impacted; keep/drop/renegotiate;
   notify Sean.

## 4. Investigation & Findings

### 4.1 Confirmed lineage (from `SteelHouse/airflow-ti`)
**Raw feed → processing → unified signal → MM:**
- **Raw 5x5 feed:** `gs://mntn-data-partners/partners/5x5/ip_to_url/y=YYYY/m=MM/d=DD/h=HH/*.snappy.parquet`.
  Cols `_COL_0`=ip, `_COL_1`=url, `_COL_2`=epoch(sec). Delivered in **~2-hour batches** (not hourly).
- **Processing:** `spark/fpa/dsid25_5x5_processing.py`, DAG `fpa_site_visit_batch_serverless` (`@hourly`, Dataproc
  serverless, **5-hour lag** for DS25). `ENABLED_DSIDS = [23, 25, 26, 28, 30, 36]`.
- **Stage 1 (raw archive):** `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=25/` (partitioned dt,hh).
- **Stage 2 (unified):** `gs://mntn-data-archive-prod/signals/site_visit_signal/dt=…/hh=…/data_source_id=25/`.
  Shared schema across all vendors: `uid, advertiser_id, ip, url, query_parameters, user_agent, time, data_source_id,
  dt, hh`. **Separable by `data_source_id` — trivial.**
- **Consumers:** `distinct_site_visit_signal_domains.py` (31-day read; regex-strips url to `protocol+domain`;
  **excludes DS23**, includes DS25) → `ddp_vertical_classification_api` / `update_website_verticals` →
  feature store `site_visit_signal_advertiser_id_dsc_id` → `mntn_match_incrementals_submit` (MNTN Matched).
- **BQ landing caveat:** `…zzz_temp.site_visit_signal` is **manual / not auto-populated** (populate trigger commented
  out). Query GCS parquet directly via temp external-table definitions (read-only; no DDL/DML).

### 4.2 site_visit_signal vendor set (who feeds MM via this table)
- **Internal:** DS23 guid_log (MNTN pixel; *excluded from vertical classification*), DS30 augmentor_log (bidstream).
- **External DDP:** DS24 Justuno, **DS25 5x5**, DS26 Predactiv, DS28 33Across, DS33 Sovrn, DS36 Cybba, DS39 Klickly,
  DS40 33Across API. (Observed in partitions; 24/33/39/40 arrive via pixel_page_view_signal backfill workflows.)

### 4.3 Cost structure (`dw-main-silver.tpa.direct_data_partners`, is_current=true) — Phase 0 blocker #1
- **5x5 (DS25): `billing_type = flat_fee`, `fixed_cpm = null`, `used_in_mntn_match = true`, `used_in_interests =
  false`, enabled.** Confirms Alyson: **flat fee** (fixed cost, marginal cost zero), feeds MNTN Matched only.
  Note field: *"we provide report but only impression counts- unknown if this was shared with the customer."*
  Current contract row `valid_from = 2025-10-17`.
- **Peer-rate benchmark for MM DDP data = $0.50 CPM:** 33Across (28), 33Across API (40), Cybba (36), Sovrn (33),
  Justuno (24), and the **disabled** LaunchLabs (27) all bill `fixed_cpm = $0.50`. Predactiv (26) + Klickly (39) are
  also `flat_fee`. → I can build a **break-even** before Sherwin's number: *what would 5x5 cost at $0.50 CPM for the
  impressions its signal drives?*
- **Cost AMOUNT still needed** — flat fee $ not in the table. **→ Ask Sherwin (billing).** Interest-side providers
  (LiveRamp 11/35 variable_cpm, ShareThis 17 $0.95→ was $1.20, Dstillery 18) are not MM and out of scope.

### 4.4 Delivery is live (answers Sean's "are they still dropping data?")
- Raw feed delivering **through today, 2026-06-16** (daily, ~2-hr batches). DS25 site_visit_signal partitions present
  on complete days (e.g. 2026-06-15 hh=00, hh=10). Gaps in some hours = the 2-hr batch cadence + 5-h processing lag,
  not a failure. **Off-switch if we drop:** remove `25` from `ENABLED_DSIDS` (Sean owns).

### 4.5 Raw-feed content — both ticket claims need nuance (sample, 2026-06-15 h=00)
First 6 rows showed:
- **Full URLs WITH paths exist** — e.g. `https://screenrant.com/walking-dead-streets-survival-new-game-release/`. So
  "they only send domain" is **not strictly true**; quantify % with path. (Even so, the vertical classifier strips to
  domain anyway — so path richness is moot for the MM path unless a URL-level consumer exists.)
- **Garbage is real** — `66.249.77.195` = **Googlebot** crawler IP; `widgets.outbrain.com` = ad widget;
  `analytics.o11.tech` = analytics tracker. Bot/infra/tracker noise present → Phase 2 must quantify the garbage share.

### 4.6 Data volumes (windowing decision)
- Full `site_visit_signal` ≈ **250 GiB/day** (dominated by internal DS23/DS30). Raw 5x5 ≈ **1.48 GiB/day**.
- Approach: BQ temp external tables reading only `ip`/`url`/`data_source_id`, scoped windows. Queries run ~16–80s
  per scan on reserved capacity. Domain registered via `NET.REG_DOMAIN(url)` (matches the consumer's tldextract
  eTLD+1). `website_crawl_verticals` = the production domain→vertical table (1,415,814 classified domains).

### 4.7 PHASE 1 — SCALE (one day, 2026-06-15; `outputs/ti_1027_scale_per_ds_2026-06-15.csv`)
Per-vendor share of `site_visit_signal` (2.57B rows total that day):
| DS | Partner | rows | distinct IPs | distinct domains | % URLs w/ path |
|---|---|---:|---:|---:|---:|
| 28 | 33Across | 834M | 67.5M | 159K | 67.5% |
| 30 | augmentor (internal) | 797M | 47.7M | 94K | 74.8% |
| 40 | 33Across API | 373M | 35.8M | 110K | 25.6% |
| 23 | guid_log (internal) | 305M | 32.9M | 12K | 72.6% |
| **25** | **5x5** | **93M** | **20.8M** | **93K** | **3.8%** |
| 26 | Predactiv | 84M | 19.0M | 228K | 73.7% |
| 33 | Sovrn | 58M | 7.9M | 32K | 89.7% |
| 24 | Justuno | 19M | 4.1M | 6K | 90.7% |
| 39 | Klickly | 4.8M | 1.1M | 0.2K | 100% |
| 36 | Cybba | 1.8M | 0.55M | 5K | 81.9% |

- **5x5 `data_share` ≈ 3.6% of rows** (the leverage-ratio denominator). But ~**20.8M IPs/day** and **93K domains/day**
  → mid-pack scale, sizable. Low frequency (~4.5 rows/IP vs 33Across 12.4) — many IPs seen once.
- **"Domain-only" claim CONFIRMED:** only **3.8% of 5x5 URLs carry a path** vs 67–100% for every other vendor.
  5x5 is the uniquely domain-level feed. (Moot for vertical classification — the consumer strips to domain anyway.)

### 4.8 PHASE 3 — CONTRIBUTION vs SCALE (7-day window 2026-06-09→15; `ti_1027_domain_overlap_7d.csv`)
Distinct-domain universe = 997,963. 5x5 touches 202,299 (20.3%).
- **5x5-unique domains (provided by NO other vendor, internal or external): 138,496 = 68.5% of 5x5's domains**,
  = **13.88% of the entire domain universe.** Also-in-internal(23/30): 45,537; also-in-other-DDP: 57,285.
- **Leverage ratio (raw unique domains) = 13.9% / 3.6% ≈ 3.85× → OUTSIZED.**

### 4.9 PHASE 2×3 — QUALITY-FILTERED contribution (`website_crawl_verticals` join)
- 5x5 domains classified to a vertical: **45.0%** (vs 38.8% universe avg — 5x5 is *more* classifiable than average).
- 5x5 **unique** domains classified: **34.0% → 47,069 unique MM-usable domains** 5x5 alone provides.
- 47,069 / (38.8%×997,963 ≈ 387K classified universe) ≈ **12.2%** of the classified-domain universe.
- **Leverage ratio (unique *classified* domains) ≈ 12.2% / 3.6% ≈ 3.4× → still OUTSIZED after quality-filtering.**

### 4.10 PHASE 3 — IP reach (1 day; `ti_1027_ip_overlap_1d.csv`)
- IP universe 85.3M. 5x5 = 20.9M IPs (24.5% touched). **5x5-only IPs = 4.1M (19.8%); 73.8% of 5x5's IPs are
  ALREADY seen by internal DS23/30.** → **5x5's value is NOT incremental reach** (we see most IPs ourselves); it is
  **incremental domains** — 5x5 observes *different sites* (long-tail our bidstream doesn't bid on) for known IPs.

### 4.11 PHASE 4 (vertical impact) — what gets hurt if 5x5 is dropped (`ti_1027_vertical_dependence_7d.csv`)
% of each vertical's classified domains that are 5x5-unique (7d). **Overwhelmingly B2B:**
B2B-Hiring 34%, B2B-Logistics 32%, B2B-Data&Analytics 31%, B2B-Workflow 30%, B2B-Sales&Marketing 30% (9,640 domains),
B2B-Healthcare 30%, B2B-IT&Engineering 25% (11,762 domains) … plus Apparel-Luxury 27%, Industrial Equipment 27%,
Jewelry 26%, Footwear 26%, Eyewear 24%, Medical Devices 23%, Furniture 22%, Auto Dealers 20%.
- **Significance (framing correction, Malachi 2026-06-17):** these are **our customers' B2B-audience targeting
  verticals** — losing 5x5 most degrades targeting for advertisers running B2B campaigns. This is NOT the same as
  MNTN's own go-to-market goal of "reaching out to mid-market B2B" (acquiring B2B advertisers); the vertical taxonomy
  is not a proxy for MNTN's customer-acquisition target. (Earlier drafts conflated the two.)

### 4.12 PHASE 3 — VENDOR COMPARISON (answers "how does 5x5 compare to other DDPs?"; 7d; `ti_1027_vendor_uniqueness_comparison_7d.csv`)
Unique **classified** domains each vendor alone contributes to MM (the MM-usable net-new signal):
| DS | Partner | billing | unique classified domains | % unique |
|---|---|---|---:|---:|
| 26 | Predactiv | flat_fee | **164,627** | 60.1% |
| **25** | **5x5** | **flat_fee** | **47,069** | **68.5%** |
| 30 | augmentor (internal) | — | 33,137 | 64.2% |
| 28 | 33Across | $0.50 CPM | 9,277 | 30.1% |
| 24 | Justuno | $0.50 CPM | 4,823 | 84.3% |
| 40 | 33Across API | $0.50 CPM | 2,802 | **3.2%** |
| 36 | Cybba | $0.50 CPM | 309 | 5.7% |
| 33 | Sovrn | $0.50 CPM | 293 | 1.6% |

- **5x5 is the #2 unique contributor and the most-unique high-volume vendor (68.5%).** Predactiv (also flat_fee) is #1.
- **The $0.50-CPM per-use vendors are largely REDUNDANT:** 33Across API 3.2% unique, Sovrn 1.6%, Cybba 5.7%,
  33Across 30.1%. They add little unique MM signal yet bill per impression. → **They, not 5x5, are the cost-review
  targets** (directly echoes TI-647's 33Across-is-replaceable finding). Bonus follow-up ticket candidate.

### 4.13 PHASE 3b — Score-tier quality of each vendor's IPs ("scored ≠ high-value") → `ti_1027_vendor_score_tiers_7d.csv`, chart `ti_1027_chart_score_tiers.png`
Joined each vendor's site-visit IPs → **delivered MM `household_score`** (`cost_impression_log`, 7d; the cheap
realized-score source — full scoring universe `household_scoring.prospecting_intent_daily` is 19.4 TB/day, not
scanned). Tier mix of **delivered** IPs (those MNTN served an impression to), by household_score:

| Vendor | % of IPs delivered | HI (10000) | PP (8000) | Mid | Max Reach | Unscored |
|---|---:|---:|---:|---:|---:|---:|
| **5x5** | **20.9%** | **39.4%** | 12.9% | 4.6% | 3.0% | 36.8% |
| Predactiv | 29.3% | 39.7% | 11.9% | 4.4% | 3.0% | 37.9% |
| augmentor (internal) | 28.8% | 35.2% | 14.1% | 4.8% | 3.3% | 39.5% |
| 33Across | 23.2% | 35.4% | 14.0% | 4.8% | 3.3% | 39.5% |
| guid_log (internal) | 17.3% | 38.5% | 13.3% | 5.0% | 3.0% | 36.5% |
| Justuno/Klickly/Cybba (small) | 21–39% | 52–57% | 7–9% | ~3% | ~2% | 28–30% |

- **The tier mix is ~uniform across the high-volume vendors** (35–40% HI, ~12–14% PP, ~37–40% unscored). The
  household score is a **property of the household, not the vendor** — and the vendors largely observe the same
  households (recall 73.8% of 5x5 IPs are also internal). So no big vendor brings "garbage" households.
- **5x5's IPs are as high-value as any vendor's — in fact 39.4% land in top-tier High Intent, the highest among the
  high-volume sources** (tied with Predactiv; above internal augmentor's 35% and 33Across's 35%). Only ~3% Max Reach.
- The small vendors (Justuno/Klickly/Cybba) skew higher HI (52–57%) but on tiny IP volumes.
- **Implication:** confirms 5x5 is not bringing low-value households, and **re-confirms its differentiation is the
  unique DOMAINS, not IP quality** (which is ~uniform across vendors). Strengthens KEEP.
- **Caveat:** uses DELIVERED scores (IPs we bid on, post-HHST). "% delivered" mixes scoring with inventory/delivery;
  the full all-IP scored universe (19.4 TB/day) was out of scope for cost.

### 4.14 PHASE 2 — Full data valuation + willingness-to-pay (Kale's "full report") → `artifacts/ti_1027_data_valuation_report.md`
Expanded from "MM impact" to a reusable data-vendor valuation + WTP framework. New findings:
- **What's in the data:** 5x5 = positional `_COL_0/1/2` (ip/url/time), **no metadata, unknown schema** — thinnest +
  highest-schema-risk of 10 vendors. **Discard finding:** pixel vendors (24/33/39/40) send `query_str/referer/mobile/
  event_id` and Predactiv sends `user_agent` — **all dropped** at site_visit_signal ("pay for rich, keep thin").
  (`outputs/ti_1027_vendor_richness.csv`.)
- **How much (5x5/day):** 1.48 GiB, 93.3M events, 20.8M IPs, 92.7K domains, **33.1M (IP×domain) pairs**, 35.1M
  (IP×url) pairs (≈ equal → domain-only). (`ti_1027_cardinality_2026-06-15.csv`.)
- **Layered uniqueness (the reframe):** unique IP **19.8%** → unique domain **68.5%** → **unique (IP×domain) event
  77.3%** (25.6M/day). The unique *data value* >> unique *reach*. No unique metadata. (`ti_1027_layered_uniqueness_5x5.csv`.)
- **WTP anchor (CIL × svs):** 5x5 touches **34.35M impr/day** (80.6% High-Intent); impr to 5x5-**unique** IPs =
  213.5K/day. (`ti_1027_wtp_anchor_5x5.csv`.)
- **Willingness to pay:** **floor ≈ $40K/yr** (unique-reach: 77.9M impr/yr × $0.50 CPM); **fair ≈ $150K–$600K/yr**
  (12% of MM unique-domain signal, B2B-weighted; typical DDP flat-fee range); **walk-away ≈ $6.3M/yr** (CPM-equivalent
  of all 12.5B/yr touched impressions). Per-unit: net-new IP ~$0.01–0.50/yr (reach is NOT the value); net-new
  (IP×domain) event ~$0.03/1k; net-new classified domain ~$3–13/yr.
- **Tie-break rubric:** cost → non-redundancy (unique events) → richness → freshness → latency → schema stability.
- **Recommendation:** renew ≤ ~$600K/yr; renegotiate $600K–$6.3M (demand full URLs); walk only near $6.3M.

### 4.15 PHASE 3 — Spend comparison, 30-day recency, definitive pricing (Kale follow-ups)
- **Per-vendor touched spend (2026-06-15; `ti_1027_vendor_spend_comparison_2026-06-15.csv`):** 5x5 ranks **5th of 10**
  ($353K media/day) — and the top 6 vendors all cluster at $264–404K/day because they overlap on the same households.
  **Touched spend can't differentiate vendors** (all swim in the same pool); only unique contribution + recency can.
  (Heavy IP overlap → these don't sum to total; relative ranking only.)
- **30-day recency (`ti_1027_recency_30d_5x5.csv`) — answers "don't other vendors already have it?":** targeting uses
  the last 30 days (`site_visit_signal` has **no TTL**; data to 2025-08-31). Over the 30-day window, of 5x5's 754.8M
  (IP×domain) pairs, **69.8% are SOLE** (no other vendor in-window) and **95.4% sole-or-freshest** — only ~4.6%
  covered fresher elsewhere. The 7-day snapshot (23% overlap) **overstated redundancy**; irregular cadences mean
  other vendors' copies expire. This raises the floor.
- **Definitive pricing (two structures, per Kale "monthly rates or CPMs"):**
  - **Monthly (recommended):** floor ~$3K/mo · **fair $15–50K/mo** (anchor ~$25–30K) · walk-away ~$525K/mo. Attach a
    **volume minimum** (≥2.5B rows/mo AND ≥25M unique IP×domain pairs/day).
  - **CPM:** ≤$0.50 on **matched** impressions (peer parity); $0.02–0.05 on **all touched** (~95% redundant);
    >$0.10 on touched = walk away. **Reconciliation:** $25K/mo ≈ $0.024 CPM (all touched) ≈ $0.50 CPM (matched).
- Deck updated (spend / recency / pricing slides + charts); `data_vendor_valuation_framework.md` updated with the
  recency step.

### 4.16 PHASE 4 — Raw numbers & per-IP depth (volume ≠ value) → `ti_1027_per_ip_depth.csv`
Per-IP depth = how many distinct sites a vendor sees each IP visit, and how many are unique to it. Key reads:
- **Raw volume is misleading.** 33Across is the **biggest** feed (834M events/day) but among the **shallowest** in
  unique depth: **0.65 unique domains/IP, only 27% of its pairs unique** — repeat-visits to common domains everyone
  has. Sovrn worse (0.10). Internal **augmentor is deepest (2.98 unique dom/IP)**.
- **5x5:** thin per IP (1.6 domains/IP, 1.23 unique) but 77% of its pairs unique — value is unique-domain *breadth*,
  not per-household *depth*.
- **Two value lenses, different rankings:** (a) **domain→vertical breadth** (what the MM classifier consumes) →
  **5x5 leads** on unique domains; (b) **per-IP behavioral depth** (unique pairs/IP) → augmentor > 33Across API >
  guid > 5x5. 33Across **API** is a case study: decent per-IP depth (1.87) but only 3.2% unique *domains* (different
  IPs on *common* domains) — redundant for classification, useful for per-IP features.
- Deck: added a **raw-numbers table slide** + **depth scatter** (events vs unique-domains/IP). Workbook: **Per-IP
  Depth** tab.
- **Per-IP distribution (not mean):** median additional unique domains 5x5 adds = **+1**, p90 +2; **85% of its
  households get ≥1 net-new domain** but broad-and-shallow (71% get exactly +1). (`ti_1027_perip_additional_domains_dist.csv`.)
- **Additive vs redundant (the key value test):** **76% of all 447M (IP×domain) pairs come from ONE vendor**; per
  shared IP, all vendors combined give ~70% more domains than the best single (5-vendor IP: 11.1 vs 6.7 = 1.65×,
  ~29% overlap). Vendors are **additive, not redundant** — that's the value of running several.
  (`ti_1027_pair_multiplicity.csv`, `ti_1027_perip_additivity.csv`.) Over-estimation guard: value = distinct
  (IP,domain) on the UNION, never raw events (~2.8× inflated) or sum (double-counts overlap).

### 4.17 Untapped upside + EXEC deck (Alyson ask, 2026-06-17)
- **Untapped data:** 5x5's raw feed is **only ip/url/timestamp** (positional `_COL_0/1/2`, no device/UA, 96% bare
  domains) — so no hidden metadata to tap; the lever is **renewal** (ask for extended URLs + device/UA). Separately,
  the **pixel feeds + Predactiv already send UA/referer/query/mobile that we drop** at site_visit_signal — untapped at
  no vendor cost (pipeline lever). Added a slide to the big deck + report §5.
- **Exec deck** (`artifacts/ti_1027_exec_deck.html`, 5 content slides): bottom line → why (net-new-vs-free + recency)
  → what to pay (pricing) → untapped upside + next steps. Pared down per Alyson for Kale.

### 4.18 Raw-data audit map (Malachi/Sean ask, 2026-06-17) → `artifacts/ti_1027_raw_data_audit.md`
Mapped every raw dump pre-`site_visit_signal`. Roots: batch `gs://mntn-data-partners/partners/<vendor>/` (10 vendors),
streaming `gs://mntn-data-archive-prod/pixel_page_view_signal/` (rawer JSON in `gs://mntn-analytics-raw/topics/...`),
internal `guid_log`/`augmentor_log`. **Big finding:** raw dumps carry far more than the 4 cols we keep —
**Predactiv 26 cols (drops hashed emails, full geo, domain_industries/firmographics, concepts/keywords), 33Across 32
cols (drops page categories+keywords, geo, device hints, GPP consent)**; pixel feeds drop event_id/mobile/referer/
query_str(+GPP). 5x5 & Cybba genuinely thin (ip/url/time). → most "more value" is a PIPELINE change, not vendor cost;
+ compliance flag (consent fields dropped). Site-visit feeds: 5x5/33across/cybba/predactiv (batch) +
Justuno/Sovrn/Klickly/33Across-API (streaming). Non-site-visit: liveramp/sharethis (interests), experian/deepsync
(CRM/identity), bombora (B2B intent), alliant (transactions).

## 5. PHASE 4 — Value of MNTN Matched (the denominator) + PHASE 5 — 5x5 attribution & recommendation

### 5.1 How to estimate the value of MM/Fangorn (Kale's question)
Value(MM) ≈ **incremental advertiser performance MM targeting produces vs a no-MM baseline, monetized via retention.**
Measurable inputs:
- **MM-touched media:** ~$17.5M media / $23.7M platform spend / **2.13B impressions per 30d** (April 2026,
  `agg__daily_sum_by_campaign`; ~all prospecting is MM-targeted). DS-catalog prospecting anchor: ~$32.1M/30d. →
  order **$210–385M/yr** of media MM targeting touches.
- **MM/Fangorn IVR lift (measured, EX50):** HI ≈11.6%, MI ≈9.8%, MI+PP ≈11.2%, PP ≈36%; OKR target ≈10% VR lift.
- **Interpretation (CPM-priced model):** MM doesn't change spend directly; it drives the **IVR/performance that
  retains advertisers**. So Value(MM) ≈ the share of the ~$210–385M/yr book whose renewal depends on MM-driven
  performance. Even conservatively that is **tens of $M/yr**.

### 5.2 5x5's attributable slice
- 5x5 uniquely supplies **~12% of MM-usable (classified) domain signal** (47K of ~387K classified domains; ~23%
  counting overlap), **B2B-weighted far higher (25–34% of B2B-vertical domain coverage)**.
- Honest bound: 5x5's value is **not** 12% of all MM revenue (the head/high-traffic domains survive without it). It
  is the **marginal degradation in the (customer-targeting) verticals where 5x5 dominates — B2B-audience verticals +
  premium retail + industrial/medical**: those verticals lose 20–34% of their fresh domain→vertical coverage if 5x5
  is cut (a hit to advertisers running those campaigns — not MNTN's own B2B customer-acquisition goal).
- Value is **domain-vertical coverage**, NOT reach (73.8% of 5x5 IPs already seen internally) and NOT URL-level
  (uniquely domain-only) — only the domain→vertical path benefits.

### 5.3 Break-even & recommendation
- **Cost:** flat fee $F/yr (pending Sherwin). Marginal cost = 0 (fixed). Peer MM-DDP rate = $0.50 CPM (per-use).
- **Break-even:** keep 5x5 iff $F < value of its 47K unique, B2B-heavy, MM-classifiable domains. Given MM value is
  tens of $M/yr and 5x5 contributes ~12% of the unique domain signal (B2B-concentrated), the threshold is high —
  5x5 is worth a **typical DDP flat fee (tens-to-low-hundreds of $K/yr)** with comfortable margin. It would take an
  unusually large fee (≳ low-$M/yr) to fail break-even.
- **RECOMMENDATION → KEEP (renew).** 5x5 passes Sean's bar decisively (68.5% unique, minimal overlap), is **outsized
  ~3.4–3.85×** vs its 3.6% data scale, is the #2 unique MM-domain contributor, and is **concentrated in B2B-audience
  verticals** (our customers' B2B-campaign targeting). Confirm the fee with Sherwin to finalize; if the fee is surprisingly large,
  fall back to **renegotiate** (demand URL paths to fix the domain-only gap, or lower fee).
- **Bonus:** review the redundant $0.50-CPM DDPs (33Across API, Sovrn, Cybba) for savings — far weaker than 5x5.
- **Action loop:** deliver to Kale/Alyson → notify **Sean** to keep `25` in `ENABLED_DSIDS` (no DAG change needed).

### 5.4 Provider scorecard (extension — rate all MM site-visit DDPs) → `artifacts/ti_1027_vendor_scorecard.md`
Rated all 8 external MM site-visit DDPs (+2 internal) on net value (unique classified domains) × non-redundancy ×
signal quality, with cost structure. Composite score + verdict per provider (`outputs/ti_1027_vendor_scorecard.csv`,
chart `ti_1027_chart_scorecard.png`):
| Provider | Cost | Unique MM domains | Score | Verdict |
|---|---|---:|---:|---|
| Predactiv | flat | 164,627 | 80 | KEEP |
| **5x5** | **flat** | **47,069** | **72** | **KEEP** |
| Justuno | $0.50 CPM | 4,823 | 64 | KEEP (efficient) |
| 33Across | $0.50 CPM | 9,277 | 46 | REVIEW (high CPM volume, 30% unique) |
| Klickly | flat | 132 | 36 | REVIEW (negligible) |
| 33Across API | $0.50 CPM | 2,802 | 32 | DROP-CANDIDATE (3% unique) |
| Cybba | $0.50 CPM | 309 | 22 | REVIEW (6% unique) |
| Sovrn | $0.50 CPM | 293 | 12 | DROP-CANDIDATE (2% unique) |
- **Takeaway:** flat-fee feeds (Predactiv, 5x5) are the best value; the per-use $0.50-CPM vendors are where the waste
  is (33Across API + Sovrn ≈ fully redundant). Cost metric used: billing_type + CPM rate (no absolute $ needed).
- Interest-segment 3P (LiveRamp/ShareThis/Dstillery) are a different modality → rated by TI-956/TI-999, not here.

## 6. Questions Answered (the ticket's questions)
- **Q: How does 5x5 impact MNTN Matched — accuracy, reach?** **A:** It impacts MM via **domain→vertical coverage**,
  not reach. 68.5% of its domains are unique; 47K unique domains classify to a vertical (~12% of the MM-usable
  domain universe). Reach impact is small (73.8% of its IPs already seen internally).
- **Q: If we lose 5x5, how are users affected?** **A:** MM loses ~12% of fresh classified-domain signal, concentrated
  in **B2B** verticals (20–34% of their domain coverage) + premium retail + industrial/medical. B2B targeting
  precision degrades most.
- **Q: Verticals heavily impacted?** **A:** B2B (Hiring, Logistics, Data&Analytics, Workflow, Sales&Marketing,
  IT&Engineering), Apparel-Luxury, Jewelry, Footwear, Industrial Equipment, Medical Devices, Auto Dealers.
- **Q: How does it compare to other DDP providers?** **A:** #2 unique contributor (behind Predactiv, also flat-fee);
  most-unique high-volume vendor (68.5%). The $0.50-CPM vendors (33Across API/Sovrn/Cybba) are largely redundant.
- **Q: Dollar value / is it worth paying for?** **A:** Outsized ~3.4× its data scale; supplies ~12% of MM's domain
  signal (B2B-weighted higher). MM is worth tens of $M/yr → 5x5's slice clears a typical DDP flat fee with margin.
  **Recommend KEEP**; finalize once billing provides the fee.
- **Q: Domain-only, not extended URL?** **A:** **Confirmed** — 3.8% of 5x5 URLs carry a path vs 67–100% elsewhere.
  But **moot for MM** (the vertical classifier strips every URL to domain regardless).

## 7. Data Documentation Updates
Added to `knowledge/data_knowledge.md`: full site-visit-signal lineage (raw vendor drops → `fpa_vendor_log` +
`site_visit_signal` → vertical classification → `website_crawl_verticals` → feature store → MNTN Matched), the
`zzz_temp.site_visit_signal` manual-BQ caveat, the vertical-classifier domain-strip behavior, and the
`tpa.direct_data_partners` registry (billing_type/used_in_mntn_match, MM-DDP roster, $0.50 peer CPM). Fixed stale
"no current use" labels in `knowledge/ds_catalog.md` for DS24/25/26/28 (active MM site-visit DDPs, not IPDSC).

## 8. Open Items / Follow-ups

### Status check 2026-07-09 (Paulo's renewal-season ask)
Paulo (Slack, 2026-07-09) asked for the value of the data sources feeding Matched (Klickly, 5x5, etc.), whether we
still use last year's sources, and keep/drop guidance — contract renewal season. Re-verified current state:
- **All 8 external MM DDPs still enabled and delivering** (site_visit_signal partitions dt=2026-07-07 and 07-08
  carry DS 23,24,25,26,28,30,33,36,39,40 — identical roster to the June analysis; LaunchLabs 27 still disabled).
- **5x5 is still dropping data through today (2026-07-09, d=09 h=16)** — ~9 days PAST the end-of-June contract
  end. No renewal decision is recorded in Jira (last comment 06-23: "being reviewed by Kale"). **Open: confirm
  whether the 5x5 renewal was actually signed, or data should have stopped.**
- **Flat-fee $ amounts still never obtained from billing** (any vendor). Related: BAE-2358 "5x5 Billing Usage
  Reporting Setup" was Canceled 2025-01-15 — likely why no fee/usage reporting exists.
- **AUDI-1051** (review redundant $0.50-CPM DDPs: 33Across API 3.2% / Sovrn 1.6% / Cybba 5.7% unique) still
  **Backlog** — renewal season is the trigger to pull it forward.
- Deck share links created (public gists via share_deck.sh):
  exec: https://gist.githack.com/mdunn-mntn/a63336f861d55f52534b8307efcb3b3c/raw/ti_1027_exec_deck.html
  full: https://gist.githack.com/mdunn-mntn/5e058531bd357e81c02f7d6c840ac7ec/raw/ti_1027_presentation_deck.html
- Registry gotcha found while verifying: `tpa.direct_data_partners` has CDC duplicate rows even at
  `is_current=true`, and DS26 has 4 conflicting current rows (broken SCD — take latest `valid_from`). Documented
  in `knowledge/data_knowledge.md`.
- **Blocker:** 5x5 flat-fee amount ← billing. **Draft ask:** *"Quick one for the 5x5 (DS25) renewal eval — what's
  our current flat-fee with 5x5 (annual or monthly)? Evaluating renewal value before the end-of-June contract end.
  Also helpful: the fees for the other MM data partners (Predactiv, Cybba, Sovrn, 33Across/33Across API, Justuno,
  Klickly) for comparison."*
- **Deferred (Kale):** full causal ablation (re-run MM/vertical classification with vs without DS25 → ΔIVR →
  ΔRevenue) — only if leadership wants to tighten the estimate beyond the measurable read.
- **Follow-up ticket candidate:** review the redundant $0.50-CPM DDPs (33Across API 3.2% unique, Sovrn 1.6%, Cybba
  5.7%) for cost savings — echoes TI-647 (33Across replaceable).
- **Action loop:** deliver readout → confirm decision → notify Sean (keep `25` in `ENABLED_DSIDS`; no DAG change if KEEP).
- **Note:** TI-647 method ask to Ryan Kleck no longer needed — the unified `site_visit_signal` table let me compute
  overlap directly (5x5 vs internal vs all DDPs), superseding the per-vendor match-rate approach.
