# Feature Importance Findings — What Predicts Visits?

**TI-790** | Malachi Dunn | 2026-03-31
**For:** Alex Knorr, Ryan Kleck, Matt Brorby

---

## How This Was Done

We trained an XGBoost classifier on **117,238 real IPs** from 2026-03-29 to predict which IPs would visit an advertiser site (visited = 1) vs not (visited = 0). The training data joined 6 tables on IP:

| Table | What It Provides | IPs with Data |
|-------|-----------------|---------------|
| win_logs | Video engagement, viewability, device details, pricing | 100% (base population) |
| cost_impression_log | Scoring, recency, cost breakdown, ad format | 100% |
| augmentor_log | IAB categories, SSP diversity, segment density | 42% |
| bidder_auction_events | Content genre, device make, publisher | 26% |
| guid_log | Browser/device from pixel, product views, UTM | 30% |
| conversion_log | Order amount, conversion type, identity signals | 8% |

**Base visit rate:** 3.4% (3,985 visitors out of 117,238 impressed IPs)

**Key methodological choice:** We split features into **pre-visit** (available before any site visit — usable for targeting) and **feedback** (available only after a visit — usable for retraining/enrichment). guid_log and conversion_log features are feedback because they only exist when an IP has already interacted with an advertiser site.

- Pre-visit model AUC: **0.896**
- Feedback model AUC: 0.999 (outcome-adjacent — expected to be near-perfect)

All results below are from the **pre-visit model** — the one that matters for targeting.

---

## The Top 10 Features That Predict Visits

Ranked by SHAP value (mean absolute contribution to each prediction). These are **empirical results from the model**, not hypotheses.

### #1: Average MNTN Segments (`al_avg_segments`)
- **Source:** augmentor_log
- **SHAP:** 0.986 (highest of all pre-visit features)
- **What it is:** The average number of MNTN segments already assigned to this IP across bid requests
- **What the model learned:** IPs with more existing segments are significantly more likely to visit. This makes sense — segment density reflects how well-known this IP is to the targeting system. An IP with 15 segments has been seen, profiled, and classified many times. An IP with 0 segments is unknown.
- **Implication:** Segment density is the single strongest signal we have before a visit. The current Fangorn pipeline is already capturing this implicitly — but explicitly tracking it as a feature could improve cold-start IPs.

### #2: Percent New Impressions (`ci_pct_new`)
- **Source:** cost_impression_log
- **SHAP:** 0.670
- **What it is:** The fraction of impressions where this IP is flagged as "new" (first impression)
- **What the model learned:** IPs that are predominantly "new" visit at lower rates. Returning IPs — those we've served before — are warmer and more likely to convert. This is the recency/frequency signal.
- **Implication:** There's a strong first-impression penalty. IPs we've reached multiple times are better targets. This supports frequency-based optimization.

### #3: Percent RTC Impressions (`ci_pct_rtc`)
- **Source:** cost_impression_log
- **SHAP:** 0.392
- **What it is:** The fraction of impressions that came through Real-Time Conquest (advertiser_household_score = 10000)
- **What the model learned:** IPs receiving RTC-targeted impressions visit more. This validates that RTC is successfully identifying high-intent IPs.
- **Implication:** RTC conquest scoring is working. IPs flagged by RTC are genuinely more likely to visit.

### #4: Total Media Cost (`ci_total_cost`)
- **Source:** cost_impression_log
- **SHAP:** 0.363
- **What it is:** Total dollars spent serving impressions to this IP
- **What the model learned:** More spend → more visits. This is partly mechanical (more impressions = more chances to visit), but also reflects that the bidding system spends more on IPs it values — which are indeed more likely to visit.
- **Implication:** Spend is a proxy for the bidding system's confidence. The existing system is directionally correct in its spend allocation.

### #5: Average Clearing Price (`wl_avg_price`)
- **Source:** win_logs
- **SHAP:** 0.231
- **What it is:** Average price paid per auction won (in USD)
- **What the model learned:** IPs we pay more for are more likely to visit. Premium inventory (higher CPMs) correlates with higher-quality audiences.
- **Implication:** Price is a quality signal. Cheap inventory ≠ good targeting.

### #6: Auction Volume (`al_n_auctions`)
- **Source:** augmentor_log
- **SHAP:** 0.228
- **What it is:** How many auctions this IP appeared in during the sample hour
- **What the model learned:** More active IPs (appearing in more auctions) are more likely to visit. Active streaming/browsing behavior correlates with engagement.
- **Implication:** IP activity level is a real signal. Highly active IPs are better prospects than rare/quiet ones.

### #7: Device Model Diversity (`wl_n_models`)
- **Source:** win_logs
- **SHAP:** 0.205
- **What it is:** Number of distinct device models seen for this IP
- **What the model learned:** IPs with multiple device models are more likely to visit. This indicates a household with multiple devices — a multi-device household is more engaged and more reachable.
- **Implication:** Household size/device diversity is a meaningful targeting signal. Single-device IPs are harder to convert.

### #8: Advertiser Count (`n_win_adv`)
- **Source:** base (win_logs derived)
- **SHAP:** 0.175
- **What it is:** Number of distinct advertisers targeting this IP
- **What the model learned:** IPs targeted by many advertisers visit more. These IPs are in high-value audience segments that multiple advertisers want.
- **Implication:** Cross-advertiser demand is a quality signal. An IP targeted by 20 advertisers is more valuable than one targeted by 1.

### #9: Fangorn Household Score (`ci_hh_score`)
- **Source:** cost_impression_log
- **SHAP:** 0.152
- **What it is:** Fangorn's existing household-level intent score
- **What the model learned:** Higher Fangorn scores → higher visit rates. The existing model is already capturing real signal.
- **Implication:** Fangorn works. But it ranks 9th, meaning there's significant additional signal in features 1-8 that Fangorn doesn't currently use.

### #10: PMP Deal Rate (`al_pct_pmp`)
- **Source:** augmentor_log
- **SHAP:** 0.105
- **What it is:** Fraction of auctions with Private Marketplace deals
- **What the model learned:** IPs seen predominantly through PMP deals visit more. PMP = curated, premium inventory with better audience quality.
- **Implication:** Inventory quality (PMP vs open exchange) is a real signal for targeting.

---

## The Content & Device Features (Bidstream)

These are the features we were most excited about from `bidder_auction_events` — content_genre, device_make, etc. Here's how they actually performed:

| Rank | Feature | SHAP | Composite Rank | Verdict |
|------|---------|------|----------------|---------|
| 13 | `bae_lg` (LG device) | — | 18.0 | Mid-tier. LG ownership has some predictive value. |
| 15 | `bae_n_auctions` (auction volume) | — | 19.7 | Mid-tier. Activity level signal. |
| 17 | `bae_pct_news` (% news genre) | — | 21.7 | Mid-tier. News watchers show different visit patterns. |
| 18 | `bae_pct_ent` (% entertainment) | — | 21.8 | Mid-tier. Entertainment is the dominant genre. |
| 21 | `bae_pct_drama` (% drama) | — | 23.0 | Lower-mid. |
| 22 | `bae_n_pubs` (publisher count) | — | 23.3 | Lower-mid. |
| 27 | `bae_n_genres` (genre diversity) | — | 26.2 | Lower. |
| 28 | `bae_roku` (Roku device) | — | 26.3 | Lower. |
| 30 | `bae_pct_sports` (% sports) | — | 26.7 | Lower. |
| 31 | `bae_pct_comedy` (% comedy) | — | 26.8 | Lower. |
| 36 | `bae_n_makes` (device make count) | — | 29.7 | Lower. |
| 41 | `bae_samsung` (Samsung device) | — | 32.3 | Lower. |

**Honest assessment:** Content genre and device make are **mid-tier for raw visit prediction.** They ranked 17-41 out of 47 features. The impression-level features (scores, cost, recency) and augmentor_log features (segment density) dominate.

**But this doesn't mean they're not valuable.** The model predicts "will this IP visit ANY advertiser." Content genre is much more valuable for predicting "will this IP visit THIS SPECIFIC advertiser" — a news watcher visiting a news site is a different question than "will they visit any site." That's the **vertical classification** use case, which is Alex's TI-791 work. We haven't tested that yet.

---

## Augmentor Log Features (Bidstream Supply-Side)

| Rank | Feature | SHAP | Composite Rank | What It Measures |
|------|---------|------|----------------|-----------------|
| 2 | `al_avg_segments` | **0.986** | 6.7 | Avg MNTN segments — **#1 by SHAP** |
| 8 | `al_n_auctions` | 0.228 | 13.0 | Auction volume |
| 14 | `al_n_domains` | — | 18.3 | Content domain diversity |
| 20 | `al_pct_iab` | 0.091 | 22.3 | % with IAB category data |
| 24 | `al_n_ssps` | — | 24.3 | SSP (exchange) diversity |
| 25 | `al_pct_pmp` | 0.105 | 25.3 | PMP deal rate |
| 35 | `al_n_networks` | — | 29.3 | Network/publisher count |
| 39 | `al_pct_video` | 0.071 | 31.3 | % video placement |
| 40 | `al_pct_ctv` | — | 32.0 | % CTV device type |
| 44 | `al_has_ctv` | — | 43.0 | Binary CTV flag |

**Verdict:** Augmentor log is the second-most valuable pre-visit source after cost_impression_log. `al_avg_segments` alone is the #1 feature by SHAP. IAB category rate and PMP rate add incremental signal.

---

## Win Logs Features (Ad Engagement)

| Rank | Feature | SHAP | Composite Rank | What It Measures |
|------|---------|------|----------------|-----------------|
| 3 | `wl_avg_price` | 0.231 | 7.7 | Clearing price |
| 6 | `wl_n_models` | 0.205 | 9.0 | Device model diversity |
| 12 | `wl_pauses` | — | 17.0 | Video pauses (active engagement) |
| 19 | `wl_n_makes` | — | 22.0 | Device manufacturer diversity |
| 23 | `wl_completes` | — | 23.7 | Video completions |
| 26 | `wl_plays` | — | 26.2 | Video plays |
| 29 | `wl_clicks` | — | 26.3 | Ad clicks |
| 33 | `wl_viewable` | — | 28.0 | Viewable impressions |
| 38 | `wl_vcr` | — | 31.0 | Video completion rate |
| 42 | `wl_mutes` | — | 34.3 | Video mutes |
| 43 | `wl_measurable` | — | 42.0 | Measurable impressions |
| 45 | `wl_skips` | 0 | 46.0 | **Zero** — CTV has no skip |
| 46 | `wl_viewability` | 0 | 46.0 | **Zero** — 100% for all CTV |
| 47 | `wl_invalid` | 0 | 46.0 | **Zero** — nearly no IVT |

**Verdict:** Pricing and device diversity are the valuable win_logs signals. Video engagement metrics (VCR, completions) are mid-tier. Three features had zero importance: skips (CTV doesn't have skip buttons), viewability rate (100% for all CTV), and IVT flags (nearly zero).

---

## Feedback Features (guid_log + conversion_log)

These are post-visit features — can't target with them, but valuable for enrichment and retraining.

| Rank | Feature | SHAP | Source | What It Measures |
|------|---------|------|--------|-----------------|
| 1 | `gl_n_os_families` | **5.255** | guid_log | OS diversity (multi-device signal) |
| 2 | `gl_n_browser_families` | **3.883** | guid_log | Browser diversity |
| 3 | `gl_pct_ip_stable` | **2.797** | guid_log | IP = original_ip match rate |
| 4 | `gl_n_adv` | 2.309 | guid_log | # advertisers visited |
| 5 | `gl_n_events` | 1.451 | guid_log | Event volume |
| 6 | `gl_pct_mobile` | 0.647 | guid_log | Mobile event % |
| 7 | `gl_pct_new` | 0.373 | guid_log | New visitor % |
| 8 | `cv_n_orders` | 0.266 | conversion_log | Distinct order count |
| 9 | `cv_n_conv` | 0.214 | conversion_log | Conversion count |
| 10 | `cv_avg_amt` | 0.206 | conversion_log | Average order value ($) |
| 11 | `cv_total_amt` | 0.129 | conversion_log | Total order value ($) |
| 12 | `cv_n_types` | — | conversion_log | Conversion type diversity |
| 13 | `gl_has_mobile` | — | guid_log | Mobile presence flag |
| 14 | `cv_n_adv` | — | conversion_log | Advertiser diversity in conversions |
| 15 | `gl_n_product_views` | — | guid_log | Product page views (purchase intent) |
| 16 | `gl_has_tablet` | — | guid_log | Tablet presence |
| 17 | `gl_has_new_visit` | — | guid_log | Any new visit flag |
| 18 | `gl_has_desktop` | 0 | guid_log | **Zero** — 0% fill in this sample |
| 19 | `gl_n_utm_events` | 0 | guid_log | **Zero** — UTM rarely present |

**Key feedback signals for enrichment:**
- **Device fingerprinting** (gl_n_os_families, gl_n_browser_families) — richest identity signal
- **IP stability** (gl_pct_ip_stable) — proxy vs direct connection
- **Conversion value** (cv_avg_amt, cv_total_amt) — spending tier segmentation
- **Product browsing** (gl_n_product_views) — purchase intent indicator

---

## Features We Identified But Haven't Modeled Yet

From the cross-table analysis, these are candidates for future iterations:

| Feature | Source | Why Not Yet | Potential |
|---------|--------|-------------|-----------|
| IAB category percentages (per-category) | augmentor_log | Need UNNEST on 1.2B rows — expensive | High for vertical classification |
| `query.ga_client_id` (67% prevalence) | conversion_log query string | Need JSON parsing | Identity resolution |
| `query.shpt` (74% prevalence) | conversion_log query string | Need JSON parsing | Product type — vertical signal |
| `query.email_data` (2.3%) | conversion_log query string | Sparse | Cross-device identity |
| `query.androidId/idfa/adid` (~3%) | conversion_log query string | Sparse | Cross-device identity |
| `ua_advanced.DeviceBrand` (799 values) | guid_log JSON | Need JSON parsing | Device demographics (Matt's prototype) |
| `ua_advanced.DeviceName` (9,984 values) | guid_log JSON | High cardinality | Device fingerprinting |
| `viewability_score` | bid_price_log | 10-day TTL | Pre-bid quality signal |
| `publisher_performance` | bid_price_log | 10-day TTL | Publisher quality ranking |
| `recency` / `conquest_score` | bid_events_log | Need to validate fill rates | Real-time signals at bid time |
| `content_series` (37% fill) | bidder_auction_events | Needs garbage filtering | Specific show affinity |
| `content_channel` (36% fill) | bidder_auction_events | Moderate fill | Channel affinity |

---

## Summary Table: All 25 Tables Analyzed

| Table | Unique Columns | In Model? | Verdict |
|-------|---------------|-----------|---------|
| **cost_impression_log** | 20 | Yes — 8 features | **#1 source.** Scores, recency, cost dominate. |
| **augmentor_log** | 7 | Yes — 10 features | **#2 source.** Segment density is top SHAP feature. |
| **win_logs** | 66 | Yes — 14 features (11 used) | **#3 source.** Price + device diversity. 3 had zero importance. |
| **bidder_auction_events** | 15 | Yes — 13 features | Mid-tier for IVR. High-value for vertical classification. |
| **guid_log** | 15 | Yes — 13 features (feedback) | Post-visit only. Device fingerprinting + product intent. |
| **conversion_log** | 3 + query JSON | Yes — 6 features (feedback) | Post-visit only. Order value + identity signals. |
| bid_price_log | 14 | No — 10d TTL | Viewability/publisher scores — worth testing. |
| bid_events_log | 3 | No | Recency + conquest score at bid time. |
| event_log_filtered | 5 | No | Pre-aggregated video quartiles. Redundant with win_logs. |
| event_log | 4 | No | VAST events — covered by win_logs aggregates. |
| conversion_signal_log | 5 | No | CallRail data. 193 rows in 6 days — too sparse. |
| tpa_membership_update_log | 7 | No | Scores field empty. No signal. |
| kochava_log | 6 | No | Mobile attribution — niche. |
| click_log | 1 | No | Only `landing_page` is unique. |
| clickpass_log | 1 | No | This is our outcome variable (IVR). |
| viewability_log | 2 | No | `viewability_type_id` — limited value. |
| impression_log | 12 | No | CPM/CPI — redundant with cost_impression_log. |
| spend_log | 9 | No | Intent scores = Fangorn output (circular). |
| bid_logs | 17 | No | 90% redundant with win_logs. |
| singular_log | 2 | No | Mobile attribution — overlap with kochava. |
| analytics_request_log | 4 | No | GA send events. Low value. |
| page_view_signal_log | 3 | No | Page URLs — niche. |
| auction_log | 0 | No | Subset of augmentor_log. |
| visit_tracking_log | 1 | No | `is_verified_visit` only. |
| guid_ip_log_visitors | 1 | No | `is_new_gl` — variant flag. |
| impression_tracking_log | 1 | No | `query_string` — tracking pixel params. |
| icloud_vv_log | 0 | No | Shares all columns with clickpass_log. |
| realtime_spend_last_3d | 0 | No | Subset of spend_log. |
