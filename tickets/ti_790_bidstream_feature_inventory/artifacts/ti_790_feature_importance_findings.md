# Feature Importance Findings — Every Feature Ranked

**TI-790** | Malachi Dunn | 2026-03-31
**For:** Alex Knorr, Ryan Kleck, Matt Brorby

---

## How This Was Done

Trained XGBoost on **117,238 real IPs** (2026-03-29) to predict site visits. Joined 6 tables on IP, extracted ~60 features. Importance measured three ways (gain, frequency, cover) then averaged into a composite rank. SHAP values provide per-feature contribution magnitude.

**Pre-visit model AUC: 0.896** (features available at bid time)
**Base visit rate:** 3.4%

---

## Every Feature, Ranked #1 to #66

Each feature is tagged:
- **NEW** = not currently used by Fangorn/targeting — genuinely new signal
- **EXISTING** = already implemented or derived from existing model outputs (RTC scores, Fangorn household scores, segment assignments)
- **FEEDBACK** = only available after a site visit (guid_log/conversion_log) — use for retraining, not targeting

### Pre-Visit Features (Available at Bid Time)

| # | Feature | Source | SHAP | Tag | What It Is | What the Model Found |
|---|---------|--------|------|-----|-----------|---------------------|
| 1 | `al_avg_segments` | augmentor_log | 0.986 | EXISTING | Avg MNTN segments on IP | More segments = more likely to visit. Expected — segments are the output of our targeting system. |
| 2 | `ci_pct_new` | cost_impression_log | 0.670 | EXISTING | % impressions where IP is "new" | New IPs visit less. Returning IPs are warmer. Already implicit in frequency capping. |
| 3 | `ci_pct_rtc` | cost_impression_log | 0.392 | EXISTING | % RTC-targeted impressions | RTC IPs visit more. Expected — RTC is designed to find high-intent IPs. Circular. |
| 4 | `ci_total_cost` | cost_impression_log | 0.363 | EXISTING | Total media spend on IP | More spend = more visits. Partly mechanical (more impressions), partly reflects bidder confidence. |
| 5 | `wl_avg_price` | win_logs | 0.231 | **NEW** | Average clearing price (USD) | Premium inventory → better audiences. Price is a quality proxy not currently used as a feature. |
| 6 | `al_n_auctions` | augmentor_log | 0.228 | **NEW** | # auctions IP appeared in | More active IPs visit more. Activity level is a real signal not explicitly tracked. |
| 7 | `wl_n_models` | win_logs | 0.205 | **NEW** | # distinct device models | Multi-device households visit more. Household size proxy — genuinely new. |
| 8 | `n_win_adv` | base | 0.175 | **NEW** | # advertisers targeting IP | Cross-advertiser demand = quality signal. IP wanted by many advertisers. |
| 9 | `ci_hh_score` | cost_impression_log | 0.152 | EXISTING | Fangorn household score | The existing model's own score. Of course it predicts visits. Circular. |
| 10 | `al_pct_pmp` | augmentor_log | 0.105 | **NEW** | % auctions with PMP deals | Premium inventory signal. PMP = curated, higher quality. Not currently a feature. |
| 11 | `ci_n_imp` | cost_impression_log | 0.078 | EXISTING | # impressions served | Impression volume. Already tracked via frequency. |
| 12 | `ci_pct_video` | cost_impression_log | 0.071 | **NEW** | % VIDEO format impressions | CTV vs display split. Not currently an explicit feature. |
| 13 | `al_pct_video` | augmentor_log | 0.071 | **NEW** | % VIDEO placement in auctions | CTV vs display from bidstream side. |
| 14 | `n_wins` | base | 0.070 | EXISTING | Total auction wins | How many ads shown. Frequency metric. |
| 15 | `ci_adv_hh_score` | cost_impression_log | — | EXISTING | Advertiser-specific score | Fangorn output. Circular. 10000 = RTC. |
| 16 | `ci_n_vendors` | cost_impression_log | 0.176 | **NEW** | # distinct supply vendors | Supply source diversity. New signal. |
| 17 | `ci_pct_new` (impressions) | cost_impression_log | — | EXISTING | New visitor % at impression level | Same as #2. |
| 18 | `wl_pauses` | win_logs | — | **NEW** | # video pauses | Active engagement — pausing means watching intentionally. |
| 19 | `bae_lg` | bidder_auction_events | — | **NEW** | Has LG device (0/1) | LG Smart TV ownership. Demographic/device signal. |
| 20 | `al_n_domains` | augmentor_log | — | **NEW** | # distinct domains in auctions | Content consumption breadth. Diverse vs single-site. |
| 21 | `bae_n_auctions` | bidder_auction_events | — | **NEW** | # dropped auctions for IP | Broader activity beyond what we bid on. |
| 22 | `bae_pct_news` | bidder_auction_events | — | **NEW** | % content = news genre | **Vertical signal.** News watchers. |
| 23 | `bae_pct_ent` | bidder_auction_events | — | **NEW** | % content = entertainment | **Vertical signal.** Entertainment viewers. |
| 24 | `wl_n_makes` | win_logs | — | **NEW** | # device manufacturers | Device diversity — household signal. |
| 25 | `al_pct_iab` | augmentor_log | 0.091 | **NEW** | % auctions with IAB categories | Content taxonomy data availability. |
| 26 | `bae_pct_drama` | bidder_auction_events | — | **NEW** | % content = drama | **Vertical signal.** |
| 27 | `bae_n_pubs` | bidder_auction_events | — | **NEW** | # distinct publishers | Publisher diversity. |
| 28 | `wl_completes` | win_logs | — | **NEW** | # video completions | Total video engagement. |
| 29 | `al_n_ssps` | augmentor_log | — | **NEW** | # distinct SSPs/exchanges | Inventory source diversity. |
| 30 | `al_pct_pmp` | augmentor_log | — | **NEW** | PMP deal rate | (Same as #10 — duplicate in ranking) |
| 31 | `wl_plays` | win_logs | — | **NEW** | # video plays | Video ad starts. |
| 32 | `bae_n_genres` | bidder_auction_events | — | **NEW** | # distinct content genres | Viewing diversity — narrow vs broad. |
| 33 | `bae_roku` | bidder_auction_events | — | **NEW** | Has Roku device (0/1) | Largest CTV platform. Device ownership signal. |
| 34 | `wl_clicks` | win_logs | — | **NEW** | # ad clicks | Direct response (rare in CTV). |
| 35 | `bae_pct_sports` | bidder_auction_events | — | **NEW** | % content = sports | **Vertical signal.** |
| 36 | `bae_pct_comedy` | bidder_auction_events | — | **NEW** | % content = comedy | **Vertical signal.** |
| 37 | `bae_pct_genre` | bidder_auction_events | — | **NEW** | % auctions with genre data | Genre data availability. |
| 38 | `wl_viewable` | win_logs | — | **NEW** | # viewable impressions | Ad actually seen. |
| 39 | `ci_n_vendors` | cost_impression_log | — | **NEW** | Supply vendor diversity | (Same as #16) |
| 40 | `al_n_networks` | augmentor_log | — | **NEW** | # networks/publishers | Content publisher diversity. |
| 41 | `bae_n_makes` | bidder_auction_events | — | **NEW** | # device manufacturers (bidstream) | Device diversity from broader auctions. |
| 42 | `wl_vcr` | win_logs | — | **NEW** | Video completion rate | % plays completed. ~1.0 for most CTV. Signal is in the variance. |
| 43 | `al_pct_ctv` | augmentor_log | — | **NEW** | % CTV device in auctions | CTV vs mobile/PC from bidstream. |
| 44 | `bae_samsung` | bidder_auction_events | — | **NEW** | Has Samsung device (0/1) | Samsung Smart TV ownership. |
| 45 | `wl_mutes` | win_logs | — | **NEW** | # video mutes | Audio-off viewing. Passive engagement. |
| 46 | `wl_measurable` | win_logs | — | **NEW** | # measurable impressions | Viewability denominator. |
| 47 | `al_has_ctv` | augmentor_log | — | **NEW** | Binary CTV flag | CTV presence. |
| 48 | `wl_skips` | win_logs | 0 | — | # video skips | **Zero importance.** CTV has no skip button. |
| 49 | `wl_viewability` | win_logs | 0 | — | Viewability rate | **Zero importance.** ~100% for all CTV. No variance. |
| 50 | `wl_invalid` | win_logs | 0 | — | # IVT flags | **Zero importance.** Nearly zero for all IPs. |

### Feedback Features (Post-Visit — For Retraining & Enrichment)

| # | Feature | Source | SHAP | What It Is | Use Case |
|---|---------|--------|------|-----------|----------|
| 51 | `gl_n_os_families` | guid_log | **5.255** | # distinct OS families | FEEDBACK — Multi-device detection. Device fingerprinting. |
| 52 | `gl_n_browser_families` | guid_log | **3.883** | # browser families | FEEDBACK — Browser diversity. Identity resolution. |
| 53 | `gl_pct_ip_stable` | guid_log | **2.797** | IP = original_ip % | FEEDBACK — Proxy vs direct connection. IP quality. |
| 54 | `gl_n_adv` | guid_log | 2.309 | # advertisers visited | FEEDBACK — Cross-advertiser activity level. |
| 55 | `gl_n_events` | guid_log | 1.451 | # pixel events | FEEDBACK — Visit frequency/engagement. |
| 56 | `gl_pct_mobile` | guid_log | 0.647 | % mobile events | FEEDBACK — Mobile vs desktop behavior. |
| 57 | `gl_pct_new` | guid_log | 0.373 | % "new" visits | FEEDBACK — New vs returning ratio. |
| 58 | `cv_n_orders` | conversion_log | 0.266 | # distinct orders | FEEDBACK — Repeat purchaser signal. |
| 59 | `cv_n_conv` | conversion_log | 0.214 | # conversions | FEEDBACK — Converter flag. |
| 60 | `cv_avg_amt` | conversion_log | 0.206 | Avg order value ($) | FEEDBACK — Spending tier. $200 buyer ≠ $5 buyer. |
| 61 | `cv_total_amt` | conversion_log | 0.129 | Total order value ($) | FEEDBACK — Lifetime purchase value. |
| 62 | `cv_n_types` | conversion_log | — | # conversion types | FEEDBACK — Diverse converter (purchase + signup + call). |
| 63 | `gl_has_mobile` | guid_log | — | Mobile presence (0/1) | FEEDBACK — Mobile device flag. |
| 64 | `cv_n_adv` | conversion_log | — | # advertisers converted on | FEEDBACK — Cross-advertiser converter. |
| 65 | `gl_n_product_views` | guid_log | — | # product page views | FEEDBACK — Purchase intent (browsing products). |
| 66 | `gl_has_tablet` | guid_log | — | Tablet presence (0/1) | FEEDBACK — Tablet device flag. |

*gl_has_desktop and gl_n_utm_events had zero importance (0% fill in sample).*

---

## The "Genuinely New" Features — What We Should Focus On

Filtering out EXISTING (circular) and zero-importance features, here are the **NEW features ranked by actual predictive value:**

| New Rank | Feature | Source | SHAP | What It Is |
|----------|---------|--------|------|-----------|
| **1** | `wl_avg_price` | win_logs | 0.231 | Clearing price — premium inventory = better audiences |
| **2** | `al_n_auctions` | augmentor_log | 0.228 | Auction activity volume — active IPs visit more |
| **3** | `wl_n_models` | win_logs | 0.205 | Device model diversity — multi-device households |
| **4** | `n_win_adv` | base | 0.175 | Cross-advertiser demand — wanted by many = high value |
| **5** | `ci_n_vendors` | cost_impression_log | 0.176 | Supply vendor diversity |
| **6** | `al_pct_pmp` | augmentor_log | 0.105 | PMP deal rate — premium inventory signal |
| **7** | `al_pct_iab` | augmentor_log | 0.091 | IAB category data availability |
| **8** | `ci_pct_video` | cost_impression_log | 0.071 | CTV vs display format split |
| **9** | `al_pct_video` | augmentor_log | 0.071 | CTV vs display from bidstream |
| **10** | `wl_pauses` | win_logs | — | Video pauses — active engagement |
| **11** | `bae_lg` | bidder_auction_events | — | LG device ownership |
| **12** | `al_n_domains` | augmentor_log | — | Content domain diversity |
| **13** | `bae_n_auctions` | bidder_auction_events | — | Broader auction activity |
| **14** | `bae_pct_news` | bidder_auction_events | — | News genre % — **vertical signal** |
| **15** | `bae_pct_ent` | bidder_auction_events | — | Entertainment genre % — **vertical signal** |
| **16** | `wl_n_makes` | win_logs | — | Device manufacturer diversity |
| **17** | `bae_pct_drama` | bidder_auction_events | — | Drama genre % — **vertical signal** |
| **18** | `bae_n_pubs` | bidder_auction_events | — | Publisher diversity |
| **19** | `wl_completes` | win_logs | — | Video completion count |
| **20** | `al_n_ssps` | augmentor_log | — | SSP/exchange diversity |
| **21** | `wl_plays` | win_logs | — | Video play count |
| **22** | `bae_n_genres` | bidder_auction_events | — | Genre diversity — narrow vs broad viewer |
| **23** | `bae_roku` | bidder_auction_events | — | Roku device ownership |
| **24** | `wl_clicks` | win_logs | — | Ad clicks (rare in CTV) |
| **25** | `bae_pct_sports` | bidder_auction_events | — | Sports genre % — **vertical signal** |
| **26** | `bae_pct_comedy` | bidder_auction_events | — | Comedy genre % — **vertical signal** |
| **27** | `bae_pct_genre` | bidder_auction_events | — | Genre data fill rate |
| **28** | `wl_viewable` | win_logs | — | Viewable impression count |
| **29** | `al_n_networks` | augmentor_log | — | Network/publisher count |
| **30** | `bae_n_makes` | bidder_auction_events | — | Device make diversity (bidstream) |
| **31** | `wl_vcr` | win_logs | — | Video completion rate |
| **32** | `al_pct_ctv` | augmentor_log | — | CTV device % in auctions |
| **33** | `bae_samsung` | bidder_auction_events | — | Samsung device ownership |
| **34** | `wl_mutes` | win_logs | — | Video mute count — passive viewing |
| **35** | `wl_measurable` | win_logs | — | Measurable impression count |
| **36** | `al_has_ctv` | augmentor_log | — | Binary CTV flag |

---

## Features Not Yet Modeled

These were identified in the cross-table analysis but not included in the XGBoost model yet:

| Feature | Source | Prevalence | Why It Could Matter |
|---------|--------|-----------|-------------------|
| IAB category percentages (per-category) | augmentor_log | 30% | Specific vertical affinity (IAB8 = Food, IAB17 = Sports) |
| `content_series` (show names) | bidder_auction_events | 37% | Specific show affinity — very granular |
| `content_channel` | bidder_auction_events | 36% | Channel affinity |
| `query.ga_client_id` | conversion_log | 67% | Cross-session identity linkage |
| `query.shpt` (product type) | conversion_log | 74% | Product category purchased — vertical signal |
| `query.email_data` | conversion_log | 2.3% | Identity resolution — cross-device |
| `ua_advanced.DeviceBrand` (799 values) | guid_log | 56% | Device demographics (Matt's prototype) |
| `ua_advanced.DeviceName` (9,984 values) | guid_log | 56% | Device fingerprinting |
| `viewability_score` | bid_price_log | — (10d TTL) | Pre-bid quality prediction |
| `publisher_performance` | bid_price_log | — (10d TTL) | Publisher quality ranking |
| `recency` / `conquest_score` | bid_events_log | — | Real-time signals at bid time |

---

## All 25 Tables — What We Checked

| Table | Unique Cols | In Model? | Verdict |
|-------|-----------|-----------|---------|
| **cost_impression_log** | 20 | Yes (8) | Top source for targeting features — but many are EXISTING model outputs |
| **augmentor_log** | 7 | Yes (10) | Segment density (#1 SHAP) + IAB categories + SSP diversity |
| **win_logs** | 66 | Yes (14) | Price, device diversity, video engagement. 3 had zero importance. |
| **bidder_auction_events** | 15 | Yes (13) | Content genre + device make. Mid-tier for IVR, high for verticals. |
| **guid_log** | 15 | Yes (13) | FEEDBACK only. Device fingerprinting + product intent. |
| **conversion_log** | 3 + query JSON | Yes (6) | FEEDBACK only. Order value + identity signals. |
| bid_price_log | 14 | No (10d TTL) | viewability_score, publisher_performance — worth testing |
| bid_events_log | 3 | No | recency, conquest_score at bid time |
| event_log_filtered | 5 | No | Pre-aggregated video quartiles. Redundant with win_logs. |
| event_log | 4 | No | VAST events — covered by win_logs. |
| conversion_signal_log | 5 | No | CallRail data. 193 rows in 6 days. Too sparse. |
| tpa_membership_update_log | 7 | No | Scores field empty. No signal. |
| kochava_log | 6 | No | Mobile attribution. Niche. |
| click_log | 1 | No | Only landing_page is unique. |
| clickpass_log | 1 | No | Outcome variable (IVR). |
| viewability_log | 2 | No | viewability_type_id only. |
| impression_log | 12 | No | CPM/CPI — redundant with cost_impression_log. |
| spend_log | 9 | No | Intent scores = Fangorn output. Circular. |
| bid_logs | 17 | No | 90% redundant with win_logs. |
| singular_log | 2 | No | Mobile attribution overlap. |
| analytics_request_log | 4 | No | GA send events. Low value. |
| page_view_signal_log | 3 | No | Page URLs. Niche. |
| auction_log | 0 | No | Subset of augmentor_log. |
| visit_tracking_log | 1 | No | is_verified_visit only. |
| guid_ip_log_visitors | 1 | No | Variant new-visitor flag. |
| impression_tracking_log | 1 | No | Tracking pixel query string. |
| icloud_vv_log | 0 | No | Shares all columns with clickpass_log. |
| realtime_spend_last_3d | 0 | No | Subset of spend_log. |
