# TI-790: Complete Variable Reference — All Features Ranked by Importance

**How to read this:** Every unique IP-level feature we can extract, ordered by XGBoost importance. Split into two sections:
1. **Pre-visit features** — available at bid/impression time. Use for **targeting decisions**.
2. **Feedback features** — available after site visit. Use for **retraining, scoring returning visitors, identity resolution**.

Pre-visit model AUC: **0.896** | Feedback model AUC: **0.999** | Combined AUC: **0.999** (leaky — don't use for targeting)

---

## Pre-Visit Features (Available at Bid Time — For Targeting)

These features exist before any visit happens. They're what the feature store should use for targeting decisions.

47 features from 4 tables + base metrics. Ordered by composite rank (average of gain rank, weight rank, cover rank).

| Rank | Feature | Source Table | SHAP | Composite Rank | What It Measures | Why It Matters for Targeting |
|------|---------|-------------|------|----------------|-----------------|------------------------------|
| 1 | `ci_total_cost` | cost_impression_log | 0.363 | 6.0 | Total media spend on this IP | More exposure → more visits. Spend = commitment signal. |
| 2 | `al_avg_segments` | augmentor_log | **0.986** | 6.7 | Avg MNTN segments assigned to IP | **#1 by SHAP.** More segments = richer profile = better targeting. |
| 3 | `wl_avg_price` | win_logs | 0.231 | 7.7 | Average auction clearing price (USD) | Premium inventory correlates with higher-value audiences. |
| 4 | `ci_pct_rtc` | cost_impression_log | 0.392 | 8.0 | % impressions via RTC (conquest) | RTC-targeted IPs already identified as high-value. |
| 5 | `ci_pct_new` | cost_impression_log | **0.670** | 8.3 | % impressions where IP is "new" | **#2 by SHAP.** New IPs visit at lower rates. Returning IPs are warmer. |
| 6 | `wl_n_models` | win_logs | 0.205 | 9.0 | # distinct device models | Multi-device households → more engaged, more likely to visit. |
| 7 | `ci_hh_score` | cost_impression_log | 0.152 | 11.0 | Fangorn household score | Existing model output — already predictive. -1 = unscored. |
| 8 | `al_n_auctions` | augmentor_log | 0.228 | 13.0 | # auctions IP appeared in (1hr) | More active IPs have more ad exposure. |
| 9 | `ci_n_imp` | cost_impression_log | 0.078 | 13.3 | # impressions served | Impression volume — frequency signal. |
| 10 | `n_win_adv` | base | 0.175 | 13.7 | # advertisers targeting this IP | Popular IPs = relevant to many advertisers. |
| 11 | `ci_adv_hh_score` | cost_impression_log | — | 15.0 | Advertiser-specific household score | 10000 = RTC conquest. Other values = Fangorn score. |
| 12 | `wl_pauses` | win_logs | — | 17.0 | # video pauses | Pausing = active engagement (vs passive viewing). |
| 13 | `bae_lg` | bidder_auction_events | — | 18.0 | Has LG device (0/1) | LG Smart TV ownership — demographic signal. |
| 14 | `al_n_domains` | augmentor_log | — | 18.3 | # distinct domains in auctions | Content breadth — diverse browsers vs single-site users. |
| 15 | `bae_n_auctions` | bidder_auction_events | — | 19.7 | # dropped auctions for IP | Broader activity beyond what we bid on. |
| 16 | `ci_pct_video` | cost_impression_log | 0.071 | 20.3 | % VIDEO format impressions | CTV vs display split at impression level. |
| 17 | `bae_pct_news` | bidder_auction_events | — | 21.7 | % content = news genre | **Vertical signal.** News watchers → relevant for news-adjacent advertisers. |
| 18 | `bae_pct_ent` | bidder_auction_events | — | 21.8 | % content = entertainment genre | **Vertical signal.** Entertainment = broad reach, lower specificity. |
| 19 | `wl_n_makes` | win_logs | — | 22.0 | # distinct device manufacturers | Device diversity — household size proxy. |
| 20 | `al_pct_iab` | augmentor_log | 0.091 | 22.3 | % auctions with IAB categories | Content taxonomy coverage — richer data = better classification. |
| 21 | `bae_pct_drama` | bidder_auction_events | — | 23.0 | % content = drama genre | **Vertical signal.** Drama viewers — specific demo. |
| 22 | `bae_n_pubs` | bidder_auction_events | — | 23.3 | # distinct publishers | Publisher diversity — content consumption breadth. |
| 23 | `wl_completes` | win_logs | — | 23.7 | # video completions | Total video engagement volume. |
| 24 | `al_n_ssps` | augmentor_log | — | 24.3 | # distinct SSPs (exchanges) | Inventory source diversity — more SSPs = more reach. |
| 25 | `al_pct_pmp` | augmentor_log | 0.105 | 25.3 | % auctions with PMP deals | Premium inventory signal. PMP = curated, higher quality. |
| 26 | `wl_plays` | win_logs | — | 26.2 | # video plays | Total video ad starts. |
| 27 | `bae_n_genres` | bidder_auction_events | — | 26.2 | # distinct content genres | Genre diversity — how varied their viewing is. |
| 28 | `bae_roku` | bidder_auction_events | — | 26.3 | Has Roku device (0/1) | Roku ownership — largest CTV platform. |
| 29 | `wl_clicks` | win_logs | — | 26.3 | # clicks on ads | Direct response signal (rare in CTV). |
| 30 | `bae_pct_sports` | bidder_auction_events | — | 26.7 | % content = sports genre | **Vertical signal.** Sports viewers = specific advertiser set. |
| 31 | `bae_pct_comedy` | bidder_auction_events | — | 26.8 | % content = comedy genre | **Vertical signal.** |
| 32 | `bae_pct_genre` | bidder_auction_events | — | 27.0 | % auctions with genre data | Genre data availability rate for this IP. |
| 33 | `wl_viewable` | win_logs | — | 28.0 | # viewable impressions | Ad actually seen (vs rendered off-screen). |
| 34 | `ci_n_vendors` | cost_impression_log | 0.176 | 29.3 | # distinct supply vendors | Supply source diversity. |
| 35 | `al_n_networks` | augmentor_log | — | 29.3 | # distinct networks/publishers | What publishers they consume — content diversity. |
| 36 | `bae_n_makes` | bidder_auction_events | — | 29.7 | # distinct device manufacturers | Device diversity from bidstream. |
| 37 | `n_wins` | base | 0.070 | 29.7 | # total auction wins | How many ads we've shown this IP. |
| 38 | `wl_vcr` | win_logs | — | 31.0 | Video completion rate | % of video plays completed. Most CTV = ~1.0. |
| 39 | `al_pct_video` | augmentor_log | 0.071 | 31.3 | % VIDEO placement in auctions | CTV vs display from bidstream side. |
| 40 | `al_pct_ctv` | augmentor_log | — | 32.0 | % CTV device type in auctions | CTV vs mobile/PC from bidstream. |
| 41 | `bae_samsung` | bidder_auction_events | — | 32.3 | Has Samsung device (0/1) | Samsung Smart TV ownership. |
| 42 | `wl_mutes` | win_logs | — | 34.3 | # video mutes | Audio-off viewing — passive engagement. |
| 43 | `wl_measurable` | win_logs | — | 42.0 | # measurable impressions | Viewability measurement denominator. |
| 44 | `al_has_ctv` | augmentor_log | — | 43.0 | Has CTV device (0/1) | Binary CTV flag from bidstream. |
| 45 | `wl_skips` | win_logs | 0 | 46.0 | # video skips | **Zero importance** — CTV doesn't have skip buttons. |
| 46 | `wl_viewability` | win_logs | 0 | 46.0 | Viewability rate | **Zero importance** — ~100% for all CTV IPs. No variance. |
| 47 | `wl_invalid` | win_logs | 0 | 46.0 | # invalid (IVT) impressions | **Zero importance** — nearly zero for all IPs in this sample. |

### Pre-Visit Summary by Source Table

| Source | # Features | # Used by Model | Avg Rank | Best Feature | Total SHAP |
|--------|-----------|----------------|----------|-------------|------------|
| **cost_impression_log** | 8 | 8 | 13.9 | ci_pct_new (SHAP 0.670) | 1.73 |
| **base** | 2 | 2 | 21.7 | n_win_adv (SHAP 0.175) | 0.25 |
| **augmentor_log** | 10 | 10 | 24.6 | al_avg_segments (SHAP 0.986) | 1.52 |
| **bidder_auction_events** | 13 | 13 | 24.8 | bae_pct_news | 0.07 |
| **win_logs** | 14 | 11 | 28.9 | wl_avg_price (SHAP 0.231) | 0.51 |

---

## Feedback Features (Available After Site Visit — For Enrichment)

These features only exist because the IP visited an advertiser site or converted. They're extremely predictive (AUC 0.999) but **cannot be used for targeting new IPs** — they're the outcome, not the predictor.

**Use for:** Retraining Fangorn, scoring returning visitors, identity resolution, conversion value segmentation.

19 features from 2 tables. Ordered by composite rank.

| Rank | Feature | Source Table | SHAP | Composite Rank | What It Measures | Use Case |
|------|---------|-------------|------|----------------|-----------------|----------|
| 1 | `gl_pct_ip_stable` | guid_log | **2.797** | 3.0 | % events where IP = original_ip | IP stability — proxy IPs vs direct connections. Useful for identity resolution. |
| 2 | `gl_n_os_families` | guid_log | **5.255** | 3.7 | # distinct OS families (Mac/Win/iOS/Android) | **#1 by SHAP.** Multi-OS users = multi-device households. Retraining signal. |
| 3 | `gl_pct_mobile` | guid_log | 0.647 | 4.7 | % events from mobile devices | Mobile vs desktop behavior split. |
| 4 | `gl_n_browser_families` | guid_log | **3.883** | 5.0 | # distinct browser families | Browser diversity — multi-device signal. |
| 5 | `gl_n_adv` | guid_log | 2.309 | 6.3 | # distinct advertisers visited | Cross-advertiser activity — high-value IP if visiting many. |
| 6 | `cv_n_types` | conversion_log | — | 7.3 | # distinct conversion types | Purchase + signup + call = diverse converter. |
| 7 | `gl_n_events` | guid_log | 1.451 | 7.7 | # guid_log events | Visit volume — frequency signal. |
| 8 | `cv_n_orders` | conversion_log | 0.266 | 8.0 | # distinct order IDs | Order count — repeat purchaser signal. |
| 9 | `cv_n_conv` | conversion_log | 0.214 | 9.0 | # conversion events | Total conversions — converter vs non-converter. |
| 10 | `gl_pct_new` | guid_log | 0.373 | 9.0 | % events flagged as "new" visit | New vs returning visitor ratio. |
| 11 | `gl_has_mobile` | guid_log | — | 9.7 | Has mobile device events (0/1) | Mobile presence flag. |
| 12 | `cv_total_amt` | conversion_log | 0.129 | 10.0 | Total order amount ($) | **Purchase value.** High spenders behave differently. |
| 13 | `cv_avg_amt` | conversion_log | 0.206 | 11.3 | Avg order amount ($) | Average basket size — spending tier. |
| 14 | `cv_n_adv` | conversion_log | — | 12.7 | # distinct advertisers converted on | Cross-advertiser converter — high-value IP. |
| 15 | `gl_n_product_views` | guid_log | — | 13.3 | # product page views | **Purchase intent.** Browsing products = considering purchase. |
| 16 | `gl_has_tablet` | guid_log | — | 15.3 | Has tablet events (0/1) | Tablet ownership — device profile. |
| 17 | `gl_has_new_visit` | guid_log | — | 17.0 | Has any "new" visit flag | First-visit signal. |
| 18 | `gl_has_desktop` | guid_log | 0 | 18.5 | Has desktop events (0/1) | **Zero importance** — 0% fill in sample (all mobile/other). |
| 19 | `gl_n_utm_events` | guid_log | 0 | 18.5 | # events with GA UTM params | **Zero importance** — 0% fill. UTM rarely present. |

### Feedback Summary by Source Table

| Source | # Features | # Used | Avg Rank | Best Feature |
|--------|-----------|--------|----------|-------------|
| **guid_log** | 13 | 11 | 10.1 | gl_n_os_families (SHAP 5.255) |
| **conversion_log** | 6 | 6 | 9.7 | cv_n_orders (SHAP 0.266) |

---

## Features Not Yet Modeled (Candidate Additions)

These features were identified in the cross-table analysis but not included in the XGBoost model. They could be added in future iterations.

### From conversion_log query string (hidden signals)
| Feature | Prevalence | What It Measures | Potential Value |
|---------|-----------|-----------------|-----------------|
| `query.ga_client_id` | 67% | Google Analytics cross-session ID | **Identity resolution** — link visits across sessions |
| `query.shoamt` | 75% | Order amount from pixel | Dollar value (supplement to order_amt column) |
| `query.shpt` | 74% | Product type purchased | **Product category** — vertical signal from demand side |
| `query.email_data` | 2.3% | Hashed email | Identity resolution — cross-device linking |
| `query.androidId` | 3.1% | Android device ID | Cross-device identity |
| `query.idfa` | 3.1% | iOS advertising ID | Cross-device identity |
| `query.adid/advertiserId` | 3% | Advertising ID | Cross-device identity |

### From guid_log ua_advanced JSON (Matt's prototype)
| Feature | Distinct Values | What It Measures | Potential Value |
|---------|----------------|-----------------|-----------------|
| `DeviceClass` | 17 | Richer device type | Better than basic device_type |
| `DeviceBrand` | 799 | Device manufacturer | Same signal as bidder_auction_events.device_make but from pixel side |
| `DeviceName` | 9,984 | Specific device model | Very granular — needs bucketing |
| `DeviceCpu` | 21 | CPU architecture | Device capability proxy |

### From bid_price_log (10-day TTL limits use)
| Feature | What It Measures | Potential Value |
|---------|-----------------|-----------------|
| `viewability_score` | Pre-bid viewability prediction | Inventory quality signal |
| `publisher_performance` | Publisher quality score | Content quality signal |
| `uncapped_bid_price` | What we'd bid without caps | True valuation of impression |

### From bid_events_log
| Feature | What It Measures | Potential Value |
|---------|-----------------|-----------------|
| `recency` / `recency_threshold` | IP recency at bid time | Frequency/freshness signal |
| `threshold_failure_reasons` | Why we didn't bid | Filtering reason distribution |
| `conquest_score` | RTC score at bid time | Real-time intent signal |

### From event_log_filtered (pre-aggregated)
| Feature | What It Measures | Potential Value |
|---------|-----------------|-----------------|
| `v_vast_complete` | Video completed (bool per impression) | Clean VCR without joining win_logs |
| `v_vast_start/firstquartile/midpoint/thirdquartile` | Video quartile flags | Granular completion funnel |

### From augmentor_log (IAB category expansion)
Not in current model: extracting top IAB category per IP, IAB entropy, and specific category percentages (similar to how we did genre percentages for bidder_auction_events). Would require UNNEST of the iab_categories array — expensive on augmentor_log scale but potentially high-value for vertical classification.

---

## Quick Reference: What Each Table Uniquely Provides

| Table | Unique Signal | Best For | Not Available Elsewhere |
|-------|--------------|----------|------------------------|
| **cost_impression_log** | Recency, scoring, cost, format | Impression-level enrichment | recency_elapsed_time, household_score, media_cost breakdown, ott_device |
| **augmentor_log** | IAB categories, segment density, SSP diversity | Supply-side context, vertical classification | iab_categories (bronze only), mntn_segments, 40 inventory sources |
| **bidder_auction_events** | Content genre, device make, show/channel/network | Vertical classification, device demographics | content_genre, device_make, content_series, content_channel |
| **win_logs** | Video engagement, viewability, IVT, device model | Ad engagement behavior | video_completes/skips/mutes, in_view_time_ms, invalid_impression flags |
| **guid_log** | Device/browser/OS from pixel, product/cart, UTM | Demand-side behavior, purchase intent | product JSON, cart JSON, ga_utm_*, ua_advanced JSON |
| **conversion_log** | Order value, conversion type, identity signals | Purchase value segmentation, identity resolution | order_amt, conversion_type, query string (ga_client_id, email, device IDs) |
