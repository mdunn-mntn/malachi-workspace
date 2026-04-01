# Feature Store: What Data Do We Have and What Predicts Visits?

**TI-789/790** | Malachi Dunn | 2026-04-01

---

## The Question

What IP-level features exist across MNTN's data, and which ones actually predict site visits?

## What We Did

1. **Scanned all 25 log tables** in the system. Identified unique columns per table programmatically.
2. **6 tables have unique signal** — the rest are redundant. Built daily snapshot queries for each.
3. **Joined into a training dataset** — 117,238 IPs from 2026-03-29, labeled: visited (1) or not (0).
4. **Trained XGBoost** (300 trees, max_depth 6) to predict visits. Measured importance via:
   - **XGBoost Gain** — average loss reduction when the feature is used in a split
   - **XGBoost Weight** — number of times the feature is used across all trees
   - **XGBoost Cover** — average number of samples affected when the feature is used
   - **Composite Rank** — average of the three ranks above (lower = more important)
   - **SHAP values** — mean absolute Shapley value per feature on 5,000-IP test sample (measures the average contribution of each feature to each individual prediction)
5. **Split pre-visit vs post-visit features** to avoid leakage from guid_log/conversion_log.

**Pre-visit model AUC: 0.896** | Base visit rate: 3.4% | Test set: 23,448 IPs

---

## What We Found

Three categories of features emerged:

- **EXISTING (ranks 1-9):** Our own system outputs — Fangorn scores, RTC targeting, segment density, impression frequency. They dominate importance because we designed them to predict visits. **This validates that the current targeting system works.** But they're not new signal for the feature store.

- **NEW (ranks 10-44):** External data from exchanges, bidstream, and user behavior. These are the **feature store candidates** — genuinely new signal not currently in Fangorn.

- **FEEDBACK (ranks 45-61):** guid_log and conversion_log features. Available only after a site visit. Can't target with them, but valuable for retraining and scoring returning visitors.

---

## All 66 Features, Ranked #1 to #66

| # | Feature | Source | Tag | Gain | SHAP | Description |
|---|---------|--------|-----|------|------|-------------|
| 1 | `ci_total_cost` | cost_impression_log | EXISTING | 33.4 | 0.363 | Total media $ spent on this IP |
| 2 | `al_avg_segments` | augmentor_log | EXISTING | 312.6 | 0.986 | Avg MNTN segments on IP (97% are 1P RTC+retargeting) |
| 3 | `ci_pct_rtc` | cost_impression_log | EXISTING | 47.1 | 0.392 | % impressions via RTC conquest targeting |
| 4 | `ci_pct_new` | cost_impression_log | EXISTING | 123.9 | 0.670 | % impressions where IP is "new" (first impression) |
| 5 | `ci_hh_score` | cost_impression_log | EXISTING | 35.5 | 0.152 | Fangorn household score (-1 = unscored) |
| 6 | `ci_n_imp` | cost_impression_log | EXISTING | 61.3 | 0.078 | # impressions served to this IP |
| 7 | `n_win_adv` | base | EXISTING | 39.5 | 0.175 | # of our advertisers targeting this IP |
| 8 | `ci_adv_hh_score` | cost_impression_log | EXISTING | 29.2 | 0.036 | Fangorn advertiser-specific score (10000 = RTC) |
| 9 | `n_wins` | base | EXISTING | 23.6 | 0.070 | Total auction wins for this IP |
| **10** | **`wl_avg_price`** | **win_logs** | **NEW** | **31.9** | **0.231** | **Clearing price per auction (USD, set by market)** |
| **11** | **`wl_n_models`** | **win_logs** | **NEW** | **307.7** | **0.205** | **# distinct device models (household diversity)** |
| **12** | **`al_n_auctions`** | **augmentor_log** | **NEW** | **33.1** | **0.228** | **# auctions this IP appeared in (market activity)** |
| 13 | `wl_pauses` | win_logs | NEW | 35.8 | 0.003 | # times viewer paused the video ad |
| 14 | `bae_lg` | bidder_auction_events | NEW | 34.1 | 0.007 | Has LG Smart TV (0/1) |
| 15 | `al_n_domains` | augmentor_log | NEW | 31.7 | 0.051 | # distinct content domains consumed |
| 16 | `bae_n_auctions` | bidder_auction_events | NEW | 27.8 | 0.056 | # dropped auctions (broader activity signal) |
| 17 | `ci_pct_video` | cost_impression_log | NEW | 30.6 | 0.071 | % VIDEO format impressions (CTV vs display) |
| 18 | `bae_pct_news` | bidder_auction_events | NEW | 29.9 | 0.023 | % content = news genre |
| 19 | `bae_pct_ent` | bidder_auction_events | NEW | 27.3 | 0.031 | % content = entertainment genre |
| 20 | `wl_n_makes` | win_logs | NEW | 47.8 | 0.053 | # distinct device manufacturers |
| 21 | `al_pct_iab` | augmentor_log | NEW | 27.9 | 0.091 | % auctions with IAB content category data |
| 22 | `bae_pct_drama` | bidder_auction_events | NEW | 26.9 | 0.018 | % content = drama genre |
| 23 | `bae_n_pubs` | bidder_auction_events | NEW | 29.3 | 0.036 | # distinct publishers consumed |
| 24 | `wl_completes` | win_logs | NEW | 30.2 | 0.048 | # video ad completions |
| 25 | `al_n_ssps` | augmentor_log | NEW | 28.5 | 0.048 | # distinct SSPs/exchanges seeing this IP |
| 26 | `al_pct_pmp` | augmentor_log | NEW | 28.2 | 0.105 | % auctions with Private Marketplace deals |
| 27 | `wl_plays` | win_logs | NEW | 27.9 | 0.052 | # video ad plays (starts) |
| 28 | `bae_n_genres` | bidder_auction_events | NEW | 30.9 | 0.032 | # distinct content genres watched |
| 29 | `bae_roku` | bidder_auction_events | NEW | 30.7 | 0.042 | Has Roku device (0/1) |
| 30 | `wl_clicks` | win_logs | NEW | 24.0 | 0.004 | # ad clicks (rare in CTV) |
| 31 | `bae_pct_sports` | bidder_auction_events | NEW | 23.7 | 0.008 | % content = sports genre |
| 32 | `bae_pct_comedy` | bidder_auction_events | NEW | 24.8 | 0.015 | % content = comedy genre |
| 33 | `bae_pct_genre` | bidder_auction_events | NEW | 27.1 | 0.055 | % auctions with any genre data |
| 34 | `wl_viewable` | win_logs | NEW | 31.1 | 0.033 | # viewable impressions |
| 35 | `ci_n_vendors` | cost_impression_log | NEW | 25.6 | 0.054 | # distinct supply vendors |
| 36 | `al_n_networks` | augmentor_log | NEW | 26.3 | 0.065 | # distinct networks/publishers in bidstream |
| 37 | `bae_n_makes` | bidder_auction_events | NEW | 26.2 | 0.032 | # distinct device manufacturers (bidstream) |
| 38 | `wl_vcr` | win_logs | NEW | 26.5 | 0.031 | Video completion rate (completes/plays) |
| 39 | `al_pct_video` | augmentor_log | NEW | 25.4 | 0.071 | % VIDEO placement in auctions |
| 40 | `al_pct_ctv` | augmentor_log | NEW | 20.7 | 0.038 | % CTV device type in auctions |
| 41 | `bae_samsung` | bidder_auction_events | NEW | 31.5 | 0.006 | Has Samsung Smart TV (0/1) |
| 42 | `wl_mutes` | win_logs | NEW | 16.2 | 0.001 | # times viewer muted the video ad |
| 43 | `wl_measurable` | win_logs | NEW | 19.8 | 0.004 | # measurable impressions |
| 44 | `al_has_ctv` | augmentor_log | NEW | 19.7 | 0.004 | Has CTV device in bidstream (0/1) |
| 45 | `gl_pct_ip_stable` | guid_log | FEEDBACK | 45.8 | 2.797 | % events where IP matches original_ip |
| 46 | `gl_n_os_families` | guid_log | FEEDBACK | 1627.5 | 5.255 | # distinct OS families on advertiser sites |
| 47 | `gl_pct_mobile` | guid_log | FEEDBACK | 26.7 | 0.647 | % events from mobile devices |
| 48 | `gl_n_browser_families` | guid_log | FEEDBACK | 734.0 | 3.883 | # distinct browser families on advertiser sites |
| 49 | `gl_n_adv` | guid_log | FEEDBACK | 9.7 | 2.309 | # distinct advertisers visited |
| 50 | `cv_n_types` | conversion_log | FEEDBACK | 38.9 | 0.102 | # distinct conversion types (purchase, signup, call) |
| 51 | `gl_n_events` | guid_log | FEEDBACK | 7.4 | 1.451 | # pixel events on advertiser sites |
| 52 | `cv_n_orders` | conversion_log | FEEDBACK | 10.7 | 0.266 | # distinct purchase orders |
| 53 | `cv_n_conv` | conversion_log | FEEDBACK | 7.9 | 0.214 | # conversion events |
| 54 | `gl_pct_new` | guid_log | FEEDBACK | 4.9 | 0.373 | % events flagged as "new" visit |
| 55 | `gl_has_mobile` | guid_log | FEEDBACK | 40.3 | 0.094 | Has mobile device events (0/1) |
| 56 | `cv_total_amt` | conversion_log | FEEDBACK | 6.2 | 0.129 | Total order value ($) |
| 57 | `cv_avg_amt` | conversion_log | FEEDBACK | 4.5 | 0.206 | Average order value ($) |
| 58 | `cv_n_adv` | conversion_log | FEEDBACK | 5.8 | 0.054 | # advertisers converted on |
| 59 | `gl_n_product_views` | guid_log | FEEDBACK | 3.8 | 0.088 | # product page views (purchase intent) |
| 60 | `gl_has_tablet` | guid_log | FEEDBACK | 3.8 | 0.003 | Has tablet events (0/1) |
| 61 | `gl_has_new_visit` | guid_log | FEEDBACK | 2.8 | 0.015 | Has any "new" visit flag |
| 62 | `gl_has_desktop` | guid_log | ZERO | 0.0 | — | Has desktop events — 0% fill in sample |
| 63 | `gl_n_utm_events` | guid_log | ZERO | 0.0 | — | # events with GA UTM params — rarely present |
| 64 | `wl_skips` | win_logs | ZERO | 0.0 | — | # video skips — CTV has no skip button |
| 65 | `wl_viewability` | win_logs | ZERO | 0.0 | — | Viewability rate — 100% for all CTV, no variance |
| 66 | `wl_invalid` | win_logs | ZERO | 0.0 | — | # IVT flags — nearly zero in sample |

---

## Three Takeaways

### 1. Our targeting system works.

Features 1-9 are our own outputs (Fangorn, RTC, retargeting). They're the strongest predictors of visits. This is expected — we built them to do this — but it's empirical confirmation that the system is directionally correct.

### 2. There are 35 genuinely new features we're not using yet.

The top new features (ranks 10-12) are:
- **Clearing price** — premium inventory correlates with higher-quality audiences
- **Device model diversity** — multi-device households are more engaged
- **Auction activity** — IPs that appear in more auctions are more reachable

Content genre features (news, entertainment, drama, sports, comedy) rank 18-32. They're mid-tier for predicting "will this IP visit ANY site" but likely much higher for "will this IP visit THIS SPECIFIC advertiser's site" — that's the vertical classification question Alex is investigating.

### 3. Post-visit features are gold for retraining, not targeting.

guid_log and conversion_log features (ranks 45-61) are extremely predictive (AUC 0.999 alone) but only exist after a visit happens. Use them for retraining Fangorn, scoring returning visitors, identity resolution (device fingerprinting), and conversion value segmentation — not for cold-start targeting.

---

## Next Steps

1. **Vertical classification model** — Test content genre features for per-advertiser IVR prediction (does a news watcher visit a news advertiser more?). This is where genre features should shine.
2. **Cold-start analysis** — Test new features specifically on IPs with no existing Fangorn score. These IPs can't rely on features 1-9.
3. **1P vs 3P segment split** — We found DS3 interest segments cover 1.3B IPs with ~20 segments each. Isolating 3P segment count from the 1P-dominated total could be a strong genuinely-new feature.
4. **Production integration** — Top new features → Fangorn feature store.
5. **Features not yet modeled** — IAB category percentages, content_series (show names), parsed conversion_log identity signals (ga_client_id at 67%, device IDs at 3%).

---

## Methodology Details

| Parameter | Value |
|-----------|-------|
| Model | XGBoost classifier |
| Trees | 300 |
| Max depth | 6 |
| Learning rate | 0.1 |
| Subsample | 0.8 |
| Column sample per tree | 0.8 |
| Class weight | Balanced (scale_pos_weight) |
| Train/test split | 80/20, stratified |
| Training IPs | 93,790 |
| Test IPs | 23,448 |
| Sample method | 1% deterministic (FARM_FINGERPRINT MOD 100) of IPs served impressions on 2026-03-29 |
| IP filtering | Excluded 0.0.0.0, 127.0.0.1, >10K wins (proxy/CDN) |
| Feature sources | 6 tables joined on IP (win_logs, cost_impression_log, augmentor_log, bidder_auction_events, guid_log, conversion_log) |
| Label | Visited advertiser site (clickpass_log) on same day = 1, else 0 |
| Importance metrics | Gain, Weight (frequency), Cover → composite rank (avg of 3 ranks) |
| SHAP | TreeExplainer on 5,000 test IPs, mean absolute SHAP value |
| Iterative paring | Tested 5 to 58 features — AUC stable at 0.896 ± 0.005 |
| Tables scanned | 25 total, 6 used (19 redundant or insufficient) |
