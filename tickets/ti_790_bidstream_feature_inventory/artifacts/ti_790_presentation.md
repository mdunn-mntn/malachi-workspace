# Feature Store: What Data Do We Have and What Predicts Visits?

**TI-789/790** | Malachi Dunn | 2026-04-01

---

## The Question

What IP-level features exist across MNTN's data, and which ones actually predict site visits?

## What We Did

1. **Scanned all 25 log tables** in the system. Identified unique columns per table programmatically.
2. **6 tables have unique signal** — the rest are redundant. Built daily snapshot queries for each.
3. **Joined into a training dataset** — 117,238 IPs from 2026-03-29, labeled: visited (1) or not (0).
4. **Trained XGBoost** (300 trees, max_depth 6) to predict visits.
5. **Ranked features by SHAP** — mean absolute Shapley value per feature on 5,000-IP test sample. SHAP measures the average contribution of each feature to each individual prediction — the most interpretable importance metric.
6. **Split pre-visit vs post-visit features** to avoid leakage from guid_log/conversion_log.

**Pre-visit model AUC: 0.896** | Base visit rate: 3.4% | Test set: 23,448 IPs

---

## What We Found

Three categories of features emerged:

- **EXISTING:** Our own system outputs — Fangorn scores, RTC targeting, segment density, impression frequency. They dominate importance because we designed them to predict visits. **This validates that the current targeting system works.** But they're not new signal for the feature store.

- **NEW:** External data from exchanges, bidstream, and user behavior. These are the **feature store candidates** — genuinely new signal not currently in Fangorn.

- **FEEDBACK:** guid_log and conversion_log features. Only available after a site visit — can't target with them, but valuable for retraining models and scoring returning visitors.

---

## All 66 Features, Ranked by SHAP (Most to Least Important)

| # | Feature | Source | Tag | SHAP | Gain | Description |
|---|---------|--------|-----|------|------|-------------|
| 1 | `gl_n_os_families` | guid_log | FEEDBACK | 5.255 | 1627.5 | # distinct OS families on advertiser sites |
| 2 | `gl_n_browser_families` | guid_log | FEEDBACK | 3.883 | 734.0 | # distinct browser families on advertiser sites |
| 3 | `gl_pct_ip_stable` | guid_log | FEEDBACK | 2.797 | 45.8 | % events where IP matches original_ip |
| 4 | `gl_n_adv` | guid_log | FEEDBACK | 2.309 | 9.7 | # distinct advertisers visited |
| 5 | `gl_n_events` | guid_log | FEEDBACK | 1.451 | 7.4 | # pixel events on advertiser sites |
| 6 | `al_avg_segments` | augmentor_log | EXISTING | 0.986 | 312.6 | Avg MNTN segments on IP (97% are 1P RTC+retargeting) |
| 7 | `ci_pct_new` | cost_impression_log | EXISTING | 0.670 | 123.9 | % impressions where IP is "new" (first impression) |
| 8 | `gl_pct_mobile` | guid_log | FEEDBACK | 0.647 | 26.7 | % events from mobile devices |
| 9 | `ci_pct_rtc` | cost_impression_log | EXISTING | 0.392 | 47.1 | % impressions via RTC conquest targeting |
| 10 | `gl_pct_new` | guid_log | FEEDBACK | 0.373 | 4.9 | % events flagged as "new" visit |
| 11 | `ci_total_cost` | cost_impression_log | EXISTING | 0.363 | 33.4 | Total media $ spent on this IP |
| 12 | `cv_n_orders` | conversion_log | FEEDBACK | 0.266 | 10.7 | # distinct purchase orders |
| **13** | **`wl_avg_price`** | **win_logs** | **NEW** | **0.231** | **31.9** | **Clearing price per auction (USD, set by market)** |
| **14** | **`al_n_auctions`** | **augmentor_log** | **NEW** | **0.228** | **33.1** | **# auctions this IP appeared in (market activity)** |
| 15 | `cv_n_conv` | conversion_log | FEEDBACK | 0.214 | 7.9 | # conversion events |
| 16 | `cv_avg_amt` | conversion_log | FEEDBACK | 0.206 | 4.5 | Average order value ($) |
| **17** | **`wl_n_models`** | **win_logs** | **NEW** | **0.205** | **307.7** | **# distinct device models (household diversity)** |
| 18 | `n_win_adv` | base | EXISTING | 0.175 | 39.5 | # of our advertisers targeting this IP |
| 19 | `ci_hh_score` | cost_impression_log | EXISTING | 0.152 | 35.5 | Fangorn household score (-1 = unscored) |
| 20 | `cv_total_amt` | conversion_log | FEEDBACK | 0.129 | 6.2 | Total order value ($) |
| **21** | **`al_pct_pmp`** | **augmentor_log** | **NEW** | **0.105** | **28.2** | **% auctions with Private Marketplace deals** |
| 22 | `cv_n_types` | conversion_log | FEEDBACK | 0.102 | 38.9 | # distinct conversion types (purchase, signup, call) |
| 23 | `gl_has_mobile` | guid_log | FEEDBACK | 0.094 | 40.3 | Has mobile device events (0/1) |
| **24** | **`al_pct_iab`** | **augmentor_log** | **NEW** | **0.091** | **27.9** | **% auctions with IAB content category data** |
| 25 | `gl_n_product_views` | guid_log | FEEDBACK | 0.088 | 3.8 | # product page views (purchase intent) |
| 26 | `ci_n_imp` | cost_impression_log | EXISTING | 0.078 | 61.3 | # impressions served to this IP |
| **27** | **`ci_pct_video`** | **cost_impression_log** | **NEW** | **0.071** | **30.6** | **% VIDEO format impressions (CTV vs display)** |
| **28** | **`al_pct_video`** | **augmentor_log** | **NEW** | **0.071** | **25.4** | **% VIDEO placement in auctions** |
| 29 | `n_wins` | base | EXISTING | 0.070 | 23.6 | Total auction wins for this IP |
| **30** | **`al_n_networks`** | **augmentor_log** | **NEW** | **0.065** | **26.3** | **# distinct networks/publishers in bidstream** |
| **31** | **`bae_n_auctions`** | **bidder_auction_events** | **NEW** | **0.056** | **27.8** | **# dropped auctions (broader activity signal)** |
| **32** | **`bae_pct_genre`** | **bidder_auction_events** | **NEW** | **0.055** | **27.1** | **% auctions with any genre data** |
| **33** | **`ci_n_vendors`** | **cost_impression_log** | **NEW** | **0.054** | **25.6** | **# distinct supply vendors** |
| 34 | `cv_n_adv` | conversion_log | FEEDBACK | 0.054 | 5.8 | # advertisers converted on |
| **35** | **`wl_n_makes`** | **win_logs** | **NEW** | **0.053** | **47.8** | **# distinct device manufacturers** |
| **36** | **`wl_plays`** | **win_logs** | **NEW** | **0.052** | **27.9** | **# video ad plays (starts)** |
| **37** | **`al_n_domains`** | **augmentor_log** | **NEW** | **0.051** | **31.7** | **# distinct content domains consumed** |
| **38** | **`wl_completes`** | **win_logs** | **NEW** | **0.048** | **30.2** | **# video ad completions** |
| **39** | **`al_n_ssps`** | **augmentor_log** | **NEW** | **0.048** | **28.5** | **# distinct SSPs/exchanges seeing this IP** |
| **40** | **`bae_roku`** | **bidder_auction_events** | **NEW** | **0.042** | **30.7** | **Has Roku device (0/1)** |
| **41** | **`al_pct_ctv`** | **augmentor_log** | **NEW** | **0.038** | **20.7** | **% CTV device type in auctions** |
| 42 | `ci_adv_hh_score` | cost_impression_log | EXISTING | 0.036 | 29.2 | Fangorn advertiser-specific score (10000 = RTC) |
| **43** | **`bae_n_pubs`** | **bidder_auction_events** | **NEW** | **0.036** | **29.3** | **# distinct publishers consumed** |
| **44** | **`wl_viewable`** | **win_logs** | **NEW** | **0.033** | **31.1** | **# viewable impressions** |
| **45** | **`bae_n_genres`** | **bidder_auction_events** | **NEW** | **0.032** | **30.9** | **# distinct content genres watched** |
| **46** | **`bae_n_makes`** | **bidder_auction_events** | **NEW** | **0.032** | **26.2** | **# distinct device manufacturers (bidstream)** |
| **47** | **`wl_vcr`** | **win_logs** | **NEW** | **0.031** | **26.5** | **Video completion rate (completes/plays)** |
| **48** | **`bae_pct_ent`** | **bidder_auction_events** | **NEW** | **0.031** | **27.3** | **% content = entertainment genre** |
| **49** | **`bae_pct_news`** | **bidder_auction_events** | **NEW** | **0.023** | **29.9** | **% content = news genre** |
| **50** | **`bae_pct_drama`** | **bidder_auction_events** | **NEW** | **0.018** | **26.9** | **% content = drama genre** |
| 51 | `gl_has_new_visit` | guid_log | FEEDBACK | 0.015 | 2.8 | Has any "new" visit flag |
| **52** | **`bae_pct_comedy`** | **bidder_auction_events** | **NEW** | **0.015** | **24.8** | **% content = comedy genre** |
| **53** | **`bae_pct_sports`** | **bidder_auction_events** | **NEW** | **0.008** | **23.7** | **% content = sports genre** |
| **54** | **`bae_lg`** | **bidder_auction_events** | **NEW** | **0.007** | **34.1** | **Has LG Smart TV (0/1)** |
| **55** | **`bae_samsung`** | **bidder_auction_events** | **NEW** | **0.006** | **31.5** | **Has Samsung Smart TV (0/1)** |
| **56** | **`wl_measurable`** | **win_logs** | **NEW** | **0.004** | **19.8** | **# measurable impressions** |
| **57** | **`al_has_ctv`** | **augmentor_log** | **NEW** | **0.004** | **19.7** | **Has CTV device in bidstream (0/1)** |
| **58** | **`wl_clicks`** | **win_logs** | **NEW** | **0.004** | **24.0** | **# ad clicks (rare in CTV)** |
| 59 | `gl_has_tablet` | guid_log | FEEDBACK | 0.003 | 3.8 | Has tablet events (0/1) |
| **60** | **`wl_pauses`** | **win_logs** | **NEW** | **0.003** | **35.8** | **# times viewer paused the video ad** |
| **61** | **`wl_mutes`** | **win_logs** | **NEW** | **0.001** | **16.2** | **# times viewer muted the video ad** |
| 62 | `gl_has_desktop` | guid_log | ZERO | 0.000 | 0.0 | Has desktop events — 0% fill in sample |
| 63 | `gl_n_utm_events` | guid_log | ZERO | 0.000 | 0.0 | # events with GA UTM params — rarely present |
| 64 | `wl_skips` | win_logs | ZERO | 0.000 | 0.0 | # video skips — CTV has no skip button |
| 65 | `wl_viewability` | win_logs | ZERO | 0.000 | 0.0 | Viewability rate — 100% for all CTV, no variance |
| 66 | `wl_invalid` | win_logs | ZERO | 0.000 | 0.0 | # IVT flags — nearly zero in sample |

---

## Three Takeaways

### 1. Our targeting system works.

The top EXISTING features (ranks 6-7, 9, 11) include Fangorn scores, RTC targeting, and segment density. They're strong predictors — empirical validation that the system is directionally correct.

### 2. There are 35 genuinely new features we're not using yet.

The top NEW features (ranks 13-14, 17) are:
- **Clearing price** (SHAP 0.231) — premium inventory correlates with higher-quality audiences
- **Auction activity** (SHAP 0.228) — IPs that appear in more auctions are more reachable
- **Device model diversity** (SHAP 0.205) — multi-device households are more engaged

Content genre features (news, entertainment, drama, sports, comedy) rank 48-53 by SHAP. They're lower for predicting "will this IP visit ANY site" but likely much higher for "will this IP visit THIS SPECIFIC advertiser's site" — the vertical classification question.

### 3. Post-visit features are gold for retraining, not targeting.

FEEDBACK features (guid_log, conversion_log) occupy the top 5 ranks by SHAP. They're extremely predictive but only exist after a visit happens. Use for retraining Fangorn, scoring returning visitors, identity resolution (device fingerprinting), and conversion value segmentation.

---

## Scoped Model: Per-Advertiser Results

We re-ran with each row = (IP, advertiser). Label = visited THIS advertiser. 363K rows, 0.95% visit rate.

**Scoped AUC: 0.842** (vs 0.896 unscoped). Harder problem — predicting per-advertiser visits.

**Features that gained importance (rose in rank):**

| Feature | Unscoped → Scoped | Change | Why |
|---------|-------------------|--------|-----|
| `al_pct_ctv` (CTV %) | 26 → 13 | **+13** | CTV-ness matters for per-advertiser matching |
| `bae_pct_comedy` (comedy %) | 36 → 28 | **+8** | Genre helps match IP to advertiser type |
| `bae_n_genres` (genre diversity) | 30 → 23 | **+7** | Content variety helps differentiate |
| `bae_pct_ent` (entertainment %) | 33 → 26 | **+7** | Content signal rises |
| `bae_pct_genre` (genre fill rate) | 18 → 12 | **+6** | Genre data availability rises |
| `bae_pct_news` (news %) | 34 → 30 | **+4** | Content signal rises |

**Features that lost importance (dropped in rank):**

| Feature | Unscoped → Scoped | Change | Why |
|---------|-------------------|--------|-----|
| `wl_n_models` (device diversity) | 7 → 22 | **-15** | Was measuring household activity, not advertiser match |
| `ci_n_imp` (impression count) | 12 → 27 | **-15** | Volume effect removed by scoping |
| `bae_roku` (Roku device) | 25 → 40 | **-15** | Device ownership less relevant per-advertiser |
| `wl_n_makes` (device makes) | 20 → 33 | **-13** | Same — household proxy, not advertiser match |

**Bottom line:** Content features (genre, CTV %) gain importance when scoped. Volume/device-diversity features drop. This confirms content signals are the most valuable NEW features for the feature store.

**Data gotcha found:** `win_logs.advertiser_id` is Beeswax ID, `clickpass_log.advertiser_id` is MNTN ID. Must join through `bronze.integrationprod.campaigns` to map between them.

---

## Next Steps

1. **Vertical classification model** — Test content genre features for per-advertiser IVR prediction.
2. **Cold-start analysis** — Test new features on IPs with no existing Fangorn score.
3. **1P vs 3P segment split** — DS3 interest segments cover 1.3B IPs with ~20 segments each. Isolating 3P count could be a strong new feature.
5. **Production integration** — Top new features → Fangorn feature store.
6. **Features not yet modeled** — IAB category percentages, content_series (show names), parsed identity signals from conversion_log (ga_client_id 67%, device IDs 3%).

---

## Methodology

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
| **Ranking metric** | **SHAP — mean absolute Shapley value per feature on 5,000-IP test sample** |
| Supporting metrics | XGBoost Gain (avg loss reduction per split), Weight (# times used in trees), Cover (avg samples per split) |
| Iterative paring | Tested 5 to 58 features — AUC stable at 0.896 ± 0.005 |
| Tables scanned | 25 total, 6 used (19 redundant or insufficient) |
