# Feature Store: What Data Do We Have and What Predicts Visits?

**TI-789/790** | Malachi Dunn | 2026-04-01

---

## The Question

For a given advertiser's campaign, which IP-level features predict whether that IP will visit that advertiser's site?

## What We Did

1. **Scanned all 25 log tables** in the system. Identified unique columns per table programmatically.
2. **6 tables have unique signal** — the rest are redundant. Built daily snapshot queries for each.
3. **Joined into a training dataset** — 363,749 (IP, advertiser) pairs from 2026-03-29. Each row = one IP served impressions by one advertiser. Label = did this IP visit THIS advertiser's site.
4. **Trained XGBoost** (300 trees, max_depth 6) to predict per-advertiser visits.
5. **Ranked features by SHAP** — mean absolute Shapley value per feature on 5,000-sample test set.
6. **Split pre-visit vs post-visit features** to avoid leakage from guid_log/conversion_log.

**Model AUC: 0.842** | Visit rate: 0.95% (3,472 visits out of 363K pairs) | Test set: 72,750 pairs

---

## What We Found

Three categories of features emerged:

- **EXISTING:** Our own system outputs — Fangorn scores, RTC targeting, segment density, impression frequency. They dominate importance because we designed them to predict visits. **This validates that the current targeting system works.** But they're not new signal for the feature store.

- **NEW:** External data from exchanges, bidstream, and user behavior. These are the **feature store candidates** — genuinely new signal not currently in Fangorn.

- **FEEDBACK:** guid_log and conversion_log features. Only available after a site visit — can't target with them, but valuable for retraining models and scoring returning visitors. Ranked separately below.

---

## Pre-Visit Features, Ranked by SHAP (Scoped to Advertiser)

44 features from win_logs, cost_impression_log, augmentor_log, bidder_auction_events + base metrics. Sorted by SHAP descending. See [Glossary](#glossary) at the bottom for metric definitions.

| # | Feature | Source | Tag | SHAP | Direction | Description |
|---|---------|--------|-----|------|-----------|-------------|
| 1 | `ci_pct_new` | cost_impression_log | EXISTING | 1.710 | ↓ fewer visits | % impressions where IP is "new" (first impression) |
| 2 | `al_avg_segments` | augmentor_log | EXISTING | 0.411 | ↑ more visits | Avg MNTN segments on IP (97% are 1P RTC+retargeting) |
| 3 | `n_wins_this_adv` | base | EXISTING | 0.333 | ↑ more visits | # wins for THIS advertiser (impression frequency to this IP) |
| **4** | **`wl_avg_price`** | **win_logs** | **NEW** | **0.273** | **↓ fewer visits** | **Clearing price per auction (USD, set by market)** |
| 5 | `ci_total_cost` | cost_impression_log | EXISTING | 0.260 | ↑ more visits | Total media $ spent on this IP |
| 6 | `ci_pct_rtc` | cost_impression_log | EXISTING | 0.243 | ↑ more visits | % impressions via RTC conquest targeting |
| **7** | **`wl_n_adv`** | **win_logs** | **NEW** | **0.213** | **↑ more visits** | **# distinct advertisers serving this IP** |
| **8** | **`wl_n_wins`** | **win_logs** | **NEW** | **0.204** | **↑ more visits** | **Total auction wins across all advertisers** |
| 9 | `ci_hh_score` | cost_impression_log | EXISTING | 0.174 | ↑ more visits | Fangorn household score (-1 = unscored) |
| **10** | **`al_pct_pmp`** | **augmentor_log** | **NEW** | **0.133** | **↑ more visits** | **% auctions with Private Marketplace deals** |
| **11** | **`al_n_auctions`** | **augmentor_log** | **NEW** | **0.127** | **↑ more visits** | **# auctions this IP appeared in (market activity)** |
| **12** | **`bae_pct_genre`** | **bidder_auction_events** | **NEW** | **0.119** | **↑ more visits** | **% auctions with any genre data** |
| **13** | **`al_pct_ctv`** | **augmentor_log** | **NEW** | **0.111** | **↑ more visits** | **% CTV device type in auctions** |
| **14** | **`ci_pct_video`** | **cost_impression_log** | **NEW** | **0.107** | **↓ fewer visits** | **% VIDEO format impressions (CTV vs display)** |
| **15** | **`al_pct_iab`** | **augmentor_log** | **NEW** | **0.104** | **↑ more visits** | **% auctions with IAB content category data** |
| **16** | **`al_pct_video`** | **augmentor_log** | **NEW** | **0.100** | **↑ more visits** | **% VIDEO placement in auctions** |
| **17** | **`wl_plays`** | **win_logs** | **NEW** | **0.100** | **↑ more visits** | **# video ad plays (starts)** |
| **18** | **`al_n_domains`** | **augmentor_log** | **NEW** | **0.095** | **↑ more visits** | **# distinct content domains consumed** |
| **19** | **`al_n_networks`** | **augmentor_log** | **NEW** | **0.093** | **↑ more visits** | **# distinct networks/publishers in bidstream** |
| 20 | `n_cgs_this_adv` | base | EXISTING | 0.088 | ↑ more visits | # campaign groups for THIS advertiser targeting this IP |
| **21** | **`al_n_ssps`** | **augmentor_log** | **NEW** | **0.083** | **↑ more visits** | **# distinct SSPs/exchanges seeing this IP** |
| **22** | **`wl_n_models`** | **win_logs** | **NEW** | **0.081** | **↑ more visits** | **# distinct device models (household diversity)** |
| **23** | **`bae_n_genres`** | **bidder_auction_events** | **NEW** | **0.077** | **↑ more visits** | **# distinct content genres watched** |
| **24** | **`bae_n_auctions`** | **bidder_auction_events** | **NEW** | **0.071** | **↑ more visits** | **# dropped auctions (broader activity signal)** |
| **25** | **`bae_n_pubs`** | **bidder_auction_events** | **NEW** | **0.069** | **↑ more visits** | **# distinct publishers consumed** |
| **26** | **`bae_pct_ent`** | **bidder_auction_events** | **NEW** | **0.068** | **↑ more visits** | **% content = entertainment genre** |
| 27 | `ci_n_imp` | cost_impression_log | EXISTING | 0.067 | ↑ more visits | # impressions served to this IP |
| **28** | **`bae_pct_comedy`** | **bidder_auction_events** | **NEW** | **0.063** | **↑ more visits** | **% content = comedy genre** |
| **29** | **`wl_viewable`** | **win_logs** | **NEW** | **0.060** | **↑ more visits** | **# viewable impressions** |
| **30** | **`bae_pct_news`** | **bidder_auction_events** | **NEW** | **0.056** | **↑ more visits** | **% content = news genre** |
| **31** | **`ci_n_vendors`** | **cost_impression_log** | **NEW** | **0.056** | **↑ more visits** | **# distinct supply vendors** |
| **32** | **`wl_completes`** | **win_logs** | **NEW** | **0.051** | **↑ more visits** | **# video ad completions** |
| **33** | **`wl_n_makes`** | **win_logs** | **NEW** | **0.049** | **↑ more visits** | **# distinct device manufacturers** |
| **34** | **`bae_pct_drama`** | **bidder_auction_events** | **NEW** | **0.045** | **↑ more visits** | **% content = drama genre** |
| **35** | **`bae_n_makes`** | **bidder_auction_events** | **NEW** | **0.038** | **↑ more visits** | **# distinct device manufacturers (bidstream)** |
| **36** | **`wl_vcr`** | **win_logs** | **NEW** | **0.033** | **— neutral** | **Video completion rate (completes/plays)** |
| **37** | **`bae_pct_sports`** | **bidder_auction_events** | **NEW** | **0.021** | **↑ more visits** | **% content = sports genre** |
| **38** | **`bae_samsung`** | **bidder_auction_events** | **NEW** | **0.018** | **↑ more visits** | **Has Samsung Smart TV (0/1)** |
| **39** | **`al_has_ctv`** | **augmentor_log** | **NEW** | **0.013** | **↑ more visits** | **Has CTV device in bidstream (0/1)** |
| **40** | **`bae_roku`** | **bidder_auction_events** | **NEW** | **0.011** | **↑ more visits** | **Has Roku device (0/1)** |
| **41** | **`bae_lg`** | **bidder_auction_events** | **NEW** | **0.011** | **↑ more visits** | **Has LG Smart TV (0/1)** |
| **42** | **`wl_mutes`** | **win_logs** | **NEW** | **0.006** | **↑ more visits** | **# times viewer muted the video ad** |
| **43** | **`wl_pauses`** | **win_logs** | **NEW** | **0.006** | **↑ more visits** | **# times viewer paused the video ad** |
| **44** | **`wl_clicks`** | **win_logs** | **NEW** | **0.005** | **↑ more visits** | **# ad clicks (rare in CTV)** |

## Feedback Features (Post-Visit — For Retraining)

These only exist after a site visit. Can't use for targeting. Ranked from the feedback-only model (AUC 0.999).

| # | Feature | Source | SHAP | Gain | Description |
|---|---------|--------|------|------|-------------|
| 1 | `gl_n_os_families` | guid_log | 5.255 | 1627.5 | # distinct OS families on advertiser sites |
| 2 | `gl_n_browser_families` | guid_log | 3.883 | 734.0 | # distinct browser families on advertiser sites |
| 3 | `gl_pct_ip_stable` | guid_log | 2.797 | 45.8 | % events where IP matches original_ip |
| 4 | `gl_n_adv` | guid_log | 2.309 | 9.7 | # distinct advertisers visited |
| 5 | `gl_n_events` | guid_log | 1.451 | 7.4 | # pixel events on advertiser sites |
| 6 | `gl_pct_mobile` | guid_log | 0.647 | 26.7 | % events from mobile devices |
| 7 | `gl_pct_new` | guid_log | 0.373 | 4.9 | % events flagged as "new" visit |
| 8 | `cv_n_orders` | conversion_log | 0.266 | 10.7 | # distinct purchase orders |
| 9 | `cv_n_conv` | conversion_log | 0.214 | 7.9 | # conversion events |
| 10 | `cv_avg_amt` | conversion_log | 0.206 | 4.5 | Average order value ($) |
| 11 | `cv_total_amt` | conversion_log | 0.129 | 6.2 | Total order value ($) |
| 12 | `cv_n_types` | conversion_log | 0.102 | 38.9 | # distinct conversion types |
| 13 | `gl_has_mobile` | guid_log | 0.094 | 40.3 | Has mobile device events (0/1) |
| 14 | `cv_n_adv` | conversion_log | 0.054 | 5.8 | # advertisers converted on |
| 15 | `gl_n_product_views` | guid_log | 0.088 | 3.8 | # product page views (purchase intent) |
| 16 | `gl_has_tablet` | guid_log | 0.003 | 3.8 | Has tablet events (0/1) |
| 17 | `gl_has_new_visit` | guid_log | 0.015 | 2.8 | Has any "new" visit flag |

*Zero importance: gl_has_desktop (0% fill), gl_n_utm_events (0% fill), wl_skips (no skip in CTV), wl_viewability (100% for CTV), wl_invalid (near zero).*

---

## Three Takeaways

### 1. Our targeting system works.

EXISTING features (ranks 1-3, 5-6, 9, 20, 27) include Fangorn scores, RTC targeting, and segment density. They're the strongest predictors — validates the current system.

### 2. Content and CTV features are the most valuable new signal.

The top NEW features in the scoped model:
- **Clearing price** (SHAP 0.273) — premium inventory = better audiences
- **PMP deal rate** (SHAP 0.133) — curated premium inventory
- **Genre data availability** (SHAP 0.119) — IPs with richer content data are more predictable
- **CTV percentage** (SHAP 0.111) — rose 13 ranks vs unscoped model
- **Content genre percentages** (entertainment, comedy, news, drama, sports) — all rose 4-8 ranks vs unscoped

Content features gained the most importance when we scoped to per-advertiser visits. This confirms they help match IPs to specific advertisers — exactly what the feature store needs.

### 3. Volume features are less important than they appeared.

Features that dropped when scoped: device model diversity (-15 ranks), impression count (-15), Roku ownership (-15), device make count (-13). These were measuring "how active is this IP" rather than "will this IP visit this specific advertiser."

---

## Next Steps

1. **Vertical classification model** — Test content genre features for per-advertiser IVR prediction.
2. **Cold-start analysis** — Test new features on IPs with no existing Fangorn score.
3. **1P vs 3P segment split** — DS3 interest segments cover 1.3B IPs with ~20 segments each. Isolating 3P count could be a strong new feature.
4. **Production integration** — Top new features → Fangorn feature store.
5. **Features not yet modeled** — IAB category percentages, content_series (show names), parsed identity signals from conversion_log (ga_client_id 67%, device IDs 3%).

---

## Methodology

| Parameter | Value |
|-----------|-------|
| Model | XGBoost classifier |
| Trees / Max depth / LR | 300 / 6 / 0.1 |
| Subsample / Col sample | 0.8 / 0.8 |
| Class weight | Balanced (scale_pos_weight) |
| Train/test split | 80/20, stratified |
| **Training rows** | **290,999 (IP, advertiser) pairs** |
| **Test rows** | **72,750 (IP, advertiser) pairs** |
| Sample method | 1% of IPs (FARM_FINGERPRINT MOD 100), expanded to all advertiser pairs |
| IP filtering | Excluded 0.0.0.0, 127.0.0.1, >10K wins (proxy/CDN) |
| **Scoping** | **Each row = (IP, advertiser). Label = visited THIS advertiser's site on same day.** |
| Advertiser ID mapping | win_logs uses Beeswax IDs; mapped to MNTN IDs via `campaigns` table |
| Feature sources | 6 tables joined on IP |
| **Ranking metric** | **SHAP — mean absolute Shapley value per feature** |
| Tables scanned | 25 total, 6 used (19 redundant or insufficient) |

---

## Glossary

### SHAP (SHapley Additive exPlanations)

From game theory. For each individual prediction, SHAP calculates how much each feature pushed that prediction up or down from the baseline. It does this by considering every possible combination of features and measuring each feature's marginal contribution. The number we report ("mean absolute SHAP") is the average magnitude of those contributions across all test IPs.

A SHAP of 0.273 for `wl_avg_price` means that feature shifts the average prediction by 0.273 on the log-odds scale. Higher SHAP = the feature matters more to individual predictions. SHAP also captures direction — it can show that a high value pushes toward "visit" while a low value pushes toward "no visit." The Direction column in the table above comes from comparing feature means between visitors and non-visitors.

### Gain

Average reduction in the model's loss function (how wrong it is) each time a feature is used to make a split in a tree. High gain = the feature produces big improvements in prediction accuracy when the model splits on it. The difference from SHAP: Gain tells you how useful a feature is to the *tree structure*, while SHAP tells you how much a feature contributes to each *individual prediction*. A feature can have high gain but low SHAP if it's used in deep tree branches that affect few predictions.

### Direction

Computed by comparing the average feature value for IPs that visited (visited=1) vs IPs that did not (visited=0). "↑ more visits" means visitors have higher values for this feature on average. "↓ fewer visits" means visitors have lower values. This is a population-level signal — XGBoost can learn non-linear relationships (e.g., medium values predict visits but very high values don't).

### Why 300 Trees, Max Depth 6

Standard XGBoost configuration for tabular data. 300 trees with learning_rate=0.1 means each tree makes a small correction — more conservative than fewer trees with a higher rate. Max depth 6 means each tree can learn interactions between up to 6 features (e.g., "high clearing price AND CTV AND PMP AND entertainment genre"). Deeper = more complex but more overfitting risk. We didn't tune hyperparameters because the goal was feature ranking, not maximizing AUC — importance rankings are stable across reasonable configurations.

### Where `ci_pct_new` Comes From

`is_new` is a boolean column on `cost_impression_log` (the enriched impression table). It's set by the MNTN impression pipeline — `TRUE` when this is the first impression ever served to this IP. `ci_pct_new` = the fraction of impressions for this IP where `is_new = TRUE`. A high value means most/all impressions were first impressions (cold IP). A low value means the IP has been served many times before (warm IP). It's tagged EXISTING because the `is_new` flag is set by our own system, not by external data.

### Tag Definitions

- **EXISTING** — Feature derived from our own targeting/scoring system. Fangorn scores, RTC flags, segment counts, impression frequency. These are circular: they predict visits because we designed them to. Valid as confirmation that the system works, but not new signal for the feature store.
- **NEW** — Feature from external sources: exchange clearing prices, bidstream content/device data, user behavior (video engagement). Not currently in Fangorn. These are the feature store candidates.
- **FEEDBACK** — Feature from guid_log or conversion_log. Only available after a site visit has already happened. Can't use for targeting new IPs. Valuable for retraining models and scoring returning visitors.
