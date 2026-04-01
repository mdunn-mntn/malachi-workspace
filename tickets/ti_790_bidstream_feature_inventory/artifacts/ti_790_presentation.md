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

## NEW Features Only — Without EXISTING Features in the Model

The table above includes EXISTING features, which can absorb signal from NEW features. If the model leans heavily on `ci_pct_new` (SHAP 1.71) first, it doesn't need `al_pct_pmp` as much. To see the true standalone importance of NEW features, we retrained the model with all 9 EXISTING features removed.

**NEW-only AUC: 0.784** (vs 0.842 with all features). Removing EXISTING features costs 0.058 AUC — meaningful but the NEW features still carry substantial signal on their own.

| # | Feature | Source | SHAP | SHAP (all) | Change | Direction | Description |
|---|---------|--------|------|------------|--------|-----------|-------------|
| **1** | **`wl_n_models`** | **win_logs** | **0.598** | 0.081 | **+639%** | ↑ more visits | # distinct device models (household diversity) |
| **2** | **`wl_n_wins`** | **win_logs** | **0.390** | 0.204 | +91% | ↑ more visits | Total auction wins across all advertisers |
| **3** | **`wl_avg_price`** | **win_logs** | **0.337** | 0.273 | +23% | ↓ fewer visits | Clearing price per auction (USD, set by market) |
| **4** | **`wl_n_adv`** | **win_logs** | **0.259** | 0.213 | +22% | ↑ more visits | # distinct advertisers serving this IP |
| **5** | **`al_n_domains`** | **augmentor_log** | **0.258** | 0.095 | **+172%** | ↑ more visits | # distinct content domains consumed |
| **6** | **`al_pct_ctv`** | **augmentor_log** | **0.225** | 0.111 | +103% | ↑ more visits | % CTV device type in auctions |
| **7** | **`ci_pct_video`** | **cost_impression_log** | **0.224** | 0.107 | +109% | ↓ fewer visits | % VIDEO format impressions (CTV vs display) |
| **8** | **`al_pct_video`** | **augmentor_log** | **0.220** | 0.100 | +120% | ↑ more visits | % VIDEO placement in auctions |
| **9** | **`al_pct_iab`** | **augmentor_log** | **0.189** | 0.104 | +82% | ↑ more visits | % auctions with IAB content category data |
| **10** | **`al_n_auctions`** | **augmentor_log** | **0.184** | 0.127 | +45% | ↑ more visits | # auctions this IP appeared in |
| **11** | **`al_pct_pmp`** | **augmentor_log** | **0.137** | 0.133 | +3% | ↑ more visits | % auctions with PMP deals |
| **12** | **`bae_n_auctions`** | **bidder_auction_events** | **0.128** | 0.071 | +80% | ↑ more visits | # dropped auctions |
| **13** | **`bae_pct_genre`** | **bidder_auction_events** | **0.114** | 0.119 | -4% | ↑ more visits | % auctions with genre data |
| **14** | **`wl_plays`** | **win_logs** | **0.113** | 0.100 | +13% | ↑ more visits | # video ad plays |
| **15** | **`al_n_networks`** | **augmentor_log** | **0.101** | 0.093 | +9% | ↑ more visits | # distinct networks/publishers |
| **16** | **`wl_viewable`** | **win_logs** | **0.096** | 0.060 | +60% | ↑ more visits | # viewable impressions |
| **17** | **`al_n_ssps`** | **augmentor_log** | **0.094** | 0.083 | +13% | ↑ more visits | # distinct SSPs/exchanges |
| **18** | **`bae_pct_news`** | **bidder_auction_events** | **0.091** | 0.056 | **+63%** | ↑ more visits | % content = news genre |
| **19** | **`ci_n_vendors`** | **cost_impression_log** | **0.091** | 0.056 | +63% | ↑ more visits | # distinct supply vendors |
| **20** | **`bae_n_pubs`** | **bidder_auction_events** | **0.088** | 0.069 | +28% | ↑ more visits | # distinct publishers consumed |
| **21** | **`wl_completes`** | **win_logs** | **0.083** | 0.051 | +63% | ↑ more visits | # video completions |
| **22** | **`bae_pct_ent`** | **bidder_auction_events** | **0.075** | 0.068 | +10% | ↑ more visits | % entertainment genre |
| **23** | **`bae_n_genres`** | **bidder_auction_events** | **0.075** | 0.077 | -3% | ↑ more visits | # distinct genres watched |
| **24** | **`wl_n_makes`** | **win_logs** | **0.057** | 0.049 | +16% | ↑ more visits | # distinct device manufacturers |
| **25** | **`bae_pct_comedy`** | **bidder_auction_events** | **0.057** | 0.063 | -10% | ↑ more visits | % comedy genre |
| 26 | `wl_vcr` | win_logs | 0.046 | 0.033 | +39% | — neutral | Video completion rate |
| 27 | `bae_pct_drama` | bidder_auction_events | 0.043 | 0.045 | -4% | ↑ more visits | % drama genre |
| 28 | `bae_samsung` | bidder_auction_events | 0.035 | 0.018 | +94% | ↑ more visits | Has Samsung Smart TV |
| 29 | `bae_n_makes` | bidder_auction_events | 0.030 | 0.038 | -21% | ↑ more visits | # device manufacturers (bidstream) |
| 30 | `bae_pct_sports` | bidder_auction_events | 0.027 | 0.021 | +29% | ↑ more visits | % sports genre |
| 31 | `bae_roku` | bidder_auction_events | 0.020 | 0.011 | +82% | ↑ more visits | Has Roku device |
| 32 | `bae_lg` | bidder_auction_events | 0.013 | 0.011 | +18% | ↑ more visits | Has LG Smart TV |
| 33 | `al_has_ctv` | augmentor_log | 0.010 | 0.013 | -23% | ↑ more visits | Has CTV device |
| 34 | `wl_mutes` | win_logs | 0.007 | 0.006 | +17% | ↑ more visits | # video mutes |
| 35 | `wl_clicks` | win_logs | 0.006 | 0.005 | +20% | ↑ more visits | # ad clicks |
| 36 | `wl_pauses` | win_logs | 0.005 | 0.006 | -17% | ↑ more visits | # video pauses |

**Biggest movers when EXISTING removed:**
- `wl_n_models` (device diversity): SHAP 0.081 → **0.598** (+639%). Was heavily suppressed by segment count and frequency features.
- `al_n_domains` (content breadth): SHAP 0.095 → **0.258** (+172%). Content diversity is a much stronger standalone signal than it appeared.
- `al_pct_video` / `al_pct_ctv` / `ci_pct_video`: All roughly doubled. CTV/video format signals were absorbed by the EXISTING features.
- `bae_pct_news` (news genre): SHAP 0.056 → **0.091** (+63%). Content genre signal rises significantly.

---

## Three Takeaways

### 1. Our targeting system works.

EXISTING features (ranks 1-3, 5-6, 9, 20, 27) include Fangorn scores, RTC targeting, and segment density. They're the strongest predictors — validates the current system.

### 2. Device diversity and content signals are the most promising new features.

In the NEW-only model (EXISTING removed):
- **Device model diversity** (`wl_n_models`, SHAP 0.598) is the #1 new feature. It's likely a household size proxy — more device models = more devices in the household = more people = higher chance someone visits. This is a genuinely novel signal not in Fangorn today.
- **Content domain breadth** (`al_n_domains`, SHAP 0.258) — IPs that consume more diverse content are more likely to visit. Content consumption patterns carry real signal.
- **CTV/video format signals** (`al_pct_ctv`, `ci_pct_video`, `al_pct_video`) all roughly doubled in importance when EXISTING features were removed.
- **Content genre** (news, entertainment, comedy, drama, sports) — all carry signal but are lower-ranked. These may become more important in a model that includes advertiser-side features (see Known Limitations).

### 3. `ci_pct_new` dominates but may be a data-availability confound.

`ci_pct_new` has the highest SHAP (1.71) and direction is ↓ (higher = fewer visits). IPs with pct_new=1 are by definition IPs we've barely seen — they have thin data everywhere. The model may partly be learning "IPs with thin data don't visit" rather than "new IPs don't visit." Both are true, but only one is actionable for the feature store.

---

## Known Limitations

### 1. Same-day temporal leakage (critical)
Features and labels are both from 2026-03-29. An IP that visited at 8am and received impressions at 9pm has those post-visit impressions counted as "pre-visit features." This inflates AUC. **Fix:** Re-run with features from day N-1 and labels from day N. This is the next model to build.

### 2. augmentor_log and bidder_auction_events use 1-hour samples
augmentor_log uses 12:00-13:00, bidder_auction_events uses the 13:00 partition hour. All other tables use the full 24-hour day. This means:
- `al_n_auctions`, `al_n_domains`, `bae_n_auctions`, etc. are ~1/24th of true daily values
- Ratio features (`al_pct_*`, `bae_pct_*`) are approximately correct (proportions are stable hour to hour)
- The relative importance of count features from these tables is likely understated

### 3. Features are IP-level, not (IP, advertiser)-level
The label is scoped to (IP, advertiser) — did this IP visit THIS advertiser. But all features are IP-level aggregates across all advertisers. The only advertiser-specific features are `n_wins_this_adv` and `n_cgs_this_adv`. The model cannot learn "this IP likes this advertiser's vertical" because the same content features are identical across all advertiser rows for a given IP. To test advertiser-specificity, we'd need advertiser-side features (vertical, category, campaign type) interacted with IP-side features.

### 4. Single day, no confidence intervals
All results are from 2026-03-29. No cross-validation, no bootstrap CIs on SHAP values, no multi-day validation. A feature with SHAP 0.081 vs 0.077 could flip on a different day. Rankings should be treated as directional, not precise.

### 5. Feedback model AUC of 0.999 is tautological
guid_log only fires when an IP visits an advertiser site. So `gl_n_events > 0` perfectly identifies visitors by definition. The 0.999 AUC doesn't mean feedback features "predict" visits — it means they indicate a visit already happened. They're still valuable for retraining and scoring returning visitors, but the AUC number is meaningless.

### 6. fillna(0) conflates "missing" with "zero"
The Python script fills all NaN with 0. For count features this is appropriate (no data = zero events). For ratio/percentage features (`pct_*`, `vcr`, `avg_price`, `avg_amt`), zero is a real value with different meaning than "missing." XGBoost handles NaN natively — a re-run should preserve NaN for ratio features.

### 7. IP-level granularity
All analysis is at IP level. Shared IPs (households, offices, CGNAT) conflate multiple users. Features like device model diversity actually benefit from this — they're measuring household size, which is a valid signal. But precision/recall at the individual user level would be lower than reported.

---

## Next Steps

1. **Fix temporal leakage** — Re-run with features from day N-1, labels from day N. This is the most important methodological fix and may change rankings.
2. **Add advertiser-side features** — Include advertiser vertical, campaign type, and funnel level. Interact with IP-level content features to test whether content genre actually helps match IPs to specific advertisers.
3. **Full-day augmentor_log / bidder_auction_events** — Re-run with 24-hour data (or larger sample) to get accurate count features.
4. **Precision/recall at thresholds** — At 0.95% base rate, AUC alone isn't actionable. Show: if the model selects the top 10% of IPs, what's their visit rate vs baseline?
5. **Multi-day validation** — Run on 3+ days to get confidence intervals on SHAP rankings.
6. **Cold-start analysis** — Test new features specifically on IPs with no existing Fangorn score.
7. **Features not yet modeled** — IAB category percentages (per-category), content_series, parsed identity signals from conversion_log.

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

### How We Decided NEW vs EXISTING

The test: **"Would this feature's value change if we turned off our targeting system?"**

- If **no** — the value comes from an external source (the exchange, the user's device, the content they watch, market pricing). Tagged **NEW**.
- If **yes** — the value is produced by or depends on our own bidding, scoring, or segmentation logic. Tagged **EXISTING**.

| Feature | Tag | Reasoning |
|---------|-----|-----------|
| `ci_pct_new` | EXISTING | Our impression pipeline sets the `is_new` flag. It's our system deciding what counts as "new." |
| `al_avg_segments` | EXISTING | MNTN segments are assigned by our RTC, retargeting, and DS3 pipelines. 97% are 1P. If we turned off targeting, this would be 0. |
| `ci_pct_rtc` | EXISTING | RTC conquest is our model. `advertiser_household_score = 10000` is a flag our system writes. |
| `ci_total_cost` | EXISTING | How much we chose to spend. Our bidder sets the price. |
| `ci_hh_score` | EXISTING | Fangorn's own household-level score. |
| `ci_adv_hh_score` | EXISTING | Fangorn's advertiser-specific score. |
| `n_wins` / `n_wins_this_adv` | EXISTING | How many auctions we won = our bidding decisions. |
| `n_win_adv` / `n_cgs_this_adv` | EXISTING | How many of our advertisers/campaigns target this IP = our targeting config. |
| `ci_n_imp` | EXISTING | Impression count = how many times we chose to serve. |
| `wl_avg_price` | **NEW** | Clearing price is set by the exchange auction, not by us. We submit a bid; the exchange determines the clearing price based on all bidders. |
| `al_pct_pmp` | **NEW** | Whether an auction has a PMP deal is determined by the publisher/exchange, not our system. |
| `al_n_auctions` | **NEW** | How many auctions this IP appears in is driven by the IP's browsing/streaming behavior, not our targeting. |
| `bae_pct_news` | **NEW** | What content the IP watches (news, entertainment, etc.) comes from the exchange's content metadata. Our system doesn't set this. |
| `bae_roku` | **NEW** | Device manufacturer is reported by the exchange from the device itself. |
| `wl_plays` / `wl_completes` | **NEW** | Whether the viewer watches or completes the ad is user behavior — we can't control it. |
| `al_pct_iab` | **NEW** | IAB content categories come from the publisher/exchange taxonomy. |
| `ci_pct_video` | **NEW** | Whether the impression slot is VIDEO or BANNER is the publisher's inventory type, not our choice. |

**FEEDBACK** features don't need this test — they're simply features that only exist in `guid_log` (website pixel) or `conversion_log` (purchase events), both of which only fire when an IP has already visited an advertiser site. They can't be used for targeting because the visit has already happened.
