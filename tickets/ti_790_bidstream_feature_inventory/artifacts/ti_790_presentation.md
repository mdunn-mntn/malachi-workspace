# Feature Store: What Data Do We Have and What Predicts Visits?

**TI-789/790** | Malachi Dunn | 2026-04-01

---

## The Question

For a given advertiser's campaign, which IP-level features predict whether that IP will visit that advertiser's site?

## What We Did

1. **Scanned all 25 log tables** in the system. Identified unique columns per table programmatically.
2. **6 tables have unique signal** — the rest are redundant. Built daily snapshot queries for each.
3. **Joined into a training dataset** — 372,379 (IP, advertiser) pairs. Features from 2026-03-28. Labels (visited THIS advertiser?) from 2026-03-29. No temporal leakage.
4. **Trained XGBoost** (300 trees, max_depth 6) to predict per-advertiser visits.
5. **Ranked features by SHAP** — mean absolute Shapley value on 5,000-sample test set.
6. **Ran two models:** all features (EXISTING + NEW) and NEW-only (EXISTING removed).

**All-features AUC: 0.831** | **NEW-only AUC: 0.777** | Visit rate: 0.84% | Test set: 74,476 pairs

### How useful is this model?

| If we target the... | Visit rate | Lift vs baseline | N (test set) |
|---------------------|-----------|-----------------|-------------|
| Top 1% of IPs | 8.2% | **9.8x** | 744 |
| Top 5% of IPs | 5.6% | **6.7x** | 3,723 |
| Top 10% of IPs | 4.0% | **4.8x** | 7,447 |
| Top 20% of IPs | 2.9% | **3.5x** | 14,895 |
| Everyone (baseline) | 0.84% | 1.0x | 74,476 |

---

## What We Found

- **EXISTING:** Our own system outputs (Fangorn scores, RTC, segments, frequency). They rank highest because we designed them to. **Validates the current system.** Not new signal.
- **NEW:** External data from exchanges, bidstream, user behavior. **Feature store candidates.**
- Tags are preliminary — to be validated by the team.

---

## All Features, Ranked by SHAP

Features from day N-1, labels from day N. No temporal leakage. See [Glossary](#glossary) for metric definitions.

| # | Feature | Source | Tag | SHAP | Direction | Description |
|---|---------|--------|-----|------|-----------|-------------|
| 1 | `ci_pct_new` | cost_impression_log | EXISTING | 1.434 | ↓ fewer visits | % impressions where IP is "new" (first impression) |
| 2 | `n_wins_this_adv` | base | EXISTING | 0.293 | ↑ more visits | # wins for THIS advertiser |
| 3 | `al_avg_segments` | augmentor_log | EXISTING | 0.223 | ↑ more visits | Avg MNTN segments on IP (97% are 1P) |
| **4** | **`al_pct_video`** | **augmentor_log** | **NEW** | **0.219** | **↑ more visits** | **% VIDEO placement in auctions** |
| **5** | **`al_pct_ctv`** | **augmentor_log** | **NEW** | **0.211** | **— neutral** | **% CTV device type in auctions** |
| **6** | **`wl_avg_price`** | **win_logs** | **NEW** | **0.205** | **↓ fewer visits** | **Clearing price per auction (USD)** |
| **7** | **`al_n_domains`** | **augmentor_log** | **NEW** | **0.185** | **↑ more visits** | **# distinct content domains consumed** |
| **8** | **`bae_pct_ent`** | **bidder_auction_events** | **NEW** | **0.185** | **↓ fewer visits** | **% content = entertainment genre** |
| **9** | **`bae_pct_genre`** | **bidder_auction_events** | **NEW** | **0.177** | **↑ more visits** | **% auctions with any genre data** |
| **10** | **`al_n_auctions`** | **augmentor_log** | **NEW** | **0.174** | **↑ more visits** | **# auctions this IP appeared in** |
| **11** | **`al_pct_iab`** | **augmentor_log** | **NEW** | **0.173** | **↑ more visits** | **% auctions with IAB category data** |
| 12 | `ci_total_cost` | cost_impression_log | EXISTING | 0.167 | ↑ more visits | Total media $ spent on this IP |
| **13** | **`al_pct_pmp`** | **augmentor_log** | **NEW** | **0.156** | **↑ more visits** | **% auctions with PMP deals** |
| **14** | **`ci_pct_video`** | **cost_impression_log** | **NEW** | **0.155** | **↓ fewer visits** | **% VIDEO format impressions** |
| 15 | `ci_pct_rtc` | cost_impression_log | EXISTING | 0.147 | ↑ more visits | % RTC conquest impressions |
| **16** | **`bae_pct_comedy`** | **bidder_auction_events** | **NEW** | **0.142** | **↑ more visits** | **% content = comedy genre** |
| **17** | **`al_n_networks`** | **augmentor_log** | **NEW** | **0.124** | **↑ more visits** | **# distinct networks/publishers** |
| **18** | **`bae_n_auctions`** | **bidder_auction_events** | **NEW** | **0.117** | **↑ more visits** | **# dropped auctions** |
| **19** | **`wl_n_adv`** | **win_logs** | **NEW** | **0.109** | **↑ more visits** | **# distinct advertisers serving this IP** |
| **20** | **`bae_pct_drama`** | **bidder_auction_events** | **NEW** | **0.109** | **↑ more visits** | **% content = drama genre** |
| 21 | `ci_hh_score` | cost_impression_log | EXISTING | 0.108 | ↑ more visits | Fangorn household score |
| **22** | **`bae_n_genres`** | **bidder_auction_events** | **NEW** | **0.108** | **↑ more visits** | **# distinct content genres watched** |
| **23** | **`bae_pct_news`** | **bidder_auction_events** | **NEW** | **0.101** | **↑ more visits** | **% content = news genre** |
| **24** | **`bae_n_makes`** | **bidder_auction_events** | **NEW** | **0.081** | **↑ more visits** | **# device manufacturers (bidstream)** |
| 25 | `n_cgs_this_adv` | base | EXISTING | 0.081 | ↑ more visits | # campaign groups for this advertiser |
| **26** | **`bae_n_pubs`** | **bidder_auction_events** | **NEW** | **0.076** | **↑ more visits** | **# distinct publishers consumed** |
| **27** | **`wl_n_wins`** | **win_logs** | **NEW** | **0.075** | **↑ more visits** | **Total auction wins** |
| **28** | **`bae_pct_sports`** | **bidder_auction_events** | **NEW** | **0.068** | **↑ more visits** | **% content = sports genre** |
| **29** | **`wl_n_models`** | **win_logs** | **NEW** | **0.065** | **↑ more visits** | **# distinct device models** |
| **30** | **`ci_n_vendors`** | **cost_impression_log** | **NEW** | **0.064** | **↑ more visits** | **# distinct supply vendors** |
| 31 | `ci_n_imp` | cost_impression_log | EXISTING | 0.064 | ↑ more visits | # impressions served |
| **32** | **`wl_plays`** | **win_logs** | **NEW** | **0.061** | **↑ more visits** | **# video ad plays** |
| **33** | **`al_n_ssps`** | **augmentor_log** | **NEW** | **0.061** | **↑ more visits** | **# distinct SSPs/exchanges** |
| **34** | **`wl_completes`** | **win_logs** | **NEW** | **0.052** | **↑ more visits** | **# video ad completions** |
| **35** | **`wl_n_makes`** | **win_logs** | **NEW** | **0.048** | **↑ more visits** | **# distinct device manufacturers** |
| **36** | **`wl_vcr`** | **win_logs** | **NEW** | **0.047** | **— neutral** | **Video completion rate** |
| **37** | **`bae_samsung`** | **bidder_auction_events** | **NEW** | **0.043** | **↑ more visits** | **Has Samsung Smart TV (0/1)** |
| **38** | **`wl_viewable`** | **win_logs** | **NEW** | **0.033** | **↑ more visits** | **# viewable impressions** |
| **39** | **`bae_roku`** | **bidder_auction_events** | **NEW** | **0.024** | **↑ more visits** | **Has Roku device (0/1)** |
| 40 | `ci_adv_hh_score` | cost_impression_log | EXISTING | 0.016 | ↓ fewer visits | Fangorn advertiser score |
| **41** | **`bae_lg`** | **bidder_auction_events** | **NEW** | **0.014** | **↑ more visits** | **Has LG Smart TV (0/1)** |
| **42** | **`wl_measurable`** | **win_logs** | **NEW** | **0.013** | **↑ more visits** | **# measurable impressions** |
| **43** | **`wl_mutes`** | **win_logs** | **NEW** | **0.004** | **↑ more visits** | **# video mutes** |
| **44** | **`al_has_ctv`** | **augmentor_log** | **NEW** | **0.004** | **↑ more visits** | **Has CTV device** |
| **45** | **`wl_pauses`** | **win_logs** | **NEW** | **0.004** | **↑ more visits** | **# video pauses** |
| **46** | **`wl_clicks`** | **win_logs** | **NEW** | **0.003** | **↑ more visits** | **# ad clicks** |

---

## NEW Features Only — Without EXISTING in the Model

EXISTING features absorb signal. Removing them shows each NEW feature's standalone importance.

**NEW-only AUC: 0.777** | Top 1% precision: 5.7% = **6.8x lift**

| # | Feature | Source | SHAP | SHAP (all) | Direction | Description |
|---|---------|--------|------|------------|-----------|-------------|
| **1** | **`wl_n_models`** | **win_logs** | **0.413** | 0.065 | ↑ more visits | # device models — household size proxy |
| **2** | **`ci_pct_video`** | **cost_impression_log** | **0.341** | 0.155 | ↓ fewer visits | % VIDEO format (display converts better per-impression) |
| **3** | **`al_n_domains`** | **augmentor_log** | **0.320** | 0.185 | ↑ more visits | Content consumption breadth |
| **4** | **`al_pct_video`** | **augmentor_log** | **0.253** | 0.219 | ↑ more visits | % VIDEO placement in auctions |
| **5** | **`wl_avg_price`** | **win_logs** | **0.245** | 0.205 | ↓ fewer visits | Clearing price — cheaper inventory converts better |
| **6** | **`bae_pct_ent`** | **bidder_auction_events** | **0.238** | 0.185 | ↓ fewer visits | % entertainment genre |
| **7** | **`wl_n_wins`** | **win_logs** | **0.237** | 0.075 | ↑ more visits | Total auction wins |
| **8** | **`al_pct_pmp`** | **augmentor_log** | **0.219** | 0.156 | ↑ more visits | PMP deal rate — premium inventory |
| **9** | **`al_n_auctions`** | **augmentor_log** | **0.189** | 0.174 | ↑ more visits | Market activity |
| **10** | **`al_pct_iab`** | **augmentor_log** | **0.185** | 0.173 | ↑ more visits | IAB category data availability |
| 11 | `bae_pct_genre` | bidder_auction_events | 0.178 | 0.177 | ↑ more visits | Genre data availability |
| 12 | `al_pct_ctv` | augmentor_log | 0.171 | 0.211 | — neutral | % CTV device type |
| 13 | `bae_n_pubs` | bidder_auction_events | 0.158 | 0.076 | ↑ more visits | Publisher diversity |
| 14 | `wl_n_adv` | win_logs | 0.155 | 0.109 | ↑ more visits | Advertiser diversity |
| 15 | `bae_pct_comedy` | bidder_auction_events | 0.142 | 0.142 | ↑ more visits | % comedy genre |
| 16 | `bae_n_genres` | bidder_auction_events | 0.125 | 0.108 | ↑ more visits | Genre diversity |
| 17 | `bae_n_auctions` | bidder_auction_events | 0.124 | 0.117 | ↑ more visits | Broader activity |
| 18 | `al_n_networks` | augmentor_log | 0.113 | 0.124 | ↑ more visits | Network diversity |
| 19 | `bae_pct_news` | bidder_auction_events | 0.111 | 0.101 | ↑ more visits | % news genre |
| 20 | `bae_n_makes` | bidder_auction_events | 0.105 | 0.081 | ↑ more visits | Device make diversity |
| 21 | `bae_pct_drama` | bidder_auction_events | 0.099 | 0.109 | ↑ more visits | % drama genre |
| 22 | `al_n_ssps` | augmentor_log | 0.082 | 0.061 | ↑ more visits | SSP diversity |
| 23 | `wl_completes` | win_logs | 0.079 | 0.052 | ↑ more visits | Video completions |
| 24 | `bae_pct_sports` | bidder_auction_events | 0.073 | 0.068 | ↑ more visits | % sports genre |
| 25 | `ci_n_vendors` | cost_impression_log | 0.069 | 0.064 | ↑ more visits | Supply vendor diversity |
| 26 | `wl_viewable` | win_logs | 0.066 | 0.033 | ↑ more visits | Viewable impressions |
| 27 | `wl_plays` | win_logs | 0.066 | 0.061 | ↑ more visits | Video plays |
| 28 | `bae_samsung` | bidder_auction_events | 0.066 | 0.043 | ↑ more visits | Has Samsung |
| 29 | `wl_vcr` | win_logs | 0.059 | 0.047 | — neutral | Video completion rate |
| 30 | `wl_n_makes` | win_logs | 0.052 | 0.048 | ↑ more visits | Device make diversity |
| 31 | `bae_roku` | bidder_auction_events | 0.025 | 0.024 | ↑ more visits | Has Roku |
| 32 | `bae_lg` | bidder_auction_events | 0.022 | 0.014 | ↑ more visits | Has LG |
| 33 | `wl_measurable` | win_logs | 0.019 | 0.013 | ↑ more visits | Measurable impressions |
| 34 | `al_has_ctv` | augmentor_log | 0.010 | 0.004 | ↑ more visits | Has CTV device |
| 35 | `wl_mutes` | win_logs | 0.009 | 0.004 | ↑ more visits | Video mutes |
| 36 | `wl_pauses` | win_logs | 0.004 | 0.004 | ↑ more visits | Video pauses |
| 37 | `wl_clicks` | win_logs | 0.003 | 0.003 | ↑ more visits | Ad clicks |

---

## Three Takeaways

### 1. Our targeting system works.

EXISTING features (ranks 1-3, 12, 15, 21, 25, 31, 40) dominate the all-features model. This validates that Fangorn, RTC, and the impression pipeline are directionally correct.

### 2. Device diversity and content signals are the most promising new features.

In the NEW-only model:
- **Device model diversity** (`wl_n_models`, SHAP 0.413) — the #1 new feature. Likely a household size proxy: more device models = more devices = bigger household = more likely someone visits.
- **Content domain breadth** (`al_n_domains`, SHAP 0.320) — IPs consuming more diverse content are more predictable.
- **CTV/video format** (`al_pct_video`, `ci_pct_video`) — strong signal. Interestingly, higher video % = fewer visits per-impression, meaning display converts at higher per-impression rates.
- **Content genre** — entertainment, comedy, news, drama, sports all carry signal. `bae_pct_ent` has ↓ direction in the all-features model, meaning entertainment-heavy IPs visit less (after controlling for other features).

### 3. Rankings are stable after temporal correction.

V1 (same-day features+labels) → V2 (day N-1 features, day N labels): AUC dropped 0.011 (0.842 → 0.831). Rankings barely shifted. The leakage was real but small — the findings hold.

---

## Known Limitations

1. **Features are IP-level, not (IP, advertiser)-level.** The label is per-(IP, advertiser) but all features are IP aggregates across all advertisers. The model can learn "this IP is generally likely to visit" but not "this IP matches this specific advertiser." To test advertiser-specificity, we need advertiser-side features (vertical, category) interacted with IP content features.

2. **augmentor_log uses a 4-hour sample** (12:00-16:00), not the full day. Count features (`al_n_auctions`, `al_n_domains`, etc.) are ~1/6th of daily values. Ratio features (`al_pct_*`) are approximately correct.

3. **Single day, no confidence intervals.** Features from 2026-03-28, labels from 2026-03-29. No cross-validation or bootstrap CIs. Rankings are directional, not precise.

4. **`ci_pct_new` may be a data-availability confound.** IPs with pct_new=1 are IPs we've barely seen — thin data everywhere. The model may partly learn "thin data → no visit" rather than "new IP → no visit."

5. **Feedback model AUC of 0.999 is tautological.** guid_log only fires on site visits, so `gl_n_events > 0` perfectly identifies visitors by definition. Feedback features are valuable for retraining — the AUC number is not meaningful.

6. **IP-level granularity.** Shared IPs (households, CGNAT) conflate multiple users. Device model diversity benefits from this — it actually measures household size.

---

## Next Steps

1. **Add advertiser-side features** — Vertical, category, funnel level interacted with IP content features. This tests whether content genre helps match IPs to specific advertisers.
2. **Multi-day validation** — Run on 3+ days to get confidence intervals on rankings.
3. **Cold-start analysis** — Test NEW features on IPs with no existing Fangorn score (`ci_hh_score = -1`).
4. **1P vs 3P segment split** — DS3 interest segments cover 1.3B IPs. Isolating 3P count may be a strong new feature.
5. **Features not yet modeled** — IAB category percentages (per-category), content_series (show names), parsed identity signals from conversion_log.

---

## Methodology

| Parameter | Value |
|-----------|-------|
| Model | XGBoost classifier |
| Trees / Max depth / LR | 300 / 6 / 0.1 |
| Subsample / Col sample | 0.8 / 0.8 |
| Class weight | scale_pos_weight = 118 (balanced for 0.84% visit rate) |
| Train/test split | 80/20, stratified |
| Training rows | 297,903 (IP, advertiser) pairs |
| Test rows | 74,476 (IP, advertiser) pairs |
| **Feature date** | **2026-03-28 (day N-1)** |
| **Label date** | **2026-03-29 clickpass_log (day N)** |
| **Temporal leakage** | **None — features strictly before labels** |
| Sample method | 1% of IPs (FARM_FINGERPRINT MOD 100), expanded to all advertiser pairs |
| IP filtering | Excluded 0.0.0.0, 127.0.0.1, >10K wins (proxy/CDN) |
| Advertiser ID mapping | win_logs Beeswax IDs → MNTN IDs via `campaigns` table |
| augmentor_log sample | 4 hours (12:00-16:00) — ratio features accurate, count features ~1/6 of daily |
| bidder_auction_events | Full day (2.95B rows aggregated to IP level) |
| NaN handling | Count features: fillna(0). Ratio features: NaN preserved (XGBoost native) |
| **Ranking metric** | **SHAP — mean absolute Shapley value per feature** |
| Tables scanned | 25 total, 6 used (19 redundant or insufficient) |

---

## Glossary

### SHAP (SHapley Additive exPlanations)
From game theory. For each prediction, SHAP calculates how much each feature pushed that prediction up or down from the baseline, considering every possible combination of features. The number we report ("mean absolute SHAP") is the average magnitude of those contributions across all test samples. Higher = more important. The table is sorted by this metric, descending.

### Direction
Computed by comparing the average feature value for IPs that visited vs IPs that did not. "↑ more visits" means visitors have higher values. "↓ fewer visits" means visitors have lower values. "— neutral" means <5% difference. This is a population-level signal — XGBoost can learn non-linear relationships where the direction isn't uniform.

### How We Decided NEW vs EXISTING
The test: **"Would this feature's value change if we turned off our targeting system?"** If no — it comes from an external source (exchange, device, content). Tagged NEW. If yes — it's produced by our bidding/scoring/segmentation logic. Tagged EXISTING. Full per-feature reasoning:

| Feature | Tag | Reasoning |
|---------|-----|-----------|
| `ci_pct_new` | EXISTING | Our pipeline sets the `is_new` flag |
| `al_avg_segments` | EXISTING | MNTN segments from RTC + retargeting. 97% are 1P. |
| `n_wins_this_adv` | EXISTING | How many impressions we chose to serve |
| `ci_pct_rtc` | EXISTING | RTC is our conquest model |
| `ci_total_cost` | EXISTING | Our spending decision |
| `ci_hh_score` | EXISTING | Fangorn's own score |
| `ci_adv_hh_score` | EXISTING | Fangorn advertiser score |
| `n_cgs_this_adv` | EXISTING | Our campaign configuration |
| `ci_n_imp` | EXISTING | Our impression frequency |
| `wl_avg_price` | NEW | Clearing price set by exchange auction |
| `al_pct_pmp` | NEW | PMP deals determined by publisher/exchange |
| `bae_pct_news` | NEW | Content metadata from exchange |
| `wl_n_models` | NEW | Device model from exchange |
| `wl_plays` | NEW | User behavior — we can't control it |

### Why 300 Trees, Max Depth 6
Standard XGBoost for tabular data. 300 trees × 0.1 learning rate = conservative ensemble. Max depth 6 allows interactions between up to 6 features. Not tuned — feature rankings are stable across reasonable configs.
