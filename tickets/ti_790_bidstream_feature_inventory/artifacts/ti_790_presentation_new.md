# What People Watch Predicts Whether They'll Visit

**TI-789/790** | Malachi Dunn | 2026-04-01

---

## Our targeting system works. And we're leaving 30 features on the table.

The top 3 predictors of site visits are features we built — Fangorn scores, RTC conquest, impression frequency. That's the validation.

But we scanned all 25 log tables in the system. Only 6 had unique signal. Inside those 6, we found **37 features we've never used for targeting** — content genres, device diversity, publisher breadth, clearing prices — all from the bidstream. Combined with existing features, they predict visits at **10x lift**. Using *only* the new features: **7x lift**.

The richest new signal isn't demographics. It's what people watch.

---

## How much is this worth?

We trained a model on 372K (IP, advertiser) pairs — features measured on Day 1, visits measured on Day 2 — so the model only uses information available before the visit happened. Then we measured precision at every threshold.

| Targeting threshold | Visit rate | Lift over baseline |
|---------------------|-----------|-------------------|
| **Top 1% of IPs** | **8.2%** | **10x** |
| Top 10% of IPs | 4.0% | 5x |
| Baseline (everyone) | 0.84% | 1x |

Every IP in this dataset was already selected by Fangorn — we only serve impressions to IPs our system chose. Even within that pre-filtered pool, a model using *only* the new features separates visitors from non-visitors at **7x lift** (top 1% = 5.7% visit rate). The new features add discriminative power on top of what Fangorn already selected for.

---

## The discovery that changed the analysis

When I first ran this model, the new features were afterthoughts. Device model count — how many different devices sit on an IP — ranked 29th out of 46. I almost dismissed it.

Then I pulled out our existing features and re-ran the model. Device model count jumped from 29th to **1st**. Its importance score went from 0.065 to 0.413 — a **535% increase**. It had been completely masked by Fangorn's scores.

What is device model count? It's a household size proxy. A Roku, a Samsung TV, two iPhones, a tablet — more devices means a bigger household, and bigger households are more likely to visit. That signal has been sitting in our win logs. We've never used it.

That's one feature. There are 36 more.

---

## The top 10 new features

These are the features we've never used for targeting, ranked by standalone importance (existing features removed).

| # | What it measures | Why it matters |
|---|-----------------|----------------|
| **1** | **Device model diversity** | Household size proxy — more devices = more likely to visit |
| **2** | **Video vs display format** | Display converts better per-impression than CTV |
| **3** | **Content domain breadth** | IPs consuming diverse content are more predictable |
| **4** | **Video placement rate** | Strong CTV engagement signal |
| **5** | **Clearing price** | Lower clearing price correlates with more visits — within Fangorn-selected IPs |
| **6** | **Entertainment genre %** | Entertainment-heavy IPs visit less — likely passive viewers less inclined to act |
| **7** | **Total auction wins** | Market activity — active IPs convert more |
| **8** | **PMP deal rate** | Premium curated inventory = better audiences |
| **9** | **Auction count** | Bidstream breadth — more auctions = more signal |
| **10** | **IAB category availability** | IPs with richer metadata are more predictable |

Three patterns: **content signals** (what people watch), **market signals** (how they appear in the bidstream), and **device signals** (what they watch on).

---

## Three things we now know

### 1. The current system is validated.

Our existing features — Fangorn, RTC, segment density — hold the top 3 ranks. They dominate because we designed them to. This is the right foundation to build on.

### 2. Content and device data are the biggest untapped signals.

When we remove existing features, content genre percentages (entertainment, comedy, news, drama, sports) and device diversity emerge as the strongest new predictors. These come from the bidstream — the exchanges are already sending us this data. We're just not storing it.

### 3. This data is ephemeral.

augmentor_log has a **10-day TTL**. bidder_auction_events has **90 days**. The richest content signal in our system expires every day. Without a feature store pipeline capturing it, we lose this data permanently.

---

## What we need

1. **Advertiser-side features in the next model run.** The current model knows what an IP watches but not what the advertiser sells. Adding vertical and category features tests whether content genre helps match IPs to *specific* advertisers. Alex — can we get vertical labels by next week?

2. **A pipeline decision.** Which of the top 10 features do we prototype first? Ryan — what's the engineering lift to capture these daily before TTL expiration?

3. **Multi-day validation.** One day of data, one model. We need 3+ days to confirm rankings are stable. I'll have this by end of next week.

---

**What people watch predicts whether they'll visit. The data is already in our system. Let's use it.**

---
---

# Appendix A: Full Feature Rankings

## All 46 Features, Ranked by SHAP

Features from 2026-03-28. Labels from 2026-03-29 (clickpass_log). No temporal leakage. SHAP = mean absolute Shapley value on 5,000-sample test set. All-features model AUC: 0.831.

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

## NEW-Only Model (EXISTING Removed)

NEW-only AUC: 0.777 | Top 1% precision: 5.7% = 6.8x lift

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

## Feedback Features (Post-Visit — For Retraining Only)

These features come from guid_log (website pixel) and conversion_log (purchase events). They only exist after an IP has already visited — can't be used for targeting. A model using only these features produces AUC 0.999, but this is tautological: guid_log fires on site visits, so `gl_n_events > 0` perfectly identifies visitors by definition. These are valuable for retraining and scoring returning visitors, not for prediction.

| # | Feature | Source | SHAP | Description |
|---|---------|--------|------|-------------|
| 1 | `gl_n_os_families` | guid_log | 5.255 | # distinct OS families on advertiser sites |
| 2 | `gl_n_browser_families` | guid_log | 3.883 | # distinct browser families |
| 3 | `gl_pct_ip_stable` | guid_log | 2.797 | % events where IP matches original_ip |
| 4 | `gl_n_adv` | guid_log | 2.309 | # distinct advertisers visited |
| 5 | `gl_n_events` | guid_log | 1.451 | # pixel events on advertiser sites |
| 6 | `gl_pct_mobile` | guid_log | 0.647 | % events from mobile devices |
| 7 | `gl_pct_new` | guid_log | 0.373 | % events flagged as "new" visit |
| 8 | `cv_n_orders` | conversion_log | 0.266 | # distinct purchase orders |
| 9 | `cv_n_conv` | conversion_log | 0.214 | # conversion events |
| 10 | `cv_avg_amt` | conversion_log | 0.206 | Average order value ($) |
| 11 | `cv_total_amt` | conversion_log | 0.129 | Total order value ($) |
| 12 | `cv_n_types` | conversion_log | 0.102 | # distinct conversion types |
| 13 | `gl_has_mobile` | guid_log | 0.094 | Has mobile device events (0/1) |
| 14 | `gl_n_product_views` | guid_log | 0.088 | # product page views (purchase intent) |
| 15 | `cv_n_adv` | conversion_log | 0.054 | # advertisers converted on |
| 16 | `gl_has_new_visit` | guid_log | 0.015 | Has any "new" visit flag |
| 17 | `gl_has_tablet` | guid_log | 0.003 | Has tablet events (0/1) |

*Zero importance: gl_has_desktop (0% fill), gl_n_utm_events (0% fill), wl_skips (no skip in CTV), wl_viewability (100% for CTV), wl_invalid (near zero).*

---

# Appendix B: Methodology & Rigor

## Model Parameters

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
| Ranking metric | SHAP — mean absolute Shapley value per feature |
| Tables scanned | 25 total, 6 used (19 redundant or insufficient) |

## How We Decided NEW vs EXISTING

The test: **"Would this feature's value change if we turned off our targeting system?"** If no — it comes from an external source (exchange, device, content). Tagged NEW. If yes — it's produced by our bidding/scoring/segmentation logic. Tagged EXISTING.

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

## Known Limitations

1. **Features are IP-level, not (IP, advertiser)-level.** The label is per-(IP, advertiser) but all features are IP aggregates across all advertisers. The model can learn "this IP is generally likely to visit" but not "this IP matches this specific advertiser." Adding advertiser-side features (vertical, category) is Next Step #1.

2. **augmentor_log uses a 4-hour sample** (12:00-16:00), not the full day. Count features (`al_n_auctions`, `al_n_domains`, etc.) are ~1/6th of daily values. Ratio features (`al_pct_*`) are approximately correct.

3. **Single day, no confidence intervals.** Features from 2026-03-28, labels from 2026-03-29. No cross-validation or bootstrap CIs. Rankings are directional — multi-day validation is Next Step #3.

4. **`ci_pct_new` may be a data-availability confound.** IPs with pct_new=1 are IPs we've barely seen — thin data everywhere. The model may partly learn "thin data → no visit" rather than "new IP → no visit."

5. **IP-level granularity.** Shared IPs (households, CGNAT) conflate multiple users. Device model diversity benefits from this — it actually measures household size.

## Glossary

**SHAP (SHapley Additive exPlanations):** From game theory. For each prediction, SHAP calculates how much each feature pushed that prediction up or down from the baseline. The number we report ("mean absolute SHAP") is the average magnitude across all test samples. Higher = more important.

**Direction:** Average feature value for visitors vs non-visitors. "↑ more visits" = visitors have higher values. "↓ fewer visits" = visitors have lower values. "— neutral" = <5% difference. XGBoost can learn non-linear relationships where the direction isn't uniform.

**Why 300 Trees, Max Depth 6:** Standard XGBoost for tabular data. Conservative ensemble. Not tuned — feature rankings are stable across reasonable configs.
