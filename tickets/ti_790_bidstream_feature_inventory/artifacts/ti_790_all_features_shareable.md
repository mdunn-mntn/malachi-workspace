# All Features Ranked — Complete XGBoost Results

**TI-790** | Malachi Dunn | 2026-04-01
**For:** Kale, Matt, Alex, Ryan, and team

---

## What This Is

We scanned all 25 log tables in MNTN's data stack, identified every unique IP-level feature, built daily snapshot queries for the 6 most valuable tables, joined them into a training dataset of 117,238 real IPs (2026-03-29), and trained XGBoost to predict which IPs would visit an advertiser site.

**Pre-visit model AUC: 0.896** | Base visit rate: 3.4%

Every feature is tagged:
- **EXISTING** — Features derived from our own targeting/scoring system (Fangorn, RTC, retargeting segments). These rank highest because they're our system's own outputs — great validation that what we're building works, but not new signal for the feature store.
- **NEW** — Features from external sources (exchanges, bidstream, user behavior). These are the genuinely new candidates for the feature store.
- **FEEDBACK** — Features from guid_log and conversion_log. Only available after a site visit, so can't be used for targeting new IPs. Valuable for retraining models and scoring returning visitors.

---

## Part 1: EXISTING Features (Our Own System Outputs)

These features dominate raw importance rankings. **This validates that our targeting system works** — Fangorn scores, RTC conquest, retargeting segments, and impression frequency all predict visits well. We investigated the segment data: 97% of `mntn_segments` are 1P (197K RTC segments + 39K retargeting segments).

| Rank | Feature | Source | SHAP | Gain | What It Is |
|------|---------|--------|------|------|-----------|
| 1 | `al_avg_segments` | augmentor_log | **0.986** | 312.6 | Avg MNTN segments on IP. 97% are 1P (RTC + retargeting). Our model's own output — and the single strongest predictor of visits. |
| 2 | `ci_pct_new` | cost_impression_log | **0.670** | 123.9 | % impressions where IP is "new." New IPs visit less; returning IPs are warmer. |
| 3 | `ci_pct_rtc` | cost_impression_log | **0.392** | 47.1 | % RTC-targeted impressions. RTC is designed to find high-intent IPs — and it works. |
| 4 | `ci_total_cost` | cost_impression_log | **0.363** | 33.4 | Total media spend on IP. More spend → more exposure → more visits. Also reflects bidder confidence. |
| 5 | `ci_hh_score` | cost_impression_log | 0.152 | 35.5 | Fangorn household score. The model's own score. -1 = unscored. |
| 6 | `ci_n_imp` | cost_impression_log | 0.078 | 61.3 | # impressions served. Frequency metric. |
| 7 | `n_win_adv` | base | 0.175 | 39.5 | # advertisers targeting this IP. Reflects how many of OUR advertisers want this IP. |
| 8 | `ci_adv_hh_score` | cost_impression_log | — | 29.2 | Advertiser-specific Fangorn score. 10000 = RTC conquest. |
| 9 | `n_wins` | base | 0.070 | 23.6 | Total auction wins. Our bidding activity. |

**Key takeaway:** The top 4 features by SHAP are all our own outputs. Combined SHAP of the 9 EXISTING features = 2.886. This is strong evidence that Fangorn, RTC, and the impression pipeline are working as designed.

---

## Part 2: NEW Features (Genuinely New Signal for Feature Store)

These come from external sources — exchanges, bidstream, user behavior. Not from our own models.

### User Behavior (how viewers interact with our ads)

| New Rank | Feature | Source | SHAP | Gain | What It Is |
|----------|---------|--------|------|------|-----------|
| 1 | `wl_avg_price` | win_logs | 0.231 | 31.9 | **#1 new feature.** Clearing price set by the market. Premium inventory → better audiences. |
| 2 | `wl_n_models` | win_logs | 0.205 | 307.7 | # distinct device models. Multi-device households visit more. Household size proxy. |
| 4 | `wl_pauses` | win_logs | — | 35.8 | # video pauses. Pausing = intentional viewing, not passive. |
| 11 | `wl_n_makes` | win_logs | — | 47.8 | # device manufacturers. Similar to n_models but at brand level. |
| 15 | `wl_completes` | win_logs | — | 30.2 | # video completions. Engagement volume. |
| 19 | `wl_plays` | win_logs | — | 27.9 | # video ad starts. |
| 21 | `wl_clicks` | win_logs | — | 24.0 | # ad clicks. Rare in CTV but meaningful when present. |
| 25 | `wl_viewable` | win_logs | — | 31.1 | # viewable impressions. Ad actually seen. |
| 29 | `wl_vcr` | win_logs | — | 26.5 | Video completion rate. ~1.0 for most CTV; signal is in the variance. |
| 33 | `wl_mutes` | win_logs | — | 16.2 | # video mutes. Audio-off = passive engagement. |
| 34 | `wl_measurable` | win_logs | — | 19.8 | # measurable impressions. Viewability denominator. |

### Exchange/Market Signals (external data from SSPs/exchanges)

| New Rank | Feature | Source | SHAP | Gain | What It Is |
|----------|---------|--------|------|------|-----------|
| 3 | `al_n_auctions` | augmentor_log | 0.228 | 33.1 | # auctions IP appeared in. Active IPs visit more. Market activity. |
| 6 | `al_n_domains` | augmentor_log | — | 31.7 | # distinct content domains. Browsing breadth. |
| 8 | `ci_pct_video` | cost_impression_log | 0.071 | 30.6 | % VIDEO format. CTV vs display — set by exchange. |
| 12 | `al_pct_iab` | augmentor_log | 0.091 | 27.9 | % auctions with IAB categories. Taxonomy data availability. |
| 16 | `al_n_ssps` | augmentor_log | — | 28.5 | # distinct SSPs. More exchanges = more activity. |
| 17 | `al_pct_pmp` | augmentor_log | 0.105 | 28.2 | % PMP deals. Premium curated inventory signal. |
| 26 | `ci_n_vendors` | cost_impression_log | 0.176 | 25.6 | # supply vendors. Supply diversity. |
| 27 | `al_n_networks` | augmentor_log | — | 26.3 | # publishers consumed. Content diversity. |
| 30 | `al_pct_video` | augmentor_log | 0.071 | 25.4 | % VIDEO placement from bidstream. |
| 31 | `al_pct_ctv` | augmentor_log | — | 20.7 | % CTV device type from bidstream. |
| 35 | `al_has_ctv` | augmentor_log | — | 19.7 | Binary CTV flag. |

### Content Signals (what the viewer watches)

| New Rank | Feature | Source | SHAP | Gain | What It Is |
|----------|---------|--------|------|------|-----------|
| 9 | `bae_pct_news` | bidder_auction_events | — | 29.9 | % news genre. **Highest-ranked content signal.** |
| 10 | `bae_pct_ent` | bidder_auction_events | — | 27.3 | % entertainment genre. Dominant but differentiating. |
| 13 | `bae_pct_drama` | bidder_auction_events | — | 26.9 | % drama genre. |
| 14 | `bae_n_pubs` | bidder_auction_events | — | 29.3 | # publishers. Content consumption breadth. |
| 18 | `bae_n_genres` | bidder_auction_events | — | 30.9 | # distinct genres. Eclectic vs focused viewer. |
| 22 | `bae_pct_sports` | bidder_auction_events | — | 23.7 | % sports genre. |
| 23 | `bae_pct_comedy` | bidder_auction_events | — | 24.8 | % comedy genre. |
| 24 | `bae_pct_genre` | bidder_auction_events | — | 27.1 | Genre data fill rate. |
| 7 | `bae_n_auctions` | bidder_auction_events | — | 27.8 | # dropped auctions. Broader market activity. |

### Device Signals (what device they use)

| New Rank | Feature | Source | SHAP | Gain | What It Is |
|----------|---------|--------|------|------|-----------|
| 5 | `bae_lg` | bidder_auction_events | — | 34.1 | Has LG device. **Highest-ranked device signal.** |
| 20 | `bae_roku` | bidder_auction_events | — | 30.7 | Has Roku. Largest CTV platform. |
| 28 | `bae_n_makes` | bidder_auction_events | — | 26.2 | # device manufacturers. Device diversity. |
| 32 | `bae_samsung` | bidder_auction_events | — | 31.5 | Has Samsung. |

---

## Part 3: FEEDBACK Features (Post-Visit — For Retraining & Enrichment)

Available only after a site visit. Can't use for targeting new IPs. Extremely predictive (AUC 0.999 alone) because they're outcome-adjacent.

**Use for:** Retraining Fangorn with richer signals, scoring returning visitors, identity resolution, conversion value segmentation.

| Rank | Feature | Source | SHAP | Gain | What It Is |
|------|---------|--------|------|------|-----------|
| 1 | `gl_n_os_families` | guid_log | **5.255** | 1627.5 | # distinct OS families (Mac/Win/iOS/Android). Multi-device detection. |
| 2 | `gl_n_browser_families` | guid_log | **3.883** | 734.0 | # browser families. Identity resolution signal. |
| 3 | `gl_pct_ip_stable` | guid_log | **2.797** | 45.8 | % events where IP = original_ip. Proxy vs direct connection. |
| 4 | `gl_n_adv` | guid_log | **2.309** | 9.7 | # advertisers visited. Cross-advertiser activity. |
| 5 | `gl_n_events` | guid_log | **1.451** | 7.4 | # pixel events. Visit frequency. |
| 6 | `gl_pct_mobile` | guid_log | 0.647 | 26.7 | % mobile events. Mobile vs desktop behavior. |
| 7 | `gl_pct_new` | guid_log | 0.373 | 4.9 | % "new" visits. New vs returning ratio. |
| 8 | `cv_n_orders` | conversion_log | 0.266 | 10.7 | # distinct orders. Repeat purchaser signal. |
| 9 | `cv_n_conv` | conversion_log | 0.214 | 7.9 | # conversions. Converter flag. |
| 10 | `cv_avg_amt` | conversion_log | 0.206 | 4.5 | Average order value ($). Spending tier. |
| 11 | `cv_total_amt` | conversion_log | 0.129 | 6.2 | Total order value ($). Lifetime spend. |
| 12 | `cv_n_types` | conversion_log | — | 38.9 | # conversion types. Diverse converter (purchase + signup + call). |
| 13 | `gl_has_mobile` | guid_log | — | 40.3 | Mobile device presence flag. |
| 14 | `cv_n_adv` | conversion_log | — | 5.8 | # advertisers converted on. Cross-advertiser converter. |
| 15 | `gl_n_product_views` | guid_log | — | 3.8 | # product page views. Purchase intent. |
| 16 | `gl_has_tablet` | guid_log | — | 3.8 | Tablet presence. |
| 17 | `gl_has_new_visit` | guid_log | — | 2.8 | Any "new" visit flag. |

---

## Part 4: Zero-Importance Features (Drop)

| Feature | Source | Category | Why Zero |
|---------|--------|----------|----------|
| `wl_skips` | win_logs | Pre-visit | CTV has no skip button. Always 0. |
| `wl_viewability` | win_logs | Pre-visit | ~100% for all CTV IPs. No variance to learn from. |
| `wl_invalid` | win_logs | Pre-visit | Nearly zero IVT flags in this sample. |
| `gl_has_desktop` | guid_log | Feedback | 0% fill in this sample. |
| `gl_n_utm_events` | guid_log | Feedback | UTM params rarely present. |

---

## Summary

| Category | # Features | AUC Contribution | Best Feature |
|----------|-----------|-----------------|-------------|
| **EXISTING** (our system) | 9 | Dominates raw rankings | al_avg_segments (SHAP 0.986) |
| **NEW** (feature store candidates) | 35 | AUC 0.896 with all pre-visit | wl_avg_price (clearing price) |
| **FEEDBACK** (post-visit enrichment) | 17 | AUC 0.999 alone | gl_n_os_families (SHAP 5.255) |
| **ZERO** (drop) | 5 | None | — |
| **Total** | **66** | | |

### Data Sources

| Table | Features | Tag Mix | Key Signal |
|-------|----------|---------|------------|
| **win_logs** | 14 | 11 NEW, 3 ZERO | Video engagement, clearing price, device diversity |
| **bidder_auction_events** | 13 | 13 NEW | Content genre, device make |
| **augmentor_log** | 10 | 9 NEW, 1 EXISTING | Auction activity, PMP deals, IAB categories, SSP diversity |
| **cost_impression_log** | 8 | 3 NEW, 5 EXISTING | CTV/video format, supply vendors |
| **guid_log** | 13 | 11 FEEDBACK, 2 ZERO | Device fingerprinting, product views |
| **conversion_log** | 6 | 6 FEEDBACK | Order value, conversion types |
| **base** | 2 | 2 EXISTING | Win count, advertiser count |

### Methodology
- XGBoost classifier, 300 trees, max_depth 6
- 117,238 IPs from 2026-03-29 (1% deterministic sample of IPs served impressions)
- Label: visited advertiser site (clickpass_log) = 1, else 0
- 3 importance metrics (gain, frequency, cover) → composite rank
- SHAP values on 5,000-IP test sample
- Iterative paring: AUC stable at 0.896 with as few as 5 features

### Files
- Training data: `outputs/ti_790_training_data.csv` (117K rows, 66 features)
- Python script: `artifacts/ti_790_xgboost_split_analysis.py`
- SHAP plot: `outputs/ti_790_shap_pre_visit.png`
- All rankings: `outputs/ti_790_all_features_ranked.csv`
