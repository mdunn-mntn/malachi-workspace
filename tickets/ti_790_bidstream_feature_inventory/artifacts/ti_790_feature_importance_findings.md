# Feature Importance Findings — Every Feature Ranked

**TI-790** | Malachi Dunn | 2026-03-31
**For:** Alex Knorr, Ryan Kleck, Matt Brorby

---

## How This Was Done

Trained XGBoost on **117,238 real IPs** (2026-03-29) to predict site visits. Joined 6 tables on IP, extracted ~60 features. Importance measured three ways (gain, frequency, cover) then averaged into a composite rank. SHAP values provide per-feature contribution magnitude.

**Pre-visit model AUC: 0.896** | Base visit rate: 3.4%

---

## Critical Distinction: Which Features Are Actually New?

Many of the top-ranking features are **our own model outputs feeding back in** — segment density, Fangorn scores, RTC targeting, impression frequency. Of course they predict visits; we designed them to.

We investigated the segment data: **97% of mntn_segments are 1P** (197K RTC segments + 39K retargeting segments). Only DS3 interest segments (175 segments, but covering 1.3B IPs with ~20 segments each) are truly 3P external data.

| Tag | Meaning | Count |
|-----|---------|-------|
| **EXISTING** | Our own model/system outputs — circular, not new signal | 9 features |
| **NEW** | External data from exchanges, bidstream, user behavior — genuinely new | 35 features |
| ZERO | No importance in model | 3 features |
| FEEDBACK | Post-visit only (guid_log, conversion_log) | 19 features |

---

## The 35 Genuinely New Features, Ranked

These are features that come from **external sources** (exchanges, bidstream, user behavior) — not from our own targeting system. Ranked by XGBoost composite importance.

### User Behavior (how viewers interact with our ads)

| New Rank | Feature | Source | Gain | What It Is | What the Model Found |
|----------|---------|--------|------|-----------|---------------------|
| 1 | `wl_avg_price` | win_logs | 31.9 | Clearing price paid (USD) | **#1 new feature.** Premium inventory → better audiences. Price is set by the market, not us. |
| 2 | `wl_n_models` | win_logs | 307.7 | # distinct device models | Multi-device households visit more. Household size/diversity proxy. |
| 4 | `wl_pauses` | win_logs | 35.8 | # video pauses | Active engagement — pausing means watching intentionally, not passively. |
| 11 | `wl_n_makes` | win_logs | 47.8 | # device manufacturers | Device diversity. Similar to n_models but at brand level. |
| 15 | `wl_completes` | win_logs | 30.2 | # video completions | Total video engagement volume. |
| 19 | `wl_plays` | win_logs | 27.9 | # video plays | Video ad starts. |
| 21 | `wl_clicks` | win_logs | 24.0 | # ad clicks | Direct response (rare in CTV but meaningful when present). |
| 25 | `wl_viewable` | win_logs | 31.1 | # viewable impressions | Ad actually seen by the viewer. |
| 29 | `wl_vcr` | win_logs | 26.5 | Video completion rate | % of plays completed. Most CTV is ~1.0; signal is in the outliers. |
| 33 | `wl_mutes` | win_logs | 16.2 | # video mutes | Audio-off viewing. Passive vs active engagement. |
| 34 | `wl_measurable` | win_logs | 19.8 | # measurable impressions | Viewability measurement denominator. |

### Exchange/Market Signals (external data from SSPs)

| New Rank | Feature | Source | Gain | What It Is | What the Model Found |
|----------|---------|--------|------|-----------|---------------------|
| 3 | `al_n_auctions` | augmentor_log | 33.1 | # auctions IP appeared in | More active IPs (more streaming/browsing) visit more. Market activity signal. |
| 6 | `al_n_domains` | augmentor_log | 31.7 | # distinct content domains | Content consumption breadth. Diverse browsers vs single-site users. |
| 8 | `ci_pct_video` | cost_impression_log | 30.6 | % VIDEO format impressions | CTV vs display split. Set by exchange, not our choice. |
| 12 | `al_pct_iab` | augmentor_log | 27.9 | % auctions with IAB categories | Content taxonomy coverage from exchanges. Richer data = better classification. |
| 16 | `al_n_ssps` | augmentor_log | 28.5 | # distinct SSPs/exchanges | How many exchanges see this IP. More SSPs = more reach/activity. |
| 17 | `al_pct_pmp` | augmentor_log | 28.2 | % auctions with PMP deals | Premium curated inventory signal. PMP = higher quality. |
| 26 | `ci_n_vendors` | cost_impression_log | 25.6 | # distinct supply vendors | Supply source diversity. |
| 27 | `al_n_networks` | augmentor_log | 26.3 | # networks/publishers consumed | Content publisher diversity from bidstream. |
| 30 | `al_pct_video` | augmentor_log | 25.4 | % VIDEO placement in auctions | CTV vs display from bidstream side. |
| 31 | `al_pct_ctv` | augmentor_log | 20.7 | % CTV device type | CTV vs mobile/PC from bidstream. |
| 35 | `al_has_ctv` | augmentor_log | 19.7 | Binary CTV flag | CTV device presence from exchange. |

### Content Signals (what the viewer watches — from bidstream)

| New Rank | Feature | Source | Gain | What It Is | What the Model Found |
|----------|---------|--------|------|-----------|---------------------|
| 9 | `bae_pct_news` | bidder_auction_events | 29.9 | % news content | **Highest-ranked genre.** News watchers show different visit patterns. |
| 10 | `bae_pct_ent` | bidder_auction_events | 27.3 | % entertainment content | Entertainment is the dominant genre. Broad but differentiating. |
| 13 | `bae_pct_drama` | bidder_auction_events | 26.9 | % drama content | Drama viewers — specific demographic signal. |
| 14 | `bae_n_pubs` | bidder_auction_events | 29.3 | # distinct publishers | Publisher diversity — broad vs narrow content consumption. |
| 18 | `bae_n_genres` | bidder_auction_events | 30.9 | # distinct genres | Genre diversity — eclectic vs focused viewer. |
| 22 | `bae_pct_sports` | bidder_auction_events | 23.7 | % sports content | Sports watchers. |
| 23 | `bae_pct_comedy` | bidder_auction_events | 24.8 | % comedy content | Comedy viewers. |
| 24 | `bae_pct_genre` | bidder_auction_events | 27.1 | % auctions with genre data | Genre data availability for this IP. |
| 7 | `bae_n_auctions` | bidder_auction_events | 27.8 | # dropped auctions | Broader activity beyond what we bid on. Market activity. |

### Device Signals (what device they use — from bidstream)

| New Rank | Feature | Source | Gain | What It Is | What the Model Found |
|----------|---------|--------|------|-----------|---------------------|
| 5 | `bae_lg` | bidder_auction_events | 34.1 | Has LG device (0/1) | **Highest-ranked device.** LG Smart TV ownership predicts visits. |
| 20 | `bae_roku` | bidder_auction_events | 30.7 | Has Roku device (0/1) | Roku is largest CTV platform. |
| 28 | `bae_n_makes` | bidder_auction_events | 26.2 | # device manufacturers | Device diversity from broader bidstream. |
| 32 | `bae_samsung` | bidder_auction_events | 31.5 | Has Samsung device (0/1) | Samsung Smart TV ownership. |

---

## The 9 Existing Features (Our Own Outputs — Circular)

Left in the model for completeness, but these are not new signal — they're our system feeding back into itself.

| Feature | Source | SHAP | What It Is | Why It's Circular |
|---------|--------|------|-----------|-------------------|
| `al_avg_segments` | augmentor_log | 0.986 | Avg MNTN segments on IP | 97% are 1P (RTC + retargeting). Our model's own output. |
| `ci_pct_new` | cost_impression_log | 0.670 | % "new" impressions | Our pipeline's is_new flag. |
| `ci_pct_rtc` | cost_impression_log | 0.392 | % RTC impressions | RTC = our conquest model. |
| `ci_total_cost` | cost_impression_log | 0.363 | Total media spend | Our spending decision. |
| `ci_hh_score` | cost_impression_log | 0.152 | Fangorn household score | Fangorn's own score. |
| `ci_adv_hh_score` | cost_impression_log | — | Advertiser household score | Fangorn per-advertiser score. 10000 = RTC. |
| `ci_n_imp` | cost_impression_log | — | # impressions | Our impression frequency. |
| `n_wins` | base | — | Total auction wins | Our bidding activity. |
| `n_win_adv` | base | — | # advertisers targeting IP | How many of OUR advertisers want this IP. |

**Note on al_avg_segments:** The 3P component (DS3 interest segments = ~20 segments per IP across 1.3B IPs) is genuinely external. Future work: split into 1P vs 3P segment counts. The 3P count alone may be valuable new signal.

---

## 3 Zero-Importance Features (Drop)

| Feature | Why Zero |
|---------|----------|
| `wl_skips` | CTV has no skip button. Always 0. |
| `wl_viewability` | ~100% for all CTV IPs. No variance. |
| `wl_invalid` | Nearly zero IVT flags in this sample. |

---

## The 19 Feedback Features (Post-Visit — For Retraining)

Available only after a site visit. Can't use for targeting new IPs, but valuable for:
- **Retraining Fangorn** with richer signals
- **Scoring returning visitors** more accurately
- **Identity resolution** (device fingerprinting, cross-session linking)
- **Conversion value segmentation** (spending tiers)

| Rank | Feature | Source | SHAP | Use Case |
|------|---------|--------|------|----------|
| 1 | `gl_n_os_families` | guid_log | 5.255 | Multi-device detection / fingerprinting |
| 2 | `gl_n_browser_families` | guid_log | 3.883 | Browser diversity / identity |
| 3 | `gl_pct_ip_stable` | guid_log | 2.797 | Proxy vs direct — IP quality |
| 4 | `gl_n_adv` | guid_log | 2.309 | Cross-advertiser activity |
| 5 | `gl_n_events` | guid_log | 1.451 | Visit frequency |
| 6 | `gl_pct_mobile` | guid_log | 0.647 | Mobile vs desktop behavior |
| 7 | `gl_pct_new` | guid_log | 0.373 | New vs returning ratio |
| 8 | `cv_n_orders` | conversion_log | 0.266 | Repeat purchaser signal |
| 9 | `cv_n_conv` | conversion_log | 0.214 | Converter flag |
| 10 | `cv_avg_amt` | conversion_log | 0.206 | Spending tier ($200 buyer ≠ $5 buyer) |
| 11 | `cv_total_amt` | conversion_log | 0.129 | Lifetime purchase value |
| 12 | `cv_n_types` | conversion_log | — | Conversion diversity (purchase + signup + call) |
| 13 | `gl_has_mobile` | guid_log | — | Mobile presence flag |
| 14 | `cv_n_adv` | conversion_log | — | Cross-advertiser converter |
| 15 | `gl_n_product_views` | guid_log | — | Product browsing = purchase intent |
| 16 | `gl_has_tablet` | guid_log | — | Tablet presence |
| 17 | `gl_has_new_visit` | guid_log | — | First-visit flag |
| 18 | `gl_has_desktop` | guid_log | 0 | Zero — 0% fill |
| 19 | `gl_n_utm_events` | guid_log | 0 | Zero — UTM rarely present |

---

## Features Not Yet Modeled

| Feature | Source | Potential |
|---------|--------|-----------|
| IAB category percentages (per-category) | augmentor_log | Specific vertical affinity — high for Alex's work |
| 3P segment count (DS3 interest only) | augmentor_log | External interest data — genuinely new |
| `content_series` (show names) | bidder_auction_events | Specific show affinity — needs cleanup |
| `content_channel` | bidder_auction_events | Channel affinity |
| `query.ga_client_id` | conversion_log | Cross-session identity (67% prevalence) |
| `query.shpt` (product type) | conversion_log | Product category — vertical signal (74%) |
| `ua_advanced.DeviceBrand` (799 values) | guid_log | Device demographics (Matt's prototype) |
| `viewability_score` | bid_price_log | Pre-bid quality prediction (10d TTL) |
| `publisher_performance` | bid_price_log | Publisher quality ranking (10d TTL) |
| `recency` / `conquest_score` | bid_events_log | Real-time signals at bid time |

---

## Summary by Category

| Category | # Features | Top Feature | Key Insight |
|----------|-----------|------------|-------------|
| **User Behavior** | 11 | wl_avg_price (clearing price) | Premium inventory and multi-device households are the strongest new signals |
| **Exchange/Market** | 11 | al_n_auctions (activity volume) | Active IPs and PMP deals correlate with visits |
| **Content (genre)** | 9 | bae_pct_news (news %) | News is the highest-ranked genre. Content features are mid-tier for general IVR but high-value for vertical classification |
| **Device (make)** | 4 | bae_lg (LG device) | LG is the most predictive device. Device make is a demographic proxy |
| **Existing (circular)** | 9 | al_avg_segments | These dominate raw rankings but are our own outputs |
| **Feedback (post-visit)** | 19 | gl_n_os_families | Gold for retraining. Can't use for new-IP targeting. |

---

## All 25 Tables Analyzed

| Table | In Model? | Verdict |
|-------|-----------|---------|
| **cost_impression_log** | Yes (8) | Top source — but 5 of 8 are EXISTING. 3 genuinely new. |
| **augmentor_log** | Yes (10) | 9 of 10 are genuinely new (only al_avg_segments is existing). |
| **win_logs** | Yes (14) | 11 genuinely new. 3 zero importance. Best source for NEW features. |
| **bidder_auction_events** | Yes (13) | All 13 are genuinely new. Content + device signals. |
| **guid_log** | Yes (13) | FEEDBACK only. Device fingerprinting + product intent. |
| **conversion_log** | Yes (6) | FEEDBACK only. Order value + identity signals. |
| bid_price_log | No (10d TTL) | viewability_score, publisher_performance — worth testing |
| bid_events_log | No | recency, conquest_score at bid time |
| event_log_filtered | No | Pre-aggregated video quartiles. Redundant with win_logs. |
| conversion_signal_log | No | CallRail data. 193 rows/6 days. Too sparse. |
| tpa_membership_update_log | No | Scores field empty. |
| 13 other tables | No | Redundant or insufficient unique signal. |
