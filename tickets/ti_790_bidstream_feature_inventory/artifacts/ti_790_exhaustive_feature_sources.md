# TI-790: Exhaustive Feature Source Map — All Log Tables

**Purpose:** Catalog every table that can contribute IP-level features to the feature store.
**Primary sources (per Matt + Ryan):** `guid_log` + `augmentor_log`
**Secondary sources:** All other log tables with unique, non-redundant signals.

---

## Source 1: guid_log (Website Pixel — Visitor Behavior)

**What it is:** JavaScript pixel fires on advertiser websites. Every site visit, page view, and conversion event.
**Key:** IP + advertiser_id + time
**TTL:** VIEW in silver.logdata (underlying table TBD)
**Why it matters:** This is the demand-side signal — what people actually do on advertiser sites.

### Unique Features (not available elsewhere)

| Feature | Type | Signal |
|---------|------|--------|
| **browser** | STRING | Chrome, Safari, Edge, Firefox — browsing behavior |
| **operating_system** | STRING | Parsed OS (more reliable than bidstream) |
| **device_type** | STRING | Desktop, Mobile, Tablet — from pixel side |
| **browser_version** | STRING | Version granularity |
| **is_new** | BOOLEAN | First visit to this advertiser |
| **is_mobile_device** | BOOLEAN | Mobile flag |
| **referer** | STRING | Where they came from before visiting |
| **parent_referer** | STRING | Upstream referrer |
| **product_category** | STRING | What product category they viewed |
| **product_brand** | STRING | What brand they viewed |
| **product_name** | STRING | Specific product viewed |
| **product_amount** | STRING | Product price |
| **product_sku** | STRING | Product SKU |
| **cart_quantity** | INTEGER | Items in cart |
| **cart_value** | STRING | Cart dollar value |
| **ga_utm_campaign** | STRING | Google Analytics campaign tag |
| **ga_utm_source** | STRING | GA traffic source |
| **ga_utm_medium** | STRING | GA traffic medium |
| **ga_client_id** | STRING | GA client identifier |
| **email** | STRING | Hashed email (when provided) |
| **phone** | STRING | Phone (when provided) |

### Aggregatable Features (Matt's daily snapshot pattern)
Per Matt's prototype, these can be rolled up per IP over a rolling window:

| Aggregated Feature | Description |
|-------------------|-------------|
| has_desktop_d | Has desktop events (bool) |
| has_mobile_d | Has mobile events (bool) |
| has_tablet_d | Has tablet events (bool) |
| has_mac_d / has_windows_d | OS flags |
| has_ios_d / has_android_d | Mobile OS flags |
| has_chrome_d / has_safari_d / has_edge_d / has_firefox_d | Browser flags |
| n_distinct_device_class_d | Number of distinct device types |
| n_distinct_os_family_d | Number of distinct OSes |
| n_distinct_browser_family_d | Number of distinct browsers |
| n_distinct_device_brand_d | Device brand diversity |
| n_distinct_device_fingerprints_d | Unique device fingerprints |
| pct_mobile_events_d | % of events from mobile |
| pct_desktop_events_d | % of events from desktop |
| pct_ios_events_d / pct_android_events_d | Mobile OS split |
| pct_chrome_events_d / pct_safari_events_d | Browser split |
| pct_ip_eq_original_ip_d | How often IP matches original IP (proxy stability) |

---

## Source 2: augmentor_log (Bidstream — Auctions We Participated In)

**What it is:** Enriched bid request data for auctions our bidder evaluated.
**Key:** IP + time
**TTL:** 10 days BQ, ~30 days parquet archive
**Scale:** 1.2B rows/hr, ~241 GB/day
**Why it matters:** Supply-side context for the IPs we actually bid on.

### Unique Features

| Feature | Fill % | Signal |
|---------|--------|--------|
| **iab_categories** | 30% | IAB content taxonomy (bronze only). Vertical classification. |
| **inventory_source** (40 SSPs) | 100% | Which exchange served the impression. Quality proxy. |
| **mntn_segments** | 86% | Segments already assigned to this IP. Incrementality baseline. |
| **network** | 71% | Publisher/network name (12.8K values) |
| **pmp** deal IDs | 98% | Private marketplace deals. Premium inventory indicator. |
| **isp** | 10% | ISP name (bronze only) |
| **categories** | 13% | Additional content categories (bronze only) |

### Aggregatable Features (per IP, rolling window)
| Aggregated Feature | Description |
|-------------------|-------------|
| n_bid_events | Total auctions seen |
| n_distinct_ssps | SSP diversity |
| pct_video_placements | % video vs banner |
| pct_ctv_device | % CTV vs mobile vs desktop |
| top_network | Most frequent network viewed |
| n_distinct_networks | Content diversity |
| n_distinct_iab_categories | Topic breadth |
| top_iab_category | Dominant content category |
| n_existing_segments | How many MNTN segments already assigned |
| pct_premium_pmp | % of auctions with PMP deals |

---

## Source 3: bidder_auction_events (Bidstream — Auctions We Didn't Bid On)

**What it is:** All auctions we saw but dropped (didn't bid).
**Key:** device_ip + _PARTITIONTIME
**TTL:** 90 days BQ
**Scale:** 112M rows/hr
**Why it matters:** Broader view of IP activity beyond what we bid on. Richer content fields.

### Unique Features (not in augmentor_log)

| Feature | Fill % | Signal |
|---------|--------|--------|
| **content_genre** | 87% | What content they watch (entertainment, news, drama, comedy, sports). **Breakout feature.** |
| **device_make** | 90% | Device manufacturer (Roku, Samsung, LG, Vizio, Amazon) |
| **content_series** | 37% | Specific show name (after cleanup) |
| **content_channel** | 36% | Channel name |
| **content_network** | 38% | Network name (structured) |
| **publisher_name** | 100% | Publisher identity (301 values, cleaner than augmentor_log.network) |
| **app_name** | 98% | App name (vs just bundle ID) |
| **geo_zip** | 95% | ZIP code |
| **auction_dropped_reason** | — | Why we didn't bid |

### Aggregatable Features
| Aggregated Feature | Description |
|-------------------|-------------|
| top_genre | Most-watched genre |
| genre_entropy | Diversity of content consumption |
| pct_entertainment / pct_news / pct_sports / etc. | Genre share breakdown |
| top_device_make | Primary device |
| n_distinct_publishers | Publisher diversity |
| top_content_series | Most-watched show |
| pct_dropped_auctions | What fraction of bid opportunities we pass on |

---

## Source 4: cost_impression_log (Enriched Impressions — Served Ads)

**What it is:** One row per impression actually served to a user.
**Key:** IP + time
**TTL:** 90 days
**Why it matters:** Post-bid context — what happened after we won.

### Unique Features

| Feature | Signal |
|---------|--------|
| **household_score** | Fangorn household-level score |
| **advertiser_household_score** | Advertiser-specific household score |
| **model_params** | Full model parameter string (contains RTC scores, etc.) |
| **recency_elapsed_time** | Time since last impression to this IP |
| **is_new** | First impression to this IP |
| **ott_device** | OTT device type |
| **partner_ad_format** | VIDEO vs BANNER (authoritative) |
| **media_cost / media_spend / data_spend / platform_spend** | Cost breakdown per impression |
| **supply_vendor** | Supply-side vendor |

### Aggregatable Features
| Aggregated Feature | Description |
|-------------------|-------------|
| avg_household_score | Average Fangorn score for this IP |
| avg_recency | Average time between impressions |
| n_impressions | Total impressions served |
| total_media_cost | Total spend on this IP |
| avg_cpm | Average cost per impression |
| pct_video_impressions | Video vs banner split |

---

## Source 5: win_logs (Beeswax Win Events)

**What it is:** One row per auction won in Beeswax.
**Key:** IP + time
**TTL:** 90 days
**Why it matters:** Richest single table — device details, video metrics, viewability.

### Unique Features (not reliably elsewhere)

| Feature | Signal |
|---------|--------|
| **platform_device_make** | Device manufacturer (from exchange) |
| **platform_device_model** | Specific device model |
| **platform_device_screen_size** | Screen size |
| **platform_os_version** | OS version |
| **content_language** | Content language |
| **content_rating** | Content maturity rating |
| **in_view** | Was the ad viewable |
| **in_view_time_ms** | Viewability time in ms |
| **video_completes / video_plays / video_midpoints / video_q1s / video_q3s** | Video engagement metrics |
| **video_skips** | Did they skip |
| **targeted_segments** | What segments we targeted |
| **bidding_strategy_id / bidding_strategy_params** | How we bid |
| **clearing_price_micros_usd** | What we actually paid |
| **page_url** | Specific page URL where ad appeared |

### Aggregatable Features
| Aggregated Feature | Description |
|-------------------|-------------|
| avg_video_completion_rate | VCR = completes / plays |
| avg_viewability | % of impressions viewable |
| avg_in_view_time_ms | Average viewability duration |
| avg_clearing_price | Average auction price |
| n_wins | Total auctions won |
| pct_video_skips | Skip rate |
| top_device_model | Most common device model |

---

## Source 6: clickpass_log (Visit-Through / Click-Through Attribution)

**What it is:** Attributed visits — an IP that saw an ad and later visited the site.
**Key:** IP + time
**Why it matters:** This IS the outcome variable (IVR), but also has unique attribution features.

### Unique Features

| Feature | Signal |
|---------|--------|
| **click_elapsed** | Seconds from ad impression to site visit |
| **view_elapsed** | Seconds from ad view to site visit |
| **is_cross_device** | Was this a cross-device attribution |
| **attribution_model_id** | Which attribution model matched |
| **viewable** | Was the triggering impression viewable |
| **first_touch_ad_served_id** | First-touch impression reference |

### Aggregatable Features
| Aggregated Feature | Description |
|-------------------|-------------|
| avg_view_elapsed | Average time from ad to visit |
| pct_cross_device | % of visits that are cross-device |
| n_visits | Total attributed visits (this IS IVR numerator) |

---

## Source 7: bid_logs (Bids We Sent)

**What it is:** One row per bid we submitted to an exchange.
**Key:** IP + auction_id + time
**TTL:** 90 days
**Why it matters:** Overlaps heavily with win_logs + augmentor_log. A few unique fields.

### Unique Features (not reliably elsewhere)

| Feature | Signal |
|---------|--------|
| **platform_device_screen_size** | Screen dimensions |
| **video_min_duration / video_max_duration** | Ad slot duration constraints |
| **video_player_width / video_player_height** | Player size |
| **available_deal_ids** | All PMP deals available for this auction |
| **bid_price_micros_usd** | What we bid |

---

## Source 8: spend_log (Spend Events)

**What it is:** Enriched spend data per impression.
**Key:** IP + time
**Why it matters:** Contains Fangorn intent scores (but these are model outputs, not inputs — potentially circular).

### Features (use with caution)

| Feature | Signal | Caution |
|---------|--------|---------|
| **advertiser_intent_score** | Fangorn advertiser-level score | This IS Fangorn output — circular if used as input |
| **campaign_intent_score** | Campaign-level score | Same circularity risk |
| **segment_intent_score** | Segment-level score | Same |
| **impression_bid_floor** | Exchange bid floor | Supply quality signal |

---

## Summary: What's Unique Per Table

| Table | What it adds that no other table has |
|-------|-------------------------------------|
| **guid_log** | Browser, OS, device from pixel side. Product/cart data. GA params. Advertiser-specific visit behavior. |
| **augmentor_log** | iab_categories (IAB taxonomy). 40 SSP inventory sources. mntn_segments (existing targeting). |
| **bidder_auction_events** | content_genre (87% fill). device_make. content_series/channel/network. |
| **cost_impression_log** | household_score. recency_elapsed_time. Cost breakdown. model_params. |
| **win_logs** | Device model. Video engagement (completes, skips). Viewability time. Content language/rating. |
| **clickpass_log** | Attribution timing (click/view elapsed). Cross-device flag. |
| **bid_logs** | Video slot constraints. Available deal IDs. Bid price. |
| **spend_log** | Intent scores (circular — use carefully). Bid floor. |

---

## Recommended Priority Order for Feature Extraction

### Build First (highest unique signal, Matt + Ryan confirmed)
1. **guid_log** — Matt already has a prototype daily snapshot. Device/browser behavior + product/cart data. Unique demand-side signal.
2. **augmentor_log** — iab_categories for vertical classification. mntn_segments for incrementality. 40 SSPs for inventory quality.

### Build Second (rich content + device signals)
3. **bidder_auction_events** — content_genre is the breakout feature. device_make adds demographic proxy.
4. **win_logs** — Video engagement metrics (VCR, skip rate, viewability) are strong behavioral signals.

### Build Third (enrichment layer)
5. **cost_impression_log** — household_score, recency, cost data.
6. **clickpass_log** — Attribution features (elapsed time, cross-device).

### Skip / Use Carefully
7. **bid_logs** — Mostly redundant with win_logs + augmentor_log.
8. **spend_log** — Intent scores are Fangorn outputs (circular). Only bid_floor is useful.
9. **conversion_log** — Matt + Ryan say avoid to prevent double-counting with guid_log.

---

## Architecture Note: Rolling Window Aggregation

Per Matt's prototype, the feature store should aggregate per IP over a rolling window (e.g., 7 or 14 days):

```
IP → {
  // guid_log features
  has_desktop: true,
  pct_mobile_events: 0.3,
  n_distinct_browsers: 2,
  n_visits_to_advertiser_X: 5,

  // augmentor_log features
  top_iab_category: "IAB1",
  n_distinct_ssps: 8,
  pct_ctv_device: 0.7,

  // bidder_auction_events features
  top_genre: "entertainment",
  genre_entropy: 1.8,
  device_make: "Roku",

  // win_logs features
  avg_video_completion_rate: 0.85,
  avg_viewability: 0.72,

  // Label (outcome)
  ivr: 0.003  // from clickpass_log
}
```

This per-IP feature vector is what gets fed into XGBoost for feature importance testing.
