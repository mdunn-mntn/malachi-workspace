# TI-790: Cross-Table Unique Column Analysis

**Method:** Programmatic comparison of all 25 log tables. Each column classified by how many tables contain it.
**Goal:** Identify truly unique, net-new data per table that could improve targeting performance.

---

## Tables Ranked by Unique Signal Value for Targeting

### 1. win_logs — 66 unique columns (RICHEST TABLE)

**What it is:** One row per auction won in Beeswax.

**High-value unique signals for targeting:**
| Column | Signal |
|--------|--------|
| `video_completes`, `video_plays`, `video_q1s`, `video_q3s`, `video_midpoints` | Video engagement — completion rate is a strong behavioral signal |
| `video_skips` | Skip behavior — disengaged users |
| `video_mutes`, `video_unmutes`, `video_pauses`, `video_resumes`, `video_fullscreens`, `video_closes` | Granular video interaction (mute = audio-off viewing) |
| `in_view`, `is_measurable`, `in_view_time_ms` | Viewability metrics — ad attention signal |
| `clearing_price_micros_usd` | Actual price paid — inventory quality |
| `clicks`, `bot_clicks` | Click behavior + bot detection |
| `conversions`, `conversion_value`, `conversion_order` | Post-impression conversion data |
| `bidding_strategy_id`, `bidding_strategy_params` | How we bid — pacing/optimization context |
| `model_id`, `model_params` | Which Fangorn model was used |
| `invalid_impression`, `invalid_automated_browser`, `invalid_data_center_traffic`, `invalid_incongruous_browser` | IVT (invalid traffic) flags — fraud signal |
| `platform_device_make`, `platform_device_model`, `platform_device_screen_size` | Device details (shared with bid_logs only) |
| `platform_device_hwv`, `platform_device_language`, `platform_device_w/h/ppi/pxratio` | Device hardware + language |
| `content_language`, `content_rating`, `content_coppa_flag` | Content metadata (shared with bid_logs only) |
| `has_frequency_cap` | Whether frequency capping was active |
| `companion_clicks`, `companion_views` | Companion ad engagement |
| `exchange_predicted_view_rate` | Exchange's viewability prediction |

**Targeting value:** Very high. Video engagement (VCR, skip rate), viewability, IVT flags, device details, and content metadata are all strong behavioral signals not available anywhere else.

---

### 2. cost_impression_log — 20 unique columns

**What it is:** Enriched impression data with cost and scoring.

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `recency_elapsed_time` | Time since last impression to this IP — frequency/recency signal |
| `household_score`, `advertiser_household_score` | Fangorn scoring output (use carefully — potentially circular) |
| `media_cost`, `media_spend`, `data_spend`, `platform_spend` | Cost breakdown per impression |
| `ott_device` | OTT device type classification |
| `sh_device` | SteelHouse device classification |
| `partner_ad_format` | VIDEO vs BANNER (authoritative) |
| `supply_vendor` | Supply-side vendor identity |
| `private_marketplace_id` | PMP deal at impression level |
| `partner_ip` | Partner-reported IP |
| `model_params` | Full model params string (shared with spend/win) |

**Targeting value:** High. `recency_elapsed_time` is a uniquely valuable frequency signal. Cost data enables CPM-efficiency features. `ott_device` and `sh_device` are unique device classifications.

---

### 3. bidder_auction_events — 15 unique columns

**What it is:** Auctions we saw but didn't bid on.

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `content_genre` | **Breakout feature** — what content they watch (87% fill) |
| `content_channel` | Channel name |
| `content_series` | Specific show name |
| `content_network` | Network name (structured) |
| `device_make` | Device manufacturer (90% fill) |
| `device_type_group` | Device type grouping |
| `site_categories` | Content taxonomy (0.13% fill — low) |
| `app_domain` | App domain |

**Targeting value:** Very high. `content_genre` and `device_make` are not available in ANY other table and are strongly differentiating.

---

### 4. guid_log — 15 unique columns

**What it is:** Website pixel fires — visitor behavior on advertiser sites.

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `product` (JSON) | Product viewed — category, brand, name, amount, SKU, inventory |
| `cart` (JSON) | Cart contents — items, value |
| `ga_utm_source`, `ga_utm_medium`, `ga_utm_campaign` | Traffic source/medium/campaign (Google Analytics) |
| `ga_gclid` | Google Ads click ID — paid search signal |
| `is_cookied` | Whether the user has a cookie |
| `available_ga` | Additional GA data |
| `user_agent` (JSON with `ua_advanced`) | Rich UA parsing: DeviceClass, DeviceBrand, DeviceName, DeviceCpu (Matt's prototype) |

**Targeting value:** Very high. Product/cart data is purchase intent. GA UTM params reveal traffic source. `ua_advanced` enables device fingerprinting.

---

### 5. bid_price_log — 14 unique columns

**What it is:** Internal bidding decision details. 10-day TTL.

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `viewability_score`, `viewability_score_threshold` | Pre-bid viewability prediction |
| `publisher_performance`, `publisher_performance_threshold` | Publisher quality score |
| `conquest_score_threshold` | RTC score threshold |
| `uncapped_bid_price` | What we would have bid without caps |
| `auction_bid_floor`, `pmp_bid_floor` | Bid floor details |

**Targeting value:** Medium-high. `viewability_score` and `publisher_performance` are pre-bid quality predictions unique to this table. Very short TTL (10 days) limits historical analysis.

---

### 6. bid_events_log / bid_attempted_log — 3 unique columns

**What it is:** Internal bidding events (which campaigns evaluated, why bids failed).

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `recency`, `recency_threshold` | Bid-time recency values |
| `conquest_score`, `conquest_score_ttl` | RTC score at bid time (shared with bid_price_log) |
| `household_score`, `household_score_threshold` | Scores + thresholds |
| `threshold_failure_reasons` | Why we didn't bid (shared with bid_price_log) |
| `campaign_impressions`, `campaign_group_impressions` | Impression counts at bid time |
| `budget_pace` | Budget pacing at bid time |
| `objective_id`, `channel_id` | Campaign type context |

**Targeting value:** Medium. Recency, pacing, and failure reasons provide bidding behavior context. 90-day TTL.

---

### 7. conversion_log — 3 unique columns + rich `query` field

**What it is:** Conversion events (purchases, signups).

**Unique columns:**
| Column | Signal |
|--------|--------|
| `conversion_source_id` | Which conversion path (5 values) |
| `order_amt_usd` | Order amount in USD (usually NULL — use `order_amt` instead) |

**Hidden in `query` field (from your Excel analysis):**
| Field | Prevalence | Signal |
|-------|-----------|--------|
| `shoamt` | 75% | Order dollar amount |
| `shpt` | 74% | Product type purchased |
| `shoid` | 88% | Order/transaction ID |
| `ga_client_id` | 67% | GA cross-session identity |
| `email_data` | 2.3% | Hashed email |
| `androidId` | 3.1% | Android device ID |
| `idfa` | 3.1% | iOS advertising ID |
| `adid` | 3% | Advertising ID |
| `appsflyerDeviceId` | 3% | AppsFlyer device ID |

**Hidden in `user_agent` JSON (56% fill):**
| Field | Distinct | Signal |
|-------|----------|--------|
| `DeviceClass` | 17 | Richer device classification |
| `DeviceBrand` | 799 | Device manufacturer |
| `DeviceName` | 9,984 | Specific device model |
| `DeviceCpu` | 21 | CPU architecture |
| `AgentName` | 687 | Browser identity |

**Targeting value:** High. Order amount, product type, and identity signals (GA client ID, device IDs) are all unique. The `query` string is a goldmine that needs parsing. Don't use for device/browser (guid_log covers that).

---

### 8. conversion_signal_log — 5 unique columns

**What it is:** Newer conversion signal format with richer metadata.

**High-value unique signals:**
| Column | Signal |
|--------|--------|
| `customer_properties` | Customer attributes at conversion time |
| `identity` | Identity resolution data |
| `mntn_conversion_type` | MNTN-classified conversion type |
| `ingestion_timestamp` | When conversion was ingested |
| `data_source_id` | Which data source |

**Targeting value:** Medium-high. `customer_properties` and `identity` could contain rich signals — need to inspect actual data.

---

### 9. event_log — 4 unique columns

**What it is:** VAST video events (start, quartiles, complete).

**Unique signals:**
| Column | Signal |
|--------|--------|
| `event_type_id`, `event_type_raw` | Specific video event (vast_start, vast_impression, etc.) |
| `root_video` | Root video identifier |
| `td_impression_id` | TTD impression ID |

**Targeting value:** Low-medium. Video event data is better captured via win_logs (video_completes, video_plays, etc.) which already aggregates this.

---

### 10. event_log_filtered — 5 unique columns

**What it is:** Pre-aggregated VAST quartile flags per ad_served_id.

**Unique signals:**
| Column | Signal |
|--------|--------|
| `v_vast_start`, `v_vast_firstquartile`, `v_vast_midpoint`, `v_vast_thirdquartile`, `v_vast_complete` | Boolean flags for each video quartile |

**Targeting value:** Medium. Clean video completion data. Could derive VCR per IP efficiently since it's pre-aggregated. 60-day TTL.

---

### 11. augmentor_log — 7 unique columns

**What it is:** Enriched bidstream data for auctions we participated in.

**Unique signals:**
| Column | Signal |
|--------|--------|
| `iab_categories` | IAB content taxonomy (30% fill, bronze only) |
| `categories` | Additional categories (13% fill, bronze only) |
| `isp` | ISP name (10% fill) |
| `is_blocked`, `blocking_site` | Brand safety (0% — no signal) |
| `page` | Page URL (15% fill) |

**Targeting value:** `iab_categories` is very high value for vertical classification. Rest is low.

---

### 12. kochava_log — 6 unique columns

**What it is:** Mobile attribution events from Kochava.

**Unique signals:**
| Column | Signal |
|--------|--------|
| `android_id` | Android device ID |
| `appid` | App identifier |
| `campaign_name` | Campaign name (from attribution) |
| `click_id` | Click identifier |
| `item_name`, `item_quantity` | Product data from mobile events |

**Targeting value:** Medium. Mobile app install/event attribution. Niche but could bridge mobile-to-CTV identity.

---

### 13-25. Lower-value tables

| Table | Unique Columns | Value | Notes |
|-------|---------------|-------|-------|
| **page_view_signal_log** | `ids`, `query_str`, `url` | Medium | `ids` might contain identity signals, `url` gives page-level granularity |
| **analytics_request_log** | `is_crossdevice`, `event_type`, `is_success`, `send_time` | Low-medium | Cross-device flag at GA send level |
| **tpa_membership_update_log** | `in_segments`, `out_segments`, `scores`, `delta`, `metadata_info` | Medium | Segment membership changes over time. `scores` could be valuable. |
| **click_log** | `landing_page` | Low | Landing page URL |
| **clickpass_log** | `first_touch_time` | Low | First-touch timestamp (other fields shared with icloud_vv_log) |
| **viewability_log** | `viewability_type_id`, `mntn_ip` | Low | Viewability type |
| **impression_log** | `cpm`, `cpi` | Low | CPM/CPI at impression level |
| **singular_log** | `datetime`, `useragent_raw` | Low | Mobile attribution (overlap with kochava) |
| **visit_tracking_log** | `is_verified_visit` | Low | Visit verification flag |
| **guid_ip_log_visitors** | `is_new_gl` | Low | Variant new visitor flag |
| **impression_tracking_log** | `query_string` | Low | Tracking pixel query string |
| **spend_log** | Intent scores | Low | Fangorn outputs (circular) |

---

## Recommended Build Order (by targeting value)

### Must Build (highest unique signal density)
1. **guid_log** — product/cart data, GA UTM params, ua_advanced device fingerprinting
2. **augmentor_log** — iab_categories, SSP diversity, MNTN segment baseline
3. **bidder_auction_events** — content_genre, device_make, content_series
4. **win_logs** — video engagement (VCR, skip, mute), viewability, IVT flags, device details

### Should Build (strong unique signals)
5. **cost_impression_log** — recency_elapsed_time, cost breakdown, ott_device
6. **conversion_log** — order_amt, product type, identity signals (parsed from query string)
7. **conversion_signal_log** — customer_properties, identity, mntn_conversion_type

### Consider Building (niche but potentially valuable)
8. **bid_price_log** — viewability_score, publisher_performance (10-day TTL limits use)
9. **bid_events_log** — recency, pacing, failure reasons
10. **event_log_filtered** — pre-aggregated video quartile flags
11. **tpa_membership_update_log** — segment change history, scores
12. **kochava_log** — mobile attribution, device IDs

### Skip (redundant or low value)
- bid_logs (redundant with win_logs), spend_log (circular), click_log, clickpass_log (outcome not feature), viewability_log, impression_log, singular_log, analytics_request_log, guid_ip_log_visitors, impression_tracking_log, visit_tracking_log, auction_log (subset of augmentor_log)
