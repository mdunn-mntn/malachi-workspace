# TI-790: Bidstream Feature Inventory (Both Tables)

**Source:** `dw-main-bronze.raw.augmentor_log`
**Sample:** 1 hour (2026-03-30 12:00–13:00 UTC) = **1.23 billion rows**
**Full day estimate:** ~29B rows, ~241 GB (dry run confirmed)
**Partition:** HOUR on `time`, clustered by `ip`. 10-day TTL in BQ; ~30 days in parquet archive.

---

## Field Profile Summary

| Field | Fill % | Distinct (1hr) | Type | Notes |
|-------|--------|----------------|------|-------|
| **ip** | 88.58% | 23.6M | STRING | Primary join key. 11.42% are IPv6-only (ipv6 field). Filter blank IPs. |
| **ipv6** | 11.42% | — | STRING | Complement of ip — together they cover ~100% |
| **device_type** | 100% | 9 | STRING | CONNECTED_TV, SET_TOP_BOX, PC, PHONE, TABLET, CONNECTED_DEVICE, etc. |
| **os** | 99.19% | 337 | STRING | Case-inconsistent! "roku os" vs "Roku OS", "android" vs "Android". Needs normalization. |
| **network** | 70.67% | 12,826 | STRING | CTV publisher/network name. NBC Universal, LG Ad Solutions, Paramount, Discovery, etc. |
| **domain** | 99.97% | 91,313 | STRING | High cardinality. Site domain. |
| **site_name** | 6.60% | 57,869 | STRING | Low fill rate — mostly empty. When present, app/site name. |
| **app_bundle** | 84.54% | — | STRING | App identifier (e.g., com.roku.xxx). High fill on CTV/mobile. |
| **inventory_source** | 100% | 40 | STRING | SSP/exchange. INDEX_EXCHANGE, STICKYADS, PUBMATIC, TREMOR, RUBICON, etc. |
| **placement_type** | 100% | 4 | STRING | VIDEO or BANNER (overwhelmingly VIDEO for CTV) |
| **environment_type** | 100% | 3 | STRING | APP or WEB |
| **video_placement** | 82.54% | — | STRING | Video ad placement type |
| **user_agent** | 93.41% | — | STRING | Raw UA string. High cardinality. Could extract device/browser/OS features. |
| **ifa** | 78.45% | — | STRING | Device advertising ID (IDFA/GAID). Privacy implications — may be limited/zeroed. |
| **isp** | 10.49% | 16,690 | STRING | Low fill rate. ISP name when available. |
| **page** | 15.42% | — | STRING | Page URL — mostly web traffic. Low fill. |
| **referrer** | 3.98% | — | STRING | Referrer URL. Very low fill. |
| **iab_categories** | 30.37% | ~330+ | REPEATED STRING | IAB content taxonomy codes (IAB1, IAB12, IAB1_7, etc.). **Key for vertical classification.** |
| **categories** | 12.53% | — | REPEATED STRING | Additional content categories. Lower fill than iab_categories. |
| **mntn_segments** | 85.68% | — | REPEATED INTEGER | MNTN segment IDs already assigned to this IP. **Critical for incrementality checks.** |
| **pmp** | 98.09% | — | REPEATED STRING | Private marketplace deal IDs (e.g., STI-USD-04605, FWM-USD-02489). |
| **is_blocked** | 0.00% | 2 | BOOLEAN | Brand safety flag. Essentially always false in this sample. |
| **geo** (raw) | ~100% | — | STRING | Raw string: `Geo(ip=X, country=X, region=X, city=X, lat=X, lon=X, metro=X, zip=X)` |
| **geo_parsed** (silver) | — | — | RECORD | Parsed in silver view: geo_city, geo_country, geo_ip, geo_latitude, geo_longitude, geo_metro, geo_region, geo_zip |

### Bronze-only fields (not in silver view)
| Field | Fill % | Notes |
|-------|--------|-------|
| **iab_categories** | 30.37% | Not in silver. Key for vertical work. |
| **categories** | 12.53% | Not in silver. Additional content taxonomy. |
| **is_blocked** | 0.00% | Not in silver. Brand safety. |
| **blocking_site** | ~0% | Not in silver. Site that triggered block. |
| **page** | 15.42% | Not in silver. Page URL. |
| **referrer** | 3.98% | Not in silver. Referrer URL. |
| **isp** | 10.49% | Not in silver. ISP name. |

---

## Geo Distribution

| Country | % of sample |
|---------|-------------|
| USA | 99.5% |
| US | 0.5% |
| Other (TTO, etc.) | <0.01% |

Alex's note confirmed: filter to `geo_country IN ('USA', 'US', 'us')`. Virtually all traffic is US.

---

## Inventory Source Distribution (Top 10)

| SSP/Exchange | Volume Rank | Primary Device | Primary Format |
|-------------|-------------|----------------|----------------|
| INDEX_EXCHANGE | 1 | CTV, STB, PC, Phone | VIDEO + BANNER |
| STICKYADS | 2 | CTV, STB | VIDEO |
| PUBMATIC | 3 | CTV, Phone | VIDEO + BANNER |
| TREMOR | 4 | STB, CTV, Phone, Tablet | VIDEO |
| RUBICON | 5 | PC, Phone | BANNER + VIDEO |
| COLUMN6 | 6 | CTV | VIDEO |
| OPENX | 7 | PC, Phone | BANNER |
| GUMGUM | 8 | PC, Phone | BANNER |
| APPNEXUS | 9 | PC, Phone, STB | BANNER + VIDEO |
| SMARTADSERVER | 10 | STB, CTV | VIDEO |

40 total inventory sources. Top 4 dominate volume.

---

## Network (Publisher) Distribution (Top 10)

| Network | Notes |
|---------|-------|
| NBC Universal - FreeWheel | Largest CTV publisher |
| LG Ad Solutions | Smart TV OEM |
| Paramount Global | Major broadcaster |
| Discovery, Inc. | Major broadcaster |
| Samsung - CTV - Publica | Smart TV OEM |
| NBCUniversal Media, LLC | Duplicate of NBC? |
| Wurl | CTV distribution |
| NBC Universal | Another NBC variant |
| LG Ads O&O | LG owned & operated |
| Paramount - Springserve | Paramount via SpringServe |

12,826 distinct networks. Note: NBC appears 3 ways — needs normalization.

---

## OS Distribution (Top 10)

| OS | Notes |
|----|-------|
| roku os / Roku OS | #1 CTV platform. **Case-inconsistent — needs LOWER() normalization.** |
| Android / android | Mobile + CTV |
| Tizen / tizen | Samsung Smart TVs |
| linux | Generic |
| Windows 10 | Desktop |
| SmartCast OS / smartcast os | Vizio TVs |
| iOS / ios | Apple mobile |
| webos / webOS | LG Smart TVs |

337 distinct values but heavy case duplication. After LOWER() normalization, probably ~100-150 real values.

---

## IAB Content Categories (Top 15)

| Code | Category Name (IAB Taxonomy v1) | Count |
|------|--------------------------------|-------|
| IAB1 | Arts & Entertainment | 19,413 |
| IAB12 | News | 6,413 |
| IAB1_7 | Television | 5,023 |
| IAB9 | Hobbies & Interests | 2,399 |
| IAB1_5 | Music | 2,170 |
| IAB17 | Sports | 1,970 |
| IAB19 | Technology & Computing | 1,849 |
| IAB8 | Food & Drink | 1,628 |
| IAB14 | Society | 1,532 |
| IAB5 | Education | 1,444 |
| IAB18 | Style & Fashion | 1,370 |
| IAB20 | Travel | 1,348 |
| IAB11 | Law, Gov't & Politics | 1,300 |
| IAB10 | Home & Garden | 1,192 |
| IAB7 | Health & Fitness | 1,162 |

30.37% fill rate. Includes both top-level (IAB1) and sub-categories (IAB1_7). **This is the most directly relevant field for vertical classification (Alex's TI-791 work).**

---

## Feature Assessment for Fangorn Integration

### Tier 1 — High Value (use first)
| Feature | Rationale |
|---------|-----------|
| **device_type** | Low cardinality, 100% fill. CTV vs mobile vs desktop behavior differs. |
| **placement_type** | VIDEO vs BANNER. 100% fill. Correlates with inventory quality. |
| **environment_type** | APP vs WEB. 100% fill. |
| **inventory_source** | SSP identity. 100% fill, 40 values. Inventory quality signal. |
| **os** | 99% fill. After normalization, ~150 values. Platform signal. |
| **network** | 71% fill. Publisher identity — premium vs long-tail content. |
| **iab_categories** | 30% fill but **highest vertical signal**. Directly maps to advertiser verticals. |
| **mntn_segments** | 86% fill. Existing segment membership — **required for incrementality checks**. |

### Tier 2 — Medium Value (investigate further)
| Feature | Rationale |
|---------|-----------|
| **app_bundle** | 85% fill. App identity. High cardinality — needs embedding or top-N bucketing. |
| **domain** | 100% fill. Site identity. Same cardinality issue as app_bundle. |
| **pmp** | 98% fill. Deal IDs indicate premium inventory. Could bucket by deal prefix. |
| **video_placement** | 83% fill. Video-specific ad placement info. |
| **user_agent** | 93% fill. Could extract structured features (browser, device model). |
| **categories** | 13% fill. Supplementary to iab_categories. |

### Tier 3 — Low Value (skip for now)
| Feature | Rationale |
|---------|-----------|
| **ifa** | 78% fill but increasingly zeroed due to privacy (ATT, etc.). Limited shelf life. |
| **site_name** | 7% fill. Too sparse. |
| **isp** | 10% fill. Too sparse. |
| **page** | 15% fill. Web-only, high cardinality. |
| **referrer** | 4% fill. Too sparse. |
| **is_blocked** | 0% true. No signal. |
| **geo** | ~100% fill but almost entirely USA. Lat/lon/metro/zip could be useful for geo-targeting. |

---

## Key Observations — Augmentor Log

1. **Scale**: 1.2B rows/hour, ~29B rows/day, ~241 GB/day. Must sample aggressively for any analysis.
2. **CTV-dominated**: Most volume is CONNECTED_TV + SET_TOP_BOX via APP environment watching VIDEO. This matches MNTN's CTV focus.
3. **Case normalization needed**: os field has "roku os" vs "Roku OS" — LOWER() everything.
4. **Bronze has fields silver drops**: iab_categories, categories, page, referrer, isp, is_blocked are only in bronze.raw. If we need iab_categories for vertical classification, we must use bronze or the parquet archive.
5. **iab_categories is the vertical signal**: 30% fill, covers IAB taxonomy v1. Most directly useful for mapping bidstream activity to advertiser verticals.
6. **mntn_segments already assigned**: 86% of bid requests already have MNTN segments. This is what we'll check against for incrementality in DS13 augmentation.

---

# Table 2: bidder_auction_events

**Source:** `dw-main-bronze.raw.bidder_auction_events`
**Sample:** 1 hour (_PARTITIONTIME = 2026-03-30 13:00 UTC) = **112M rows**
**Full table:** 160B rows, 496 TB. 90-day TTL. HOUR partitioned by `_PARTITIONTIME`.
**Daily estimate:** ~2.8B rows/day

## Field Profile Summary

| Field | Fill % | Distinct (1hr) | Type | Notes |
|-------|--------|----------------|------|-------|
| **device_ip** | 88.75% | — | STRING | Same IP as augmentor_log.ip |
| **device_make** | 89.57% | 457 | STRING | **NEW** — device manufacturer (Roku, Samsung, Vizio, LG, Amazon, etc.) |
| **device_os** | 97.61% | — | STRING | Same as augmentor_log.os |
| **device_os_version** | 0.00% | — | STRING | **Empty** — not populated |
| **device_type_group** | 100% | — | STRING | Same as augmentor_log.device_type |
| **content_genre** | 86.79% | 37,038 | STRING | **NEW — HIGH VALUE** — what they're watching (Entertainment, news, drama, comedy, etc.) |
| **content_channel** | 36.24% | 25,259 | STRING | **NEW** — channel name |
| **content_series** | 37.09% | 8,817 | STRING | **NEW** — specific show/series name (Hawaii Five-0, NCIS, newscasts, etc.) |
| **content_network** | 37.57% | 4,378 | STRING | **NEW** — network name (structured) |
| **publisher_name** | 100% | 301 | STRING | **NEW** — publisher identity (cleaner than augmentor_log.network) |
| **publisher_domain** | 75.91% | — | STRING | **NEW** — publisher domain |
| **app_name** | 98.22% | 2,873 | STRING | **NEW** — app name (vs just bundle ID in augmentor_log) |
| **app_bundle** | 98.89% | — | STRING | Same as augmentor_log |
| **site_domain** | 0.76% | — | STRING | Nearly empty (CTV-dominated) |
| **site_page** | 0.75% | — | STRING | Nearly empty |
| **site_categories** | 0.13% | — | REPEATED STRING | Nearly empty (unlike augmentor_log.iab_categories at 30%) |
| **inventory_source** | 100% | 3 | STRING | Only 3 sources (vs 40 in augmentor_log) — likely just Magnite/Tremor/etc. |
| **environment_type** | 100% | — | STRING | APP vs WEB |
| **placement_type** | 100% | — | STRING | VIDEO vs BANNER |
| **geo_country** | 100% | — | STRING | Structured (not raw string like augmentor_log.geo) |
| **geo_zip** | 94.56% | — | STRING | ZIP code — higher fill than augmentor_log |
| **auction_dropped** | 100% | — | BOOLEAN | All rows = dropped auctions (we didn't bid) |
| **auction_dropped_reason** | — | — | STRING | Why we didn't bid |
| **segment_ids** | — | — | REPEATED INT | Same as augmentor_log.mntn_segments |
| **pmp_deal_ids** | — | — | REPEATED STRING | Same as augmentor_log.pmp |

## Content Genre Distribution (Top 20)

| Genre | Volume | Notes |
|-------|--------|-------|
| Entertainment | 168K | Broad category |
| news | 143K | Case-inconsistent with "News" (23K) |
| entertainment | 50K | Duplicate of Entertainment |
| documentary,music,short | 45K | Comma-delimited multi-genre |
| drama | 32K | |
| comedy | 30K | |
| Drama | 21K | Case duplicate |
| reality | 14K | |
| documentary | 11K | |
| western | 11K | |
| Comedy | 10K | Case duplicate |
| News & Opinion | 9K | |
| reality-tv | 4K | vs "Reality" (8K) — needs normalization |
| sports | 6K | |
| sci-fi & fantasy | 6K | |
| sitcom,comedy | 5K | Multi-genre |
| thriller | 5K | |
| game show | 5K | |
| crime | 4K | |
| GENRE_COMEDY | 4K | Prefixed format — different provider |

**37,038 distinct values.** Massive case inconsistency + multi-genre comma-delimited values + provider-specific formats (GENRE_*). Needs heavy normalization but **extremely rich signal for vertical classification**.

## Device Make Distribution (Top 10)

| Make | Volume | Notes |
|------|--------|-------|
| Roku | 421K | Dominant CTV platform |
| SAMSUNG | 135K | Case variant of Samsung (92K) |
| Vizio | 128K | |
| Samsung | 92K | |
| LG | 81K | |
| Amazon | 53K | Fire TV |
| Telly | 11K | Free ad-supported TV |
| DirecTV | 10K | STB |
| Onn | 10K | Walmart brand |
| Sony | 9K | |

457 distinct values. Needs UPPER() normalization (SAMSUNG vs Samsung).

## Content Series (Top Shows)

| Series | Volume | Notes |
|--------|--------|-------|
| Spanglish Films and Episodes | 137K | |
| d41d8cd98f00b204e9800998ecf8427e | 138K | MD5 hash — obfuscated |
| {{CONTENT_SERIES}} | 12K | Template placeholder — bad data |
| Scripps Local Live Newscast | 8K | |
| Hawaii Five-0 | 5K + 1.4K (URL-encoded) | Duplicate formats |
| WDBJ News | 5K | Local news |
| NCIS New Orleans | 3K | |
| CNN Headline Express | 2K | |

8,817 distinct values. Mix of real show names, hashed values, and template placeholders. **Usable after filtering garbage values.**

---

## Combined Feature Ranking (Both Tables)

### Tier 1 — Highest Value for IVR Testing
| Feature | Source Table | Fill % | Why |
|---------|-------------|--------|-----|
| **content_genre** | bidder_auction_events | 87% | What they're watching. Strongest vertical signal. Maps to advertiser categories. |
| **device_type / device_type_group** | both | 100% | CTV vs mobile vs desktop. Different behavior profiles. |
| **device_make** | bidder_auction_events | 90% | Roku vs Samsung vs LG — correlates with demographics. |
| **inventory_source** | augmentor_log (40 sources) | 100% | SSP identity = inventory quality signal. |
| **iab_categories** | augmentor_log (bronze only) | 30% | IAB taxonomy — direct vertical mapping. |
| **network / publisher_name** | both | 71-100% | Premium vs long-tail content. |
| **placement_type** | both | 100% | VIDEO vs BANNER. |
| **os** | both | 97-99% | Platform signal (after normalization). |

### Tier 2 — Medium Value
| Feature | Source Table | Fill % | Why |
|---------|-------------|--------|-----|
| **content_series** | bidder_auction_events | 37% | Specific show — very granular. Needs cleanup. |
| **content_channel** | bidder_auction_events | 36% | Channel identity. |
| **content_network** | bidder_auction_events | 38% | Network identity (structured). |
| **app_name / app_bundle** | both | 85-99% | App identity. High cardinality. |
| **pmp_deal_ids** | both | 98% | Premium deal signal. |
| **domain** | augmentor_log | 100% | Site identity. High cardinality. |
| **geo_zip** | bidder_auction_events | 95% | Geographic signal. |

### Tier 3 — Low Value / Skip
| Feature | Source Table | Why Skip |
|---------|-------------|----------|
| device_os_version | bidder_auction_events | 0% fill — empty |
| site_domain / site_page | bidder_auction_events | <1% fill — CTV dominated |
| site_categories | bidder_auction_events | 0.13% fill |
| ifa / device_ifa | both | Privacy-limited, decreasing value |
| referrer | augmentor_log | 4% fill |
| isp | augmentor_log | 10% fill |
| is_blocked | augmentor_log | 0% true |

---

## Methodology: Testing Against IVR

Per Matt Brorby's guidance (meeting 2026-03-30), the evaluation methodology is:

1. **Sample**: Pull small random IP samples with bidstream features attached
2. **Join to IVR outcome**: Match IPs to visit/conversion data from clickpass_log + impression logs
3. **XGBoost feature importance**: Train model with all Tier 1+2 features predicting IVR
4. **Composite scoring**: Use 3 importance methods (information gain, frequency, weighted) to rank features
5. **Iterative paring**: Start with all features → drop least important → retrain → verify performance holds
6. **Validate**: Use SHAP values for fine-tuning on the final feature set
7. **BIC check**: Balance model fit vs complexity

This is the same methodology used for Fangorn's existing feature selection, extended to bidstream features.

---

## Key Observations — Combined

1. **Two complementary tables**: augmentor_log has iab_categories (30% fill, vertical taxonomy) and 40 inventory sources. bidder_auction_events has content_genre (87% fill, what they're watching), device_make, and structured publisher/content fields.
2. **bidder_auction_events = dropped auctions only**: 100% auction_dropped. These are auctions we saw but didn't bid on. augmentor_log includes auctions we did participate in.
3. **Data quality issues everywhere**: Case inconsistency (os, content_genre, device_make), multi-value fields (comma-delimited genres), hashed/template garbage (content_series), provider-specific formats (GENRE_COMEDY). Heavy normalization required.
4. **content_genre is the breakout feature**: 87% fill, directly maps to what content the viewer is consuming. After normalization, ~50-100 real genres. This is the strongest new signal not currently in Fangorn.
5. **Both tables needed**: Best feature set combines augmentor_log (iab_categories, mntn_segments, 40 SSPs) with bidder_auction_events (content_genre, device_make, content_series, publisher_name).
6. **Scale consideration for modeling**: Need to sample aggressively. A 0.1% sample of 1 hour still gives ~1.2M rows from augmentor_log, ~112K from bidder_auction_events. Plenty for XGBoost feature importance.
