# TI-790: Augmentor Log Feature Inventory

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

## Key Observations

1. **Scale**: 1.2B rows/hour, ~29B rows/day, ~241 GB/day. Must sample aggressively for any analysis.
2. **CTV-dominated**: Most volume is CONNECTED_TV + SET_TOP_BOX via APP environment watching VIDEO. This matches MNTN's CTV focus.
3. **Case normalization needed**: os field has "roku os" vs "Roku OS" — LOWER() everything.
4. **Bronze has fields silver drops**: iab_categories, categories, page, referrer, isp, is_blocked are only in bronze.raw. If we need iab_categories for vertical classification, we must use bronze or the parquet archive.
5. **iab_categories is the vertical signal**: 30% fill, covers IAB taxonomy v1. Most directly useful for mapping bidstream activity to advertiser verticals.
6. **mntn_segments already assigned**: 86% of bid requests already have MNTN segments. This is what we'll check against for incrementality in DS13 augmentation.
