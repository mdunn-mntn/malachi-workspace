# Raw Vendor Data — Audit Map (pre-`site_visit_signal`)

**Every raw data dump feeding (or adjacent to) `site_visit_signal`, before we drop columns.** Locations confirmed
with Sean Yang (2026-06-17). `site_visit_signal` keeps only **`ip, url, user_agent, time`** (+ uid/dt/hh/ds) — so any
column below not in that set is **dropped at ingestion**. Sample any dump read-only via a BQ temp external table:
`bq query --external_table_definition="r::PARQUET=gs://…/*.parquet" 'SELECT * FROM r LIMIT 1'` (for TSV: `gsutil cat … | gunzip | head`).

## Roots
- **Batch:** `gs://mntn-data-partners/partners/<vendor>/`
- **Streaming:** `gs://mntn-data-archive-prod/pixel_page_view_signal/dt=/hh=/` (parsed parquet) — even-rawer JSON
  source at `gs://mntn-analytics-raw/topics/pixel-page-view-signal/YYYY/MM/DD/HH/*.json` (see `_source_file`).
- **Internal (free) logs:** `gs://mntn-data-archive-prod/guid_log/` and `…/augmentor_log/region={east,west}/`.
- Stage-1 post-ingestion archive (already column-selected): `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=NN/`.

## Site-visit feeds (these load `site_visit_signal`)
| Vendor | DS | Type | Raw location | Fmt | Raw columns | Kept | **Dropped** |
|---|---|---|---|---|---|---|---|
| **5x5** | 25 | batch | `partners/5x5/ip_to_url/y=/m=/d=/h=/` | parquet | `_COL_0`(ip), `_COL_1`(url), `_COL_2`(epoch) | all 3 | none — **thin** |
| **Cybba** | 36 | batch | `partners/cybba/date=/hour=/` | parquet | ip, time, url | all 3 | none — **thin** |
| **Predactiv** | 26 | batch | `partners/predactiv/dt=YYYYMMDDHH/` | parquet | **26 cols** (below) | ip, url, userAgent, standardTimestamp | **22**: `hem_md5/sha1/sha256` (hashed emails!), `geo_city/country/dma/dmaCode/iso/postal/subISO`, `concepts`(topic+score), `entities`, `keywords`, `domain_industries`, `domain_description/descriptors`, `deviceType`, `os`, `browserFamily`, `lang`, `refDomain`, `domain` |
| **33Across** | 28 | batch | `partners/33across/YYYY-MM-DD-HH/` | **TSV.gz** | **32 cols** (below) | TIMESTAMP, CLIENT_IP_ADDRESS, USER_AGENT, PAGE_URL | **28**: `PAGE_CATEGORY(+_KEYWORDS)`, `PAGE_CATEGORY_2(+_KEYWORDS)`, `TITLE`, geo (`ZIP`,`DMA`,`REGION`,`MAXMIND_GEO_ID`,`COUNTRY`), device (`SEC_CH_UA_*` ×6, `DEVICE_IDS`), `LANGUAGE`, consent (`GPP`,`GPP_SID`,`GPC`,`US_PRIVACY`,`DNT`), `COOKIE`,`COOKIE_33`,`ADDRESSBAR_GUID`,`X_FORWARDED_FOR`,`INITIAL_VISIT` |
| **Justuno / Sovrn / Klickly / 33Across API** | 24/33/39/40 | streaming | `pixel_page_view_signal/dt=/hh=/` (filter `data_source_id`) | parquet | _batch_id, _source_file, data_source_id, epoch, event_id, ip, mobile, query_str, referer, url, user_agent | ip, url, user_agent, time | `event_id`, `mobile`, `referer`, `query_str` (URL-encoded: data_source_key, referer, user_agent, **gpp/gpp_sid** consent) |
| **guid_log** (internal/free) | 23 | internal | `guid_log/dt=/hh=/` | parquet | ip, product_referer, query, ua_raw, advertiser_id, time, … | ip, url(=product_referer), query_parameters, user_agent, advertiser_id, time | (pixel-isolation filter applied) |
| **augmentor_log** (internal/free) | 30 | internal | `augmentor_log/region={east,west}/dt=/hh=/` | parquet | ip, user_agent, time, page, referrer, placement_type, **iab_categories**, **mntn_segments**, inventory_source, device_type, pmp, … | ip, url(page+referrer), user_agent, time | placement_type, iab_categories, mntn_segments, etc. (added to site_visit_signal ~Apr 2026 — absent in older partitions, per Ryan Kleck) |

## Big audit finding — we discard rich enrichment we already receive
- **Predactiv (4 of 26 kept):** we drop **hashed emails** (identity linkage), **full geo**, **`domain_industries`**
  (firmographics — directly relevant to B2B), and pre-computed **`concepts`/`keywords`/`entities`** (topic
  classification with confidence scores, e.g. "sourdough" 0.99). This is a content+identity+geo feed we're treating
  as ip/url only.
- **33Across (4 of 32 kept):** we drop **page categories + keywords**, **geo** (zip/DMA/region), `TITLE`, **device
  client-hints**, language, and **consent** signals (GPP/GPC).
- **Pixel feeds:** we drop user-agent-derived device, referrer, mobile flag, and **GPP consent**.
- **5x5 and Cybba are genuinely thin** (ip/url/time only) — nothing to tap without asking the vendor.
- **Implication:** much of the "more value" is a **pipeline change** (parse columns we already pay for), not new
  vendor cost. Also a **compliance note**: GPP/GPC/US_PRIVACY/DNT consent fields arrive in the raw feeds but are
  dropped — worth confirming downstream consent handling.

## How valuable is the discarded data? (Predactiv fill rates, 2026-06-17 sample)
The dropped fields are present on **most events** — Predactiv (~84M events/day): **OS 100% · device 98.8% · geo
(city/ZIP) 75.8% · keywords 65.5% · concepts 64.7% · hashed-email 60.5%** (`ti_1027_untapped_fill_rates.csv`).
Mapped to capability:
| Discarded field | ~% events | Unlocks | Why it matters |
|---|---|---|---|
| Page categories + keywords (33Across) · concepts/keywords (Predactiv) | ~65% | Page-level content classification | We pay OpenAI to classify domains→verticals; this is richer (page-level) and ~free |
| Geo — city/ZIP/DMA (Predactiv, 33Across) | ~76% | Geo without MaxMind lookups | Fills the **20–25% of bids that lack geodata** (known revenue gap, per north star) |
| Hashed emails (Predactiv) | ~60% | Identity resolution | Feeds the identity graph / CRM match |
| Device / OS / user-agent (most feeds) | ~99% | Device features + bot filtering | CTV vs mobile vs desktop; cleaner signal |
| domain_industries (Predactiv) | — | Firmographics | B2B targeting (Q2 growth theme) |
**Bottom line:** we already pay for these feeds, so tapping the metadata is a **pipeline change, not new vendor cost**
(high ROI). 5x5/Cybba are thin (nothing to tap). Compliance: GPP/GPC consent fields arrive raw and are dropped.

## Non-site-visit batch dumps (other pipelines — audit with the same technique)
| Vendor | Location | Apparent type |
|---|---|---|
| LiveRamp | `partners/liveramp/{data_marketplace,onboarding}/` | Interest segments / onboarding (DS11/35 — dominant 3P) |
| ShareThis | `partners/sharethis/{categories,ip-unified-userprofile,segments}/` | Interest segments + IP profiles (DS17) |
| Experian | `partners/experian/CV/…` | Demographics / CRM (DS22) |
| Deepsync | `partners/deepsync/ip_hem/` | IP↔hashed-email identity (feeds DS29 / CRM) |
| Bombora | `partners/bombora/{impressions,reporting}/` | B2B intent |
| Alliant | `partners/alliant/*.parquet` | Transaction / CPG (purchase data) |

*Source: TI-1027 audit, 2026-06-17. Confirmed delivering through 2026-06-17 (5x5, 33across, predactiv, cybba, pixel).*
