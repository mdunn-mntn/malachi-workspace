# AUDI-1091 [SPIKE] Full augmentor_log as a free site-visit source

**Status:** DONE — NO-GO (closed 2026-07-22) · **Parent:** AUDI-1054 · **Assignee:** Malachi

## Introduction
Alex's claim (AUDI-1089 review, 2026-07-13): augmentor display rows are site visits with URLs, and the full augmentor_log is a much bigger free site-visit source than the DS30 subset in `site_visit_signal` (svs) today. If true, ingesting it could displace paid DDP credit at $0 (relates to the AUDI-1093 free-log preemption thesis).

## Problem
Quantify the full augmentor_log vs the DS30 subset already in svs, estimate vendor-displacement, and scope ingestion. Give a go/no-go.

## How DS30/DS23 are populated (from the actual Spark jobs)
Sean Yang wrote the svs feeders (`SteelHouse/airflow-ti/spark/fpa/`):
- **`dsid30_augmentor_log_processing.py`** — reads full `augmentor_log` (east+west), then **filters to `placement_type IN ("BANNER","BANNER_AND_VIDEO")`**. Builds site visits from BOTH `page` and `referrer` (referrer timestamped 1s earlier), normalizes URLs (prepends `http://`), requires non-empty `ip`, first-touch dedup per (ip, url). So **DS30-in-svs = the BANNER slice of augmentor only.** The dropped placements (VIDEO/other) are the AUDI-1091 headroom.
- **`dsid23_guid_log_processing.py`** — guid_log, left-anti-joins pixel-isolation blocked advertisers, URL = `product_referer`, distinct. (Context, not the headroom.)

## Finding (first cut, 1-hour sample 2026-07-20 18:00–19:00 UTC; ~67 GB)
Query: `queries/audi_1091_placement_headroom.sql` · Output: `outputs/audi_1091_placement_headroom_1hr.json`

| placement_type | rows/hr | approx IPs | rows w/ URL | % w/ URL | in svs? |
|---|---|---|---|---|---|
| VIDEO | 954.4M | 12.3M | 3.73M | **0.39%** | no (dropped) |
| BANNER | 314.7M | 9.14M | 314.7M | 100% | yes |
| BANNER_AND_VIDEO | 0.53M | 0.20M | 0.52M | 98.3% | yes |

**Headline: the dropped placements are not site visits.** VIDEO is 75% of augmentor row volume but **99.6% URL-less** (only 0.39% carry a page/referrer URL) — they are CTV/video impressions with no webpage, consistent with MNTN's CTV-heavy bid stream. The URL-bearing, site-visit-usable augmentor rows are almost entirely the BANNER slice, which is **already fully ingested** into svs.

Incremental site-visit signal from ingesting full augmentor:
- **+~1.2% more site-visit rows** (3.73M VIDEO-with-URL vs 315.2M banner rows already in svs, per hour).
- **≤ +16% more IPs as an UPPER BOUND** (1.47M VIDEO-with-URL IPs vs ~9.3M banner IPs) — before removing overlap with banner IPs already ingested and with paid vendors, so true net-new is smaller.
- Cost side: ingesting "full augmentor" means carrying ~4x the row volume (the 954M/hr URL-less VIDEO rows) for that thin slice.

## Preliminary go/no-go: NO-GO / low priority
The "bigger free site-visit source" does not materialize: the extra augmentor volume is CTV/video without URLs, and the URL-bearing portion is already in svs. Net-new site-visit signal is ~1% of rows and ≤16% of IPs (upper bound, pre-overlap), versus ~2 sprints of Data Eng ingestion plus storage/compute for mostly-unusable rows.

## Open / to formalize the close
- Confirm placement mix on a **full day** (1-hr sample; mix is stable but the formal spike deserves a day). Table ~1.54 TB/day → Databricks/Spark on the GCS archive, not BQ.
- Measure **net-new IPs** = VIDEO-with-URL minus (BANNER svs IPs ∪ paid-vendor IPs) → the real displacement ceiling.
- Convert to **meter-displacement $** (credit-model aware, per AUDI-1092: 1/N fractional split). Expected negligible given ~1% incremental signal.
- Caveat: URL test matches the DS30 pipeline (page/referrer only). If web-video URLs lived in another column, they'd be missed — but the existing pipeline would not capture them either without new extraction logic.

## Related
AUDI-1089 (vendor evals), AUDI-1093 (free-log credit preemption — the DS30 banner augmentor is already the dominant free source there), AUDI-1092 (1/N fractional credit model), AUDI-1117 (widen free windows before paying vendors for reach).
