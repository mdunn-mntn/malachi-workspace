---
doc_type: ticket
title: "AUDI-1091 [SPIKE]: Full augmentor_log as a free site-visit source"
status: done
date: 2026-07-22
summary: "AUDI-1091 [SPIKE]: Full augmentor_log as a free site-visit source"
result: "NO-GO (closed 2026-07-22). The URL-bearing augmentor rows are already the BANNER slice ingested into svs (DS30); the dropped VIDEO placements are 99.6% URL-less CTV, not site visits. Full ingestion yields only ~+1.2% more site-visit rows and <=+16% more IPs (upper bound), not worth ~2 sprints of Data Eng effort."
keywords: [augmentor_log, site_visit_signal, svs, ds30, ds23, placement_type, video, banner, dsid30_augmentor_log_processing, dsid23_guid_log_processing, free site-visit source, audi-1091, vendor displacement, no-go]
---

## TL;DR

**Q:** TL;DR for AUDI-1091 [SPIKE]: is the full augmentor_log a bigger free site-visit source than the DS30 subset already in site_visit_signal, and should MNTN ingest it?

**A:** NO-GO (closed 2026-07-22). Alex's claim that the full augmentor_log is a much bigger free site-visit source does not materialize. In a 1-hr sample (2026-07-20 18:00-19:00 UTC, ~67 GB), VIDEO is ~75% of augmentor row volume but 99.6% URL-less (only 0.39% carry a page/referrer URL) - CTV/video impressions with no webpage, not site visits. The URL-bearing rows are almost entirely the BANNER slice, which the DS30 svs feeder already ingests fully. Ingesting the full log adds only ~+1.2% more site-visit rows (3.73M VIDEO-with-URL vs 315.2M banner rows/hr) and <=+16% more IPs as an UPPER BOUND (1.47M vs ~9.3M, before removing overlap with banner and paid-vendor IPs, so true net-new is smaller), at the cost of carrying ~4x the row volume in URL-less VIDEO rows. Verdict: net-new signal ~1% of rows and <=16% of IPs vs ~2 sprints of Data Eng ingestion plus storage/compute for mostly-unusable rows.

**How:** Read the two Spark svs feeders (SteelHouse/airflow-ti/spark/fpa/dsid30_augmentor_log_processing.py and dsid23_guid_log_processing.py) to establish that DS30-in-svs = the BANNER slice of augmentor only. Then ran a 1-hour BQ sample query (audi_1091_placement_headroom.sql, ~67 GB) breaking augmentor rows by placement_type with rows/hr, approx IPs, and % carrying a page/referrer URL. First-cut only; full-day confirmation on the GCS archive via Databricks/Spark (~1.54 TB/day) still to formalize the close.

**Tables:** `bronze.raw.augmentor_log`, `site_visit_signal`, `guid_log`

**Learned:**
- DS30-in-svs is only the BANNER (+BANNER_AND_VIDEO) slice of augmentor_log; the dropped VIDEO placements were the hypothesized headroom but are 99.6% URL-less CTV/video, not site visits.
- VIDEO is ~75% of augmentor row volume but only 0.39% of VIDEO rows carry a page/referrer URL; BANNER (~25% of rows) is ~100% URL-bearing at ~9.14M IPs/hr.
- The dsid30 feeder builds site visits from BOTH page and referrer (referrer timestamped 1s earlier), normalizes URLs by prepending http://, requires non-empty ip, and does first-touch dedup per (ip, url).
- The dsid23 feeder reads guid_log, left-anti-joins pixel-isolation blocked advertisers, uses URL = product_referer, distinct.
- Full-day placement mix not yet confirmed (1-hr sample; mix stated stable but formal spike deserves a day); net-new IPs and meter-displacement $ (1/N model, per AUDI-1092) still to be measured to formalize the close.

**Reuse when:**
- Evaluating whether a raw log (augmentor, guid, other) is a viable free/incremental site-visit source before proposing ingestion
- Estimating DDP vendor/credit displacement from a free log
- Questions about what the DS30 / DS23 site_visit_signal feeders actually keep vs drop
- Sizing augmentor_log placement composition or scan cost

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
