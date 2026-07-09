# AUDI-1089 / Klickly (DS39) — Renewal Pass/Play

**Status:** IN PROGRESS — verdict due **2026-07-13** (renewal is live; Paulo needs pass/play Monday)
**Billing:** `flat_fee` per registry (Paulo believes "waterfall usage-based" — reconcile with renewal schedule)
**Prior evidence (TI-1027, 7d windows):** 4.8M rows/day · 1.1M IPs/day · ~200 domains/day · 100% URLs with path ·
**132 unique classified domains** · 77.7% domain-unique (of a tiny base) · delivered IPs skew HI (52-57%) on
small volume · scorecard 35.8 → REVIEW ("negligible — too small to matter")

## Verdict plan

Fee band: floor = T1 (HHST-gated + scored, non-RTC, HS-not-AHS impressions to Klickly-sole IPs) × data-CPM
lens; ceiling = T2 (all impressions to sole IPs) × media lens; peer anchor $0.50 CPM. Expected shape from
priors: band near $0 — decision-ready even before the actual fee arrives. The only possible rescue: a non-MM
consumer of DS39 (lineage blast-radius check) or a 30d uniqueness picture materially better than the 7d one.

## Findings

*(filled as steps complete — steps per ticket-root summary.md §3)*

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-09

**Registry** (`tpa.direct_data_partners`, CDC-deduped → 1 row): billing_type **flat_fee** (fixed_cpm null),
enabled, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched, **valid_from 2025-07-01**
(≈ contract anniversary July 1 — consistent with Paulo's "they're up right now"), valid_to null,
**notes NULL — no fee amount anywhere in our data; must come from Paulo's renewal schedule.**

**Delivery — LIVE, streaming.** Klickly is NOT a file-drop partner (nothing under `gs://mntn-data-partners/`).
Path: vendor server-side sends → `pixel-page-view-signal` Kafka topic → hourly parquet
`gs://mntn-data-archive-prod/pixel_page_view_signal/dt=/hh=/` (all DSIDs commingled) →
`site_visit_signal/data_source_id=39` (landing hourly through today 2026-07-09).
Volume: **~850 of 110K rows in a sampled hour (<1% of the pixel topic; DS40 dominates it).**

**Raw richness — thinnest possible.** DS39 payload = IP + URL + timestamp ONLY: referer/user_agent/query_str
0% filled, mobile always false (server-side send), advertiser_id 100% NULL in svs.
**93.4% of DS39 URLs are `*.myshopify.com` product pages** (~155 distinct shops in sample: pulsetto, draxe,
ekster…) → Klickly = a Shopify-network product-page-view feed, mostly long-tail shop domains.

**Lineage blast radius (org-wide code sweep, file-level evidence):**
- **MM site-visit path** (vertical classification → DS19/DS13 signal builds → MM reporting/tpa_export) — the known consumer.
- **BUK training enrichment** (the ONLY real non-MM consumer): feature-store site-visit rollups
  (`site_visit_signal_advertiser_id_dsc_id`, excludes only DS23) feed the Bottom-Up Keywords ALS training
  pipeline at **source_weight=0.05 and a 5% stratified sample** — DSID-agnostic, degrades gracefully; no
  hard DS39 dependency.
- Passive monitoring/audit dashboards (would just show DS39 → 0).
- **Verified NON-consumers:** identity graph (idg: zero hits), attribution (page-view topic → analytics only;
  no data_source_id propagation to guidv2), bidder/serving/interests.
- **Conclusion: no hard dependency rescues a drop.** Off-switch is vendor-side (streaming), not a DAG change —
  DS39 isn't in ENABLED_DSIDS (that's the batch DAG); ingestion stops when their pixel stops sending.

### 2. Scale + freshness (30d)
- pending

### 3. Uniqueness (30d)
- pending

### 4. Quality (delivered score tiers, junk check, IPv6)
- pending

### 5. Value anchor (media/data-cost lens, tiered)
- pending

### 6. Verdict
- pending
