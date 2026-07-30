---
name: reference_aud22_geo_reporting_sync
description: Mission-control aud22 (Geo Includes/Excludes) is a recurring reporting-geo-vs-audience-config sync bug; AUDI-1072 fixed one facet but it recurs per geo_version. The reporting side (network_locations→hierarchy) is what's out of sync, not the audience config.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [aud22, audit 22, audit #22, geo includes excludes, AUDI-1072, network_locations, v_location_data_lat_long, geo_version, DMA 638, metro_id, hierarchy, mission control, Nivas Nalla, ipdata, reporting geo, geo violation]
domain: [data-catalog, infra]
lifecycle: active
last_verified: 2026-07-29
---

Mission-control **audit 22 "Geo Includes / Excludes"** is a **recurring** violation class, not a one-off. Root shape: for a small set of IPs the **reporting geo** resolves the IP to a DMA/metro *outside* the campaign's targeted geo, while the **audience definition and the tpa export are correct** (in-geo). The out-of-sync surface is the geo reporting DB, NOT the targeting config — so it flags "violations" that never actually mis-delivered. Source of truth = the ad-buying-ui `ipdata` lookup (`https://ad-buying-ui.prod.in.mountain.com/ui/ipdata?id=<IP>`) + CIL; it agrees with the audience config, disagrees with `network_locations`.

**Mechanism (BQ):** the reporting path is `geo.network_locations` (CIDR→postal_code, geo_version-pinned) → `dw-main-bronze.geo.v_location_data_lat_long` (postal→`hierarchy`, `location_type_id=7`) → DMA from the hierarchy array. The bug lives in the location data: the `hierarchy` rolls a ZIP up to the **wrong metro**, disagreeing with the row's own `metro_id`. **Metro/DMA 638 recurs** as the wrong value (e.g. a Columbus-OH ZIP that should be DMA 517 shows 638 in hierarchy). See `data_catalog.md` "Geo Location Mapping Discrepancy" (`parent_location_id` coreDW vs BQ facet).

**Ticket lineage:**
- **AUDI-1072** ("Audit #22 - Investigation", Sean Yang, resolved 2026-07-23). Sonali (BER) found only 2 of 6 flagged ZIPs had a real `metro_id`↔`hierarchy` mismatch (43221 loc 708867, 45814 loc 173370 — both hierarchy-metro 638). Fix = sqlmesh **PR #1147** (COALESCE `metro_id` for `location_type_id` 6/7 in the `with_parents` CTE). Benny set aud22 to ignore Template ID 55 and planned a separate Win-Log Geo Audit. Sean closed it "DM said false positive, fixed on their end."
- **Recurrence 2026-07-28** (geo_version `1783900800`, 6 CGs: 337294, 620519, 642894, 311521, 311104, 613551). Harry Connelly: `network_locations`→DMA 638 (out) vs `audience.audiences` + ipdata→DMA 517 Columbus OH (in). zach.schoenberger: "same issue Nivas deployed a fix for last time — **not fully fixed, or an aspect still needs fixing; the system is not in sync**." Open Q (Jen Wang → Nivas Nalla): does Nivas need to redeploy for the specific CGID?

**Confirmed vs open:** the reporting-vs-config divergence and the metro-638 hierarchy bug are confirmed (queried both sides). "Recurs per geo_version rebuild" and "AUDI-1072 fix incomplete" are zach's working hypothesis, not yet proven. The 07-28 fire is tiny — [[reference_oncall_runbook]] INC-004 scoped it as **real but boundary-noise: 6 CGs, 11 IPs, 12 imps, $0.12**, below aud22's own FAIL noise floor, and **ruled out** as related to the same-night late `ipdsc_geo` drops. Distinct pipeline from [[feedback_source_table_ips]] work.
