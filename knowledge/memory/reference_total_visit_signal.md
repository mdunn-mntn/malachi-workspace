---
name: reference_total_visit_signal
description: "Attribution-independent total-visit signal: guid_log = page-views (dedup to visit-days per advertiser_id,ip,date; 366TB, partition-prune or Databricks/GCS), clickpass_log = visits; enriched.lift__ghost_bid_visits = INCR total-visit lift (7d, ghost/submitted arms, platform ref only); TI-835 ~0% total-traffic lift → use total visits (not attributed VV) for frequency RCTs but expect non-inferiority-shaped readouts"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2f8d4ec8-78d6-419a-9c3f-4da329f3c216
doc_type: memory
keywords: [total visit signal, guid_log, clickpass_log, attribution-independent, ghost bid, lift__ghost_bid_visits, frequency experiment, AUDI-1173, non-inferiority, ui_visits, TI-835]
domain: [experimentation, incrementality, data-catalog]
lifecycle: active
last_verified: 2026-07-28
---
**guid_log = PAGE-VIEWS, not visits (AUDI-1173).** `logdata.guid_log` (physical `dw-main-bronze.history.guid_log_physical`, ~107B rows / 366TB, DAY-partitioned on `time`) is the attribution-independent "did this household hit the advertiser site" signal — one row per page view, fires regardless of whether MNTN served an ad. **`clickpass_log` is the (MNTN-attributed) VISIT log.** For a total-site-visit metric, **unit = distinct visit-days per `(advertiser_id, ip, date)`** (dedup page-views; join key `(advertiser_id, ip)`, IP CIDR-stripped). Query discipline: **always partition-prune on `time` + cohort-restrict, or read via Databricks / GCS Spark** (`gs://mntn-data-archive-prod/guid_log/`) — never full-scan (bills 366TB).

**enriched.lift__ghost_bid_visits = the INCR total-visit lift table (Matt Brorby).** Binary `visited`/`converted` per arm×ip; `arm` ∈ {`ghost`=holdout, `submitted`=treatment}; guid_log-derived outcome. **Hard-coded 7-day-from-first-bid window with fixed arms** → a **platform 7d sanity/reference** only, NOT usable to size a ≥30-day or custom-arm (cap-8/cap-3) estimand. For a custom-arm/longer design, build the total-visit outcome directly from guid_log. See [[ghost_bid_lift_register]].

**Use total visits (guid_log), NOT attributed VV (`ui_visits`), for a FREQUENCY experiment (AUDI-1173).** Frequency drives last-touch attribution — the higher-frequency arm wins the attribution tiebreak, inflating its *attributed* visits independent of real behavior → attributed VV is mechanically biased in a frequency test. Total visits is attribution-independent. **TI-835 caveat:** total guid_log traffic barely moves with MNTN ads (~0% platform lift), so total-visit metrics suit **non-inferiority** (stable total visits = a safe cap) but are **insensitive for superiority**; keep attributed VV as a diagnostic companion, never the headline. Cross-device symmetric-coverage note: a relative contrast is coverage-invariant, a fixed absolute-pp margin is anti-conservative → size arm-symmetric RCTs on RELATIVE margin. See [[reference_frequency_capping]], [[project_audi_1173_freq_cap_bandit]].
