---
name: reference_aud22_geo_reporting_sync
description: Mission-control aud22 (Geo Includes/Excludes) is a recurring geo-data-sync bug in location_data (metro_id vs hierarchy disagree on DMA). AUDI-1072's fix (PR #1147) was written but NEVER deployed; the ticket closed only because DM suppressed the audit. Recurs per geo_version with rotating ZIPs.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [aud22, audit 22, audit #22, FA022, geo includes excludes, AUDI-1072, PR 1147, DEV-8264, dw-main-gold, network_locations, location_data, v_location_data_lat_long, geo_version, DMA 638, metro_id, hierarchy, template 55, win_logs, mission control, measurement team, Sonali Vengurlekar, Nivas Nalla, Nate Gardner, Brian Teller, ipdata, geo violation]
domain: [data-catalog, infra, routing-people]
lifecycle: active
last_verified: 2026-07-29
---

Mission-control **audit 22 "Geo Includes / Excludes" (FA022)** reconciles the last 24h of impressions against the audience Geo expressions and flags IPs served outside the targeted geo. It is a **recurring** violation class, not a one-off.

**Root cause (confirmed):** the `dw-main-bronze.geo.location_data` model has rows where the `metro_id` field and the parent `hierarchy` chain disagree on the metro/DMA (e.g. ZIP 43221 Columbus-OH loc 708867: `metro_id`=535/Columbus but `hierarchy`→298>638/Toledo). The audience/TPA-export side reads the **hierarchy**; the impression side reads a **different field**, so the same IP resolves in-geo on one and out-of-geo on the other → a false violation. Source of truth = ad-buying-ui `ipdata` (`https://ad-buying-ui.prod.in.mountain.com/ui/ipdata?id=<IP>`), which agrees with the audience config. **Metro/DMA 638 (Toledo) recurs** as the wrong value.

**Which field the impression side reads (lizz, CIL owner):** for `campaign_template_id = 55` CIL geo comes from **win_logs** (auction_id join); for **all other** campaigns from `geo.network_locations` (bid_ip → network prefix → geo_version match → most-specific block). So aud22 on non-template-55 CGs compares network_locations-derived geo vs the audience expression.

**Fix status — the key fact:** the fix is **sqlmesh PR #1147** ("keep location_data.metro_id and hierarchy in sync" — a GENERAL `metro_id` COALESCE for `location_type_id` 6/7, already reflecting Sonali's review). **It was NEVER deployed** — still an OPEN DRAFT (as of 2026-07-13). Blocked because `sqlmesh plan` tried to backfill/break downstream (needs **forward-only**) and Nivas couldn't run plan locally without gold access → **DEV-8264** = "Identity team needs read/write access on bigquery on project **dw-main-gold**" (to run sqlmesh models locally; had bronze/silver, missing gold). Filed by Nivas 2026-07-13, **still In Progress**, assignee Brian Teller. **Operational blocker (Nivas, 2026-07-29):** the `sqlmesh plan` for this change takes **>24h** to finish, but dw-main-gold access is granted via **PAM in 8h windows** — so the grant expired mid-plan (Nivas had to re-request every 8h) and it never completed. DPLAT has started looking at deploying it. **A forward-only deploy (Sean's own suggestion) sidesteps the multi-hour backfill and the PAM-expiry trap entirely** — that's the unlock, not just persistent gold access. **AUDI-1072 was marked Done only because DM suppressed the audit** (ignore template-55, filter World Cup), not because the root cause shipped. Sean's "DM said false positive, fixed on their end" = the DM-side suppression, not the model fix.

**Open caveat:** PR #1147 fixes `location_data` but does **not** touch `network_locations` (Nivas) — since non-template-55 CIL geo comes from network_locations, confirm #1147 alone clears the firing (Harry's 07-28 trace shows the wrong 638 living in `v_location_data_lat_long.hierarchy`, which IS #1147's target, so likely yes).

**Recurrence PROVEN (2026-07-29, queried):** at geo_version `1783900800` the 2 original ZIPs (43221, 45814) are clean, but 5 **different** US ZIPs now carry the same `metro_id`↔`hierarchy` mismatch (01223, 17371, 66214, 92545, 95245). The affected ZIPs **rotate each geo rebuild** because the systematic fix stays undeployed. Diagnostic: left-join `location_type_id=7` ZIPs to `location_type_id=3` metros on `metro_id`, flag where metro `location_id` NOT IN `UNNEST(hierarchy)`.

**Ownership + handoff (2026-07-29):** the geo pipeline is now owned by the **Measurement team** (former BER+ATTR). Nivas moved teams and no longer has deploy perms. **Sonali Vengurlekar** owns the `location_data` logic. In-thread resolution: **Nate Gardner (Measurement)** took the action to resurrect/review/ship #1147 and to investigate a parallel `network_locations` fix. Note DEV-8264 was filed under **Identity**, so Measurement likely needs its OWN dw-main-gold access to run the plan/backfill. Answer to "redeploy per-CGID?" = **no**, it's a source-model fix, not campaign-specific.

**07-28 fire scope:** tiny — 6 non-World-Cup CGs (337294, 620519, 642894, 311521, 311104, 613551), 11 IPs, 12 imps, $0.12. Low-volume but a **real data artifact** (not noise), and **unrelated** to the same-night late `ipdsc_geo` drops ([[reference_oncall_runbook]] INC-004). The audit_22 log itself (`dw-main-bronze.external_ddm.audit_22_geo_inclusion_exclusion_logs`, external over `gs://mntn-data-monitoring/feature-audits/audit_22/logs`) is not readable by `malachi@mountain.com` (no storage.objects.list). Distinct pipeline from [[feedback_source_table_ips]].
