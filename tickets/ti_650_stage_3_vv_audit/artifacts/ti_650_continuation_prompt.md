# TI-650: Continuation Prompt for Next Session

Copy this into a new Claude session to continue the work.

---

## Prompt

I'm continuing work on TI-650 (Stage 3 VV Audit). Read the ticket summary at `tickets/ti_650_stage_3_vv_audit/summary.md` to orient, then read the latest outputs:

- `outputs/ti_650_v19_zach_summary.md` — latest findings (v19 full pipeline trace + exhaustive IP trace)
- `outputs/ti_650_v18_exhaustive_ip_trace.md` — detailed v18 results

### Current state (as of 2026-03-13):

**Completed:**
- v14: campaign_group_id scoping — S3 resolution 91.98% for adv 37775
- v15: forensic trace — IP 100% identical across all 8 source tables
- v17: CIDR fix — minimal impact (+0.19pp)
- v18: exhaustive IP trace — all 3 cross-stage tables (event_log, viewability_log, impression_log), 2yr lookback, cg 93957 + full advertiser
- v19: full VV pipeline trace — IP consistent at every stage, split into 2-stage optimized query

**Key findings proven:**
- 92% is the IP-based resolution ceiling under campaign_group_id scoping
- Unresolved VVs entered S3 via identity graph, not prior MNTN impression
- IP `216.126.34.185` has zero S1/S2 records in ANY table for ANY campaign in cg 93957, 2+ years, including deleted campaigns
- Same IP was served S1/S2 in 6 other campaign groups for the same advertiser — correctly excluded by campaign_group scoping

**Pending:**
1. **GUID bridge on v14 unresolved (1,761 → 1,716 after CIDR fix)** — haven't run yet with campaign_group_id scoping
2. **Update SQLMesh model** from v10.1 to v14 architecture + campaign_group_id + GUID bridge
3. **Decision from Zach:** include retargeting in pools? GUID bridge scope? Final architecture sign-off?
4. **Deployment:** Confirm dataset name with Dustin, PR the SQLMesh model, backfill from 2026-01-01

### Optimization notes for future queries:
- event_log/viewability_log/impression_log: 2yr scans are ~26 TB each. Use UNION ALL to combine 3 tables into 1 query (saves 2 full scans)
- Replace `SPLIT(ip, '/')[OFFSET(0)]` with `ip = 'x' OR ip LIKE 'x/%'` — avoids function overhead
- Filter event_log to `event_type_raw IN ('vast_impression', 'vast_start')` — other event types not needed for cross-stage tracing
- For single-VV traces: use independent CTEs with pushed-down ad_served_id instead of CTE + LEFT JOINs
- win_logs + bid_logs: get auction_id first, then query with literal value (3s vs minutes)
- We're on slot reservation (`dw-main-bronze:us-central1.adhoc`), not on-demand billing — byte cost is for tracking, not billing
