# TI-650: Resolve 752 Unresolved S3 VVs

## Context

Read these files first (in order):
1. `tickets/ti_650_stage_3_vv_audit/summary.md` — full ticket state, start here
2. `outputs/ti_650_s3_tier_analysis.md` — S3 path analysis and unresolved profile
3. `outputs/ti_650_s2_tier_analysis.md` — S2 tier analysis (same pattern, for comparison)
4. `outputs/ti_650_s3_unresolved.json` — the 752 unresolved S3 VVs with all diagnostic columns

## What we've established

**v12 systematic rebuild** replaced the v11 10-tier CASE cascade (13 LEFT JOINs) with 2 cross-stage links:
1. **imp_direct**: S1 impression `vast_start_ip` = VV's `bid_ip`
2. **imp_visit**: S1 impression `vast_start_ip` = `ui_visits.impression_ip`

**Resolution rates (adv 37775, Feb 4–11, 90-day lookback, objective_id IN 1,5,6):**
- S1: 100% (93,274 VVs) — within-stage, ad_served_id deterministic
- S2: 99.95% (16,745/16,753) — 8 unresolved
- S3: 96.85% (23,092/23,844) — **752 unresolved** ← THIS IS THE PROBLEM

**Within-stage self-resolution is 100% at all levels.** Every VV has a matching impression via ad_served_id. The gap is only in CROSS-STAGE linking (tracing S3 VV back to an S1 impression).

**Chain paths (S3→S2→S1) are redundant.** Tested independently — only 2 unique contributions. S3→S1 direct (imp_direct + imp_visit) is the minimal set.

## Profile of the 752 unresolved

| Dimension | Value |
|-----------|-------|
| All have S3 impression | Yes — bid_ip is populated |
| **S1 IP exists (any time)** | **727/752 = NO (96.7%)** — the bid_ip has NEVER appeared as an S1 vast_start_ip |
| Attribution model | 512 competing (68%), 240 primary (32%) |
| is_cross_device | 415 true (55%), 337 false (45%) |
| first_touch_ad_served_id | Only 14 have one (1.9%) |
| visit_impression_ip available | 740 (98.4%) — also doesn't match S1 |
| bid_ip = vast_start_ip | 750/752 identical |
| Top subnets | 172.5x.x.x (T-Mobile CGNAT), 40.138.x (Verizon) |

**Root cause:** These IPs entered S3 via identity graph resolution (LiveRamp/CRM), not via an S1 impression on the same IP. The household had an S1 impression on a *different* IP, but CGNAT rotation means the S3 IP was never associated with S1.

## Task: Investigate and resolve these 752 VVs

Possible approaches to explore:

1. **Household IP graph**: `bronze.tpa.graph_ips_aa_100pct_ip` links IPs to households. Prior S2 analysis showed this resolved 46/232 VVs. Check how many of the 752 S3 IPs have household-linked IPs with S1 impressions.

2. **ipdsc (CRM HEM→IP)**: `dw-main-bronze.external.ipdsc__v1` maps hashed emails to IPs. If the S3 IP maps to the same hashed email as an S1 IP, that's a link.

3. **tmul_daily / tpa_membership_update_log**: Segment membership logs. If both the S3 IP and an S1 IP are in the same segments, that's evidence of the same household.

4. **CGNAT /24 subnet relaxation**: 727 unresolved IPs are mostly T-Mobile CGNAT (172.5x.x.x). If we relax the match from exact IP to same /24 subnet, how many resolve? This is less precise but may be acceptable for CGNAT pools.

5. **Accept as structural ceiling**: 752/23,844 = 3.15%. Primary unresolved = 240/23,844 = 1.01%. These are identity-graph-entry VVs with no IP-to-IP path. Document as structural limitation.

**Key constraints:**
- Filter: `objective_id IN (1, 5, 6)` — prospecting only
- 90-day lookback from trace window start
- Impression pool = event_log (VAST events) + cost_impression_log (display)
- S1 impressions dedup'd by vast_start_ip, earliest per IP
- All queries in `queries/ti_650_systematic_trace.sql`

**The unresolved JSON file has these columns per row:**
ad_served_id, campaign_id, vv_time, attribution_model_id, is_cross_device, first_touch_ad_served_id, redirect_ip, vv_guid, s3_bid_ip, s3_vast_start_ip, s3_vast_impression_ip, s3_impression_time, visit_impression_ip, diagnosis, has_s2_vv, s1_ip_exists_any_time
