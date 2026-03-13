# TI-650 v17: CIDR Fix Plan — Prompt for Next Chat

## Context for the LLM

Copy-paste this as your opening prompt:

---

I'm working on TI-650 (Stage 3 VV Audit). We just discovered a critical data quality issue:

**The `ip` column in `silver.logdata.event_log` has CIDR notation (`/32` or `/128` suffix) on ALL data before 2026-01-01.** On Jan 1 2026 it switched to bare IPs. The `bid_ip` column is always bare (no suffix). This means any query that joins `event_log.ip = some_other_table.ip` using exact string matching will silently fail to match on pre-2026 data.

**Fix:** Wrap event_log.ip with `SPLIT(ip, '/')[OFFSET(0)]` wherever it's used in a join or comparison.

There are two tasks. Do the fast one first.

### Task 1: Update the ad_served_id trace query (fast, ~5 min)

File: `tickets/ti_650_stage_3_vv_audit/queries/ti_650_ad_served_id_trace.sql`

The `ev_imp.ip` and `ev_start.ip` columns in the SELECT and the `ip_mutated` comparison need CIDR stripping for pre-2026 data. The fix is cosmetic — add `SPLIT(ev_imp.ip, '/')[OFFSET(0)]` and `SPLIT(ev_start.ip, '/')[OFFSET(0)]` in the SELECT aliases and in the `ip_mutated` expression. bid_ip is fine as-is.

Update the file header comment to note the CIDR handling. Commit and push.

### Task 2: Re-run v14 resolution rates with CIDR fix (the big one)

File: `tickets/ti_650_stage_3_vv_audit/queries/ti_650_resolution_rate_v14.sql`

**Why this matters:** The S1 pool CTE builds from `event_log.ip` (vast_start/vast_impression) with a lookback to 2025-11-06 — fully in CIDR territory. Every `el.ip` value has `/32` appended. But the downstream joins compare `s1_pool.match_ip = b.bid_ip` where bid_ip is bare. **These joins silently fail on all pre-2026 event_log rows.** The S1 pool is missing a huge chunk of its IPs from Nov-Dec 2025.

Similarly, the S2 chain CTE (`s2_chain_reachable`) joins S2 vast IPs from event_log — same CIDR issue.

**Fixes needed in v14 query:**

1. **s1_pool CTE, line 39:** Change `el.ip AS match_ip` → `SPLIT(el.ip, '/')[OFFSET(0)] AS match_ip`
2. **s2_chain_reachable CTE, line 69:** Change `el.ip AS vast_ip` → `SPLIT(el.ip, '/')[OFFSET(0)] AS vast_ip`
3. That's it — CIL.ip (bid_ip) is always clean, no fix needed there.

**Steps:**
1. Apply the two SPLIT fixes
2. Save as `ti_650_resolution_rate_v17.sql` (v17 = CIDR-corrected v14)
3. Dry run to check bytes
4. Run for the same 10 advertisers, Feb 4-11 window, 90-day lookback
5. Compare results to v14 — specifically:
   - Do resolved_pct values increase? (Expected: yes, because S1 pool now includes Nov-Dec 2025 vast IPs that were previously CIDR-broken)
   - Does the unresolved count for S3 drop below 540?
   - Which advertisers see the biggest improvement?
6. Save comparison to `outputs/ti_650_v17_cidr_impact.md`
7. Commit and push

**Expected outcome:** Resolution rates should improve, especially for S3 VVs whose S1 impressions were in Nov-Dec 2025. The 540 unresolved S3 VVs for adv 37775 should drop. This doesn't fix CRM campaign_groups (those are structurally unresolvable via IP), but it should fix false negatives from the string matching bug.

---

## Notes

- Read `summary.md` and `knowledge/data_catalog.md` + `knowledge/data_knowledge.md` at session start per standard protocol
- The v14 query is ~1.1 TB per run, takes ~35s on the adhoc reservation
- Prior v14 results are in `outputs/ti_650_v14_resolution_rates.md` (if exists) or inline in summary.md
- CIL (cost_impression_log) ip column is NOT affected — it's always bare IP
- impression_log.ip is NOT affected either (post-serve enrichment)
- Only event_log.ip has the CIDR issue
