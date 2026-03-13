# TI-650 v17: Next Session Prompt

## Context for the LLM

Copy-paste this as your opening prompt:

---

I'm working on TI-650 (Stage 3 VV Audit). In the last session we did two things:

### 1. v17 CIDR fix — completed, minimal impact

We stripped `/32` CIDR suffixes from `event_log.ip` in the resolution rate query (v14 → v17). Results: **minimal impact** (+0.19pp for adv 37775 S3, +45 VVs). CIL's bare `bid_ip` in the S1 pool already compensated. The ~92% IP-based resolution ceiling is structural, not a CIDR artifact. Full results in `outputs/ti_650_v17_cidr_impact.md`.

### 2. Single-VV trace for unresolved `80207c6e` — identity graph confirmed, BUT we missed display impressions

We traced ad_served_id `80207c6e-1fb9-427b-b019-29e15fb3323c` (adv 37775, cg 93957, funnel_level 3, bid_ip `216.126.34.185` from bid_logs).

We searched `event_log` for this IP in campaign_group 93957 with:
- SPLIT on BOTH `ip` and `bid_ip` columns (CIDR-stripped)
- Time window extended to the clickpass timestamp (`2026-02-04 00:06:14`)
- Lookback all the way to `2025-01-01`

Result: only the VV's own vast_start + vast_impression found. Zero prior S1/S2 impressions.

**BUT** — we only searched `event_type_raw IN ('vast_start', 'vast_impression')`. This misses **display impressions**, which don't generate VAST events. I can't remember exactly where display impressions land. They should be in:
- `cost_impression_log` (CIL) — confirmed, the S1 pool CTE uses CIL for this reason ("covers display + failed vast events")
- `impression_log` — likely
- `bid_logs` / `win_logs` — likely (bids happen regardless of creative type)
- `event_log` with a different `event_type_raw`? — unknown, needs checking

**Task: Check if display impressions for this IP exist in cg 93957.**

Steps:
1. First, check what `event_type_raw` values exist in event_log (we may be filtering too narrowly):
   ```sql
   SELECT DISTINCT event_type_raw, COUNT(*) as cnt
   FROM `dw-main-silver.logdata.event_log`
   WHERE DATE(time) BETWEEN '2026-01-01' AND '2026-01-07'
   GROUP BY event_type_raw
   ORDER BY cnt DESC
   ```

2. Search CIL for bid_ip `216.126.34.185` in campaign_group 93957 (any funnel_level, any time before clickpass):
   ```sql
   SELECT cil.ad_served_id, cil.ip AS bid_ip, cil.campaign_id,
          c.campaign_group_id, c.funnel_level, c.objective_id, c.name,
          cil.time AS impression_time
   FROM `dw-main-silver.logdata.cost_impression_log` cil
   JOIN `dw-main-bronze.integrationprod.campaigns` c
     ON c.campaign_id = cil.campaign_id
     AND c.campaign_group_id = 93957
   WHERE cil.ip = '216.126.34.185'
     AND cil.time >= TIMESTAMP('2025-11-06')
     AND cil.time < TIMESTAMP('2026-02-04')
   ORDER BY cil.time ASC
   LIMIT 100
   ```

3. If CIL finds nothing, also check impression_log and bid_logs for completeness:
   ```sql
   -- impression_log
   SELECT il.ad_served_id, il.ip, il.time
   FROM `dw-main-silver.logdata.impression_log` il
   JOIN `dw-main-bronze.integrationprod.campaigns` c
     ON c.campaign_id = il.campaign_id AND c.campaign_group_id = 93957
   WHERE il.ip = '216.126.34.185'
     AND il.time >= TIMESTAMP('2025-11-06')
     AND il.time < TIMESTAMP('2026-02-04')
   LIMIT 100
   ```

4. Search event_log WITHOUT the vast filter — check ALL event types for this IP in cg 93957:
   ```sql
   SELECT el.ad_served_id, el.event_type_raw, el.campaign_id,
          c.funnel_level, SPLIT(el.ip, '/')[OFFSET(0)] AS vast_ip,
          SPLIT(el.bid_ip, '/')[OFFSET(0)] AS bid_ip, el.time
   FROM `dw-main-silver.logdata.event_log` el
   JOIN `dw-main-bronze.integrationprod.campaigns` c
     ON c.campaign_id = el.campaign_id AND c.campaign_group_id = 93957
   WHERE (SPLIT(el.ip, '/')[OFFSET(0)] = '216.126.34.185'
          OR SPLIT(el.bid_ip, '/')[OFFSET(0)] = '216.126.34.185')
     AND el.time >= TIMESTAMP('2025-01-01')
     AND el.time < TIMESTAMP('2026-02-04 00:06:14')
   ORDER BY el.time ASC
   LIMIT 100
   ```

**Why this matters:** If there's a display impression for this IP in a funnel_level 1 campaign within cg 93957 that we missed because we only looked at VAST events, it would mean the v14 resolution query has a gap — it should be finding these VVs but isn't. The CIL portion of the S1 pool should catch display impressions, but we need to verify empirically.

**Important principle from this session:** Always get IPs from the source table for each pipeline step (bid_logs.ip for bid_ip, not CIL). Saved to memory.

### Key context from prior work

- bid_ip for `80207c6e`: `216.126.34.185` (from bid_logs, verified identical across all 6 pipeline tables)
- campaign_group_id: `93957`, advertiser_id: `37775`, funnel_level: 3
- Bid timestamp: `2026-01-27 14:52:20`
- Clickpass timestamp: `2026-02-04 00:06:14`
- This VV is one of 1,716 unresolved S3 VVs (v17 campaign_group_id-scoped)
- v16 Step 3 already confirmed this IP has 1,200+ events across OTHER advertisers but zero for cg 93957 — but that search was also limited to vast events

### Remaining open items on TI-650

- GUID bridge on v14/v17 unresolved (1,716 VVs) — pending
- Update SQLMesh model to v14 architecture + campaign_group_id + GUID bridge
- Decide with Zach: include retargeting in pools?

---

## Notes

- Read `summary.md` and `knowledge/data_catalog.md` + `knowledge/data_knowledge.md` at session start per standard protocol
- CIL is 90-day rolling — lookback to Nov 6 is within window for Feb queries
- event_log has no TTL but CIDR suffix on pre-2026 data (always SPLIT)
- The v16 365-day lookup (`queries/ti_650_365d_ip_lookup.sql`) also only searched vast events — may need re-running without the vast filter too
