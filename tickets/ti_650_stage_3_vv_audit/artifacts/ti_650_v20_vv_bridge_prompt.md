# TI-650: v20 — VV Bridge Cross-Stage Correction

## The Breakthrough

Zach provided a traced IP guide (`queries/ti_650_zach_traced_ip_guide`) showing how IP `216.126.34.185` actually entered S3 targeting for campaign group 93957. **We were searching the wrong table for the cross-stage link.**

### What Zach proved:

```
S1 impression (450305 Prospecting)  IP: 172.59.117.71  (Roku CTV)
    | VAST ip match (impression-based S1→S2 link)
S2 impression (450301 Multi-Touch)  IP: 172.59.117.71  (Tubi CTV, same household)
    | cross-device visit
S2 VV (clickpass_log)               IP: 216.126.34.185 (iPhone)
    | VV ip enters S3 targeting (VV-based S2→S3 link)
S3 impression (450300 MT Plus)      IP: 216.126.34.185
    |
S3 VV (clickpass_log)               IP: 216.126.34.185 (our original VV)
```

**Key details from Zach's trace:**
- S2 VV: ad_served_id `dff2bce6-d35a-4455-9e1a-f7036e971af2`, campaign 450301, clickpass IP `216.126.34.185`, clickpass time 2026-01-24, impression_time 2026-01-13
- S2 CIL: bid_ip `172.59.117.71` (DIFFERENT from clickpass IP — cross-device!)
- S2 event_log: VAST ip `172.59.112.229`, bid_ip `172.59.117.71`
- S1 event_log: ip `172.59.117.71`, campaign 450305, vast_impression + vast_start on 2026-01-27
- S2 VV has `first_touch_ad_served_id = d508dc77-5910-41e0-8829-908f86241075` (an earlier S1 touch)

### The fundamental correction:

| Cross-stage link | What enters the next stage's targeting | Table to search | Old (wrong) approach |
|---|---|---|---|
| S1 → S2 | S1 **impression** (VAST ip) | `event_log` | Was correct |
| S1/S2 → S3 | S1 or S2 **verified visit** (clickpass ip) | **`clickpass_log`** | Searched event_log (wrong!) |

**S3 targeting is VV-based, not impression-based.** To get into S3, the IP needed a prior S1 or S2 verified visit in the same campaign_group_id. In cross-device scenarios, the VV's clickpass IP is completely different from the impression's bid IP. We were searching impression tables (event_log, viewability_log, impression_log) and finding zero — because `216.126.34.185` never had an S1/S2 impression. But it DID have an S2 VV in clickpass_log.

## Campaign Group 93957

"7 2025 Wedding CRM", advertiser 37775, created 2025-07-11. 6 campaigns:

```
campaign_id  name                                    funnel  stage  channel  objective_id
450300       Beeswax Television Multi-Touch Plus      3       S3     CTV      1
450301       Beeswax Television Multi-Touch           2       S2     CTV      1
450302       Beeswax Television Prospecting - Ego     4       Ego    CTV      7
450303       Multi-Touch                              2       S2     Display  5
450304       Multi-Touch - Plus                       3       S3     Display  6
450305       Beeswax Television Prospecting           1       S1     CTV      1
```

## Tasks

### Task 1: Verify Zach's trace independently

Run these queries to confirm the chain Zach showed. This validates the corrected methodology before we apply it at scale.

**Step 1a: Find the prior S2 VV in clickpass_log**
```sql
-- Search clickpass_log for prior S1/S2 VVs with IP = 216.126.34.185
-- in cg 93957, BEFORE the S3 impression time (2026-01-27)
SELECT
  cl.ad_served_id,
  cl.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  cl.ip,
  cl.time AS vv_time,
  cl.impression_time,
  cl.first_touch_ad_served_id,
  cl.guid,
  cl.is_new,
  cl.attribution_model_id
FROM `dw-main-silver.logdata.clickpass_log` cl
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = cl.campaign_id
WHERE cl.ip = '216.126.34.185'
  AND c.campaign_group_id = 93957
  AND c.funnel_level IN (1, 2)
  AND cl.time >= TIMESTAMP('2025-07-11')
  AND cl.time < TIMESTAMP('2026-01-27')  -- before S3 impression
ORDER BY cl.time
;
```

Expected: Find S2 VV `dff2bce6` on campaign 450301, clickpass time 2026-01-24.

**Step 1b: Get the S2 impression's bid_ip (may differ from VV ip!)**
```sql
-- Get the S2 impression bid_ip from CIL
SELECT
  cil.ad_served_id,
  cil.campaign_id,
  cil.ip AS bid_ip,
  cil.time,
  cil.guid,
  cil.device_type,
  cil.domain,
  cil.model_params
FROM `dw-main-silver.logdata.cost_impression_log` cil
WHERE cil.ad_served_id = 'dff2bce6-d35a-4455-9e1a-f7036e971af2'
  AND cil.time >= TIMESTAMP('2026-01-13')
  AND cil.time < TIMESTAMP('2026-01-14')
;
```

Expected: bid_ip = `172.59.117.71` (DIFFERENT from `216.126.34.185`).

**Step 1c: Get the S2 VAST events**
```sql
-- Get the S2 event_log records (VAST ip may also differ)
SELECT
  ev.ad_served_id,
  ev.campaign_id,
  ev.event_type_raw,
  ev.ip AS vast_ip,
  ev.bid_ip,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
WHERE ev.ad_served_id = 'dff2bce6-d35a-4455-9e1a-f7036e971af2'
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND ev.time >= TIMESTAMP('2026-01-13')
  AND ev.time < TIMESTAMP('2026-01-14')
;
```

Expected: bid_ip = `172.59.117.71`, VAST ip = `172.59.112.229`.

**Step 1d: Find S1 impression using the S2 bid_ip**
```sql
-- Search event_log for S1 VAST events matching the S2 bid_ip
SELECT
  ev.ad_served_id,
  ev.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  ev.event_type_raw,
  ev.ip,
  ev.bid_ip,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
WHERE (ev.ip = '172.59.117.71' OR ev.bid_ip = '172.59.117.71')
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND c.campaign_group_id = 93957
  AND c.funnel_level = 1
  AND ev.time >= TIMESTAMP('2025-07-11')
  AND ev.time < TIMESTAMP('2026-01-28')
ORDER BY ev.time
;
```

Expected: Find S1 campaign 450305 events with ip = `172.59.117.71`.

### Task 2: Rewrite the cross-stage trace query

Rewrite `queries/ti_650_ip_funnel_trace_cross_stage.sql` with the corrected methodology. The `prior_funnel` CTE currently searches `event_log` for S1/S2 VAST events matching the S3 bid_ip. It should instead:

1. Search `clickpass_log` for prior S1/S2 VVs where `clickpass.ip = S3.bid_ip`
2. Get the prior VV's ad_served_id
3. Look up the prior VV's impression bid_ip from CIL (may differ from clickpass ip!)
4. Search `event_log` for S1 VAST events matching the prior VV's bid_ip (for S2 VV → S1 chain)

**Corrected architecture for the prior_funnel CTE:**
```sql
-- Step A: Find prior S1/S2 VVs in clickpass_log where ip = S3.bid_ip
prior_vvs AS (
  SELECT
    cl.ad_served_id AS prior_vv_ad_served_id,
    cl.campaign_id AS prior_vv_campaign_id,
    pc.funnel_level AS prior_vv_funnel_level,
    pc.name AS prior_vv_campaign_name,
    cl.ip AS prior_vv_ip,
    cl.time AS prior_vv_time,
    cl.impression_time AS prior_vv_impression_time,
    cl.first_touch_ad_served_id
  FROM clickpass_log cl
  JOIN prior_campaigns pc ON pc.campaign_id = cl.campaign_id
  WHERE cl.ip = (SELECT bid_ip FROM s3_trace LIMIT 1)
    AND cl.time < (SELECT bid_timestamp FROM s3_trace LIMIT 1)
    AND cl.time >= TIMESTAMP_SUB((SELECT bid_timestamp FROM s3_trace LIMIT 1), INTERVAL 90 DAY)
    -- add date filter for partition pruning
),

-- Step B: Get the prior VV's impression bid_ip (may differ from VV clickpass ip!)
prior_vv_bid_ips AS (
  SELECT
    cil.ad_served_id,
    cil.ip AS prior_impression_bid_ip,
    cil.time AS prior_impression_time
  FROM cost_impression_log cil
  WHERE cil.ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vvs)
    -- add date filter for partition pruning
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time) = 1
),

-- Step C: For S2 VVs, search for S1 VAST events matching the S2 impression bid_ip
prior_s1_impressions AS (
  SELECT
    ev.ad_served_id AS s1_ad_served_id,
    ev.campaign_id AS s1_campaign_id,
    ev.ip AS s1_vast_ip,
    ev.bid_ip AS s1_bid_ip,
    ev.time AS s1_time
  FROM event_log ev
  JOIN prior_campaigns pc ON pc.campaign_id = ev.campaign_id AND pc.funnel_level = 1
  WHERE ev.ip IN (SELECT prior_impression_bid_ip FROM prior_vv_bid_ips)
    AND ev.event_type_raw IN ('vast_impression', 'vast_start')
    -- add date filter
)
```

Save the rewritten query to `queries/ti_650_ip_funnel_trace_cross_stage_v2.sql`.

### Task 3: Rewrite the resolution rate query (v14 → v20)

The main production query `queries/ti_650_resolution_rate_v14.sql` has the same bug in its `s2_chain_reachable` CTE (lines 61-88). It matches `S3.bid_ip → S2 VAST ip`, but should match `S3.bid_ip → S2 clickpass ip`.

**Current (wrong) `s2_chain_reachable`:**
```
S2 VAST IP (match to S3 bid_ip) → S2 bid_ip → S1 pool ✓
```

**Corrected `s2_chain_reachable`:**
```
S2 clickpass IP (match to S3 bid_ip) → S2 impression bid_ip → S1 pool ✓
```

The corrected CTE should:
1. Get S2 VVs from `clickpass_log` (ip = the match column for S3)
2. Get S2 impression bid_ip from `cost_impression_log` via ad_served_id
3. Check S2 bid_ip exists in `s1_pool` (same campaign_group_id)

Also add an S1 VV path: S3 can also come from S1 VVs directly. Add a similar `s1_vv_chain` CTE that searches clickpass_log for S1 VVs.

Save to `queries/ti_650_resolution_rate_v20.sql`.

### Task 4: Run v20 and compare to v14

Run the v20 resolution rate query for the same 10 advertisers and Feb 4-11 window as v14. Compare:
- S3 resolved count and percentage (expect INCREASE — cross-device cases now resolved)
- S3 via S2→S1 chain count (expect INCREASE — now uses VV bridge instead of VAST ip)
- S3 unresolved count (expect DECREASE)
- S2 rates should be UNCHANGED (S2→S1 link was already correct)

Save comparison to `outputs/ti_650_v20_vv_bridge_impact.md`.

### Task 5: Update documentation

After running the queries:
1. Add finding #27 to `summary.md` Key Findings documenting the VV bridge correction
2. Update `knowledge/data_knowledge.md` with the corrected cross-stage architecture:
   - S1→S2: impression-based (VAST ip)
   - S1/S2→S3: VV-based (clickpass ip) — cross-device ip divergence expected
3. Update the cross-stage key in `summary.md` MES Pipeline IP Map

## Corrected Cross-Stage Architecture (for reference)

```
Within-stage (all stages): deterministic via ad_served_id / auction_id
  clickpass_log → event_log → impression_log → win_logs → bid_logs
  IP should be identical at each stage (validated v15: 100%)

Cross-stage:
  S1 → S2:  S2.bid_ip  → S1.event_log.ip (VAST) or impression_log.ip or viewability_log.ip
             (impression-based — S2 targeting = "had an S1 impression")

  S1/S2 → S3:  S3.bid_ip  → S1_or_S2.clickpass_log.ip (prior VV!)
                (VV-based — S3 targeting = "had a prior verified visit")
                Then: prior_VV.ad_served_id → CIL.ip (= prior impression bid_ip, may differ!)
                Then: prior_impression_bid_ip → S1.event_log.ip (for S2 VV → S1 chain)

  Key insight: In cross-device scenarios, the VV clickpass IP ≠ the impression bid IP.
  The VV ip is what enters the next stage's targeting, not the impression ip.
```

## Existing Queries and What They Do

| Query | Purpose | Cross-stage method | Needs fix? |
|---|---|---|---|
| `ti_650_ip_funnel_trace.sql` | Within-stage trace (single VV) | None | No |
| `ti_650_ip_funnel_trace_cross_stage.sql` | Single VV cross-stage trace | event_log (wrong) | **YES → Task 2** |
| `ti_650_resolution_rate_v14.sql` | Aggregate resolution rates | S2 VAST ip (wrong) | **YES → Task 3** |
| `ti_650_systematic_trace.sql` | Full trace for adv 37775 | event_log + CIL (wrong) | Archive |
| `ti_650_resolution_rate_v17.sql` | CIDR-corrected v14 | Same as v14 (wrong) | Archive |

## BQ Optimization Rules (MUST follow)

1. **TIMESTAMP filters, not DATE():** `time >= TIMESTAMP('YYYY-MM-DD') AND time < TIMESTAMP('YYYY-MM-DD+1')` — DATE(time) defeats partition pruning
2. **Narrow date ranges:** Use impression dates, not wide scans
3. **No LIKE wildcards for IP matching:** Silver log table IPs don't carry CIDR suffixes. Use exact `=`
4. **Always date-filter clickpass_log:** Even with ad_served_id filter, no date = full 110 GB scan
5. **Run tables individually:** Don't UNION ALL across tables — run each separately
6. **Use `--dry_run` first** for unfamiliar queries — abort if >5 GB
7. **Use `bq_run.sh` wrapper** for all queries:
   ```bash
   bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "description" \
     --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver \
     'YOUR SQL HERE'
   ```

## Key Gotchas

- **funnel_level is authoritative for stage** — NOT objective_id (48,934 S3 campaigns have obj=1 due to UI bug)
- **campaign_group_id scoping is mandatory** (Zach directive)
- **Campaign group 93957 created 2025-07-11** — no impressions before that date
- **clickpass_log.impression_time** gives you the impression date; `clickpass_log.time` is the VV date. These often differ by days/weeks.
- **CIL (cost_impression_log) has 90-day TTL** — use it for recent data, fall back to impression_log for older
- **bid_logs/win_logs also have 90-day TTL**
- **objective_id reference:** 1=Prospecting, 4=Retargeting, 5=Multi-Touch(S2), 6=MT Full Funnel(S3), 7=Ego
- **Prospecting filter:** objective_id IN (1, 5, 6) — but funnel_level is more reliable

## What success looks like

1. Zach's trace independently confirmed via BQ queries
2. Cross-stage trace query rewritten with clickpass_log VV bridge
3. Resolution rate v20 shows measurable improvement over v14 for S3 (cross-device cases now traceable)
4. Documentation updated with corrected architecture
5. The 92% IP-based ceiling from v14 should INCREASE — how much depends on how many S3 VVs had cross-device prior VVs
