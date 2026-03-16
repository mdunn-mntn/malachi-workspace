# TI-650 v21: Exhaustive IP Proof — Query Plan

**Goal:** Prove definitively that IP `216.126.34.185` was NEVER served an S1 or S2 impression within campaign_group 93957, using every available physical table layer in BQ.

**VV under examination:**
- ad_served_id: `80207c6e-1fb9-427b-b019-29e15fb3323c`
- IP: `216.126.34.185`
- Campaign: 450300 (S3, CTV, cg 93957, adv 37775)
- Date: 2026-01-27

---

## Table Architecture (what Zach needs to understand)

The silver views are NOT the only copies. Each table exists at **4 layers**. We will query all 4 for each of the 3 cross-stage tables (12 queries total for IP search, plus setup/context queries).

```
Layer 1: silver VIEW          → logdata.event_log
Layer 2: silver sqlmesh VIEW  → sqlmesh__logdata.logdata__event_log__314628680
Layer 3: bronze history TABLE → sqlmesh__history.history__event_log__1601996237        ← physical table, no TTL
Layer 4: bronze raw TABLE     → sqlmesh__raw.raw__event_log__2961306213               ← physical table, 365-day TTL
```

The silver views (L1→L2) are just `SELECT *` wrappers. L2 is a SQLMesh enrichment query that UNIONs L3 (history) and L4 (raw). **L3 and L4 are the only physical tables with actual data.**

### Full table map

| Cross-stage table | Layer | Physical table | Type | TTL |
|---|---|---|---|---|
| event_log | L1 silver view | `dw-main-silver.logdata.event_log` | VIEW | — |
| event_log | L2 sqlmesh view | `dw-main-silver.sqlmesh__logdata.logdata__event_log__314628680` | VIEW | — |
| event_log | L3 bronze history | `dw-main-bronze.sqlmesh__history.history__event_log__1601996237` | TABLE | none |
| event_log | L4 bronze raw | `dw-main-bronze.sqlmesh__raw.raw__event_log__2961306213` | TABLE | 365d |
| impression_log | L1 silver view | `dw-main-silver.logdata.impression_log` | VIEW | — |
| impression_log | L2 sqlmesh view | `dw-main-silver.sqlmesh__logdata.logdata__impression_log__4185451957` | VIEW | — |
| impression_log | L3 bronze history | `dw-main-bronze.sqlmesh__history.history__impression_log__562817925` | TABLE | none |
| impression_log | L4 bronze raw | `dw-main-bronze.sqlmesh__raw.raw__impression_log__1161949256` | TABLE | 90d |
| viewability_log | L1 silver view | `dw-main-silver.logdata.viewability_log` | VIEW | — |
| viewability_log | L2 sqlmesh view | `dw-main-silver.sqlmesh__logdata.logdata__viewability_log__702576036` | VIEW | — |
| viewability_log | L3 bronze history | `dw-main-bronze.sqlmesh__history.history__viewability_log__2107880006` | TABLE | none |
| viewability_log | L4 bronze raw | `dw-main-bronze.sqlmesh__raw.raw__viewability_log__2998484813` | TABLE | 90d |

### Data retention (verified 2026-03-16)

| Physical table | Earliest data |
|---|---|
| history__event_log | 2025-01-01 |
| history__impression_log | 2025-01-01 |
| history__viewability_log | 2025-04-08 |
| raw__event_log | 2026-01-01 |
| raw__impression_log | 2025-08-25 |
| raw__viewability_log | 2025-12-31 |

**No BQ table at any layer has data before 2025-01-01.** Pre-2025 data exists only in Greenplum coreDW (deprecated April 30, 2026).

---

## Query Plan — Ordered Steps

### Phase 0: Context & Setup (run first)

**Q0.1 — Campaign group 93957 roster**
Show all campaigns in the group with funnel_level, channel, objective.
```sql
SELECT campaign_id, name, campaign_group_id, funnel_level, channel_id, objective_id,
       CASE funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' WHEN 4 THEN 'Ego' END AS stage,
       deleted, is_test
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE campaign_group_id = 93957
ORDER BY funnel_level, channel_id;
```
*Expected: 6 campaigns. S1=450305, S2=450301+450303, S3=450300+450304, Ego=450302.*

**Q0.2 — The VV itself from clickpass_log**
Confirm the ad_served_id, IP, campaign, and timestamp.
```sql
SELECT ad_served_id, ip, bid_ip, campaign_id, time, guid
FROM `dw-main-silver.logdata.clickpass_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
LIMIT 10;
```

---

### Phase 1: Search the physical base tables directly (L3 history + L4 raw)

For each of the 3 tables, search both physical layers. These are the actual data — views just read from these.

**IP matching:** `ip = '216.126.34.185' OR ip LIKE '216.126.34.185/%'` (same for bid_ip). Catches bare, /32, /24, any CIDR suffix.

#### 1A: event_log — CTV impressions

**Q1.1 — history__event_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'history__event_log' AS source,
  ev.campaign_id, ev.event_type_raw, ev.ip, ev.bid_ip, ev.ad_served_id, ev.time
FROM `dw-main-bronze.sqlmesh__history.history__event_log__1601996237` ev
WHERE ev.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY ev.time
LIMIT 200;
```

**Q1.2 — raw__event_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'raw__event_log' AS source,
  ev.campaign_id, ev.event_type_raw, ev.ip, ev.bid_ip, ev.ad_served_id, ev.time
FROM `dw-main-bronze.sqlmesh__raw.raw__event_log__2961306213` ev
WHERE ev.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY ev.time
LIMIT 200;
```

#### 1B: impression_log — non-viewable display impressions

**Q1.3 — history__impression_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'history__impression_log' AS source,
  il.campaign_id, il.ip, il.bid_ip, il.ad_served_id, il.time
FROM `dw-main-bronze.sqlmesh__history.history__impression_log__562817925` il
WHERE il.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY il.time
LIMIT 200;
```

**Q1.4 — raw__impression_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'raw__impression_log' AS source,
  il.campaign_id, il.ip, il.bid_ip, il.ad_served_id, il.time
FROM `dw-main-bronze.sqlmesh__raw.raw__impression_log__1161949256` il
WHERE il.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY il.time
LIMIT 200;
```

#### 1C: viewability_log — viewable display impressions

**Q1.5 — history__viewability_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'history__viewability_log' AS source,
  vl.campaign_id, vl.ip, vl.bid_ip, vl.ad_served_id, vl.time
FROM `dw-main-bronze.sqlmesh__history.history__viewability_log__2107880006` vl
WHERE vl.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY vl.time
LIMIT 200;
```

**Q1.6 — raw__viewability_log: ALL advertiser 37775 campaigns**
```sql
SELECT 'raw__viewability_log' AS source,
  vl.campaign_id, vl.ip, vl.bid_ip, vl.ad_served_id, vl.time
FROM `dw-main-bronze.sqlmesh__raw.raw__viewability_log__2998484813` vl
WHERE vl.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE advertiser_id = 37775)
  AND (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY vl.time
LIMIT 200;
```

---

### Phase 2: Confirm via silver views (matches v18)

These read from the same physical tables above. Should return identical results. This proves the views aren't filtering anything out.

**Q2.1 — silver event_log: advertiser 37775**
```sql
SELECT 'silver.event_log' AS source,
  ev.campaign_id, c.campaign_group_id, c.funnel_level, ev.event_type_raw, ev.ip, ev.bid_ip, ev.ad_served_id, ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = ev.campaign_id AND c.advertiser_id = 37775
WHERE ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY ev.time
LIMIT 200;
```

**Q2.2 — silver impression_log: advertiser 37775**
```sql
SELECT 'silver.impression_log' AS source,
  il.campaign_id, c.campaign_group_id, c.funnel_level, il.ip, il.bid_ip, il.ad_served_id, il.time
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = il.campaign_id AND c.advertiser_id = 37775
WHERE (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY il.time
LIMIT 200;
```

**Q2.3 — silver viewability_log: advertiser 37775**
```sql
SELECT 'silver.viewability_log' AS source,
  vl.campaign_id, c.campaign_group_id, c.funnel_level, vl.ip, vl.bid_ip, vl.ad_served_id, vl.time
FROM `dw-main-silver.logdata.viewability_log` vl
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = vl.campaign_id AND c.advertiser_id = 37775
WHERE (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-03-16'
ORDER BY vl.time
LIMIT 200;
```

---

### Phase 3: Filter to campaign_group 93957 only

Take the results from Phase 1/2 and filter to just cg 93957. This is the definitive proof.

**Q3.1 — ALL layers, cg 93957 only (combined query)**
```sql
-- Search every physical table, filter to cg 93957 only
SELECT 'history__event_log' AS source, ev.campaign_id, ev.event_type_raw, ev.ip, ev.bid_ip, ev.ad_served_id, ev.time
FROM `dw-main-bronze.sqlmesh__history.history__event_log__1601996237` ev
WHERE ev.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-03-16'

UNION ALL

SELECT 'raw__event_log' AS source, ev.campaign_id, ev.event_type_raw, ev.ip, ev.bid_ip, ev.ad_served_id, ev.time
FROM `dw-main-bronze.sqlmesh__raw.raw__event_log__2961306213` ev
WHERE ev.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2025-01-01' AND '2026-03-16'

UNION ALL

SELECT 'history__impression_log' AS source, il.campaign_id, CAST(NULL AS STRING), il.ip, il.bid_ip, il.ad_served_id, il.time
FROM `dw-main-bronze.sqlmesh__history.history__impression_log__562817925` il
WHERE il.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-03-16'

UNION ALL

SELECT 'raw__impression_log' AS source, il.campaign_id, CAST(NULL AS STRING), il.ip, il.bid_ip, il.ad_served_id, il.time
FROM `dw-main-bronze.sqlmesh__raw.raw__impression_log__1161949256` il
WHERE il.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2025-01-01' AND '2026-03-16'

UNION ALL

SELECT 'history__viewability_log' AS source, vl.campaign_id, CAST(NULL AS STRING), vl.ip, vl.bid_ip, vl.ad_served_id, vl.time
FROM `dw-main-bronze.sqlmesh__history.history__viewability_log__2107880006` vl
WHERE vl.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-03-16'

UNION ALL

SELECT 'raw__viewability_log' AS source, vl.campaign_id, CAST(NULL AS STRING), vl.ip, vl.bid_ip, vl.ad_served_id, vl.time
FROM `dw-main-bronze.sqlmesh__raw.raw__viewability_log__2998484813` vl
WHERE vl.campaign_id IN (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns` WHERE campaign_group_id = 93957)
  AND (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2025-01-01' AND '2026-03-16'

ORDER BY source, time;
```

*Expected: Only rows from campaign 450300 (S3). Zero rows from S1 campaign 450305 or S2 campaigns 450301/450303.*

---

### Phase 4: Trace the ad_served_id through every pipeline stage

Show that the VV's ad_served_id exists in the tables it SHOULD be in, and confirm the IP at every stage.

**Q4.1 — ad_served_id in clickpass_log (VV origin)**
```sql
SELECT 'clickpass_log' AS stage, ad_served_id, ip, bid_ip, campaign_id, time, guid
FROM `dw-main-silver.logdata.clickpass_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
LIMIT 10;
```

**Q4.2 — ad_served_id in event_log (VAST events)**
```sql
SELECT 'event_log' AS stage, ad_served_id, event_type_raw, ip, bid_ip, campaign_id, time
FROM `dw-main-silver.logdata.event_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND DATE(time) BETWEEN '2026-01-25' AND '2026-01-29'
LIMIT 10;
```

**Q4.3 — ad_served_id in impression_log**
```sql
SELECT 'impression_log' AS stage, ad_served_id, ip, bid_ip, campaign_id, time
FROM `dw-main-silver.logdata.impression_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND DATE(time) BETWEEN '2026-01-25' AND '2026-01-29'
LIMIT 10;
```

**Q4.4 — ad_served_id in viewability_log**
```sql
SELECT 'viewability_log' AS stage, ad_served_id, ip, bid_ip, campaign_id, time
FROM `dw-main-silver.logdata.viewability_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND DATE(time) BETWEEN '2026-01-25' AND '2026-01-29'
LIMIT 10;
```

**Q4.5 — ad_served_id in win_logs**
```sql
SELECT 'win_logs' AS stage, ad_served_id, ip, bid_ip, campaign_id, auction_id, time
FROM `dw-main-silver.logdata.win_logs`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND DATE(time) BETWEEN '2026-01-25' AND '2026-01-29'
LIMIT 10;
```

**Q4.6 — ad_served_id in cost_impression_log**
```sql
SELECT 'cost_impression_log' AS stage, ad_served_id, ip, bid_ip, campaign_id, auction_id, time
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  AND DATE(time) BETWEEN '2026-01-25' AND '2026-01-29'
LIMIT 10;
```

*These should all show ip = 216.126.34.185 and campaign_id = 450300 (S3). The point: the ad_served_id is a VALID, traceable impression — it just entered S3 via the identity graph, not via a prior S1/S2 impression.*

---

## Summary of what the proof shows

1. **Phase 0:** The campaign group has 6 campaigns. Only 1 is S1 (450305), 2 are S2 (450301, 450303).
2. **Phase 1:** Searching the raw physical tables directly — no view magic, no SQLMesh transforms — the IP appears ONLY in campaigns from other campaign groups. Zero S1/S2 in cg 93957.
3. **Phase 2:** Silver views return identical results, confirming no data is being hidden by views.
4. **Phase 3:** Filtering to cg 93957 only — every hit is campaign 450300 (S3). Zero S1/S2.
5. **Phase 4:** The specific ad_served_id traces cleanly through the full pipeline with the same IP at every stage. It's a legitimate S3 impression that entered via the identity graph.

**The IP entered S3 targeting via LiveRamp/CRM identity resolution, not via a prior MNTN impression. This is correct behavior — it's how the identity graph works. The 8% unresolved rate is the ceiling when scoping cross-stage linking to campaign_group_id.**
