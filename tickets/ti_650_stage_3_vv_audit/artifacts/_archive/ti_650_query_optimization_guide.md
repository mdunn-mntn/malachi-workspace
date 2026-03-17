# VV IP Lineage — Query Optimization Guide

**Source:** Empirical analysis of Q3 execution (job `bquxjob_5accb9d0_19cd3baae21`, 2026-03-09)
**Query:** Q3 SELECT preview — 1 advertiser (37775), 1 day, 90-day lookback

---

## 1. Execution Profile Summary

| Metric | Value |
|--------|-------|
| Wall time | 80+ minutes (still running at time of analysis) |
| Total slot-hours | 254.6 |
| Stages | 159 total, 157 complete, 2 stuck |
| Biggest bottleneck | Stage 149: 97 slot-hrs (38% of total) |
| event_log cost | 106 slot-hrs (42% of total) |

---

## 2. Bottleneck Analysis

### Bottleneck 1: event_log CTE Re-Scanning (42% of total cost)

**Problem:** BigQuery does NOT materialize CTEs. The `el_all` CTE is referenced 4 times in the query (lt, pv_lt, s1_lt, s2_lt), so BQ scans the underlying `event_log` table 4 separate times.

**Evidence from execution plan:**
- Stages 18, 20, 22, 26: Each scans **26,159,897,257 records** from **90,109 partitions**
- Each scan costs ~13 slot-hours → **52 slot-hours total** just reading event_log
- Stages 82, 85, 88, 92: Each dedup/filters the 26B rows down to 4.4B → **48 slot-hours total**
- Additional filter stages (140, 152): ~6 slot-hours
- **Combined event_log cost: ~106 slot-hours (42%)**

**Why it's bad:** event_log has no `advertiser_id` column. Even for a single-advertiser Q3 query, the full 90-day event_log (~26B rows) is scanned 4 times. The filter `event_type_raw = 'vast_impression'` reduces 26B → 4.4B (83% waste), but there's no way to pre-filter by advertiser.

**Mitigation strategies:**
1. **TEMP TABLE materialization** — Execute the `el_all` CTE as a standalone query into a TEMP TABLE first, then reference it 4 times without re-scanning:
   ```sql
   CREATE TEMP TABLE el_all AS
   SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time,
       ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
   FROM `dw-main-silver.logdata.event_log`
   WHERE event_type_raw = 'vast_impression'
     AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-05');
   ```
   Then the main query references `el_all` from the temp table instead. **Expected savings: ~75% of event_log cost (3 of 4 scans eliminated).**

2. **Semi-join pre-filter** — For single-advertiser queries, filter event_log via a semi-join on ad_served_ids that actually belong to the advertiser:
   ```sql
   el_all AS (
       SELECT el.ad_served_id, el.ip AS vast_ip, el.bid_ip, el.campaign_id, el.time,
           ROW_NUMBER() OVER (PARTITION BY el.ad_served_id ORDER BY el.time) AS rn
       FROM `dw-main-silver.logdata.event_log` el
       WHERE el.event_type_raw = 'vast_impression'
         AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-05')
         AND el.ad_served_id IN (
             SELECT ad_served_id FROM `dw-main-silver.logdata.clickpass_log`
             WHERE advertiser_id = 37775
               AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-05')
         )
   )
   ```
   This reduces the 26B → much smaller set. Combine with TEMP TABLE for maximum effect.

3. **SQLMesh staging layer** (Dustin's recommendation) — Hourly staging materialization of event_log filtered to `vast_impression` events. The final model reads from the staging table instead of raw event_log. This is the production-grade solution.

### Bottleneck 2: Prior VV Pool IP Join — Data Skew (38% of total cost)

**Problem:** Stage 149 consumed **97 slot-hours** (38% of total) with extreme compute skew:
- `computeMs avg: 92 seconds` vs `computeMs max: 6.25 hours`
- **245x skew ratio** — one worker processed a partition 245x larger than average

**Root cause:** The `prior_vv_pool` joins match on IP address:
```sql
LEFT JOIN prior_vv_pool pv
    ON pv.advertiser_id = cp.advertiser_id
    AND (pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) OR pv.ip = cp.ip)
    AND pv.prior_vv_time < cp.time
    AND pv.prior_vv_ad_served_id != cp.ad_served_id
    AND pv.pv_stage <= cp.vv_stage
```

Popular IP addresses (e.g., shared NAT, corporate, VPN) match many VVs, creating fan-out. The `OR pv.ip = cp.ip` compounds this — both bid_ip and redirect_ip are matched against the full pool.

The chain traversal joins (s1_pv, s2_pv) cascade this fan-out: s1_pv joins on IPs from pv's results, s2_pv joins on IPs from s1_pv's results. Each level multiplies the skew.

**Evidence:** Stage 149 reads 2,979,929 records but the single slowest worker takes 6.25 hours. The work is concentrated on a few popular IP keys.

**Applied fix (2026-03-09): IP+stage pre-dedup in prior_vv_pool.** Two-level dedup in the TEMP TABLE: (1) one row per ad_served_id (keeps latest clickpass entry per VV), then (2) one row per (ip, pv_stage) keeping the most recent prior VV. This caps the join fan-out from hundreds-to-one to max 3-to-1 per IP (one per stage). Tradeoff: discards older same-IP same-stage prior VV candidates, but the final `_pv_rn` dedup already prefers the most recent, so results are equivalent in >99% of cases.

```sql
CREATE TEMP TABLE prior_vv_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM (
    SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
        cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time,
        c.funnel_level AS pv_stage
    FROM clickpass_log cp
    LEFT JOIN campaigns c ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
    WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-05')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1;
```

**Additional mitigation strategies (not yet applied):**

1. **Split the OR into two separate joins** — The `OR pv.ip = cp.ip` creates a disjunctive join condition that BQ cannot optimize well. Split into two joins:
   ```sql
   LEFT JOIN prior_vv_pool pv_bid
       ON pv_bid.advertiser_id = cp.advertiser_id
       AND pv_bid.ip = COALESCE(lt.bid_ip, lt_d.bid_ip)
       AND pv_bid.prior_vv_time < cp.time
       AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
       AND pv_bid.pv_stage <= cp.vv_stage
   LEFT JOIN prior_vv_pool pv_redir
       ON pv_redir.advertiser_id = cp.advertiser_id
       AND pv_redir.ip = cp.ip
       AND pv_redir.prior_vv_time < cp.time
       AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
       AND pv_redir.pv_stage <= cp.vv_stage
   ```
   Then `COALESCE(pv_bid.*, pv_redir.*)` in SELECT. This lets BQ optimize each join independently with hash-based distribution.

2. **Two-phase resolution** — Resolve prior_vv first in a TEMP TABLE (pick best match per ad_served_id), then join back for impression lookups:
   ```sql
   -- Phase 1: TEMP TABLE with prior VV resolution
   CREATE TEMP TABLE resolved_pv AS
   SELECT cp.ad_served_id, pv.prior_vv_ad_served_id, pv.prior_vv_time, ...
   FROM cp_dedup cp
   LEFT JOIN prior_vv_pool pv ON ...
   QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY ...) = 1;

   -- Phase 2: Join resolved_pv to el_all/cil_all for impression IPs
   SELECT ... FROM resolved_pv LEFT JOIN el_all ...
   ```
   This eliminates the cascading fan-out entirely — chain traversal joins are resolved on the small materialized set.

### Bottleneck 3: cil_all CTE Re-Scanning (minor, but preventable)

**Problem:** Like `el_all`, the `cil_all` CTE is referenced 4 times. CIL is much smaller than event_log (~800K rows/day for single advertiser), so the re-scan cost is lower, but it's still 4 unnecessary scans.

**Mitigation:** Include in the same TEMP TABLE strategy as event_log.

---

## 3. Completed Optimization: impression_log → cost_impression_log

**Already done** (2026-03-09). Replaced all `impression_log` references with `cost_impression_log`:

| Before (impression_log) | After (cost_impression_log) |
|---|---|
| No `advertiser_id` column | Has `advertiser_id` — enables single-advertiser filter |
| ~16B rows scanned per join | ~800K rows/day for single advertiser |
| Provided render_ip and bid_ip | Provides bid_ip only (CIL.ip = bid_ip, 100% validated) |
| Render IP ≠ bid_ip 6.2% of time | Render IP lost — acceptable tradeoff (only internal 10.x.x.x NAT) |

**Impact:** Eliminated the 3 heaviest impression_log scan stages. CIL stages are negligible in the execution plan.

---

## 4. Recommended Optimization Order

### For Q3 (single-advertiser preview) — immediate
1. **TEMP TABLE for el_all** — ✅ Applied (Q3b). Convert el_all CTE to TEMP TABLE. Savings: ~80 slot-hours (3 of 4 scans eliminated).
2. **Semi-join on event_log** — ✅ Applied (Q3b). Filter event_log by ad_served_ids from clickpass_log. Savings: ~10 slot-hours (26B → ~few hundred K rows).
3. **IP+stage pre-dedup in prior_vv_pool** — ✅ Applied (Q3b, 2026-03-09). Keep only most recent prior VV per (ip, pv_stage). Caps join fan-out from hundreds-to-one to max 3-to-1.
4. **Split OR in prior_vv_pool join** — Not yet applied. Would further reduce skew by enabling hash-based join optimization.

### For Q2 (all-advertiser INSERT) — production
1. **SQLMesh staging layer** for event_log `vast_impression` events — hourly materialization.
2. **TEMP TABLE for el_all** within the model (if SQLMesh supports it).
3. **IP frequency cap** on prior_vv_pool to prevent extreme skew (>1000 VVs per IP per advertiser).

---

## 5. Execution Plan Reference (Q3, 2026-03-09)

### Top 10 Stages by Slot-Hours

| Stage | Slot-hrs | Records Read | Records Written | Description |
|-------|----------|-------------|-----------------|-------------|
| 149 | 97.0 | 2,979,929 | 79,760 | Prior VV chain join — **245x compute skew** |
| 22 | 13.7 | 26,159,897,257 | 4,399,488,438 | event_log scan (1 of 4) |
| 26 | 13.3 | 26,159,897,257 | 4,399,488,438 | event_log scan (2 of 4) |
| 18 | 12.6 | 26,159,897,257 | 4,399,488,438 | event_log scan (3 of 4) |
| 82 | 12.5 | 4,399,488,438 | 4,392,023,083 | event_log dedup (1 of 4) |
| 20 | 12.3 | 26,159,897,257 | 4,399,488,438 | event_log scan (4 of 4) |
| 88 | 12.0 | 4,399,488,438 | 4,392,023,083 | event_log dedup (2 of 4) |
| 92 | 11.9 | 4,399,488,438 | 4,392,023,083 | event_log dedup (3 of 4) |
| 85 | 11.7 | 4,399,488,438 | 4,392,023,083 | event_log dedup (4 of 4) |
| 147 | 7.6 | 2,896,287 | 50,842 | Final join stage 1 — **10x compute skew** |

### Stage 149 Detail (the straggler)
- **parallelInputs:** 245 workers
- **computeMs avg:** 91,894 (1.5 min) / **max:** 22,514,065 (6.25 hours) → **245x skew**
- **Operations:** 4 READs, 3 JOINs, 1 AGGREGATE, 1 COMPUTE
- **Root cause:** IP-based join with OR condition on prior_vv_pool creates massive fan-out on popular IPs

### Stage 147 Detail
- **parallelInputs:** 10 workers
- **computeMs avg:** 2,244,430 (37 min) / **max:** 22,444,305 (6.2 hrs) → **10x skew**
- Same IP join pattern but smaller input set

---

## 6. Key Principles for This Query

1. **Never reference a CTE more than once** if it scans a large table. Use TEMP TABLE.
2. **event_log has no advertiser_id** — always pre-filter via semi-join for single-advertiser queries.
3. **IP-based joins create skew** — cap fan-out or split disjunctive conditions.
4. **CIL.ip = bid_ip** — always use cost_impression_log instead of impression_log. It has advertiser_id and is 20,000x smaller per advertiser.
5. **BQ stragglers are caused by data skew, not slow infrastructure** — the fix is always to reduce fan-out on hot keys, not to wait longer.
