# TI-650: Query Validation Plan (v2 Refactored Queries)

Execute this plan to validate the 3 refactored queries on a fresh set of advertisers. Only edit the 3 query files if bugs are found — no new query files.

**Estimated cost:** ~10-12 TB total | **Time:** ~1-2 hours including analysis

---

## What Changed from the Proven Originals

Before validating, understand exactly what the refactor changed:

**resolution_check.sql** (from s3_broad_sample_combined.sql): Cosmetic only — parameter markers and comments. SQL logic is identical.

**full_trace.sql** (from s3_trace_table.sql):
- **Added:** timestamp columns on all 5-source IP CTEs (bid_time, win_time, impression_log_time, viewability_time, event_log_time)
- **Added:** impression_type on S2/S1 linked VV rows (derived from `campaigns.channel_id` instead of NULL)
- **Changed:** UNION ALL now has CAST(NULL AS TIMESTAMP) entries to match new timestamp columns

**impression_detail.sql:** New query (no original to compare). Uses UNNEST for ad_served_id input, returns full metadata + timestamps.

---

## Phase 0: Advertiser Selection

**Audit window:** March 10-17, 2026 (most recent full week)

**Derived date parameters:**
- AUDIT_WINDOW: `2026-03-10` to `2026-03-17`
- LOOKBACK_START: `2025-03-10` (365d before audit start)
- SOURCE_WINDOW: `2026-02-08` to `2026-03-20` (±30d, capped at today)

### Step 0.1 — Discovery query (~0.5 GB)

Find advertisers with S3 VV volume using the cheapest table:

```sql
SELECT
    c.advertiser_id,
    adv.company_name,
    SUM(a.verified_visits) AS s3_vvs_7d
FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign` a
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = a.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level = 3
    AND c.objective_id IN (1, 5, 6)
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
WHERE a.dt BETWEEN '2026-03-10' AND '2026-03-16'
  AND c.advertiser_id NOT IN (
    -- Previously tested advertisers (24 broad sample + 5 deep dives)
    32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
    33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
    38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104,
    31357, 35573, 37056, 34094, 32230
  )
GROUP BY c.advertiser_id, adv.company_name
HAVING SUM(a.verified_visits) >= 500
ORDER BY s3_vvs_7d DESC
LIMIT 50;
```

### Step 0.2 — Select ~20 advertisers

Pick ~20 that sum to ~50K S3 VVs. Aim for a mix:
- 3-5 large (2,000-5,000 VVs each)
- 10-12 medium (1,000-2,000 VVs each)
- 3-5 smaller (500-1,000 VVs each)

### Step 0.3 — Confirm exact count with clickpass_log (optional, ~200 MB)

```sql
SELECT COUNT(DISTINCT cp.ad_served_id) AS exact_s3_vvs
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
WHERE cp.time >= TIMESTAMP('2026-03-10') AND cp.time < TIMESTAMP('2026-03-17')
  AND cp.advertiser_id IN (<selected list>)
  AND cp.ip IS NOT NULL
```

The agg table counts may differ from clickpass_log due to the QUALIFY ROW_NUMBER dedup.

**Pass:** ~20 advertiser IDs totaling 45K-55K S3 VVs, no overlap with excluded list.

---

## Phase 1: Dry Run All Three Queries

Parameterize all 3 queries with the selected advertisers and dates. Run each with `--dry_run`.

```bash
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "v2 resolution_check dry_run" \
  --use_legacy_sql=false --dry_run --project_id=dw-main-silver '<SQL>'
```

| Query | Expected Cost |
|-------|--------------|
| resolution_check | ~4-5 TB |
| full_trace | ~4-5 TB |
| impression_detail (1 dummy ID) | <2 TB |

**Pass:** All parse without error. Costs within expected ranges.

---

## Phase 2: Run resolution_check.sql (~4-5 TB)

```bash
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "v2 resolution_check 20adv mar10-17" \
  --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver '<SQL>'
```

Save output to `outputs/ti_650_v2_resolution_check_20adv.json`.

### Validation checks

| # | Check | Pass Criteria |
|---|-------|--------------|
| 2.1 | Row count | Exactly 1 row per selected advertiser |
| 2.2 | total_s3_vvs sum | Within 10% of Phase 0 estimate |
| 2.3 | no_ip | 0 for ALL advertisers |
| 2.4 | unresolved_no_ip | 0 for ALL advertisers |
| 2.5 | has_any_ip | = total_s3_vvs for every advertiser |
| 2.6 | resolved_vv_pct | >= 99% for most, >= 98% for all |
| 2.7 | t1 + t2 >= resolved_vv | T1 and T2 can overlap; resolved is the OR |
| 2.8 | unresolved_with_ip | < 2% per advertiser |
| 2.9 | Aggregate rate | Comparable to original 99.77% |

Record: GB processed, GB billed, slot seconds, wall seconds, cache hit.

---

## Phase 3: Run full_trace.sql (~4-5 TB)

**CRITICAL: Do NOT run simultaneously with Phase 2. Slot contention causes 3-5x slowdown.**

```bash
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "v2 full_trace 20adv mar10-17" \
  --use_legacy_sql=false --format=prettyjson --max_rows=200000 --project_id=dw-main-silver '<SQL>'
```

Save output to `outputs/ti_650_v2_trace_table_20adv.json`.

### Structural validation

| # | Check | Pass Criteria |
|---|-------|--------------|
| 3.1 | No orphan UUIDs | Every trace_uuid has exactly 1 stage=3 row |
| 3.2 | T1 row count | resolution='T1' → exactly 2 rows per trace_uuid |
| 3.3 | T2 row count | resolution='T2' → exactly 2 rows per trace_uuid |
| 3.4 | Unresolved row count | resolution='unresolved' → exactly 1 row per trace_uuid |
| 3.5 | Total unique UUIDs | = SUM(total_s3_vvs) from resolution_check |
| 3.6 | Total rows | = (T1 * 2) + (T2 * 2) + unresolved |

### New feature: timestamps

| # | Check | Pass Criteria |
|---|-------|--------------|
| 3.7 | Not all NULL on S3 rows | At least one timestamp NOT NULL for every stage=3 row with resolved_ip |
| 3.8 | CTV time ordering | bid_time <= impression_log_time <= win_time <= event_log_time <= vv_time |
| 3.9 | Display time ordering | bid_time <= win_time <= impression_log_time <= vv_time |
| 3.10 | Timestamps NULL on linked rows | All 5 timestamp cols NULL for stage=2 and stage=1 rows |
| 3.11 | IP-timestamp pairing | If bid_ip IS NOT NULL then bid_time IS NOT NULL (same for all 5 pairs) |

### New feature: impression_type on linked rows

| # | Check | Pass Criteria |
|---|-------|--------------|
| 3.12 | S2 bridge rows | impression_type IN ('CTV', 'Display') — not NULL |
| 3.13 | S1 direct rows | impression_type IN ('CTV', 'Display') — not NULL |
| 3.14 | S3 rows | impression_type IN ('CTV', 'Viewable Display', 'Non-Viewable Display') |
| 3.15 | Channel consistency | impression_type on linked rows matches channel column |

### IP link integrity

| # | Check | Pass Criteria |
|---|-------|--------------|
| 3.16 | S3→S2 IP match | For T1: stage=3 resolved_ip = stage=2 clickpass_ip (same UUID) |
| 3.17 | S3→S1 IP match | For T2: stage=3 resolved_ip = stage=1 clickpass_ip (same UUID) |

### Cross-validate against resolution_check

| # | Check | Pass Criteria |
|---|-------|--------------|
| 3.18 | Total S3 VVs | COUNT(DISTINCT trace_uuid) = SUM(total_s3_vvs) from Phase 2 |
| 3.19 | Per-advertiser T1 | Counts match per advertiser |
| 3.20 | Per-advertiser T2 | Counts match per advertiser |
| 3.21 | Per-advertiser unresolved | Counts match per advertiser |

**If 3.18-3.21 mismatch:** resolution_check uses aggregated pools (`MIN(vv_time)` per IP/cg), full_trace uses `ROW_NUMBER()` for specific matches. Both answer "does any prior VV exist?" so counts should match. If not, investigate the join conditions.

---

## Phase 4: Run impression_detail.sql (~0.5-2 TB)

### Step 4.1 — Select test ad_served_ids

From full_trace output, pick 10-15 IDs covering:
- 3 T1-resolved CTV
- 3 T1-resolved Viewable Display
- 2 T2-resolved (any channel)
- 2 unresolved
- 2 Non-Viewable Display (if any)
- 1-2 edge cases (earliest/latest in window)

### Step 4.2 — Run

```bash
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "v2 impression_detail 15ids" \
  --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver '<SQL>'
```

### Validation checks

| # | Check | Pass Criteria |
|---|-------|--------------|
| 4.1 | Row count | 1 row per ad_served_id |
| 4.2 | IPs match full_trace | resolved_ip, bid_ip, win_ip, impression_ip, viewability_ip, event_log_ip match stage=3 row |
| 4.3 | Timestamps match | bid_time, win_time, impression_log_time match full_trace |
| 4.4 | impression_type match | Matches full_trace stage=3 row |
| 4.5 | Metadata populated | advertiser_name, campaign_group_name, campaign_name all non-NULL |
| 4.6 | Unresolved IPs | Check if they match known problematic patterns (Google proxy, CGNAT, private) |

---

## Phase 5: Optimization Analysis

### Step 5.1 — Review bq_perf_log.jsonl

After all runs, compare:

| Metric | resolution_check | full_trace | impression_detail |
|--------|-----------------|------------|-------------------|
| GB processed | ~4-5 TB expected | ~4-5 TB expected | <2 TB expected |
| GB billed | | | |
| Slot seconds | | | |
| Wall seconds | | | |

### Step 5.2 — Check for optimization opportunities

1. **Source window tightening:** What's the actual max gap between impression and VV? If <20d, ±20d saves scan.
2. **Partition pruning:** Is BQ pruning partitions on time-filtered tables?
3. **bid_logs/win_logs TTL:** Any NULL bid/win IPs that shouldn't be? (90-day TTL should cover the source window)
4. **Join cardinality:** Are s3_s2_match / s3_s1_match producing excessive intermediate rows?
5. **Advertiser ID repetition:** The IN list appears 6 times. Not a perf issue but a usability concern.

### Step 5.3 — Compare to original run

Original 24-advertiser run: 36,388 VVs, ~4.25 TB, ~5 min wall time.
New 20-advertiser run: ~50K VVs (38% more). Wall time should scale roughly linearly.

---

## Phase 6: Write Findings Doc

Save to `outputs/ti_650_v2_validation_findings.md`:

```markdown
# TI-650: Query Validation Findings (v2 Refactored Queries)

## Summary
- Date: YYYY-MM-DD
- Audit window: Mar 10-17 2026
- Advertisers: 20 (new, no overlap with original 29)
- Total S3 VVs: [actual]
- Overall resolution: [X.XX%]

## Advertiser Selection
[table of selected advertisers with VV counts]

## Query 1: resolution_check.sql
- Status: PASS/FAIL
- [per-check results]
- Performance: X TB / Y seconds

## Query 2: full_trace.sql
- Status: PASS/FAIL
- Timestamps: PASS/FAIL
- Impression type on linked rows: PASS/FAIL
- Performance: X TB / Y seconds

## Query 3: impression_detail.sql
- Status: PASS/FAIL
- [per-check results]
- Performance: X TB / Y seconds

## Cross-Query Consistency
- resolution_check vs full_trace VV counts: MATCH/MISMATCH
- resolution_check vs full_trace T1/T2 counts: MATCH/MISMATCH
- full_trace vs impression_detail IP values: MATCH/MISMATCH

## Optimization Opportunities
- [findings]

## Issues Found & Edits Made
- [any bugs found, edits to the 3 query files, or "None"]
```

Commit everything and update `summary.md` Section 4 with the new validation results.

---

## Error Handling

**If resolution_check fails:** Most likely parameter substitution error (missing comma, wrong date format). Check the 6 ADVERTISER_IDS locations match.

**If full_trace fails:** Most likely UNION ALL column mismatch from new timestamp columns. Count columns in each SELECT — must be identical.

**If cross-query counts mismatch:** The pool-vs-match approach should yield same resolved/unresolved classification. If not, compare the join conditions between resolution_check's `s2_vv_pool` and full_trace's `s3_s2_match`.

**If impression_detail IPs don't match full_trace:** Check that SOURCE_WINDOW covers the same range in both queries.

---

## Execution Order (Sequential — Never Overlap Large Queries)

| Step | Phase | Est. Cost | Est. Time |
|------|-------|-----------|-----------|
| 1 | 0: Advertiser discovery | ~0.5 GB | 30 sec |
| 2 | 0: Clickpass confirmation | ~0.2 GB | 30 sec |
| 3 | 1: Dry runs (x3) | 0 GB | 1 min |
| 4 | 2: resolution_check | ~4-5 TB | 2-5 min |
| 5 | 3: full_trace | ~4-5 TB | 3-7 min |
| 6 | 4: impression_detail | ~0.5-2 TB | 1-3 min |
| 7 | 5-6: Analysis + doc | 0 GB | 30-60 min |
| **Total** | | **~10-12 TB** | **~1-2 hours** |
