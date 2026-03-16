# TI-650: Bid IP Divergence Analysis — New Chat Prompt

## Hypothesis

We've proven that IP `216.126.34.185` has **zero** S1/S2 impressions within campaign group 93957. But what if there are other S3 impressions for the same campaign (450300) where the `bid_ip` differs from `216.126.34.185`? If the bid happened on a different IP, that different IP might have S1/S2 history in cg 93957 — meaning the identity graph resolved the household correctly, and the S3 entry IS traceable, just via a different IP than the VV's clickpass IP.

## Context

**Ticket:** TI-650 — Stage 3 VV Audit
**Campaign group 93957** ("7 2025 Wedding CRM"), advertiser 37775. 6 campaigns:

```
campaign_id  name                                    funnel  stage  channel
450300       Beeswax Television Multi-Touch Plus      3       S3     CTV
450301       Beeswax Television Multi-Touch           2       S2     CTV
450302       Beeswax Television Prospecting - Ego     4       Ego    CTV
450303       Multi-Touch                              2       S2     Display
450304       Multi-Touch - Plus                       3       S3     Display
450305       Beeswax Television Prospecting           1       S1     CTV
```

S1/S2 campaigns to search: **450305** (S1 CTV), **450301** (S2 CTV), **450303** (S2 Display).

**Original VV:** ad_served_id `80207c6e-1fb9-427b-b019-29e15fb3323c` in clickpass_log on 2026-02-04, IP `216.126.34.185`, impression_time 2026-01-27. Full pipeline trace shows IP is identical at every stage (bid → win → serve → VAST → clickpass). Zero S1/S2 impressions for this IP in cg 93957.

**7 distinct S3 ad_served_ids** found for IP `216.126.34.185` in campaign 450300 (cg 93957):

| ad_served_id | Earliest Impression | Source Tables |
|---|---|---|
| `d65c4799-4e6d-461d-bdbe-0bf96ab425ad` | 2026-01-25 00:35:22 | impression_log only |
| `80207c6e-1fb9-427b-b019-29e15fb3323c` | 2026-01-27 14:52:20 | impression_log + full VAST chain **(our original VV)** |
| `3887adcd-2dc9-41ea-8dc2-c80840caf4a4` | 2026-02-09 19:00:28 | impression_log only |
| `bb657a8b-472a-4c7f-8f1c-42e89c4deac4` | 2026-02-11 16:21:25 | impression_log + full VAST chain |
| `74c6a03d-ac07-4e2b-bc51-2f3f93807801` | 2026-02-22 09:06:03 | impression_log + full VAST chain |
| `f5df758c-2980-43f8-9f30-03192a4c9e0a` | 2026-02-22 18:15:29 | impression_log + full VAST chain |
| `c890f55a-1515-4c4e-8977-a80a6c2337db` | 2026-02-23 09:06:22 | impression_log + full VAST chain |

## Task

### Step 1: Full pipeline trace for all 7 ad_served_ids

For each of the 7 ad_served_ids above, run the full funnel trace to get the IP at each pipeline stage. The key join chain is:

- `clickpass_log` (if it exists for this ad_served_id) — join by `ad_served_id`
- `impression_log` — join by `ad_served_id` — gives us `ttd_impression_id`
- `event_log` (vast_impression, vast_start) — join by `ad_served_id`
- `win_logs` — join by `auction_id` = `impression_log.ttd_impression_id`
- `bid_logs` — join by `auction_id` = `impression_log.ttd_impression_id`

Use the funnel trace query pattern below. For each ad_served_id, you need TWO dates:
1. **clickpass_date** — the date in clickpass_log (if it has a VV)
2. **impression_date** — the date in impression_log/event_log (from the table above)

**Important:** The clickpass_date and impression_date are often DIFFERENT (VV can happen days/weeks after the impression). Query upstream tables on the impression_date, not the clickpass_date.

**Funnel trace query template:**
```sql
-- Update ad_served_id, clickpass_date range, and impression_date range per trace
WITH cl AS (
  SELECT ad_served_id, ip, advertiser_id, campaign_id, time, impression_time
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE ad_served_id = '<<AD_SERVED_ID>>'
    AND time >= TIMESTAMP('<<CLICKPASS_DATE>>') AND time < TIMESTAMP('<<CLICKPASS_DATE+1>>')
  LIMIT 1
)
SELECT
  '<<AD_SERVED_ID>>' AS ad_served_id,
  b.ip AS bid_ip,
  w.ip AS win_ip,
  imp.ip AS impression_ip,
  ev_imp.ip AS event_impression_ip,
  ev_start.ip AS event_start_ip,
  cl.ip AS clickpass_ip,
  b.time AS bid_timestamp,
  imp.time AS impression_timestamp,
  ev_imp.time AS event_impression_timestamp,
  cl.time AS clickpass_timestamp,
  (b.ip != COALESCE(cl.ip, imp.ip)) AS ip_mutated
FROM `dw-main-silver.logdata.impression_log` imp
LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
  ON ev_imp.ad_served_id = imp.ad_served_id
  AND ev_imp.event_type_raw = 'vast_impression'
  AND ev_imp.time >= TIMESTAMP('<<IMP_DATE>>') AND ev_imp.time < TIMESTAMP('<<IMP_DATE+1>>')
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = imp.ad_served_id
  AND ev_start.event_type_raw = 'vast_start'
  AND ev_start.time >= TIMESTAMP('<<IMP_DATE>>') AND ev_start.time < TIMESTAMP('<<IMP_DATE+1>>')
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = imp.ttd_impression_id
  AND w.time >= TIMESTAMP('<<IMP_DATE>>') AND w.time < TIMESTAMP('<<IMP_DATE+1>>')
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = imp.ttd_impression_id
  AND b.time >= TIMESTAMP('<<IMP_DATE>>') AND b.time < TIMESTAMP('<<IMP_DATE+1>>')
LEFT JOIN cl ON cl.ad_served_id = imp.ad_served_id
WHERE imp.ad_served_id = '<<AD_SERVED_ID>>'
  AND imp.time >= TIMESTAMP('<<IMP_DATE>>') AND imp.time < TIMESTAMP('<<IMP_DATE+1>>')
;
```

If you don't know the clickpass_date for a given ad_served_id, first check if it exists in clickpass_log:
```sql
SELECT ad_served_id, ip, time, impression_time
FROM `dw-main-silver.logdata.clickpass_log`
WHERE ad_served_id IN (
  'd65c4799-4e6d-461d-bdbe-0bf96ab425ad',
  '3887adcd-2dc9-41ea-8dc2-c80840caf4a4',
  'bb657a8b-472a-4c7f-8f1c-42e89c4deac4',
  '74c6a03d-ac07-4e2b-bc51-2f3f93807801',
  'f5df758c-2980-43f8-9f30-03192a4c9e0a',
  'c890f55a-1515-4c4e-8977-a80a6c2337db'
)
  AND time >= TIMESTAMP('2026-01-25') AND time < TIMESTAMP('2026-03-16')
;
```

### Step 2: Compare bid_ip to clickpass_ip

For each ad_served_id, compare:
- `bid_logs.ip` (the IP at auction time — this is the IP the targeting system used)
- `clickpass_log.ip` (the IP at visit time — what we've been searching with)
- `impression_log.ip` (the IP at serve time)

**We already know `80207c6e` has identical IPs at every stage.** We need to check the other 6.

If any ad_served_id has a `bid_ip` that differs from `216.126.34.185`, that's a new IP to investigate.

### Step 3: Cross-stage search on any new IPs

For each unique bid_ip that is NOT `216.126.34.185`, search for S1/S2 impressions within cg 93957 using the same cross-stage methodology:

Search these three tables (depending on impression type):
- **CTV:** `event_log` for `vast_impression` / `vast_start` with matching IP
- **Viewable display:** `viewability_log` with matching IP
- **Non-viewable display:** `impression_log` with matching IP

Filter to S1/S2 campaigns in cg 93957: campaign_ids `450305` (S1), `450301` (S2), `450303` (S2).

```sql
-- Example: search event_log for a new bid_ip in S1/S2 campaigns
SELECT
  ev.campaign_id,
  c.funnel_level,
  c.name AS campaign_name,
  ev.event_type_raw,
  ev.ip,
  ev.bid_ip,
  ev.ad_served_id,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
WHERE ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '<<NEW_BID_IP>>' OR ev.bid_ip = '<<NEW_BID_IP>>')
  AND c.campaign_group_id = 93957
  AND c.funnel_level IN (1, 2)
  AND ev.time >= TIMESTAMP('2025-07-11')
  AND ev.time < TIMESTAMP('2026-03-16')
ORDER BY ev.time
;

-- Repeat for impression_log and viewability_log with the same pattern
```

### Step 4: Document results

For each ad_served_id, document:
1. The bid_ip, impression_ip, clickpass_ip (if exists)
2. Whether bid_ip differs from `216.126.34.185`
3. If different: did the new IP have S1/S2 impressions in cg 93957?
4. Conclusion: can we trace the S3 targeting entry for this household through a different IP?

Save results to `outputs/ti_650_bid_ip_divergence_results.md`.

## BQ Optimization Rules (MUST follow)

1. **TIMESTAMP filters, not DATE():** `time >= TIMESTAMP('YYYY-MM-DD') AND time < TIMESTAMP('YYYY-MM-DD+1')` — DATE(time) defeats partition pruning and causes 10x+ cost increase
2. **Narrow date ranges:** Use the impression dates from the table above, not wide open scans
3. **No LIKE wildcards for IP matching:** Silver log table IPs don't carry CIDR suffixes. Use exact `=` match
4. **Always date-filter clickpass_log:** Even with ad_served_id filter, no date = full 110 GB scan
5. **Run tables individually:** Don't UNION ALL across tables in a single query — run each separately for better slot allocation
6. **Use `--dry_run` first** for any unfamiliar query — abort if >5 GB
7. **Use `bq_run.sh` wrapper** for all queries (logs performance metrics):
   ```bash
   bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "description" \
     --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver \
     'YOUR SQL HERE'
   ```

## Key Reference

- **Join chain:** clickpass_log → impression_log → event_log (all via `ad_served_id`); impression_log.ttd_impression_id = win_logs.auction_id = bid_logs.auction_id
- **Cross-stage key:** `next_stage.bid_ip` → `prev_stage.vast_start_ip OR vast_impression_ip` (CTV) or `viewability_log.ip` (viewable display) or `impression_log.ip` (non-viewable display)
- **funnel_level is authoritative for stage** (NOT objective_id — 48,934 S3 campaigns have obj=1 due to UI bug)
- **campaign_group_id scoping is mandatory** (Zach directive) — cross-stage linking must be within the same campaign_group_id
- **Campaign group 93957 created 2025-07-11** — no impressions for this group can exist before that date

## What success looks like

If ANY of the 7 ad_served_ids has a bid_ip different from `216.126.34.185`, AND that different IP has S1/S2 impressions in cg 93957, then we've found the missing link — the identity graph correctly placed this household in S3 targeting via a different IP address that DID have prior S1/S2 exposure. This would strengthen the audit's conclusion.

If ALL 7 have the same bid_ip (216.126.34.185), that further confirms the identity-graph-only entry conclusion — this household was placed in S3 targeting without any MNTN S1/S2 impression from any IP associated with this device/household within cg 93957.
