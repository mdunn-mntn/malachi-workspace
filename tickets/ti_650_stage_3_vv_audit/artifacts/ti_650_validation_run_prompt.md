# TI-650: VV Trace Validation Run

Copy everything below the line into a new Claude Code session.

---

## Task

Run a full VV trace validation for TI-650. Pick random advertisers, trace every VV from clickpass back to bid_ip through the full pipeline, resolve cross-stage links, and investigate any VV that doesn't resolve. Every step produces a `.sql` file and a corresponding output file.

**Working directory:** `/Users/malachi/Developer/work/mntn/workspace`

**Query directory:** `tickets/ti_650_stage_3_vv_audit/queries/validation_run/` (create if it doesn't exist)

**Output directory:** `tickets/ti_650_stage_3_vv_audit/outputs/validation_run/` (create if it doesn't exist)

**Read these files first for full context:**
- `tickets/ti_650_stage_3_vv_audit/summary.md`
- `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_column_reference.md`
- `knowledge/data_catalog.md` and `knowledge/data_knowledge.md`

**Reference queries (use as templates, DO NOT modify the originals):**
- `tickets/ti_650_stage_3_vv_audit/queries/ti_650_advertiser_discovery.sql`
- `tickets/ti_650_stage_3_vv_audit/queries/ti_650_resolution_rate.sql`
- `tickets/ti_650_stage_3_vv_audit/queries/ti_650_trace_table.sql`
- `tickets/ti_650_stage_3_vv_audit/queries/ti_650_unresolved_investigation.sql`
- `tickets/ti_650_stage_3_vv_audit/queries/ti_650_impression_detail.sql`

---

## Rules (non-negotiable — apply to every query you write)

### Trace paths — every IP from its actual source table, no skipping, no proxies

**CTV** (trace-back order, most recent → oldest):
```
clickpass_log.ip → event_log.ip (vast_start) → event_log.ip (vast_impression)
  → win_logs.ip → impression_log.ip → bid_logs.ip
```

**Viewable Display** (trace-back order):
```
clickpass_log.ip → viewability_log.ip → impression_log.ip
  → win_logs.ip → bid_logs.ip
```
For display, impression_log comes BEFORE win_logs in the pipeline (opposite of CTV).

**Non-Viewable Display** (trace-back order):
```
clickpass_log.ip → impression_log.ip → win_logs.ip → bid_logs.ip
```

### Impression type classification
- `vast_start_ip IS NOT NULL` → CTV
- `viewability_ip IS NOT NULL` (vast columns NULL) → Viewable Display
- `impression_ip IS NOT NULL` (vast + viewability NULL) → Non-Viewable Display

### Implementation details
- **CIDR stripping:** `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix.
- **bid_ip extraction:** `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id` → `bid_logs.ip`. No shortcuts. No proxy columns.
- **0.0.0.0 sentinel:** `NULLIF(bid_ip, '0.0.0.0')` — bid_logs uses 0.0.0.0 as null sentinel.
- **Prospecting only:** `objective_id IN (1, 5, 6)`. Excludes retargeting (4) and ego (7).
- **funnel_level > objective_id:** funnel_level is authoritative for stage. 48,934 S3 campaigns have wrong objective_id.
- **campaign_group_id scoping:** All cross-stage matches within same campaign_group_id.
- **UUID generation:** `MD5(ad_served_id)` formatted as UUID (GENERATE_UUID is non-deterministic across CTE refs).
- **No table skipping:** Each IP MUST come from its actual source table. `bid_ip` from `bid_logs.ip`, NOT `event_log.bid_ip`.
- **ROW_NUMBER dedup:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1`.
- **Default project:** `dw-main-silver` for logdata, `dw-main-bronze` for integrationprod.
- **Campaign filters:** `c.deleted = FALSE AND c.is_test = FALSE`.
- **Always dry_run before unfamiliar queries. Always LIMIT on raw selects.**

### Cross-stage resolution

**S1 VVs:** No cross-stage needed. The VV's impression IS the S1 impression.

**S2 VVs → S1 (event-based):** S2's `bid_ip` matched against S1 `event_log.ip` (vast_start preferred, vast_impression fallback). Same campaign_group_id, S1 event before S2 VV. To get into S2 you MUST have had a VAST impression from S1 — this MUST resolve.

**S3 VVs → prior S2/S1 VV (VV-based):** S3's `bid_ip` matched against prior VV `clickpass_log.ip`, same campaign_group_id, prior in time. Check S2 VV first. If none, check S1 VV. Most recent match (last touch). Then:
- If prior is S2: trace S2's impression to its bid_ip, then S2→S1 resolution.
- If prior is S1: done.

**100% resolution should occur** with sufficient lookback. Unresolved means: lookback window too short, table TTL truncated, or a bug. It is IMPOSSIBLE for CRM/LiveRamp identity graph to cause unresolved — every VV MUST follow the IP path.

---

## Execution Path

### STEP 1: Pick advertisers
**File:** `queries/validation_run/01_discovery.sql`
**Output:** `outputs/validation_run/01_discovery.json`

Use `ti_650_advertiser_discovery.sql` as template. Pick a 7-day window in the last 2 weeks. Find advertisers with >= 100 S3 VVs. Pick 10 randomly (mix of large and small — not just the top 10).

**Run the query. Save the output.**

→ You now have 10 advertiser_ids. Use these in ALL subsequent steps.

---

### STEP 2: Resolution rate check
**File:** `queries/validation_run/02_resolution_rate.sql`
**Output:** `outputs/validation_run/02_resolution_rate.json`

Use `ti_650_resolution_rate.sql` as template. Plug in 10 advertisers from Step 1. 365-day lookback. Source window = audit window ±30d.

**Run the query. Save the output. Then check:**

| Check | Expected | Action if FAIL |
|-------|----------|----------------|
| Every advertiser has rows | 10 rows | Debug — missing advertiser has no S3 VVs in window |
| `no_bid_ip` is near 0 | < 1% of total | Acceptable — bid_logs 90-day TTL |
| `resolved_pct` >= 99% for each | 99%+ | Proceed — Step 5 will investigate |
| `unresolved` count | Note exact number | These will be investigated in Step 5 |

→ Record: total S3 VVs, total resolved, total unresolved. Proceed to Step 3.

---

### STEP 3: Full trace table (main deliverable)
**File:** `queries/validation_run/03_trace_table.sql`
**Output:** `outputs/validation_run/03_trace_table.json`

Use `ti_650_trace_table.sql` as template. Same 10 advertisers, same date range. One row per VV with the full schema:

**Columns (in order):**
1. **Identity:** trace_uuid, ad_served_id, advertiser_id, advertiser_name, campaign_id, campaign_name, campaign_group_id, campaign_group_name, funnel_level, objective_id, channel_id, impression_type
2. **VV details:** clickpass_ip, clickpass_time, guid, is_new, is_cross_device, attribution_model_id, first_touch_ad_served_id
3. **This VV's 7-IP trace:** vast_start_ip/time, vast_impression_ip/time, viewability_ip/time, impression_ip/time, win_ip/time, bid_ip/time
4. **Prior VV (S3 only):** prior_vv_ad_served_id, prior_vv_funnel_level, prior_vv_campaign_id, prior_vv_clickpass_ip, prior_vv_time
5. **Prior VV's 7-IP trace (S3 only):** prior_vv_vast_start_ip/time, prior_vv_vast_impression_ip/time, prior_vv_viewability_ip/time, prior_vv_impression_ip/time, prior_vv_win_ip/time, prior_vv_bid_ip/time
6. **S1 event resolution (S2 + S3-with-S2-prior):** s1_event_ad_served_id, s1_event_vast_start_ip, s1_event_vast_impression_ip, s1_event_time, s1_event_campaign_id
7. **Resolution:** resolution_status (resolved/unresolved/no_bid_ip), resolution_method (current_is_s1/s1_event_match/s2_vv_bridge/s1_vv_bridge)
8. **Metadata:** trace_date, trace_run_timestamp

**Run the query. Save the output. Then proceed to Step 4.**

---

### STEP 4: Validate trace table
**File:** `queries/validation_run/04_validation.sql`
**Output:** `outputs/validation_run/04_validation.json`

Write a validation query against the Step 3 output (or re-query with validation logic). Check:

| # | Check | Expected | Action if FAIL |
|---|-------|----------|----------------|
| 4.1 | Total VVs = Step 2 total | Exact match | Debug — rows dropped somewhere |
| 4.2 | Every S1 VV: resolution_status = 'resolved' | 100% | Bug — S1 is deterministic |
| 4.3 | Every resolved S3 VV: prior_vv_ad_served_id IS NOT NULL | 100% | Bug — resolved without prior VV |
| 4.4 | Every S3 with S2 prior: s1_event_ad_served_id IS NOT NULL | 100% | Investigate — S2→S1 must resolve |
| 4.5 | No duplicate trace_uuids | 0 duplicates | Bug — dedup failure |
| 4.6 | impression_type not NULL for resolved VVs | 100% | Bug — missing pipeline data |
| 4.7 | bid_ip IS NOT NULL for resolved VVs | 100% | Bug — bid extraction failed |
| 4.8 | Count by resolution_status | Record | Compare to Step 2 |
| 4.9 | Count by resolution_method | Record | For analysis |
| 4.10 | Count by impression_type | Record | CTV vs display breakdown |

**Run the validation. Save results.**

→ **IF all checks pass:** Proceed to Step 5 (investigate unresolved).
→ **IF check 4.2 fails (S1 not 100%):** STOP. Debug. S1 resolution is deterministic — this is a query bug.
→ **IF check 4.4 fails (S2→S1 not resolving):** Run Step 5a (S2 event investigation) before proceeding.

---

### STEP 4a (conditional): S2→S1 event investigation
**Only run if Step 4 check 4.4 fails.**
**File:** `queries/validation_run/04a_s2_event_investigation.sql`
**Output:** `outputs/validation_run/04a_s2_event_investigation.json`

For S3 VVs with S2 prior where s1_event_ad_served_id IS NULL:
1. Get the prior VV's bid_ip
2. Search event_log ALL TIME for S1 impressions (funnel_level=1, same campaign_group_id) where vast_start_ip OR vast_impression_ip = prior VV's bid_ip
3. Check: is the S1 event older than the source_window? → Extend window.
4. Check: does the S1 campaign even exist? → `campaigns.deleted` or data issue.

Save with classification: `FOUND_EXTENDED` (outside window), `NO_S1_CAMPAIGN` (data issue), `TRULY_MISSING` (bug).

→ Proceed to Step 5.

---

### STEP 5: Investigate unresolved S3 VVs
**File:** `queries/validation_run/05_unresolved_s3.sql`
**Output:** `outputs/validation_run/05_unresolved_s3.json`

Take all S3 ad_served_ids where resolution_status IN ('unresolved', 'no_bid_ip') from Step 3.

**For `no_bid_ip` VVs:** These can't be traced (bid_logs TTL expired). Record them with all available info. No further investigation possible.

**For `unresolved` VVs (have bid_ip but no prior VV match):**
1. Get each VV's bid_ip and campaign_group_id
2. Search clickpass_log **ALL TIME** (no time filter on clickpass_log) for any prior S2/S1 VV where:
   - `SPLIT(clickpass_log.ip, '/')[SAFE_OFFSET(0)] = bid_ip`
   - Same campaign_group_id
   - clickpass_log.time < this VV's clickpass_time
   - funnel_level IN (1, 2), objective_id IN (1, 5, 6)
3. Also get the campaign creation date: `SELECT MIN(created_at) FROM campaigns WHERE campaign_group_id = X` to see the max possible lookback window.

**Classify each unresolved VV:**

| Classification | Meaning | Next step |
|---------------|---------|-----------|
| `RESOLVED_EXTENDED` | Found prior VV beyond 365-day lookback | Record: how many days back? |
| `NO_BID_IP` | bid_logs missing (TTL) | No further action possible |
| `TRULY_UNRESOLVED` | No match found anywhere, all time | → Go to Step 6 |

**Run the query. Save the output.**

→ **IF 0 TRULY_UNRESOLVED:** Done! Write summary (Step 7).
→ **IF any TRULY_UNRESOLVED:** Proceed to Step 6.

---

### STEP 6: Full details on truly unresolved
**File:** `queries/validation_run/06_truly_unresolved_details.sql`
**Output:** `outputs/validation_run/06_truly_unresolved_details.json`

For each TRULY_UNRESOLVED VV from Step 5, get everything we know:

1. **Full VV trace** (all 7 IPs with timestamps from Step 3)
2. **Campaign metadata:** campaign_id, campaign_name, campaign_group_id, funnel_level, objective_id, channel_id
3. **Advertiser:** advertiser_id, advertiser_name
4. **Campaign creation date:** `MIN(created_at)` for this campaign_group
5. **Does bid_ip exist ANYWHERE in clickpass_log?** (any advertiser, any campaign_group — just to see if the IP has ever visited any site)
6. **Does bid_ip exist in event_log for S1 campaigns in the same campaign_group?** (check if there's a VAST impression that should have created a prior VV)
7. **Does bid_ip exist in event_log for ANY S1 campaign for this advertiser?** (check if it's a campaign_group scoping issue)

This is the diagnostic dump. Save it. These are either:
- Data bugs (impression exists but VV was never recorded)
- Pipeline gaps (bid_logs has the IP but it was never served an impression in this campaign_group)
- Table TTL edge cases (the prior VV existed but has been purged from clickpass_log)

---

### STEP 5a (conditional): Investigate unresolved S2 VVs
**Only run if Step 4 found S2 VVs where s1_event resolution failed.**
**File:** `queries/validation_run/05a_unresolved_s2.sql`
**Output:** `outputs/validation_run/05a_unresolved_s2.json`

Same approach as Step 5 but for S2 VVs. Search event_log ALL TIME for S1 impressions where vast_start_ip = S2's bid_ip. Classify as:
- `RESOLVED_EXTENDED` — S1 event found beyond source window
- `EVENT_LOG_TTL` — event_log_filtered has 60-day TTL; S1 event may have expired
- `TRULY_UNRESOLVED` — no S1 event found anywhere

→ **IF any TRULY_UNRESOLVED S2:** These are potential bugs. A S2 VV without an S1 VAST impression should be impossible.

---

### STEP 7: Summary
**File:** `outputs/validation_run/00_summary.md`

Write a summary with:
- Date range and 10 advertisers tested
- Total VVs by stage (S1/S2/S3)
- Resolution rates by stage
- Unresolved breakdown: no_bid_ip vs unresolved vs truly_unresolved
- How many resolved with extended all-time lookback
- Any bugs or data issues found
- Whether 100% resolution was achieved after all-time scan
- List of any truly unresolved ad_served_ids with their diagnostic classification

**Commit and push after each step. Use commit message format: `TI-650: validation run step N — description`**
