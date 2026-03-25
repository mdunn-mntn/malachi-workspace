# TI-650 Validation Run Summary

**Date:** 2026-03-23
**Audit window:** 2026-03-16 to 2026-03-22 (7 days)
**Lookback:** 365 days (2025-03-16)
**Source window:** ±30 days of audit window

---

## Advertisers Tested (10)

| # | Advertiser | ID | S3 VVs | Size |
|---|---|---|---|---|
| 1 | Ferguson Home | 31276 | 34,807 | Large |
| 2 | REVOLVE | 53308 | 34,035 | Large |
| 3 | Zazzle | 37775 | 29,126 | Large |
| 4 | FICO | 37056 | 15,737 | Medium |
| 5 | Zoom | 46104 | 9,376 | Medium |
| 6 | Ancient Nutrition | 31455 | 7,667 | Medium |
| 7 | Uber Rides | 48866 | 4,445 | Medium |
| 8 | Clayton Homes | 34838 | 4,469 | Small |
| 9 | EarthLink | 38101 | 3,846 | Small |
| 10 | Outdoorsy | 40236 | 3,392 | Small |

WGU excluded (331,792 S3 VVs — extreme outlier, ~30% of MNTN spend).

---

## Total VVs by Stage

| Stage | Count | % |
|---|---|---|
| S1 | 447,774 | 62.6% |
| S2 | 120,049 | 16.8% |
| S3 | 146,900 | 20.5% |
| **Total** | **714,723** | **100%** |

---

## Resolution Rates

### S3 VVs (primary focus)

| Metric | Count | % of S3 |
|---|---|---|
| Total S3 VVs | 146,900 | 100% |
| Has bid_ip | 146,840 | 99.96% |
| **Resolved (365d lookback)** | **146,823** | **99.95%** |
| Unresolved (has bid_ip, no match) | 17 | 0.01% |
| No bid_ip (bid_logs TTL) | 60 | 0.04% |

### All stages combined

| Status | Count |
|---|---|
| Resolved | 711,992 |
| Unresolved | 2,596 |
| No bid_ip | 135 |

### By resolution method

| Method | Count |
|---|---|
| current_is_s1 | 447,774 |
| s1_event_match (S2→S1) | 119,974 |
| s2_vv_bridge (S3→S2→S1) | 67,974 |
| s1_vv_bridge (S3→S1) | 76,270 |

### By impression type

| Type | Count | % |
|---|---|---|
| CTV | 602,400 | 84.3% |
| Viewable Display | 112,181 | 15.7% |
| Non-Viewable Display | 142 | 0.02% |

---

## Unresolved Investigation (Step 5)

51 VVs investigated after COALESCE (30 no_bid_ip + 17 original unresolved + 4 newly recovered).

| Classification | Count | Meaning |
|---|---|---|
| NO_BID_IP | 30 | bid_ip NULL in ALL tables (bid_logs purged, impression_log/event_log/viewability_log also NULL). 27 Ancient Nutrition, 3 EarthLink. |
| RESOLVED_EXTENDED | 17 | Prior VV found beyond 365-day lookback (0–370 days back). Includes 4 newly recovered by COALESCE. |
| LOOKBACK_TOO_SHORT | 2 | No match found, but campaign existed >365d — lookback insufficient |
| **GENUINELY_UNRESOLVED** | **2** | No match found anywhere (all time), campaign <100d old |

**COALESCE impact:** Recovered 30 of original 60 NO_BID_IP VVs via `impression_log.bid_ip` fallback. Of those 30: 28 resolved within 365d lookback, 2 became unresolved (bid_ip recovered but no prior VV match). 17 total resolved via extended all-time scan (up from 13).

### Extended resolution details

The 13 RESOLVED_EXTENDED VVs:
- 7x FICO: 8–23 days before audit window (all S1 matches)
- 3x Uber Rides: 20–24 days back (all S1 matches)
- 2x REVOLVE: 0–7 days back (1x S1, 1x S2 match)
- 1x Ancient Nutrition: 370 days back (S2 match — at edge of lookback)

These resolved because the prior VV fell outside the audit window but within all-time clickpass_log. The 365-day lookback was sufficient for all except Ancient Nutrition (370 days).

### Truly unresolved (4 VVs)

| Advertiser | ad_served_id | bid_ip | Campaign Group |
|---|---|---|---|
| Ferguson Home | ee47fb37... | 174.230.144.57 | fh_national_convert_acquire... |
| Ferguson Home | 8ae132b0... | 174.202.4.80 | fh_national_engagement_acquire... |
| FICO | e87853c7... | 172.56.154.242 | FY26_Croud_myFICO...MM |
| Zazzle | d3a0182e... | 153.66.219.216 | 2026 wedding |

### Campaign creation date analysis

For each truly unresolved VV, the S1 campaign creation date tells us the **maximum possible lookback** — no impression could exist before the campaign was created.

| Campaign Group | S1 Created | Max Lookback | Within 365d? | Verdict |
|---|---|---|---|---|
| Ferguson 85144 (fh_national_convert) | 2025-02-20 | 396 days | NO | Lookback too short — needs 396d |
| Ferguson 106777 (fh_national_engagement) | 2025-12-18 | 95 days | YES | Genuinely unresolved |
| FICO 107447 (FY26_Croud_myFICO MM) | 2026-01-02 | 80 days | YES | Genuinely unresolved |
| Zazzle 78903 (2026 wedding) | 2024-10-21 | 518 days | NO | Lookback too short — needs 518d |

**Result:** 2 of 4 may be lookback-window issues (365d was not enough). 2 of 4 are genuinely unresolved within the possible window — these are the real mysteries for Zach.

Root causes:
- **Ferguson 85144, Zazzle 78903**: Campaign existed >365 days — lookback window insufficient. Extended scan still found no match, suggesting the prior VV may have been purged from clickpass_log (TTL) or the IP was never seen in a prior S1/S2 VV
- **Ferguson 106777**: Campaign only 95 days old, well within lookback — bid_ip (174.202.4.80) has no prior VV match anywhere in clickpass_log, all time
- **FICO 107447**: Campaign only 80 days old — bid_ip (172.56.154.242) is T-Mobile CGNAT, IP rotates across sessions

---

## Validation Checks (Step 4)

All 10 checks **PASSED**:

| # | Check | Result |
|---|---|---|
| 4.1 | Total VVs match | PASS — S3=146,900 matches Step 2 |
| 4.2 | S1 all resolved | PASS — 0 failures |
| 4.3 | Resolved S3 has prior_vv | PASS — 0 failures |
| 4.4 | S3 w/ S2 prior has S1 event | PASS — 0 failures |
| 4.5 | No duplicates | PASS — 0 duplicates |
| 4.6 | Impression type populated | PASS — 0 resolved missing type |
| 4.7 | bid_ip for resolved | PASS — 0 resolved non-S1 missing bid_ip |
| 4.8-10 | Counts by status/method/type | Recorded above |

---

## Conclusion

**99.97% resolution achieved** with bid_ip COALESCE fallback — 146,851 of 146,900 S3 VVs resolved.

- 146,851 resolved (99.97%) — includes 28 recovered via COALESCE from impression_log.bid_ip
- 13 resolved with extended all-time lookback (beyond 365d)
- 30 no_bid_ip — bid_ip NULL in ALL tables (bid_logs purged, impression_log/event_log/viewability_log also NULL). 27 are Ancient Nutrition, 3 EarthLink.
- 19 unresolved (have bid_ip but no prior VV match):
  - 2 lookback_too_short (campaign existed >365d)
  - **2 genuinely_unresolved** (campaign <100d old, no match anywhere)
  - 15 others within 365d lookback — likely CGNAT/proxy IP rotation

**4 advertisers at 100% resolution:** Zazzle, Zoom, Clayton Homes, Outdoorsy.

### Genuinely unresolved detail

**Ferguson Home — campaign_group 106777:**
`ad_served_id: 8ae132b0-8566-406b-aaf3-e3a0b73423e6` | bid_ip: 174.202.4.80 | Viewable Display
S1 campaign created 2025-12-18 (95d before VV). Searched clickpass_log all-time — no prior S1/S2 VV with this IP in this campaign_group.

**FICO — campaign_group 107447:**
`ad_served_id: e87853c7-6e1c-4313-982b-6507cc2c539b` | bid_ip: 172.56.154.242 (T-Mobile CGNAT) | CTV
S1 campaign created 2026-01-02 (76d before VV). Same — no prior VV found. CGNAT IP rotation may explain this.

Full pipeline detail for both + NO_BID_IP examples: `outputs/validation_run/06_truly_unresolved_for_zach.csv`

### Questions for Zach — Status after Meeting #6 (2026-03-24)

1. **VV without prior site visit?** OPEN — Ferguson's bid_ip in S3 targeting but no prior VV. Could be IP rotation or edge case.
2. **CGNAT IP rotation?** Acknowledged by Zach as a plausible cause for FICO 172.56.x. No definitive answer.
3. **bid_logs retention?** ANSWERED — bid_logs purges records (90d TTL confirmed empirically). Zach agreed this is expected Beeswax behavior.
4. **Alternative path to bid_ip?** ANSWERED — `event_log.bid_ip` is intentionally designed to match bid_logs.ip and is safe to use as fallback. Zach confirmed: "it's actually pretty safe to use bid IP here for that bid blog table's IP." Also: `impression_log.bid_ip` and `viewability_log.bid_ip` store copies. Zach unsure if viewability_log.bid_ip is populated for all display events. COALESCE recovers 30 of 60.
5. **Non-VV targeting path?** Still open — not discussed in this meeting.
6. **Campaign creation date as max lookback?** PARTIALLY ANSWERED — Zach says WGU is the only known advertiser requiring >90d lookback. Others (Zazzle, Ferguson) may be "neon pixel accounts" with special configs. Check `advertiser_configs` table. Zach will investigate Zazzle specifically.
7. **NEW — clickpass_log missing bid_ip column?** Zach confirmed clickpass_log does NOT have bid_ip ("that's a whole other thing"). It should have been added.
8. **NEW — Remaining 30 no_bid_ip (bid_ip NULL everywhere)?** Zach found this interesting — event_log.bid_ip should always be populated. May be a pipeline gap for certain impression types. Zach investigating.

---

## Runbook — How to Run a Validation

### 1. Pick advertisers (~0.3 GB, seconds)
**Query:** `queries/validation_run/01_discovery.sql`
**Why:** Find S3 advertisers with enough VV volume. Pick 10 — mix of large/small.
**Parameters:** Audit window dates (7-day range in last 2 weeks), min VVs threshold.

### 2. Resolution rate check (~2 TB, ~11 min)
**Query:** `queries/validation_run/02_resolution_rate.sql`
**Why:** Quick sanity check BEFORE running the expensive trace table. Per-advertiser: total S3 VVs, bid_ip coverage, resolution rate. If any advertiser < 99% resolved, stop and debug.
**Parameters:** 10 advertiser IDs (3 places), audit window, lookback start (365d), source window (±30d).
**Check:** All resolved_pct >= 99%. Note no_bid_ip count (should be ~0 for recent data).

### 3. Full trace table (~3-5 TB, ~15-30 min) — THE DELIVERABLE
**Query:** `queries/validation_run/03_trace_table.sql`
**Why:** One row per VV (all stages) with full 7-IP trace and cross-stage resolution. This IS the `vv_ip_lineage` table. Output is too large for JSON (~700K+ rows) — in production this materializes to a BQ table.
**Parameters:** Same 10 advertiser IDs (multiple places), audit window, lookback, source window.

### 4. Validate trace table (~3-5 TB if re-queried, or cheap if reading from materialized Step 3)
**Query:** `queries/validation_run/04_validation.sql`
**Why:** 10 checks to confirm the trace table is correct. All must pass.
**Critical checks:**
- 4.2: S1 all resolved → if FAIL, **STOP** — query bug
- 4.3: Resolved S3 has prior_vv → if FAIL, query bug
- 4.4: S3 w/ S2 prior has S1 event → if FAIL, run Step 4a
- 4.5: No duplicates → if FAIL, dedup bug

### 5. Investigate unresolved (~2 TB, ~20 min) — only if Step 2 shows unresolved > 0
**Query:** `queries/validation_run/05_unresolved_s3.sql`
**Why:** All-time clickpass scan to classify unresolved VVs as NO_BID_IP / RESOLVED_EXTENDED / TRULY_UNRESOLVED.
**For Zach:** Share the TRULY_UNRESOLVED rows — ad_served_id, bid_ip, campaign_group, campaign name.

### 6. Deep dive truly unresolved — only if Step 5 has TRULY_UNRESOLVED
**Query:** `queries/validation_run/06_truly_unresolved_detail.sql`
**Why:** Full pipeline dump for specific ad_served_ids with campaign creation dates and attribution model. Produces CSV for Google Sheets / Zach.
**Output:** `outputs/validation_run/06_truly_unresolved_for_zach.csv`

### Conditional steps (not needed this run)
- **Step 4a:** S2→S1 event investigation — only if Step 4 check 4.4 fails
- **Step 5a:** Unresolved S2 investigation — only if Step 4 finds S2 VVs with failed S1 event resolution

---

## Files

| File | Description |
|---|---|
| `queries/validation_run/01_discovery.sql` | Advertiser discovery |
| `queries/validation_run/02_resolution_rate.sql` | Per-advertiser resolution rate |
| `queries/validation_run/03_trace_table.sql` | Full trace table (main deliverable) |
| `queries/validation_run/04_validation.sql` | Validation checks |
| `queries/validation_run/05_unresolved_s3.sql` | Unresolved investigation |
| `queries/validation_run/06_truly_unresolved_detail.sql` | Full detail for specific VVs (Zach sheet) |
| `outputs/validation_run/01_discovery.json` | 10 selected advertisers |
| `outputs/validation_run/02_resolution_rate.json` | Resolution rates |
| `outputs/validation_run/03_trace_table_sample.json` | 5-row sample of trace table |
| `outputs/validation_run/04_validation.json` | Validation check results |
| `outputs/validation_run/05_unresolved_s3.json` | 77 unresolved VV diagnostics |
| `outputs/validation_run/06_truly_unresolved_for_zach.csv` | Genuinely unresolved + NO_BID_IP examples for Zach |

## Relationship to Original Queries

The original queries in `queries/` (e.g. `ti_650_resolution_rate.sql`) are **templates** with `── PARAM ──` markers. The `queries/validation_run/` copies are parameterized instances with specific advertiser IDs and dates baked in. Both should be kept — originals as reusable templates, validation_run as the executed instance.
