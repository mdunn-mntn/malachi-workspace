# TI-650: Validation Run — Complete Guide

This is the full explanation of what each file does, when to use it, and in what order.

---

## What is this?

We're auditing S3 (Stage 3 / Full Funnel) Verified Visits. Every S3 VV should be traceable back to a prior S1 impression via IP matching. This validation proves that the IP trace works — and investigates any VVs that can't be traced.

## The trace logic in plain English

1. A user visits an advertiser's site → **clickpass_log** records the VV with an `ad_served_id`
2. That `ad_served_id` links to an impression in **impression_log** → gives us `ttd_impression_id`
3. `ttd_impression_id` links to the auction in **bid_logs** → gives us `bid_ip` (the IP that was bid on)
4. We search **clickpass_log** for a prior S1 or S2 VV where `clickpass_ip = bid_ip` in the same campaign_group
5. If found → **resolved**. If not → investigate.

## Classification of unresolved VVs

| Classification | Chain status | What broke | Can we investigate? |
|---|---|---|---|
| **RESOLVED** | Complete | Nothing — traced successfully | N/A |
| **NO_BID_IP** | Breaks at step 2-3 | `bid_logs` purged AND `impression_log.bid_ip` / `event_log.bid_ip` / `viewability_log.bid_ip` all NULL. COALESCE recovered 30 of original 60. Remaining 30 have no bid_ip in any table. | **No** — dead end |
| **RESOLVED_EXTENDED** | Complete | Prior VV found beyond 365-day lookback. Window was too short. | N/A — it resolved |
| **LOOKBACK_TOO_SHORT** | Complete through step 3 | bid_ip exists, no prior VV found, but campaign existed >365d — lookback may not have reached far enough | Not a bug — increase lookback |
| **GENUINELY_UNRESOLVED** | Complete through step 3 | bid_ip exists, no prior VV found, campaign <365d old — impossible to be a lookback issue | **Yes** — give to Zach |

---

## Files — What each one is, in execution order

### Step 1: `queries/validation_run/01_discovery.sql`
**Purpose:** Find which advertisers to audit.
**When:** First. Always start here.
**What it does:** Counts S3 VVs per advertiser in a date range using clickpass_log. Pick 10 from the results — mix of large/small.
**Output:** `outputs/validation_run/01_discovery.json` — list of advertisers with S3 VV counts.
**Cost:** ~0.3 GB (cheap, seconds).

### Step 2: `queries/validation_run/02_resolution_rate.sql`
**Purpose:** Quick sanity check before running the big trace table.
**When:** After Step 1. Plug in your 10 advertiser IDs.
**What it does:** For each advertiser's S3 VVs: extracts bid_ip, searches for prior S2/S1 VVs, reports resolution rate. Does NOT produce row-level data — just aggregates per advertiser.
**Output:** `outputs/validation_run/02_resolution_rate.json` — per-advertiser counts: total, has_bid_ip, no_bid_ip, matched_to_s2, matched_to_s1, resolved_pct, unresolved.
**Check:** Every advertiser should be ≥99% resolved. If not, stop and debug.
**Cost:** ~2 TB, ~11 min.

### Step 3: `queries/validation_run/03_trace_table.sql`
**Purpose:** THE DELIVERABLE. One row per VV with the full IP audit trail.
**When:** After Step 2 passes sanity checks.
**What it does:** For every VV (all stages, not just S3) from the 10 advertisers: extracts all 7 pipeline IPs (clickpass → vast_start → vast_impression → viewability → impression → win → bid), does cross-stage matching (S3→S2/S1), and computes resolution_status.
**Output:** Too large for JSON (~700K rows). In production this becomes a BQ table. For validation, we run Step 4 against the same logic to check it.
**Columns:** Full schema in `artifacts/ti_650_column_reference.md`. Key columns: trace_uuid, ad_served_id, all 7 IPs with timestamps, prior_vv details, s1_event details, resolution_status, resolution_method.
**Cost:** ~3-5 TB, ~15-30 min.

### Step 4: `queries/validation_run/04_validation.sql`
**Purpose:** Confirm the trace table logic is correct.
**When:** After Step 3 (or run in parallel — same logic, just aggregates instead of rows).
**What it does:** Runs the same trace logic as Step 3 but outputs a single row of 22 aggregate checks instead of row-level data.
**Output:** `outputs/validation_run/04_validation.json` — one row with: total_vvs, duplicates, s1_not_resolved, s3_resolved_no_prior, etc.
**Key checks:**
- `s1_not_resolved = 0` → S1 resolution is deterministic. If this fails, there's a query bug. STOP.
- `duplicates = 0` → No duplicate trace_uuids.
- `s3_resolved_no_prior = 0` → Every resolved S3 has a prior VV link.
- `resolved_no_bid_ip = 0` → Every resolved non-S1 VV has a bid_ip.
**Cost:** ~3-5 TB (re-runs the full logic). Would be cheap if reading from a materialized Step 3 table.

### Step 5: `queries/validation_run/05_unresolved_s3.sql`
**Purpose:** Investigate every unresolved VV.
**When:** Only if Step 2 showed unresolved > 0.
**What it does:** Re-derives the unresolved S3 VVs from the trace logic, then for each one:
1. Checks if bid_ip exists (impression_log → bid_logs join)
2. If no bid_ip → classified as `NO_BID_IP`
3. If has bid_ip → searches clickpass_log **ALL TIME** (no date filter) for a prior S1/S2 VV matching bid_ip + campaign_group_id
4. If found → `RESOLVED_EXTENDED` (prior VV was beyond 365-day lookback)
5. If not found → `TRULY_UNRESOLVED`
**Output:** `outputs/validation_run/05_unresolved_s3.json` — 77 rows with all pipeline details + classification.
**Cost:** ~2 TB, ~20 min.

### Step 6: `queries/validation_run/06_truly_unresolved_detail.sql`
**Purpose:** Full pipeline dump for specific VVs, formatted for Google Sheets / sharing with Zach.
**When:** After Step 5, for the TRULY_UNRESOLVED (and optionally NO_BID_IP) ad_served_ids.
**What it does:** For specific ad_served_ids: gets all 7 pipeline IPs + timestamps + campaign metadata + attribution model + S1 campaign creation date + refined classification (GENUINELY_UNRESOLVED vs LOOKBACK_TOO_SHORT based on campaign age).
**Output:**
- `outputs/validation_run/06_truly_unresolved_for_zach.csv` — 2 genuinely unresolved VVs (paste into Sheets)
- `outputs/validation_run/06_zach_unresolved_summary.md` — write-up for Zach with questions to answer
**Cost:** ~3 TB (scans all pipeline tables without time filters for the specific IDs).

### Not a step — reference/context files:
- `outputs/validation_run/00_summary.md` — Full validation run report with all results + the runbook
- `outputs/validation_run/07_bid_logs_ttl_check.md` — Empirical proof that bid_logs TTL is real (10/10 NO_BID_IP VVs have impression_log but no bid_logs)
- `artifacts/ti_650_column_reference.md` — Definitive schema for every column in the trace table
- `artifacts/ti_650_pipeline_explained.md` — Plain-English pipeline guide

---

## Template queries (queries/) vs validation_run queries (queries/validation_run/)

**Template queries** in `queries/` have `── PARAM ──` markers where you plug in advertiser IDs, dates, etc. These are the reusable versions for copy/paste into BQ console.

**Validation_run queries** in `queries/validation_run/` are parameterized instances from the Mar 16-22 run with specific advertiser IDs and dates baked in. These are what was actually executed.

| Template | Validation Instance | Step |
|---|---|---|
| `ti_650_advertiser_discovery.sql` | `01_discovery.sql` | 1 |
| `ti_650_resolution_rate.sql` | `02_resolution_rate.sql` | 2 |
| `ti_650_trace_table.sql` | `03_trace_table.sql` | 3 |
| — | `04_validation.sql` | 4 |
| `ti_650_unresolved_investigation.sql` | `05_unresolved_s3.sql` | 5 |
| `ti_650_impression_detail.sql` | `06_truly_unresolved_detail.sql` | 6 |

---

## Decision tree

```
START
  │
  ├─ Run Step 1 (discovery) → pick 10 advertisers
  │
  ├─ Run Step 2 (resolution rate)
  │    ├─ All ≥ 99% resolved? → proceed to Step 3
  │    └─ Any < 99%? → STOP, debug
  │
  ├─ Run Step 3 (trace table) — the deliverable
  │
  ├─ Run Step 4 (validation)
  │    ├─ All checks pass? → proceed
  │    ├─ Check 4.2 fails (S1 not resolved)? → STOP, query bug
  │    └─ Check 4.4 fails (S2→S1 missing)? → run Step 4a
  │
  ├─ Any unresolved from Step 2?
  │    ├─ No → DONE, write summary
  │    └─ Yes → Run Step 5 (investigate)
  │         │
  │         ├─ 0 TRULY_UNRESOLVED? → DONE, write summary
  │         └─ Any TRULY_UNRESOLVED? → Run Step 6 (detail for Zach)
  │              │
  │              ├─ Check campaign creation dates
  │              ├─ LOOKBACK_TOO_SHORT? → note, not a real issue
  │              └─ GENUINELY_UNRESOLVED? → share with Zach
  │
  └─ Write summary (Step 7)
```
