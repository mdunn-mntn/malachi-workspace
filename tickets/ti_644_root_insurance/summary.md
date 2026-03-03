# TI-644: Root Insurance — CRM Audience Match Investigation

**Jira:** https://mntn.atlassian.net/browse/TI-644
**Status:** Complete (investigation completed; some questions unresolved)
**Date Started:** ~2026-02-18
**Date Completed:** ~2026-02-28 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Root Insurance ran a prospecting CTV campaign through MNTN using a CRM email list split into ~5M test and ~5M control households. When Root matched MNTN-attributed conversions to their test/control groups, only ~500 of ~6,521 validated conversions could be matched — a ~92% miss rate. Investigation focused on understanding why.

---

## 2. The Problem

- MNTN reported ~7,387 converting profiles (6,521 validated by Root)
- Root could only match ~300 test and ~200 control group members → ~92% unmatched
- Root needed ~2,500+ test group matches for a valid measurement
- Root's experiment was invalidated; needed to explain why

---

## 3. Plan of Action

1. Verify campaign configuration (correct data sources, geo targeting)
2. Count CRM emails uploaded (HEM counts)
3. Estimate IP audience size from match rate
4. Query `ipdsc__v1` for actual IP counts
5. ISP analysis on IPs
6. Bounce IPs against identity graph for household match rate

---

## 4. Investigation & Findings

### Key Results

| Metric | Count |
|---|---|
| Include HEMs (UPPERCASE) | ~23,326,989 |
| Exclude HEMs (UPPERCASE) | ~24,371,231 |
| Net HEMs (include NOT in exclude) | ~23,302,736 |
| Estimated IPs from match rate | ~14,524,214 |
| Campaign flight | Oct 17 – Dec 4, 2025 |
| Prospecting campaign_id | 492449 |
| advertiser_id | 39542 |

### Key Findings

1. **Campaign configured correctly** — Prospecting uses DS 4 (CRM) only, geo-targeted to US minus 18 states
2. **Root's "5M" = households; MNTN sees 23M HEMs** — multiple emails per household + hashing creates multiple entries per email
3. **`tmul_daily` (DS 4) has expired** — 14-day TTL, campaign ended Dec 4, 2025
4. **`ipdsc__v1` is the key table** for DS 4 IP resolution: `dw-main-bronze.external.ipdsc__v1`
5. **`audience_upload_ips` is empty** for email uploads (only populated for direct IP uploads — Victor confirmed)
6. **Root's segments not found in `tpa_membership_update_log`** — Root's CRM data is DS 4, which doesn't appear in TMUL row-level data

### Tables Investigated

- `dw-main-bronze.tpa.audience_upload_hashed_emails` — HEM counts confirmed
- `dw-main-bronze.integrationprod.audience_uploads` — match rates confirmed (~61–63%)
- `dw-main-bronze.raw.tmul_daily` — expired; only DS 2 and 3 remain
- `dw-main-bronze.raw.tpa_membership_update_log` — Root segments not found
- `dw-main-bronze.external.ipdsc__v1` — DS 4 IP resolution table (GCS-backed parquet)

**Full context and all queries:** `artifacts/complete_context.md`
**Query reference:** `artifacts/query_reference.md`

---

## 5. Solution

- Confirmed campaign configuration was correct on MNTN's side
- Documented why Root's match is likely low (IP/household matching gap, not MNTN attribution error)
- Provided IP list analysis approach via `ipdsc__v1`
- Blocked on ISP and identity graph bounce until Zach confirms tables

---

## 6. Questions Answered

- **Q:** Was the campaign configured correctly?
  **A:** Yes — prospecting campaign uses DS 4 (CRM) only, correct geo targeting.

- **Q:** How many CRM emails did Root upload?
  **A:** ~23.3M include, ~24.4M exclude (UPPERCASE HEMs).

- **Q:** Why does Root say "5M" but we see 23M?
  **A:** Root's 5M = households; MNTN stores multiple emails per household + multiple hash cases.

- **Q:** Where are DS 4 IPs stored?
  **A:** `dw-main-bronze.external.ipdsc__v1` — GCS-backed parquet, partitioned by `dt` and `data_source_id`.

- **Q:** Does `audience_upload_ips` have Root's IPs?
  **A:** No — only populated for direct IP uploads, not email CRM uploads.

---

## 7. Data Documentation Updates

Added to `knowledge/data_catalog.md`:
- `dw-main-bronze.external.ipdsc__v1` — schema, partition keys, unnest pattern
- `dw-main-bronze.raw.tmul_daily` — schema, 14-day TTL, unnest pattern
- `dw-main-bronze.raw.tpa_membership_update_log` — schema, unnest pattern (different from tmul_daily)

Added to `knowledge/data_knowledge.md`:
- `tmul_daily` vs `tpa_membership_update_log` schema differences (especially unnest path)
- DS 4 (CRM) does not appear in tmul_daily at row level
- `ipdsc__v1` is the authoritative DS 4 IP resolution table

---

## 8. Open Items / Follow-ups

- ISP analysis table: need confirmation from Zach
- Identity graph household bounce table: need confirmation from Zach
- `ipdsc__v1` dt value to use for historical campaign: need confirmation from Zach
- segment_id (545007) vs audience_segment_id (594162) relationship: unresolved

---

## Drive Files

📁 `Tickets/TI-644 Root Insurance Analysis/`
- `TI-644 Root Insurance.gdoc` — main investigation doc
- `TI-644 Root Insurance Kale Talk Track.gdoc` — stakeholder talk track
- `TI-644 Root Insurance Audience List.gsheet` — audience data
- `bid_vs_served_ips_root_insurance.csv` — bid vs served IP comparison
- `city_analysis.csv` — city-level IP analysis
- `conversion_ips.csv` — converting IP list
- `conversion_ips.gsheet` — converting IP list (spreadsheet)
- `cost_impression_log_ips_root_insurance.csv`
- `impression_log_ips_root_insurance.csv`
- `ipdsc_excluded_ips_root_insurance.csv` — IPs from exclude list
- `ipdsc_included_ips_root_insurance.csv` — IPs from include list
