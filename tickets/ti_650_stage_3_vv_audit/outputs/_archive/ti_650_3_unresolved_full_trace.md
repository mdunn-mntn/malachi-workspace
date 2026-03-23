# TI-650: Full Trace — 3 Structurally Unresolved S3 VVs

**Advertiser:** 31357 — Western Governors University (WGU)
**VV Window:** 2026-02-04 to 2026-02-11
**Analysis date:** 2026-03-19

All 3 IPs belong to the same advertiser (WGU), across 3 different campaign groups.

---

## Campaign & Advertiser Context

All 3 campaign groups share the same structure — WGU prospecting groups created 2021-07-01:

| Campaign Group | Name | Campaigns (by stage) |
|---|---|---|
| **24081** | GETop - FY22 - CTV Prospecting | S1: 147574 (CTV), S2: 361037 (CTV), 361040 (Display), S3: 361038 (CTV), **361039** (Display) |
| **24083** | HPTop - FY22 - CTV Prospecting | S1: 147578 (CTV), S2: 361169 (CTV), 361172 (Display), S3: 361170 (CTV), **361171** (Display) |
| **24087** | TCTop - FY22 - CTV - Prospecting | S1: 147586 (CTV), S2: 361230 (CTV), 361233 (Display), S3: 361231 (CTV), **361232** (Display) |

**Note:** All 3 unresolved VVs are on **display** S3 campaigns (channel_id=1, objective_id=6, "Multi-Touch - Plus"). The S1 campaigns (147574/147578/147586) are CTV ("Trade Desk Television").

---

## VV 1: 64.60.221.62 — Lookback Boundary

**Campaign Group:** 24087 (TCTop)
**Campaign:** 361232 — "Multi-Touch - Plus" (Display, S3, obj=6)
**ad_served_id:** cca15462-1301-4762-94ac-f6c09a609a28
**Root cause:** 207d gap — last prior VV is 21d outside the 180d lookback window

### 5-Source Pipeline Trace (within-stage)

| Step | Table | Time | IP | Link |
|------|-------|------|----|------|
| 1. Bid | bid_logs | 2026-02-09 06:27:17 | 64.60.221.62 | auction_id |
| 2. Win | win_logs | 2026-02-09 06:27:17 | 64.60.221.62 | auction_id |
| 3. Impression | impression_log | 2026-02-09 06:27:19 | 64.60.221.62 | ad_served_id |
| 4. Viewability | viewability_log | 2026-02-09 06:27:19 | 64.60.221.62 | ad_served_id |
| 5. Clickpass (VV) | clickpass_log | 2026-02-10 03:41:54 | 64.60.221.62 | ad_served_id |

**IP consistent across all 5 sources.** No event_log entry (expected — S3 display, not CTV VAST).

### Full VV History in cg 24087 (no date limit, prospecting only)

| # | Time | Campaign | Stage | Notes |
|---|------|----------|-------|-------|
| 1 | 2025-02-05 14:13 | 147586 (S1 CTV) | **S1** | First VV for this IP in cg |
| 2 | 2025-04-10 18:24 | 361233 (S2 Display) | **S2** | 64d after S1 |
| 3 | 2025-07-18 10:55 | 147586 (S1 CTV) | **S1** | Second S1 VV — **latest prior to S3 window** |
| 4 | 2025-07-18 22:36 | 361232 (S3 Display) | S3 | 12h after S1 VV |
| 5 | 2025-07-19 08:54 | 361232 (S3 Display) | S3 | |
| 6 | 2025-07-19 19:30 | 361232 (S3 Display) | S3 | |
| 7 | 2025-07-24 00:18 | 361232 (S3 Display) | S3 | |
| 8 | 2025-07-27 02:53 | 361232 (S3 Display) | S3 | Last S3 before gap |
| — | *197-day gap* | — | — | — |
| **9** | **2026-02-10 03:41** | **361232 (S3 Display)** | **S3** | **UNRESOLVED — in audit window** |
| 10 | 2026-03-07 17:50 | 361232 (S3 Display) | S3 | After window |
| 11 | 2026-03-14 17:46 | 361232 (S3 Display) | S3 | After window |

**Gap analysis:** The MAX prior S1/S2 VV is at 2025-07-18 (row 3). The S3 VV in the audit window is at 2026-02-10. That's a **207-day gap**. The 180d lookback starts at 2025-08-08, so the Jul 18 VV falls 21 days outside it. Would require **210d+ lookback** to resolve.

---

## VV 2: 57.138.133.212 — No Prior VV, Extensive Impressions (T3 Resolvable)

**Campaign Group:** 24081 (GETop)
**Campaign:** 361039 — "Multi-Touch - Plus" (Display, S3, obj=6)
**ad_served_id:** f9c4acd8-fa90-4793-a358-180e436fcc52
**Root cause (VV path):** Zero S1/S2 VVs exist in cg 24081 before the S3 VV. First S2 VV came 5d after.
**Resolution:** T3 (impression fallback) — 623 S1 impressions, latest 20d before S3 VV. See `ti_650_deep_dive_57138_172159.md`.

### 5-Source Pipeline Trace (within-stage)

| Step | Table | Time | IP | Link |
|------|-------|------|----|------|
| 1. Bid | bid_logs | 2026-02-09 03:31:43 | 57.138.133.212 | auction_id |
| 2. Win | win_logs | 2026-02-09 03:31:43 | 57.138.133.212 | auction_id |
| 3. Impression | impression_log | 2026-02-09 03:31:48 | 57.138.133.212 | ad_served_id |
| 4. Viewability | viewability_log | 2026-02-09 03:31:50 | 57.138.133.212 | ad_served_id |
| 5. Clickpass (VV) | clickpass_log | 2026-02-09 04:31:30 | 57.138.133.212 | ad_served_id |

**IP consistent across all 5 sources.** No event_log entry (display, not CTV).

### Full VV History in cg 24081 (no date limit, prospecting only)

| # | Time | Campaign | Stage | Notes |
|---|------|----------|-------|-------|
| **1** | **2026-02-09 04:31** | **361039 (S3 Display)** | **S3** | **UNRESOLVED — first event in this cg** |
| 2 | 2026-02-13 02:06 | 361039 (S3 Display) | S3 | |
| 3 | 2026-02-14 04:33 | 361040 (S2 Display) | **S2** | First S2 — 5d AFTER S3 |
| 4 | 2026-02-14 04:34 | 361039 (S3 Display) | S3 | 8 sec after S2 (resolves) |
| 5 | 2026-02-14 21:29 | 361040 (S2 Display) | S2 | |
| 6 | 2026-02-15 05:57 | 361039 (S3 Display) | S3 | |
| 7 | 2026-02-15 20:00 | 361040 (S2 Display) | S2 | |
| 8 | 2026-02-17 01:28 | 361039 (S3 Display) | S3 | |
| 9 | 2026-02-22 19:10 | 361039 (S3 Display) | S3 | |

**Verified with full-history query (no date limit):** There are zero S1 or S2 VVs for this IP in cg 24081 at any time before 2026-02-09. The S3 VV is the very first event for this IP in this campaign group.

**Cross-group context:** This IP has 66 VVs across 30+ different campaign groups (other WGU groups and other advertisers). Many S1/S2 VVs exist in other groups. The segment builder qualified this IP for S3 targeting in cg 24081 using cross-group VV history, but our attribution is scoped to `campaign_group_id`.

**T3 resolves at any lookback ≥20d.** See `ti_650_deep_dive_57138_172159.md` for full table evidence.

---

## VV 3: 172.59.169.152 — No Prior VV, Has S1/S2 Impressions (T3 Resolvable)

**Campaign Group:** 24083 (HPTop)
**Campaign:** 361171 — "Multi-Touch - Plus" (Display, S3, obj=6)
**ad_served_id:** 2c037d9d-26e1-4a6c-9dd3-e1f9e217a185
**Root cause (VV path):** T-Mobile CGNAT IP. Only S3 VVs in clickpass_log — no S1/S2 VVs at any time.
**Resolution:** T3 (impression fallback) — 2 S1 CTV impressions at 80d + 14 S2 display impressions. See `ti_650_deep_dive_57138_172159.md`.

### 5-Source Pipeline Trace (within-stage)

| Step | Table | Time | IP | Link |
|------|-------|------|----|------|
| 1. Bid | bid_logs | 2026-02-09 14:47:10 | 172.59.169.152 | auction_id |
| 2. Win | win_logs | 2026-02-09 14:47:10 | 172.59.169.152 | auction_id |
| 3. Impression | impression_log | 2026-02-09 14:47:39 | 172.59.169.152 | ad_served_id |
| 4. Viewability | viewability_log | 2026-02-09 14:47:42 | 172.59.169.152 | ad_served_id |
| 5. Clickpass (VV) | clickpass_log | 2026-02-09 19:25:06 | 172.59.169.152 | ad_served_id |

**IP consistent across all 5 sources.** No event_log entry (display, not CTV).

### Full VV History in cg 24083 (no date limit, prospecting only)

| # | Time | Campaign | Stage | Notes |
|---|------|----------|-------|-------|
| 1 | 2025-05-13 15:59 | 361171 (S3 Display) | S3 | |
| 2 | 2025-07-27 01:22 | 361171 (S3 Display) | S3 | |
| 3 | 2025-10-12 15:54 | 361171 (S3 Display) | S3 | |
| 4 | 2025-10-22 14:54 | 361171 (S3 Display) | S3 | |
| 5 | 2025-11-05 23:10 | 361171 (S3 Display) | S3 | |
| 6 | 2025-11-09 02:35 | 361171 (S3 Display) | S3 | |
| 7 | 2025-11-11 02:27 | 361171 (S3 Display) | S3 | |
| 8 | 2025-11-25 15:22 | 361171 (S3 Display) | S3 | |
| 9 | 2026-02-01 16:40 | 361171 (S3 Display) | S3 | |
| **10** | **2026-02-09 19:25** | **361171 (S3 Display)** | **S3** | **UNRESOLVED — in audit window** |
| 11 | 2026-03-01 02:59 | 361171 (S3 Display) | S3 | After window |
| 12-20 | 2026-03-01 to 03-13 | 361171 (S3 Display) | S3 | After window (9 more) |

**20 S3 VVs spanning 10 months. Zero S1 or S2 VVs in clickpass_log.** But deep dive revealed 2 S1 CTV impressions (event_log, Nov 2025, campaign 147578) and 14 S2 display impressions (viewability_log, Jun 2025–Feb 2026, campaign 361172). The same IP was served S1/S2 ads — it didn't rotate for impressions. The user never converted to a VV for S1/S2.

**T3 resolves at any lookback ≥80d** (latest S1 CTV impression at 2025-11-21, 80d before S3 VV). See `ti_650_deep_dive_57138_172159.md` for full table evidence.

---

## Summary

| # | IP | CG | Root Cause (VV path) | Resolution | S1 Impressions | Lookback |
|---|----|----|---------------------|-----------|---------------|----------|
| 1 | 64.60.221.62 | 24087 (TCTop) | 207d lookback gap | VV path at 210d | N/A (has VVs) | 210d |
| 2 | 57.138.133.212 | 24081 (GETop) | No prior VV in cg | **T3 (impression fallback)** | 623 S1, 20d gap | 90d+ |
| 3 | 172.59.169.152 | 24083 (HPTop) | No prior VV in cg | **T3 (impression fallback)** | 2 S1 CTV, 80d gap | 90d+ |

**All 3 are display S3 VVs** (campaign names "Multi-Touch - Plus", objective_id=6). The S1 campaigns in each group are CTV ("Trade Desk Television").

**All 3 have consistent IPs across all 5 pipeline tables** — no cross-device or IP mismatch within the S3 VV itself.

**All 3 resolve at 210d lookback.** 64.60 needs VV path at 210d. 57.138 and 172.59 resolve via T3 impression fallback (confirmed by deep-dive analysis 2026-03-19 — see `ti_650_deep_dive_57138_172159.md`).
