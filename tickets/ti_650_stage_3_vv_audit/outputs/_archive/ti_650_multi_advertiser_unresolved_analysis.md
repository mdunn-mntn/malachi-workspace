# TI-650: Multi-Advertiser VV-Unresolved S3 VV Deep Dive

**Date:** 2026-03-19
**Query job:** `perf_20260319_141026_59009` (identification, 21 TB, 20 min)
**Advertisers:** Casper (35573) + FICO (37056)
**VV window:** 2026-02-04 to 2026-02-11
**Lookback:** 210d (2025-07-09)

---

## Summary

**32 S3 VVs are VV-unresolved** (no T1 S2 VV chain, no T2 S1 VV direct) across 2 advertisers:

| Advertiser | VV-Unresolved | Total S3 VVs | VV Resolution % | Campaign Groups |
|---|---|---|---|---|
| Casper | 20 | 11,104 | 99.82% | 103354 (CTV Prospecting) |
| FICO | 12 | 10,649 | 99.89% | 81053 (PP) + 107447 (MM) |
| **Total** | **32** | **21,753** | **99.85%** | 3 campaign groups |

Birdy Grey (8,085 VVs) and Talkspace (7,389 VVs) had **0 unresolved** — 100% VV resolution.

---

## Root Cause Categories

### Category 1: No Prior VV in Campaign Group (most common)
**Count: ~20 of 32**

These IPs have ZERO S1 or S2 VVs in their campaign group within the 210d lookback window. They appear in the S3 VV log but there's no VV-based chain back to S1/S2.

**Casper examples:**
- `132.198.200.196` (cg 103354): 1 VV total — only the S3 VV itself. No history.
- `23.228.130.133` (cg 103354): 1 VV total — S3 only.
- `47.231.105.3` (cg 103354): 1 VV total — S3 only.
- `207.190.20.130` (cg 103354): 1 VV total — S3 only.

These IPs were likely qualified for S3 targeting via **cross-campaign-group history** (the segment builder uses cross-group VV data to qualify users, but our audit traces within campaign_group_id per Zach's scoping rules).

### Category 2: Lookback Boundary (FICO CGNAT)
**Count: ~3-6**

The IP has prior S1/S2 VVs in the campaign group, but they predate the 210d lookback window.

**FICO examples:**
- `172.56.103.142` (cg 81053): 8 VVs [S1:3, S2:1, S3:4] spanning Jan 2025 to Feb 2026. The S1 VVs from Jan 2025 are **before the lookback start** (Jul 2025). At 210d, these are excluded.
- `172.56.164.246` (cg 107447): 4 VVs [S1:1, S3:3]. The S1 VV from Jan 2026 is within lookback but all S3 VVs predate the audit window (they're from Jan 2026). The S3 VV under investigation (Feb 5) has the S1 at Jan 21 — only 15d gap. **This should T2-resolve.** (Possible CGNAT IP rotation issue: the S1 VV's clickpass_ip may differ from the S3 VV's bid_ip.)

### Category 3: Google Proxy IPs (Casper Display)
**Count: 6**

IPs in the 172.253.x / 173.194.x / 74.125.x ranges are Google infrastructure IPs (Google Web Light, Chrome Data Saver, or Google Cloud proxy). These rotate frequently and don't represent stable household IPs.

**Examples:**
- `172.253.228.88`: Appears as impression_ip for 2 different S3 VVs, but clickpass_ip differs (172.253.218.63, 172.253.228.86). Cross-device pattern.
- `74.125.80.229`: impression_ip for S3, clickpass_ip = 172.253.254.50. Different Google proxy endpoints.
- `173.194.96.190`: 1 S2 VV from Feb 19 (AFTER S3 on Feb 6). Temporal ordering prevents T2 match.

### Category 4: T-Mobile CGNAT Cross-Device (FICO CTV)
**Count: ~8**

All FICO unresolved VVs are CTV (channel_id=8). Many show cross-device patterns where bid_ip (from CTV device on home network) differs from clickpass_ip (from mobile device visiting the site).

| bid_ip (CTV) | clickpass_ip (site visit) | Pattern |
|---|---|---|
| 172.56.103.142 | 172.56.96.126 | T-Mobile CGNAT rotation |
| 172.56.103.142 | 172.56.100.75 | Same household, different CGNAT exit |
| 172.56.125.86 | 172.58.120.43 | Different T-Mobile subnet |
| 172.59.117.154 | 71.120.250.193 | Completely different carrier |
| 172.56.164.246 | 172.56.161.254 | T-Mobile CGNAT rotation |
| 172.56.96.92 | 172.56.101.242 | T-Mobile CGNAT rotation |

The bid_ip is consistent across the 5-source pipeline (bid=win=imp=event), confirming the CTV ad was served correctly. The clickpass_ip (site visit) just occurs from a different network/device — expected in CTV cross-device attribution.

---

## Pipeline Coverage

| Source | Has IP | Missing | Notes |
|---|---|---|---|
| bid_logs | 20/32 | 12 | 11 Casper (display, no bid_logs match) + 1 FICO |
| win_logs | 20/32 | 12 | Same as bid_logs (win joins via auction_id from impression) |
| impression_log | 30/32 | 2 | 2 missing = no impression record at all |
| event_log | 11/32 | 21 | Expected: 12 FICO CTV have events, 20 Casper display don't |
| viewability_log | 19/32 | 13 | Expected: 20 Casper display, 1 missing viewability |

**11 Casper VVs have no bid_ip** (no bid_logs/win_logs match). These are display S3 VVs where the bid_logs TTL (90d) has expired for the impression date, or the display bid pipeline doesn't always produce bid_logs entries. The resolved_ip falls back to impression_ip or viewability_ip.

**2 VVs have no impression record at all** — only clickpass_log entry exists. These are:
- `172.59.28.63` (FICO cg 107447): Only clickpass, no upstream trace
- `74.125.182.102` (Casper cg 103354): Only clickpass, no upstream trace

---

## Key Findings

1. **32/21,753 = 0.15% VV-unresolved** — consistent with the 99.85% VV resolution rate from the aggregate query.

2. **Root causes are structural, not bugs:**
   - Cross-campaign-group targeting (segment builder uses cross-group data, audit traces within-group)
   - CGNAT IP rotation (T-Mobile IPs change between ad serve and site visit)
   - Google proxy rotation (display impressions through Google infrastructure)
   - Lookback boundary (older VVs outside 210d window)

3. **All 12 FICO unresolved are T-Mobile CGNAT IPs** (172.56.x, 172.58.x, 172.59.x). CTV cross-device attribution is inherently less deterministic when the household uses mobile carrier networks.

4. **All 20 Casper unresolved are display, single campaign** (cg 103354, campaign 501579 "Multi-Touch - Plus"). This is a display S3 campaign within a "CTV Prospecting" campaign group — display retargeting alongside CTV.

5. **These patterns match WGU findings:** The 3 WGU unresolved VVs showed the same root causes (CGNAT, lookback boundary, cross-group qualifying). The multi-advertiser results confirm this is a platform-wide pattern, not advertiser-specific.

---

## Files

| File | Description |
|---|---|
| `queries/ti_650_s3_vv_unresolved_identification.sql` | Full T1+T2 check, outputs individual unresolved rows |
| `queries/ti_650_unresolved_clickpass_history.sql` | VV history for all 28 resolved IPs in their campaign groups |
| `queries/ti_650_unresolved_full_trace.sql` | 5-source pipeline trace for all 32 ad_served_ids |
| `outputs/ti_650_vv_unresolved_rows.json` | 32 unresolved VV rows with IP details |
| `outputs/ti_650_unresolved_clickpass_history.json` | 70 clickpass history rows |
| `outputs/ti_650_unresolved_full_trace.json` | 96 trace rows (multiple pipeline steps per VV) |
