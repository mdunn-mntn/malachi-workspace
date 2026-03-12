# TI-650: v15 Forensic Trace Results

**Date:** 2026-03-12
**Scope:** 50 unresolved S3 VVs (adv 37775, Feb 4–11, campaign_group_id scoped, v14 logic)
**Query cost:** Step 1: 2.8 TB / 110s | Step 2: 10.4 TB / 390s

---

## Executive Summary

**Zach's hypothesis — that using source tables (bid_log, win_log, impression_log) instead of CIL as proxy would reveal different IPs and improve resolution — is definitively disproven.**

The IP address is **100% identical across ALL pipeline tables** for all 50 unresolved VVs:
- event_log.bid_ip = CIL.ip = impression_log.bid_ip = bid_logs.ip = win_logs.ip = **100%**
- serve_ip = bid_ip = **100%**
- vast_start_ip = vast_imp_ip = **100%**

There is **zero IP variation** anywhere in the pipeline. The same IP appears at every step: bid, win, serve, vast_start, vast_impression, CIL, impression_log, bid_logs, win_logs.

**Root cause:** These IPs were NEVER in any S1 impression for the same campaign_group_id. They entered the S3 targeting segment via the identity graph (data_source_id=3 in tmul_daily), not via a prior MNTN impression. There is no IP-based trace path — the only link is identity graph (non-IP).

**Adding bid_logs, win_logs, or impression_log to the S1 pool would have ZERO impact on resolution rate.**

---

## Detailed Findings

### 1. IP Consistency Across All Tables

| Check | Result | Count |
|-------|--------|-------|
| event_log.bid_ip = CIL.ip | TRUE | 50/50 (100%) |
| event_log.bid_ip = impression_log.bid_ip | TRUE | 50/50 (100%) |
| event_log.bid_ip = bid_logs.ip | TRUE | 50/50 (100%) |
| event_log.bid_ip = win_logs.ip | TRUE | 50/50 (100%) |
| vast_start_ip = vast_imp_ip | TRUE | 50/50 (100%) |
| serve_ip = bid_ip | TRUE | 50/50 (100%) |
| ALL pipeline IPs identical | TRUE | 43/50 (86%) |

The 7 cases where IPs differ are **exclusively** the redirect_ip (cross-device visit): the ad played on a CTV (T-Mobile CGNAT IP) and the user visited the website on a different device (home IP). The pipeline IP is still 100% consistent.

### 2. Table Presence

| Table | Records Found | Notes |
|-------|--------------|-------|
| event_log (vast_start) | 50/50 | |
| event_log (vast_imp) | 50/50 | |
| impression_log | 50/50 | |
| cost_impression_log (CIL) | 50/50 | |
| ui_visits | 50/50 | |
| bid_logs (Beeswax) | 50/50 | |
| win_logs (Beeswax) | 50/50 | |
| **bid_events_log (MNTN)** | **0/50** | **Investigation needed** |

All source tables have records for all 50 VVs. The single exception is bid_events_log (MNTN-native), which has ZERO records. This requires investigation — the join is `bid_events_log.auction_id = event_log.td_impression_id`, and while this works for bid_logs and win_logs (both 50/50), it fails for bid_events_log. Possible causes: different auction_id format, or bid_events_log captures different bid event types.

### 3. Impression to VV Gap

| Metric | Value |
|--------|-------|
| Min gap | 0.0 days |
| Max gap | 8.9 days |
| Mean gap | 1.8 days |
| Median gap | 1.0 days |
| > 30 days | 0 |
| > 60 days | 0 |
| > 90 days | 0 |

**No TTL issues.** All impressions are within 9 days of the VV. Expanding the lookback window would have zero impact for these cases.

### 4. IP Profile

- **T-Mobile CGNAT (172.56/58/59.x.x):** 40/50 (80%)
- **Cross-device:** 26/50 (52%)
- **IP never in S1 pool (same campaign_group_id):** 50/50 (100%)

### 5. 7 IP-Variation Cases

All 7 are **cross-device visits** where the redirect happens on a different device than the CTV that received the impression:

| VV | Pipeline IP | Redirect IP | Cross-Device |
|----|-------------|-------------|-------------|
| 8d28c468 | 172.58.244.220 (T-Mobile) | 75.75.48.156 | true |
| c5cd082d | 155.190.18.4 | 155.190.22.4 | true |
| a882afd3 | 172.56.67.25 (T-Mobile) | 107.197.82.124 | true |
| d49c9ca2 | 172.59.213.32 (T-Mobile) | 71.125.83.5 | false* |
| 1f1c1edb | 172.58.242.252 (T-Mobile) | 108.45.173.160 | false* |
| 8272ec87 | 172.56.16.222 (T-Mobile) | 174.104.33.199 | true |
| c3695cc0 | 172.58.164.118 (T-Mobile) | 73.8.5.98 | true |

*is_cross_device=false but IP clearly differs — possible misclassification in VVS.

---

## Conclusions for Zach

1. **Source tables add nothing.** The IP is the same at every pipeline step. Using bid_log, win_log, impression_log instead of CIL does not reveal any new IPs.

2. **Lookback window doesn't matter.** All impressions are < 9 days old. Expanding to 120/150/180 days would have zero impact.

3. **The 8% unresolved is genuinely irreducible via IP matching.** These IPs entered the S3 segment via the identity graph (linking a CTV IP to a previously-seen household), NOT via a direct MNTN impression.

4. **GUID bridge remains the only resolution path.** Already tested: 85.4% of IP-unresolved VVs can be linked via guid_identity_daily. True irreducible = 83 VVs (0.36% of CIL cohort, 0.04% primary attribution).

5. **bid_events_log.auction_id does not match event_log.td_impression_id.** This is a new finding — needs investigation. bid_events_log may use a different ID format than the Beeswax-native bid_logs/win_logs.

---

## Recommendations

1. **Accept 92% as the IP-based ceiling** for campaign_group_id-scoped resolution. Adding more tables to the S1 pool will not improve this.

2. **Implement GUID bridge** as a second-pass resolution method in the production table (v14 architecture + GUID fallback). This brings resolution to ~99.6%.

3. **Do NOT expand lookback beyond 90 days** — no temporal gap exists in these cases.

4. **Investigate bid_events_log separately** — the 0/50 match rate suggests an auction_id format mismatch, not data absence. This won't help resolution but is important for understanding the data model.
