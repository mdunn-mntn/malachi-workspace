# VV IP Lineage — Column Reference (v9)

**Table:** `{dataset}.vv_ip_lineage`
**Rows:** One per verified visit (VV), all advertisers, all stages
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `vv_stage`

Columns are ordered **left-to-right to trace backward from VV to S1**. For a S3 VV, read left to right: VV identity → visit IPs → S3 impression → S2 impression → S1 impression.

---

## Quick Reference: Reading a Row

For a Stage 3 VV, every column group is populated:

```
VV identity → visit/redirect IPs → S3 (vast_ip, bid_ip) → S2 (vast_ip, bid_ip) → S1 (vast_ip, bid_ip)
                                         ↑ ad_served_id        ↑ cross-stage         ↑ cross-stage
                                         (deterministic)       s3_bid_ip ≈ s2_vast_ip s2_bid_ip ≈ s1_vast_ip
```

For S2 VVs: s3 columns = this VV's impression, s2 columns = NULL. For S1 VVs: only s1 populated.

**NULL rules:** NULLs in s2/s3 columns are expected when the chain doesn't extend (S1 VV has no S2/S3). NULLs in the cross-stage link when the chain DOES exist = a bug (the ~11% unresolved are structural — no IP lineage path exists).

**Cross-stage link validation:** `s3_bid_ip ≈ s2_vast_ip` and `s2_bid_ip ≈ s1_vast_ip`. When they differ, it's CGNAT rotation within the same /24 (empirically validated, 2026-03-10).

---

## 1. Identity

| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | UUID of the VV and its triggering impression. Same UUID in clickpass_log, event_log, CIL. Primary key. |
| `advertiser_id` | INT64 | MNTN advertiser ID (integrationprod.advertisers). |
| `campaign_id` | INT64 | Campaign that received last-touch credit for this VV. |
| `vv_stage` | INT64 | Stage of `campaign_id` per `campaigns.funnel_level`. 1=S1, 2=S2, 3=S3. |
| `vv_time` | TIMESTAMP | When the verified visit was recorded (`clickpass_log.time`). |

---

## 2. VV Visit IPs

IPs recorded when the user visited the advertiser's site after seeing the ad.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `visit_ip` | STRING | `ui_visits.ip` | Page view IP. |
| `impression_ip` | STRING | `ui_visits.impression_ip` | Pixel-side IP — may differ from bid_ip for mobile/CGNAT. Used as S1 fallback (tier 6). |
| `redirect_ip` | STRING | `clickpass_log.ip` | Redirect/clickpass IP. This is where IP mutation from VAST is observed. |

---

## 3. S3 Impression IPs (This VV's Impression)

The impression that triggered this VV. Linked via `ad_served_id` (deterministic, zero IP joining).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s3_vast_ip` | STRING | `event_log.ip` (CTV) or `CIL.ip` (display) | IP at VAST playback. **This is the IP that enters the next stage's segment.** For display, vast_ip = bid_ip (CIL.ip = bid_ip, validated). |
| `s3_bid_ip` | STRING | `event_log.bid_ip` (CTV) or `CIL.ip` (display) | IP at auction/bid time. = win_ip = serve_ip = segment_ip (validated 38.2M rows, 47 differ). The targeting identity. |

> **For S1/S2 VVs:** These columns hold this VV's own impression IPs (not S3 specifically). The column names use "s3" because they are the highest-stage position in the layout. An S1 VV uses these columns for its S1 impression IPs, with s2 and s1 columns NULL.

**Wait — actually, for S1/S2 VVs the naming needs to reflect "this VV's impression" not "s3".** This is a design decision to resolve in v9 implementation. Options:
- (A) Name columns by stage position: `s3_vast_ip`, `s2_vast_ip`, `s1_vast_ip` — NULLs for lower VV stages
- (B) Name columns by role: `lt_vast_ip` (last-touch = this VV), `pv_vast_ip` (prior VV), `s1_vast_ip` (S1 chain)
- Current leaning: **(B)** — role-based names are clearer since an S1 VV's impression goes in `lt_vast_ip`, not `s3_vast_ip`.

---

## 4. S2 Impression IPs (Prior VV's Impression)

The most recent prior VV that advanced this IP into the current stage. Must be strictly lower stage (`pv_stage < vv_stage`).

**Cross-stage link:** `s3_bid_ip` should approximately equal `s2_vast_ip` (the prior VV's VAST IP entered the S3 segment, which is what the S3 bidder targeted).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s2_vast_ip` | STRING | `event_log.ip` or `CIL.ip` | Prior VV's VAST playback IP. Cross-stage link: should ≈ `s3_bid_ip`. |
| `s2_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | Prior VV's bid/auction IP. |
| `prior_vv_ad_served_id` | STRING | `clickpass_log` (self-join) | Impression ID of the prior VV. NULL if no prior VV in 180-day lookback. |
| `prior_vv_time` | TIMESTAMP | `clickpass_log` | When the prior VV occurred. |
| `pv_campaign_id` | INT64 | `clickpass_log` | Campaign of the prior VV. |
| `pv_stage` | INT64 | `campaigns.funnel_level` | Stage of the prior VV. Must be < vv_stage. |

**Prior VV match logic:** Primary: `prior_vv_pool.vast_ip = current_impression.bid_ip` (the vast_ip that entered the segment = the bid_ip targeted). Fallback: `prior_vv_pool.redirect_ip = current.redirect_ip` (household identity, covers cross-device). Dedup prefers vast_ip matches. Advertiser_id constraint prevents CGNAT false positives.

---

## 5. S1 Impression IPs (Chain-Traversed)

The Stage 1 impression that started this IP's funnel. Resolved via 7-tier CASE.

**Cross-stage link:** `s2_bid_ip` should approximately equal `s1_vast_ip`.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s1_vast_ip` | STRING | `event_log.ip` or `CIL.ip` | S1 impression VAST playback IP. |
| `s1_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | S1 impression bid IP — the original targeting identity. **This is the end of the chain.** |
| `s1_ad_served_id` | STRING | 7-tier chain traversal | S1 impression ID. ~89% populated for S2/S3 (structural ceiling). |
| `s1_resolution_method` | STRING | CASE expression | Which tier resolved S1: `current_is_s1`, `vv_chain_direct`, `vv_chain_s2_s1`, `imp_chain`, `imp_direct`, `imp_visit_ip`, `cp_ft_fallback`. NULL = unresolved. |
| `cp_ft_ad_served_id` | STRING | `clickpass_log.first_touch_ad_served_id` | System-recorded S1 shortcut. NULL ~40%. Retained as comparison reference. |

---

## 6. Classification

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `clickpass_is_new` | BOOL | `clickpass_log.is_new` | NTB flag (client-side JS). |
| `visit_is_new` | BOOL | `ui_visits.is_new` | NTB flag (independent client-side JS). Disagrees with clickpass 41-56% — architectural, not a bug. |
| `is_cross_device` | BOOL | `clickpass_log.is_cross_device` | Ad served on one device, visit on another. |

---

## 7. Metadata

| Column | Type | Description |
|--------|------|-------------|
| `trace_date` | DATE | Partition key. `DATE(vv_time)`. |
| `trace_run_timestamp` | TIMESTAMP | When this row was written. |

---

## Empirical IP Validation (2026-03-10)

These findings inform the column design:

| Claim | Result | Sample Size |
|-------|--------|-------------|
| bid_ip = win_ip | 99.9999% match (47 differ) | 38,204,354 rows |
| vast_impression_ip = vast_start_ip | 99.95% match (374 differ) | 812,609 rows |
| bid_ip = vast_ip | 98.98% match (8,353 differ — CGNAT /24 rotation) | 815,433 rows |
| Cross-stage: S2 bid_ip in S1 vast_ip | 309 vast_only matches (vs 45 bid_only) | 97,655 distinct S2 bid_ips |
| win_logs.impression_ip_address | Infrastructure/CDN IP, not user | 661,987 differ from win_ip |

**Conclusion:** Only 2 user IPs per stage: `bid_ip` (targeting identity) and `vast_ip` (VAST playback, enters next segment). win_ip = bid_ip. vast_start_ip ≈ vast_impression_ip. win_impression_ip = server infra.
