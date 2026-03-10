# VV IP Lineage — Column Reference (v9)

**Table:** `{dataset}.vv_ip_lineage`
**Rows:** One per verified visit (VV), all advertisers, all stages
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `vv_stage`

Columns are ordered **left-to-right to trace backward from VV to S1**. For a S3 VV, read left to right: VV identity → visit IPs → S3 impression → S2 impression → S1 impression.

---

## Quick Reference: Reading a Row

```
Stage 3 VV: visit_ip | s3_vast_start  s3_vast_imp  s3_serve  s3_bid | s2_vast_start  s2_vast_imp  s2_serve  s2_bid | s1_vast_start  s1_vast_imp  s1_serve  s1_bid
                                                              ↓ cross-stage                                   ↓ cross-stage
                                                     s3_bid ≈ s2_vast_imp                           s2_bid ≈ s1_vast_imp

Stage 2 VV: visit_ip | NULL NULL NULL NULL           | s2_vast_start  s2_vast_imp  s2_serve  s2_bid | s1_vast_start  s1_vast_imp  s1_serve  s1_bid
Stage 1 VV: visit_ip | NULL NULL NULL NULL           | NULL NULL NULL NULL                          | s1_vast_start  s1_vast_imp  s1_serve  s1_bid
```

**Column naming:** Stage-based (s3/s2/s1). Columns always refer to the same funnel stage regardless of VV type. S1 VVs have s3 and s2 columns NULL.

**NULL rules:** NULLs in s2/s3 columns are expected when the VV's stage doesn't extend that far. NULLs in the cross-stage link when the chain DOES exist = structural (~11% unresolved — IP entered segment via CRM/cross-device graph, no IP breadcrumbs).

**Cross-stage link validation:** `s3_bid_ip ≈ s2_vast_impression_ip` and `s2_bid_ip ≈ s1_vast_impression_ip`. Differs ~1.2% of the time due to 5 mechanisms: CGNAT rotation (66% of diffs — /24, /16, or /8 rotation), SSAI proxies (6%), dual-stack IPv4→IPv6 (12%), VPN/CDN/network switches (16%). See Finding #26.

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
| `redirect_ip` | STRING | `clickpass_log.ip` | Redirect/clickpass IP. Where IP mutation from VAST is observed. |

---

## 3. S3 Impression IPs (This VV's S3 Impression)

The S3 impression that triggered this VV. Linked via `ad_served_id` (deterministic, zero IP joining). **NULL for S1 and S2 VVs.**

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s3_vast_start_ip` | STRING | `event_log.ip` (event_type_raw='vast_start') | IP at VAST start callback. 99.85% = vast_impression_ip. When differs: SSAI proxy (see Finding #25). |
| `s3_vast_impression_ip` | STRING | `event_log.ip` (event_type_raw='vast_impression') | IP at VAST impression callback. **Cross-stage key — this IP enters the S2 segment (if chain continues backward).** |
| `s3_serve_ip` | STRING | `impression_log.ip` via `ad_served_id` | IP at ad serve request. 93.6% = bid_ip. When differs: 96.9% internal 10.x.x.x NAT, 3.1% AWS infra. |
| `s3_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` (display) | IP at auction/bid time. = win_ip = segment_ip (100% validated). The targeting identity. **Cross-stage link: should ≈ s2_vast_impression_ip.** |

---

## 4. S2 Impression IPs (Prior VV's S2 Impression)

The most recent prior VV that advanced this IP into S3. Must be strictly lower stage (`pv_stage < vv_stage`). **NULL for S1 VVs.**

**Cross-stage link:** `s3_bid_ip` should approximately equal `s2_vast_impression_ip` (the S2 VV's VAST IP entered the S3 segment, which the S3 bidder targeted).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s2_vast_start_ip` | STRING | `event_log.ip` (vast_start) | S2 impression VAST start IP. |
| `s2_vast_impression_ip` | STRING | `event_log.ip` (vast_impression) | S2 impression VAST impression IP. **Cross-stage link: should ≈ s3_bid_ip.** |
| `s2_serve_ip` | STRING | `impression_log.ip` via prior VV's `ad_served_id` | S2 impression serve request IP. |
| `s2_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | S2 impression bid/auction IP. **Cross-stage link to S1: should ≈ s1_vast_impression_ip.** |
| `prior_vv_ad_served_id` | STRING | `clickpass_log` (self-join) | Impression ID of the prior VV. NULL if no prior VV in 180-day lookback. |
| `prior_vv_time` | TIMESTAMP | `clickpass_log` | When the prior VV occurred. |
| `pv_campaign_id` | INT64 | `clickpass_log` | Campaign of the prior VV. |
| `pv_stage` | INT64 | `campaigns.funnel_level` | Stage of the prior VV. Must be < vv_stage. |
| `pv_redirect_ip` | STRING | `clickpass_log.ip` (prior VV) | Prior VV's redirect IP. Fallback match key for cross-device cases. |

**Prior VV match logic:** Primary: `prior_vv_pool.vast_ip = current_impression.bid_ip` (the vast_ip that entered the segment = the bid_ip targeted). Fallback: `prior_vv_pool.redirect_ip = current.redirect_ip` (household identity, covers cross-device). Dedup prefers vast_ip matches. Advertiser_id constraint prevents CGNAT false positives.

---

## 5. S1 Impression IPs (Chain-Traversed)

The Stage 1 impression that started this IP's funnel. Resolved via 7-tier CASE. **NULL for S1 VVs (their own impression IS S1 — see s3 columns).**

Wait — for S1 VVs, the S1 impression data goes in the **s1 columns**, not s3. The s3 and s2 columns are NULL. The s1 columns are always populated (for S1 VVs: directly from ad_served_id; for S2/S3 VVs: via chain traversal).

**Cross-stage link:** `s2_bid_ip` should approximately equal `s1_vast_impression_ip`.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s1_vast_start_ip` | STRING | `event_log.ip` (vast_start) | S1 impression VAST start IP. |
| `s1_vast_impression_ip` | STRING | `event_log.ip` (vast_impression) | S1 impression VAST impression IP. |
| `s1_serve_ip` | STRING | `impression_log.ip` via S1 `ad_served_id` | S1 impression serve request IP. |
| `s1_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | S1 impression bid IP — the original targeting identity. **End of the chain.** |
| `s1_ad_served_id` | STRING | 7-tier chain traversal | S1 impression ID. ~89% populated for S2/S3 (structural ceiling). For S1 VVs: = ad_served_id. |
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

## IP Sources Per Stage (4 each)

| IP | Source Table | Source Column | Join Key | Role | Validated |
|----|-------------|---------------|----------|------|-----------|
| `vast_start_ip` | `event_log` | `ip` (event_type_raw='vast_start') | `ad_served_id` | VAST start callback IP. 99.85% = vast_impression_ip. | 288.7M rows, 442K differ |
| `vast_impression_ip` | `event_log` | `ip` (event_type_raw='vast_impression') | `ad_served_id` | VAST impression callback IP. **Cross-stage key.** | Same as above |
| `serve_ip` | `impression_log` | `ip` | `ad_served_id` | Ad serve request IP. 93.6% = bid_ip. Differs = infra IP. | Zach: "almost always bid_ip" |
| `bid_ip` | `event_log` | `bid_ip` | `ad_served_id` | Targeting identity. = win_ip = segment_ip (100%). | 38.2M rows |

For display impressions (CIL only, no event_log): `CIL.ip = bid_ip` (100% validated). All 4 IPs = bid_ip for display.

**Collapsed from 6:** Original pipeline has segment_ip, bid_ip, win_ip, serve_ip, vast_impression_ip, vast_start_ip. Dropped win_ip (=bid_ip 100%) and segment_ip (=bid_ip 100%, Zach confirmed).

**Could collapse further to 3:** vast_start_ip ≈ vast_impression_ip (99.85%). When they differ, 58% of the time both are SSAI proxies (neither is the user). Keeping both enables SSAI detection.

---

## Empirical IP Validation (2026-03-10)

| Claim | Result | Sample Size |
|-------|--------|-------------|
| bid_ip = win_ip | 100% (47 apparent diffs = 0.0.0.0 null sentinel) | 38,204,354 rows |
| bid_ip = segment_ip | 100% (Zach confirmed) | N/A |
| vast_impression_ip = vast_start_ip | 99.847% (442,617 differ) | 288,693,500 rows |
| vast_impression ≠ vast_start: neither matches bid | 58% of diffs = both SSAI proxy IPs | 256,794 rows |
| vast_impression ≠ vast_start: same /24 | 26.5% of diffs = CGNAT rotation | 117,207 rows |
| bid_ip = vast_ip | ~98.8% match (~3.54M differ) | 288,693,500 rows |
| bid_ip ≠ vast_ip: CGNAT /24 | 35.2% of diffs (1.25M) — last octet rotation | Same carrier NAT pool |
| bid_ip ≠ vast_ip: CGNAT /16 | 24.5% of diffs (867K) — wider pool rotation | Same carrier, adjacent /24 |
| bid_ip ≠ vast_ip: carrier /8 | 6.5% of diffs (230K) — ISP reallocation | Same ISP, different subnet |
| bid_ip ≠ vast_ip: SSAI proxy | 5.7% of diffs (200K) — AWS SSAI servers | ~196 proxy IPs, ~1,200 each |
| bid_ip ≠ vast_ip: IPv4→IPv6 | 11.7% of diffs (415K) — dual-stack devices | 208K distinct IPv6 addrs |
| bid_ip ≠ vast_ip: other | 16.4% of diffs (580K) — VPN, CDN, network switch | 88K singleton IPs |
| serve_ip = bid_ip | 93.6% | CTV impressions |
| serve_ip when differs | 96.9% internal 10.x.x.x, 3.1% AWS | 6.4% of CTV |
| win_logs.impression_ip_address | Infrastructure/CDN IP, not user | 661,987 differ from win_ip |
