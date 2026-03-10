# VV IP Lineage — Column Reference (v9)

**Table:** `{dataset}.vv_ip_lineage`
**Rows:** One per verified visit (VV), all advertisers, all stages
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `vv_stage`

Columns are ordered **left-to-right to trace backward from VV to S1**. For a S3 VV, read left to right: VV identity → visit IPs → last-touch impression → prior VV impression → S1 impression.

---

## Quick Reference: Reading a Row

For a Stage 3 VV, every column group is populated:

```
VV identity → visit/redirect IPs → LT (vast_ip, serve_ip, bid_ip) → PV (vast_ip, serve_ip, bid_ip) → S1 (vast_ip, serve_ip, bid_ip)
                                         ↑ ad_served_id                  ↑ cross-stage                    ↑ cross-stage
                                         (deterministic)                 lt_bid_ip ≈ pv_vast_ip            pv_bid_ip ≈ s1_vast_ip
```

**Column naming convention:** Role-based, not stage-based.
- `lt_` = **last-touch** — this VV's own impression (whatever stage the VV is)
- `pv_` = **prior VV** — the VV that advanced this IP into its current stage
- `s1_` = **S1 chain** — the Stage 1 impression at the start of the funnel

For S2 VVs: lt = this VV's S2 impression, pv = NULL (no prior VV needed for S2), s1 = chain-traversed S1.
For S1 VVs: lt = this VV's S1 impression, pv = NULL, s1 = NULL (lt IS S1).

**NULL rules:** NULLs in pv/s1 columns are expected when the chain doesn't extend (S1 VV has no prior VV or S1 chain). NULLs in the cross-stage link when the chain DOES exist = structural (~11% unresolved — no IP lineage path exists, entered segment via CRM/cross-device graph).

**Cross-stage link validation:** `lt_bid_ip ≈ pv_vast_ip` and `pv_bid_ip ≈ s1_vast_ip`. When they differ, it's CGNAT rotation within the same /24 (empirically validated, 2026-03-10).

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

## 3. Last-Touch Impression IPs (This VV's Impression)

The impression that triggered this VV. Linked via `ad_served_id` (deterministic, zero IP joining).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `lt_vast_ip` | STRING | `event_log.ip` (CTV) or `CIL.ip` (display) | IP at VAST playback. **Cross-stage key — this is the IP that enters the next stage's segment.** For display, vast_ip = bid_ip (CIL.ip = bid_ip, validated). |
| `lt_serve_ip` | STRING | `impression_log.ip` via `ad_served_id` | IP at ad serve request. 93.6% = bid_ip. When different: 96.9% internal 10.x.x.x (NAT), 3.1% AWS infra IPs. The ad server's IP, not the user's, when it differs. Included for audit completeness — may mutate between stages. |
| `lt_bid_ip` | STRING | `event_log.bid_ip` (CTV) or `CIL.ip` (display) | IP at auction/bid time. = win_ip = segment_ip (validated 38.2M rows). The targeting identity. |

**For any VV stage:** These columns always hold THIS VV's impression IPs. An S1 VV's impression goes in `lt_*`, an S2 VV's impression goes in `lt_*`, etc.

---

## 4. Prior VV Impression IPs

The most recent prior VV that advanced this IP into the current stage. Must be strictly lower stage (`pv_stage < vv_stage`).

**Cross-stage link:** `lt_bid_ip` should approximately equal `pv_vast_ip` (the prior VV's VAST IP entered the current stage's segment, which the current stage's bidder targeted).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pv_vast_ip` | STRING | `event_log.ip` or `CIL.ip` | Prior VV's VAST playback IP. Cross-stage link: should ≈ `lt_bid_ip`. |
| `pv_serve_ip` | STRING | `impression_log.ip` via prior VV's `ad_served_id` | Prior VV's serve request IP. Included for audit completeness. |
| `pv_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | Prior VV's bid/auction IP. Cross-stage link to S1: should ≈ `s1_vast_ip`. |
| `prior_vv_ad_served_id` | STRING | `clickpass_log` (self-join) | Impression ID of the prior VV. NULL if no prior VV in 180-day lookback. |
| `prior_vv_time` | TIMESTAMP | `clickpass_log` | When the prior VV occurred. |
| `pv_campaign_id` | INT64 | `clickpass_log` | Campaign of the prior VV. |
| `pv_stage` | INT64 | `campaigns.funnel_level` | Stage of the prior VV. Must be < vv_stage. |
| `pv_redirect_ip` | STRING | `clickpass_log.ip` (prior VV) | Prior VV's redirect IP. Used as fallback match key for cross-device cases. |

**Prior VV match logic:** Primary: `prior_vv_pool.vast_ip = current_impression.bid_ip` (the vast_ip that entered the segment = the bid_ip targeted). Fallback: `prior_vv_pool.redirect_ip = current.redirect_ip` (household identity, covers cross-device). Dedup prefers vast_ip matches. Advertiser_id constraint prevents CGNAT false positives.

---

## 5. S1 Impression IPs (Chain-Traversed)

The Stage 1 impression that started this IP's funnel. Resolved via 7-tier CASE.

**Cross-stage link:** `pv_bid_ip` should approximately equal `s1_vast_ip`.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s1_vast_ip` | STRING | `event_log.ip` or `CIL.ip` | S1 impression VAST playback IP. |
| `s1_serve_ip` | STRING | `impression_log.ip` via S1 `ad_served_id` | S1 impression serve request IP. Included for audit completeness. |
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

## IP Sources Per Stage (3 IPs each)

| IP | Source Table | Source Column | Join Key | Role | Validated |
|----|-------------|---------------|----------|------|-----------|
| `vast_ip` | `event_log` | `ip` | `ad_served_id` | VAST playback IP. **Cross-stage key** — enters next stage's segment. | bid≠vast in 1% (CGNAT /24) |
| `serve_ip` | `impression_log` | `ip` | `ad_served_id` | Ad serve request IP. 93.6% = bid_ip. Differs = infra IP (10.x.x.x NAT, AWS). | Zach: "almost always bid_ip" |
| `bid_ip` | `event_log` | `bid_ip` | `ad_served_id` | Targeting identity. = win_ip = segment_ip (100%). | 38.2M rows, 47 differ (0.0.0.0 sentinel) |

For display impressions (CIL only, no event_log): `CIL.ip = bid_ip` (100% validated). vast_ip = bid_ip for display.

---

## Empirical IP Validation (2026-03-10)

These findings inform the column design:

| Claim | Result | Sample Size |
|-------|--------|-------------|
| bid_ip = win_ip | 100% match (47 apparent diffs = 0.0.0.0 null sentinel) | 38,204,354 rows |
| vast_impression_ip = vast_start_ip | 99.95% match (374 differ) | 812,609 rows |
| bid_ip = vast_ip | 98.98% match (8,353 differ — CGNAT /24 rotation) | 815,433 rows |
| Cross-stage: S2 bid_ip in S1 vast_ip | 309 vast_only matches (vs 45 bid_only) | 97,655 distinct S2 bid_ips |
| serve_ip = bid_ip | 93.6% match | CTV impressions |
| serve_ip when differs | 96.9% internal 10.x.x.x, 3.1% AWS IPs | 6.4% of CTV |
| win_logs.impression_ip_address | Infrastructure/CDN IP, not user | 661,987 differ from win_ip |
| segment_ip = bid_ip | 100% (Zach confirmation) | N/A |

**Conclusion:** 3 IPs per stage in the table: `bid_ip` (targeting identity = segment = win), `serve_ip` (ad serve request, mostly = bid_ip), `vast_ip` (VAST playback, cross-stage key). serve_ip included for audit completeness per Zach's note that it's "almost always bid_ip, but not always" and may mutate between stages.
