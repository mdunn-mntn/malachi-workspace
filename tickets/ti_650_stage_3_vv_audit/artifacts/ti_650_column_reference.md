# VV IP Lineage — Column Reference (v10.1)

**Table:** `{dataset}.vv_ip_lineage`
**Rows:** One per verified visit (VV), all advertisers, all stages
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `vv_stage`

Columns are ordered **left-to-right to trace backward from VV to S1**. For a S3 VV, read left to right: VV identity → visit IPs → S3 impression → S2 impression → S1 impression.

---

## Quick Reference: Reading a Row

```
                      |  S3 impression IPs              |  S2 impression IPs                           |  S1 impression IPs
                      |  (this VV's impression)         |  (prior VV at S2)                            |  (chain-traversed to S1)
                      |  s3_vast_start  s3_vast_imp     |  s2_vast_start  s2_vast_imp                  |  s1_vast_start  s1_vast_imp
                      |  s3_serve  s3_bid  s3_win       |  s2_serve  s2_bid  s2_win                    |  s1_serve  s1_bid  s1_win
                      |  s3_impression_time             |  s2_impression_time                          |  s1_impression_time
                      |                                 |  s2_ad_served_id  s2_vv_time                 |  s1_ad_served_id  s1_resolution_method
                      |                                 |  s2_campaign_id   s2_redirect_ip             |

Stage 3 VV (S3→S2→S1):
  visit_ip            |  ✓ populated                    |  ✓ populated (prior VV is S2)                |  ✓ populated (chain to S1)
                                                  ↓ cross-stage                                  ↓ cross-stage
                                         s3_bid ≈ s2_vast (match_ip)                   s2_bid ≈ s1_vast (match_ip)

Stage 3 VV (S3→S1 skip — no S2 step):
  visit_ip            |  ✓ populated                    |  NULL (no S2 impression exists)              |  ✓ populated (prior VV is S1)

Stage 2 VV:
  visit_ip            |  NULL                           |  ✓ populated (this VV's impression)          |  ✓ populated (chain to S1)
                                                                                                 ↓ cross-stage
                                                                                      s2_bid ≈ s1_vast (match_ip)

Stage 1 VV:
  visit_ip            |  NULL                           |  NULL                                        |  ✓ populated (this VV's impression)
```

**Column naming:** Stage-based (s3/s2/s1). Columns always refer to the same funnel stage regardless of VV type. S1 VVs have s3 and s2 columns NULL.

**VAST event order:** vast_impression fires FIRST (creative loaded), vast_start fires SECOND (playback begins). vast_start is the last VAST callback — the most recent IP observation before VV.

**NULL rules:** S3 columns NULL for S1/S2 VVs. S2 columns NULL for S1 VVs AND for S3 VVs whose prior VV is S1 (IP went S1→S3 directly, skipping S2 — no S2 impression exists). S1 columns are always attempted (~89% populated for S2/S3 VVs; ~11% structural ceiling from CRM/cross-device entry with no IP breadcrumbs). NULLs in the cross-stage link when the chain DOES exist = structural (~28% of S3 VVs find no prior VV).

**Cross-stage link:** Uses merged vast pool (`pv_pool_vast`) with `match_ip` key. vast_start_ip preferred (priority 1), vast_impression_ip fallback (priority 2), dedup'd by `(match_ip, pv_stage)`. Redirect_ip is a separate pool for cross-device fallback. Dedup prefers vast match over redirect, then last touch (most recent).

**Cross-stage link is IP-only:** No deterministic ID links across stages. first_touch_ad_served_id links S3/S2→S1 directly (skips S2) but only 25-51% available. IP matching is the only mechanism — same as the production bidder. See Finding #28.

---

## 1. Identity

| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | UUID of the VV and its triggering impression. Same UUID in clickpass_log, event_log, CIL. Primary key. |
| `advertiser_id` | INT64 | MNTN advertiser ID (integrationprod.advertisers). |
| `campaign_id` | INT64 | Campaign that received last-touch credit for this VV. |
| `vv_stage` | INT64 | Stage of `campaign_id` per `campaigns.funnel_level`. 1=S1, 2=S2, 3=S3. |
| `vv_time` | TIMESTAMP | When the verified visit was recorded (`clickpass_log.time`). |
| `vv_guid` | STRING | User/device cookie ID from `clickpass_log.guid`. Persists across multiple VVs (same user). Matches ui_visits.guid 99.99%. |
| `vv_original_guid` | STRING | Pre-reattribution guid from `clickpass_log.original_guid`. Differs from vv_guid in 16% of VVs (those that were reattributed). NULL when no reattribution occurred. |
| `vv_attribution_model_id` | INT64 | Attribution model used for this VV from `clickpass_log.attribution_model_id`. Values: 1, 2, 3, 9, 10, 11. |

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
| `s3_vast_start_ip` | STRING | `event_log.ip` (event_type_raw='vast_start') | IP at VAST start callback. Fires AFTER vast_impression (last VAST callback). 99.85% = vast_impression_ip. When differs: SSAI proxy (see Finding #25). |
| `s3_vast_impression_ip` | STRING | `event_log.ip` (event_type_raw='vast_impression') | IP at VAST impression callback. Fires BEFORE vast_start (first VAST callback). |
| `s3_serve_ip` | STRING | `impression_log.ip` via `ad_served_id` | IP at ad serve request. 93.6% = bid_ip. When differs: 96.9% internal 10.x.x.x NAT, 3.1% AWS infra. |
| `s3_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` (display) | IP at auction/bid time. = win_ip = segment_ip (100% validated). The targeting identity. **Cross-stage link: should ≈ s2 match_ip.** |
| `s3_win_ip` | STRING | `event_log.bid_ip` | = bid_ip today (100% validated). Kept for Mountain Bidder SSP future-proofing where win callback may return a different IP. |
| `s3_impression_time` | TIMESTAMP | `impression_pool.time` | When the S3 impression was served (`MIN(event_log.time)` for CTV, `cost_impression_log.time` for display). |
| `s3_guid` | STRING | `event_log.guid` or `CIL.guid` | Impression-side guid for the S3 impression. |

---

## 4. S2 Impression IPs (Prior VV's S2 Impression)

The S2 impression in this IP's funnel. **NULL for S1 VVs.** Also NULL for S3 VVs whose prior VV is S1 (IP went S1→S3 directly, skipping S2 — no S2 impression exists).

**Cross-stage link:** `s3_bid_ip` should approximately equal the S2 impression's vast IP (the S2 VV's VAST IP entered the S3 segment, which the S3 bidder targeted). Matched via `pv_pool_vast.match_ip = lt.bid_ip` (merged pool: vast_start preferred, vast_impression fallback).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s2_vast_start_ip` | STRING | `event_log.ip` (vast_start) | S2 impression VAST start IP. |
| `s2_vast_impression_ip` | STRING | `event_log.ip` (vast_impression) | S2 impression VAST impression IP. |
| `s2_serve_ip` | STRING | `impression_log.ip` via prior VV's `ad_served_id` | S2 impression serve request IP. |
| `s2_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | S2 impression bid/auction IP. **Cross-stage link to S1: should ≈ s1 match_ip.** |
| `s2_win_ip` | STRING | `event_log.bid_ip` | = bid_ip today. Future-proofing for Mountain Bidder SSP. |
| `s2_ad_served_id` | STRING | `clickpass_log` (self-join) | S2 impression ID. For S2 VVs: = ad_served_id (self). For S3 VVs: prior VV's ad_served_id when pv_stage=2. NULL when no S2 step. |
| `s2_vv_time` | TIMESTAMP | `clickpass_log` | When the S2 VV occurred. For S2 VVs: = vv_time. For S3 VVs: prior VV's time when pv_stage=2. |
| `s2_impression_time` | TIMESTAMP | `impression_pool.time` | When the S2 impression was served. |
| `s2_campaign_id` | INT64 | `clickpass_log` | S2 campaign. For S2 VVs: = campaign_id. For S3 VVs: prior VV's campaign when pv_stage=2. |
| `s2_redirect_ip` | STRING | `clickpass_log.ip` | S2 VV's redirect IP. Fallback match key for cross-device cases. |
| `s2_guid` | STRING | `event_log.guid` or `CIL.guid` | Impression-side guid for the S2 impression. |
| `s2_attribution_model_id` | INT64 | `clickpass_log.attribution_model_id` | Attribution model used for the S2 VV. For S2 VVs: this VV's model. For S3 VVs: prior VV's model. |

**Prior VV match logic:** Primary: `pv_pool_vast.match_ip = current.bid_ip` (merged pool: vast_start preferred, vast_impression fallback, dedup'd by `(match_ip, pv_stage)`). Fallback: `pv_pool_redir.redirect_ip = current.redirect_ip` (household identity, covers cross-device). Dedup prefers vast match, then last touch (most recent). Advertiser_id constraint prevents CGNAT false positives.

---

## 5. S1 Impression IPs (Chain-Traversed)

The Stage 1 impression that started this IP's funnel. Resolved via 7-tier CASE.

For S1 VVs: s1 columns are populated directly from `ad_served_id` (the VV's own impression IS S1). s3 and s2 columns are NULL.

For S2/S3 VVs: resolved via chain traversal (~89% populated; ~11% structural ceiling).

**Cross-stage link:** `s2_bid_ip` should approximately equal the S1 impression's vast IP (matched via `pv_pool_vast.match_ip = pv_lt.bid_ip` with inline `pv_stage=1` filter).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `s1_vast_start_ip` | STRING | `event_log.ip` (vast_start) | S1 impression VAST start IP. |
| `s1_vast_impression_ip` | STRING | `event_log.ip` (vast_impression) | S1 impression VAST impression IP. |
| `s1_serve_ip` | STRING | `impression_log.ip` via S1 `ad_served_id` | S1 impression serve request IP. |
| `s1_bid_ip` | STRING | `event_log.bid_ip` or `CIL.ip` | S1 impression bid IP — the original targeting identity. **End of the chain.** |
| `s1_win_ip` | STRING | `event_log.bid_ip` | = bid_ip today. Future-proofing for Mountain Bidder SSP. |
| `s1_ad_served_id` | STRING | 10-tier chain traversal | S1 impression ID. For S1 VVs: = ad_served_id. |
| `s1_impression_time` | TIMESTAMP | `impression_pool.time` | When the S1 impression was served. |
| `s1_guid` | STRING | `event_log.guid` or `CIL.guid` | Impression-side guid for the S1 impression. Resolved via same 10-tier chain as s1_bid_ip. |
| `s1_resolution_method` | STRING | CASE expression | Which tier resolved S1. 10 tiers: `current_is_s1`, `vv_chain_direct`, `vv_chain_s2_s1`, `imp_chain`, `imp_direct`, `imp_visit_ip`, `cp_ft_fallback`, `guid_vv_match`, `guid_imp_match`, `s1_imp_redirect`. NULL = unresolved. See tier definitions below. |
| `cp_ft_ad_served_id` | STRING | `clickpass_log.first_touch_ad_served_id` | System-recorded S1 shortcut. NULL ~40%. Retained as comparison reference. |

### S1 Resolution Tier Definitions (v11)

| Tier | Value | Join Logic | Description |
|------|-------|-----------|-------------|
| 1 | `current_is_s1` | `vv_stage = 1` | VV itself is S1. Impression IS the S1 impression. |
| 2 | `vv_chain_direct` | `pv_pool_vast/redir.pv_stage = 1` | Prior VV (via IP match) is S1. One hop. |
| 3 | `vv_chain_s2_s1` | Two-hop: prior VV is S2, whose prior VV is S1 | S3 → S2 → S1 VV chain. Two hops. |
| 4 | `imp_chain` | `s1_imp_pool.bid_ip = pv_lt.bid_ip` | S1 impression at prior VV's bid_ip (no S1 VV, but S1 ad served). |
| 5 | `imp_direct` | `s1_imp_pool.bid_ip = lt.bid_ip` | S1 impression at current VV's bid_ip. |
| 6 | `imp_visit_ip` | `s1_imp_pool.bid_ip = v.impression_ip` | S1 impression at ui_visits.impression_ip (pixel-side). |
| 7 | `cp_ft_fallback` | `impression_pool ON cp.first_touch_ad_served_id` | Clickpass first_touch_ad_served_id → impression lookup. |
| 8 | `guid_vv_match` | `s1_vv_guid.guid = cp.guid` | S1 VV with same guid (same user, different IP). |
| 9 | `guid_imp_match` | `s1_imp_guid.guid = cp.guid` | S1 impression with same guid (same user, different IP). |
| 10 | `s1_imp_redirect` | `s1_imp_pool.bid_ip = cp.redirect_ip` | S1 impression at current VV's redirect_ip (cross-device). |

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

## IP Sources Per Stage (5 each + timestamp)

| IP | Source Table | Source Column | Join Key | Role | Validated |
|----|-------------|---------------|----------|------|-----------|
| `vast_start_ip` | `event_log` | `ip` (event_type_raw='vast_start') | `ad_served_id` | VAST start callback IP. Fires AFTER impression. Last VAST event. 99.85% = vast_impression_ip. **Cross-stage key (merged pool: priority 1).** | 288.7M rows, 442K differ |
| `vast_impression_ip` | `event_log` | `ip` (event_type_raw='vast_impression') | `ad_served_id` | VAST impression callback IP. Fires BEFORE start. First VAST event. **Cross-stage key (merged pool: priority 2).** | Same as above |
| `serve_ip` | `impression_log` | `ip` | `ad_served_id` | Ad serve request IP. 93.6% = bid_ip. Differs = infra IP. | Zach: "almost always bid_ip" |
| `bid_ip` | `event_log` | `bid_ip` | `ad_served_id` | Targeting identity. = win_ip = segment_ip (100%). | 38.2M rows |
| `win_ip` | `event_log` | `bid_ip` | `ad_served_id` | = bid_ip today (100%). Kept for Mountain Bidder SSP future-proofing. | 38.2M rows |
| `impression_time` | `event_log`/`CIL` | `MIN(time)` | `ad_served_id` | When impression was served. For CTV: MIN(vast_start, vast_impression) time. For display: CIL.time. | N/A |

For display impressions (CIL only, no event_log): `CIL.ip = bid_ip` (100% validated). All 5 IPs = bid_ip for display.

---

## v10 Architecture Summary

**CTEs (9 total, was 13 in v9):**
1. `campaigns_stage` — campaign → stage classification
2. `cp_dedup` — anchor VVs in target date range
3. `impression_pool` — pivoted event_log + CIL (all IPs + timestamp per ad_served_id)
4. `v_dedup` — ui_visits (visit_ip, impression_ip)
5. `prior_vv_raw` — clickpass + impression_pool join (prior VVs with IPs + imp_time)
6. `pv_pool_vast` — merged vast pool (vast_start priority 1, vast_impression priority 2, dedup by match_ip+pv_stage)
7. `pv_pool_redir` — redirect_ip pool (cross-device fallback)
8. `s1_imp_pool` — S1 impressions by bid_ip (for imp_chain/imp_direct/imp_visit_ip tiers)
9. `with_all_joins` — main assembly with 10 LEFT JOINs

**LEFT JOINs (10 total, was 14 in v9):**
1. `lt` — this VV's impression (ad_served_id)
2. `v` — ui_visits (ad_served_id)
3. `pv_vast` — prior VV via merged vast pool (match_ip = bid_ip)
4. `pv_redir` — prior VV via redirect_ip (cross-device)
5. `pv_lt` — prior VV's impression (ad_served_id)
6. `s1_vast` — S1 VV chain via merged vast pool (pv_stage=1 inline)
7. `s1_redir` — S1 VV chain via redirect_ip (pv_stage=1 inline)
8. `s1_lt` — S1 VV's impression (ad_served_id)
9-10. `s1_imp_chain`, `s1_imp_direct` — S1 impression at prior/current bid_ip
11. `s1_imp_visit_ip` — S1 impression at visit's impression_ip
12. `ft_lt` — first_touch fallback impression

**Lookback:** 90 days (Zach confirmed max = 88 days: 14+30+14+30).

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
| vast_impression as cross-stage key | 98.434% match (bid_ip=vast_imp within-impression) | 252,929,133 rows |
| vast_start as cross-stage key | 98.431% match (bid_ip=vast_start within-impression) | 252,929,133 rows |
| Either/or fallback vs impression-only | +48,277 extra matches (0.019%) | 252,929,133 rows |
| Neither vast matches bid_ip | 1.558% (structural — CGNAT/SSAI/IPv6/VPN) | 3,941,738 rows |
| Cross-stage: vast_start matches S3 bid | 99.937% (start marginally better cross-stage) | 487,304 pairs |
| Cross-stage: vast_impression matches S3 bid | 99.884% (start wins by 257 out of 487K) | 487,304 pairs |
