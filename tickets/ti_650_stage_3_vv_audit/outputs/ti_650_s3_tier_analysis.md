# S3 Cross-Stage Resolution: Independent Path Analysis

**Date:** 2026-03-11
**Advertiser:** 37775 | **Trace:** Feb 4–11 | **Lookback:** 90 days | **Filter:** objective_id IN (1,5,6)
**Total S3 VVs:** 23,844

## Path Tests (each tested independently)

### Test 1: S3 → S1 direct

| Path | Resolves | % |
|------|---------|---|
| S3 bid_ip → S1 vast_start_ip (imp_direct) | 22,192 | 93.07% |
| S3 impression_ip → S1 vast_start_ip (imp_visit) | 23,027 | 96.57% |
| **Either** | **23,092** | **96.85%** |

### Test 2: S3 → S2 (first hop only)

| | Count | % of total |
|---|------|---|
| Found S2 VV via bid_ip → S2 vast_start_ip | 11,400 | 47.81% |

### Test 3: S2 → S1 (of S3s that found S2)

| | Count | % of S2-found |
|---|------|---|
| S2 → S1 resolved | 10,307 | 90.41% |
| S2 found but no S1 | 1,093 | 9.59% |

### Test 4: S3 → S2 → S1 full chain

| | Count | % of total |
|---|------|---|
| Full chain resolved | 10,307 | 43.23% |

## Overlap: S3→S1 direct vs S3→S2→S1 chain

| Category | Count |
|----------|-------|
| Both direct AND chain resolve | 10,305 |
| Direct only (chain can't) | 12,787 |
| **Chain only (direct can't)** | **2** |
| **Neither** | **750** |

**Key finding:** Same pattern as S2 — the chain (S3→S2→S1) has only **2 unique** contributions. S3→S1 direct (imp_direct + imp_visit) gets 96.85% alone. The chain is almost entirely redundant.

## Minimum Set: imp_direct + imp_visit = 96.85%

| Tier | Role | Resolves |
|------|------|---------|
| imp_direct (bid_ip → S1 vast_start) | Primary | 22,192 |
| imp_visit (impression_ip → S1 vast_start) | Fallback | +900 incremental |
| **Combined** | | **23,092 (96.85%)** |

## Within-Stage Self-Resolution: 100% at all levels

Every VV at every funnel level has a matching impression via ad_served_id in the impression_pool.
No IP matching needed for within-stage linking — it's deterministic.

| Stage | Total VVs | Has Impression | Self-Resolve % |
|-------|----------|---------------|---------------|
| S1 | 93,274 | 93,274 | 100% |
| S2 | 16,753 | 16,753 | 100% |
| S3 | 23,844 | 23,844 | 100% |

This means the only place IP matching is needed is for CROSS-STAGE linking (S2→S1, S3→S1).

## Unresolved: 752 VVs (3.15%)

Full list: `outputs/ti_650_s3_unresolved.json` (752 rows with diagnostic columns)

### Profile of unresolved S3 VVs

| Dimension | Breakdown |
|-----------|-----------|
| **Diagnosis** | All 752 have S3 impression — just no S1 IP match |
| **Attribution model** | 512 competing (68%: models 9,10,11), 240 primary (32%: models 1,2,3) |
| **is_cross_device** | 415 true (55%), 337 false (45%) |
| **S1 IP exists (any time)** | **727 = false (96.7%)** — bid_ip has NEVER been an S1 vast_start_ip |
| **Has S2 VV** | 526 no (70%), 226 yes (30%) |
| **first_touch_ad_served_id** | Only 14 have one (1.9%) |
| **visit_impression_ip** | 740 have it (98.4%) — also doesn't match S1 |
| **bid_ip = vast_start_ip** | 750/752 identical — no VAST divergence to exploit |
| **Top subnets** | 172.5x.x.x (T-Mobile CGNAT), 40.138.x (Verizon) |

### Root cause

**727/752 (96.7%) have a bid_ip that has NEVER appeared as an S1 vast_start_ip.** These IPs entered the S3 segment through identity graph resolution (LiveRamp/CRM), not through an S1 impression on the same IP. The household was served an S1 impression on a different IP — but CGNAT rotation means the current IP was never associated with S1.

Same root cause as the S2 unresolved (8 VVs) — just a larger population at S3 because S3 is further from S1 in time (more opportunity for IP rotation).

### Primary vs competing VV impact

| Type | Count | % |
|------|-------|---|
| Competing (models 9,10,11) | 512 | 68% |
| **Primary (models 1,2,3)** | **240** | **32%** |
| **Primary unresolved rate** | **240/23,844** | **1.01%** |
