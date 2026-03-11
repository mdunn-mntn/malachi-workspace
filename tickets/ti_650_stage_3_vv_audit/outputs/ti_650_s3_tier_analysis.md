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

## Unresolved: 750 VVs (3.15%)

Investigation needed — 750 S3 VVs where no path finds an S1 impression.
See `outputs/ti_650_s3_unresolved.json` for full list.
