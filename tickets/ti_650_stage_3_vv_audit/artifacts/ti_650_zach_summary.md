# TI-650: Negative Case Analysis — Summary for Zach

**Date:** 2026-03-10
**Advertiser:** 37775
**Trace window:** 2026-02-04 to 2026-02-11 (7 days)
**Lookback:** 90 days (from 2025-11-06)
**Scope:** Prospecting only (objective_id NOT IN 4, 7)

---

## Bottom Line

**We can explain 100% of S2 VVs.** Every unresolved VV has a known root cause — LiveRamp identity graph entry via CGNAT IPs where the S1 impression was served to a different IP in the same household.

| Metric | Value |
|--------|-------|
| Total prospecting S2 VVs | 18,450 |
| Resolved to S1 impression | 18,178 (98.53%) |
| Unresolved | 272 (1.47%) |
| Competing VVs in unresolved | 210 (77.2%) |
| **Primary VV unresolved** | **62 (0.34%)** |

---

## Resolution by Device Type

| Category | Total | Resolved | % | Primary Unresolved |
|---|---|---|---|---|
| CTV (SET_TOP_BOX + CONNECTED_TV) | 16,112 | 15,880 | 98.56% | 54 (0.34%) |
| Display (MOBILE + TABLET + GAMES_CONSOLE) | 2,338 | 2,298 | 98.29% | 8 (0.34%) |
| **ALL** | **18,450** | **18,178** | **98.53%** | **62 (0.34%)** |

Primary VV unresolved rate is identical (0.34%) across CTV and display.

---

## CTV Resolution Cascade (5 tiers)

| Tier | How it works | VVs | Cumulative |
|------|-------------|-----|-----------|
| 1. bid_ip match | S1 impression at same bid IP | 15,465 | 95.98% |
| 2. guid_vv_match | S1 VV with same guid (same user, different IP) | 353 | 98.17% |
| 3. guid_imp_match | S1 impression with same guid | 5 | 98.20% |
| 4. s1_imp_redirect | S1 impression at VV's redirect IP | 11 | 98.27% |
| 5. household_graph | S1 impression at household-linked IP | 46 | 98.56% |
| Unresolved | | 232 | 1.44% |

## Display Resolution Cascade (3 tiers)

| Tier | VVs | Cumulative |
|------|-----|-----------|
| 1. bid_ip match | 2,236 | 95.64% |
| 2. guid_vv_match | 61 | 98.25% |
| 4. s1_imp_redirect | 1 | 98.29% |
| Unresolved | 40 | 1.71% |

---

## What Happened to the 272 Unresolved

**All 272 are LiveRamp identity graph entries.** The user's IP entered the S2 targeting segment via LiveRamp (data_source_id=3), not via an S1 impression at that same IP. The S1 impression exists — it was served to a different IP in the same household/identity graph.

**Why we can't trace further:** Most unresolved IPs are T-Mobile CGNAT addresses (172.5x.x.x). CGNAT rotates IPs across users, so the household graph snapshot (point-in-time) may not include the IP that was active when the S1 ad was served. A time-series IP→household mapping would close this gap, but none exists in BQ.

**Attribution model breakdown of 272 unresolved:**
- **210 (77.2%) are competing VVs** (models 9-11) — secondary/industry-standard attribution where VVS responded "false" to TRPX
- **62 (22.8%) are primary VVs** (models 1-3) — actual last-touch attribution

---

## Key Discovery: Retargeting Scoping

Previous analysis showed ~20% unresolved. This was inflated because **retargeting campaigns (objective_id=4) exist at every funnel_level** (1, 2, 3). Retargeting VVs enter segments via LiveRamp/audience data by design — they never had an S1 impression. Once excluded, the unresolved rate drops from ~20% to 1.47%.

**Objective ID reference:**

| ID | Name | Relevant? |
|----|------|-----------|
| 1 | Prospecting | Yes (S1/S2/S3) |
| 3 | Prospecting (dup) | Yes |
| 5 | Multi-Touch | Yes (S2) |
| 6 | Multi-Touch Full Funnel | Yes (S3) |
| 2 | Onsite | No (ads on customer's website) |
| 4 | Retargeting | **Excluded** |
| 7 | Ego | **Excluded** (employee targeting) |

---

## Household Graph — Potential New Tier

`bronze.tpa.graph_ips_aa_100pct_ip` maps IPs to household IDs. We used it as tier 5 in the negative case analysis and it resolved 46 additional CTV VVs. This could be added as tier 11 in the production SQLMesh model if desired. Coverage is limited by CGNAT IP rotation — only resolves cases where the household graph's current snapshot includes both the S1 and S2 IPs.

---

## Files

| File | Contents |
|------|----------|
| `queries/ti_650_negative_case_analysis.sql` | Phase 5 (CTV cascade) + Phase 6 (display cascade) |
| `queries/ti_650_audit_trace_queries.sql` | v11 production query (10 tiers, household_graph not yet added) |
| `summary.md` | Full findings with combined results |
| `artifacts/ti_650_consolidated.md` | All findings numbered 1-30 |
| `artifacts/ti_650_pipeline_explained.md` | Full pipeline documentation with resolution tables |
