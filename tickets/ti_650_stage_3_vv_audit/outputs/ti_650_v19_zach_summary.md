# TI-650: Unresolved VV — Exhaustive Proof (for Zach)

## Bottom Line

IP `216.126.34.185` was **never served a single S1 or S2 impression** within campaign_group 93957. This was verified exhaustively across **all 3 cross-stage connecting tables** (event_log, viewability_log, impression_log) over **2+ years of data** (Jan 2024 – Feb 2026). The VV is correctly unresolvable under campaign_group_id scoping.

The IP *was* served S1 impressions for the same advertiser — but in **different campaign groups** (78903, 78904, 78893). Campaign_group scoping correctly blocks those from being treated as funnel links.

---

## What We Checked

### Scope: campaign_group_id 93957 (all campaigns, no filters)

We searched all 6 campaigns in cg 93957 — including S1, S2, S3, and Ego. No deleted or test campaigns exist, but the query did not filter on `deleted` or `is_test`, so any would have been included.

| campaign_id | funnel_level | channel | name |
|---|---|---|---|
| 450305 | 1 (S1) | CTV | Beeswax Television Prospecting |
| 450301 | 2 (S2) | CTV | Beeswax Television Multi-Touch |
| 450303 | 2 (S2) | Display | Multi-Touch |
| **450300** | **3 (S3)** | **CTV** | **Beeswax Television Multi-Touch Plus** ← VV's campaign |
| 450304 | 3 (S3) | Display | Multi-Touch - Plus |
| 450302 | 4 (Ego) | CTV | Beeswax Television Prospecting - Ego |

### Tables searched (all 3 cross-stage connecting paths)

Per the VV trace flowchart, the cross-stage IP link uses a different table depending on impression type:

| Impression Type | Connecting Table | Cross-Stage Key |
|---|---|---|
| CTV | event_log | `next_stage.bid_ip → event_log.ip` (vast_start/vast_impression) |
| Viewable display | viewability_log | `next_stage.bid_ip → viewability_log.ip` |
| Non-viewable display | impression_log | `next_stage.bid_ip → impression_log.ip` |

All 3 tables were searched for IP `216.126.34.185` (both `ip` and `bid_ip` columns). CIDR-safe: matched both bare IP and `/32` suffix format.

### Lookback window

**Jan 1, 2024 → Feb 4, 2026** — over 2 years. The VV occurred on Jan 27, 2026.

---

## Results

### Within campaign_group 93957

| Table | S1/S2 Campaigns | S3+ Campaigns | Total |
|---|---|---|---|
| event_log (vast_impression/vast_start only) | **0 records** | 2 records (campaign 450300, Jan 27 2026) | 2 |
| viewability_log | **0 records** | **0 records** | 0 |
| impression_log | **0 records** | 2 records (campaign 450300, Jan 25 + Jan 27 2026) | 2 |
| **Total** | **0** | **4** | **4** |

The only records found are from the VV's own campaign (450300, S3 CTV). **Zero records for campaigns 450305 (S1), 450301 (S2 CTV), 450303 (S2 display), 450304 (S3 display), or 450302 (Ego).**

### Across all advertiser 37775 campaigns (where the IP was actually served)

When we remove the campaign_group filter and search across all campaigns for advertiser 37775, the IP appears in **multiple other campaign groups**:

| Campaign Group | Campaign | Funnel Level | Channel | Type | Date Range |
|---|---|---|---|---|---|
| 78904 | 311974 | **S1** | CTV | Prospecting | Feb 24, 2025 |
| 78903 | 311968 | **S1** | CTV | Prospecting | Feb 24, 2025 |
| 78893 | 311900 | **S1** | CTV | Prospecting | Apr 20, 2025 |
| 78903 | 311966 | S3 | CTV | Multi-Touch Plus | Mar 31 – Jun 5, 2025 (many events) |
| 84697 | 394578 | S3 | CTV | TV Retargeting - Cart | May 20, 2025 |
| 84697 | 394577 | **S2** | CTV | TV Retargeting - 5+ PV | Jul 7, 2025 |
| 69778 | 260986 | **S2** | CTV | TV Retargeting - 5+ PV | Jul 9–22, 2025 |
| 92881 | 443844 | **S2** | CTV | TV Retargeting - 5+ PV | Jul 21, 2025 |
| 93957 | 450300 | S3 | CTV | Multi-Touch Plus | Jan 25–27, 2026 (the VV) |

578 total events found across the advertiser. The IP is a real, heavily-served MNTN viewer with S1 prospecting exposure — just not in campaign_group 93957.

### Full pipeline trace (v19)

We also traced the VV through every stage of the CTV pipeline to confirm IP consistency:

| Pipeline Stage | IP | Timestamp |
|---|---|---|
| bid_logs | 216.126.34.185 | 2026-01-27 14:52:20 |
| win_logs | 216.126.34.185 | 2026-01-27 14:52:20 |
| impression_log | 216.126.34.185 | 2026-01-27 14:52:20 |
| event_log (vast_impression) | 216.126.34.185 | 2026-01-27 14:53:39 |
| event_log (vast_start) | 216.126.34.185 | 2026-01-27 14:53:39 |
| clickpass_log (VV) | 216.126.34.185 | 2026-02-04 00:06:14 |

IP is **100% identical** across all pipeline stages. No mutation. `ip_mutated = false`.

---

## Why This VV Is Unresolvable

1. **No S1/S2 impression exists** for this IP within campaign_group 93957. We checked all 3 connecting tables, all campaigns (including deleted), 2+ years of data.
2. **The IP entered S3 via identity graph** (data_source_id=3 in tmul_daily), not via any prior MNTN impression within its campaign group.
3. **Campaign_group scoping is working correctly.** The IP had real S1 exposure in other campaign groups (78903, 78904, 78893) — but linking across campaign groups would be a coincidental IP match, not a funnel trace.
4. **This is not a data quality issue.** The VV pipeline is intact (IP consistent at every stage). The IP was correctly targeted via identity graph. There is simply no same-campaign-group S1/S2 impression to resolve against.

---

## Queries

- **v18 exhaustive trace:** `queries/ti_650_v18_exhaustive_ip_trace.sql` — Part A (cg 93957, 3 tables UNION ALL) + Part B (advertiser 37775, 3 tables UNION ALL)
- **v19 full pipeline trace:** `queries/ti_650_v19_vv_full_trace.sql` — 2-stage pipeline trace (core + win/bid by auction_id)

## Performance

| Query | Bytes Processed | Wall Time |
|---|---|---|
| v19 Stage 1 (core trace) | 374 GB | 25s |
| v19 Stage 2 (win/bid) | 1,099 GB | 3s |
| v18 Part A (cg 93957, 3 tables) | 26,531 GB | 1,235s (~21 min) |
| v18 Part B (advertiser 37775, 3 tables) | 26,531 GB | 1,120s (~19 min) |
