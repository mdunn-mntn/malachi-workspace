# History floor: which table reaches furthest back

Run 2026-08-20 for AUDI-1213. Question: the 2024-01-01 floor hides pre-2024 churned advertisers. Is there a table that reaches further?

## Answer: yes, `summarydata.all_facts`, back to 2020-10-01. It costs 2.68 TB per run.

| Table | Layer | Earliest day | Advertisers resolvable | Scan for a per-advertiser last-active | Notes |
|---|---|---|---|---|---|
| `summarydata.sum_by_advertiser_by_day` | silver | 2024-01-01 | 6,232 | 0.161 GB | Advertiser-facing spend (media + data + platform). Current AUDI-1204 source |
| `summarydata.sum_by_campaign_by_day` | silver | 2024-01-01 | 6,232 | 0.615 GB | Same floor, same counts. Campaign grain |
| `summarydata.all_facts` | silver | **2020-10-01** | **7,642** | **2,683 GB** | `media_spend` only, no data/platform legs. Hourly, 180 columns |
| `aggregates.agg__daily_sum_by_campaign` | silver | n/a | n/a | n/a | Deleted 2026-08-19 |

The two silver day-rollups agree exactly (6,232 advertisers, 1,863 delivering, 4,369 lapsed, `MIN(day)` 2024-01-01 in both), so 2024-01-01 is a platform-wide silver floor, not a quirk of one table.

## What the deeper floor buys

Measured over `all_facts` 2019-01-01 to 2024-01-01 (job `perf_20260820_154321_92350`, 2,683.4 GB, 44s wall, 13,932 slot-sec, `dw-main-bronze:us-central1.background-jobs`):

- 2,281 advertisers delivered impressions before 2024-01-01.
- **1,410 of them never delivered again on or after 2024-01-01**, so they are invisible in every silver day-rollup and absent from the current cohort count.
- 953 of those 1,410 carry >= $10k lifetime pre-2024 media spend.
- $149.03M pre-2024 media spend attached to the invisible set.
- Earliest impression day in the table: 2020-10-01, which is the table's own start, not a cutoff we chose.

Full picker population if the deeper floor is used: **7,642** (6,232 + 1,410), against 879 embedded today.

## Caveats

- `all_facts` carries `media_spend` only. The $149.03M is NOT on the advertiser-facing basis (media + data + platform) that AUDI-1204, INCR-75 and the rest of the refresh standardize on, so it is not comparable to the $468.13M lapsed figure in `ti_1019_lapsed_cohort_size.md`.
- 2.68 TB is slot time on the reservation, not dollars, but it is ~14k slot-seconds per refresh against 0.161 GB for the silver path. Only worth paying on a schedule if the pre-2024 names are actually wanted.
- Recency, not churn status: an advertiser last delivering in 2021 is five years lapsed. Whether that is a win-back target or noise is Al's call, not a data question.
- `all_facts` has `require_partition_filter: false`, so an unbounded query against it full-scans. Always bound `hour`.

---

# How far back the calculator actually needs

Run 2026-08-20, job `perf_20260820_154...`, 0.161 GB.

The MDE math never touches deep history. Per advertiser it needs exactly two windows, both anchored on that advertiser's last active day:

1. **56 days of delivery** for the rate and reach inputs (baseline rate, CPM, imps/IP, distinct IPs). 56 days because the test horizon is 8 weeks and `distinct_ips_56d` removes the linear extrapolation that overstates reach 1.25-1.32x.
2. **12 months** for the typical-active-month budget basis (median of months over $1k).

Deep history only decides **who appears in the picker**, never what the calculator computes for them. So the floor is set by the recency cut, plus 12 months behind it.

| Recency cut (days since last delivery) | Advertisers | >= $10k lifetime | Full 12mo budget history | Full 56d rate history | Earliest day needed |
|---|---|---|---|---|---|
| 30 (delivering only) | 1,863 | 1,326 | 1,863 | 1,863 | 2025-07-21 |
| 180 | 3,123 | 2,051 | 3,123 | 3,123 | 2025-02-21 |
| **365** | **4,409** | **2,759** | **4,409** | **4,409** | **2024-08-20** |
| 730 | 5,739 | 3,594 | 5,343 (396 short) | 5,739 | 2023-08-21 |

**Recommendation: a 365-day recency cut.** It is the largest cut silver serves losslessly. All 4,409 advertisers get both windows in full, the earliest day touched is 2024-08-20 against a 2024-01-01 floor, and the scan is 0.161 GB.

At 730 days, 396 advertisers need history back to 2023-08-21, before the silver floor, so their budget basis silently computes on a truncated window. That is the failure mode the frozen `agg__daily_sum_by_campaign` already produced once (340 of 879 rows capped at 8 months instead of 12). Going past 365 days means either accepting truncated budget baselines or paying 2.68 TB for `all_facts`.

Dropped by the 365-day cut: 1,823 advertisers last delivering over a year ago, plus the 1,410 pre-2024 names only `all_facts` can see.
