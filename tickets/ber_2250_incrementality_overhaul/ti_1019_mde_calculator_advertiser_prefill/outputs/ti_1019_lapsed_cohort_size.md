# TI-1019 lapsed cohort size

Run 2026-08-20 via `bq_run.sh`, job `perf_20260820_152808_86908`, 0.161 GB, 1.7s.
Query: `queries/ti_1019_lapsed_cohort_sizing.sql`. Source `summarydata.sum_by_advertiser_by_day`, `impressions > 0`, history floor 2024-01-01.

| Bucket (days since last delivery) | Advertisers | Lifetime spend >=$10k | >=28 delivering days | Lifetime spend | Median lifetime spend |
|---|---|---|---|---|---|
| 0-30 (delivering) | 1,863 | 1,326 | 1,675 | $728.22M | $37,198 |
| 31-90 | 606 | 353 | 492 | $70.45M | $15,022 |
| 91-180 | 654 | 372 | 499 | $72.65M | $14,268 |
| 181-365 | 1,286 | 708 | 1,082 | $135.01M | $12,663 |
| >365 | 1,823 | 1,129 | 1,376 | $190.02M | $18,367 |

**Lapsed total: 4,369 advertisers, $468.13M lifetime spend.** Full picker population (delivering + lapsed) = 6,232 rows, 7.1x the 879 currently embedded.

Applying the AUDI-1204 measurability shape (lifetime spend >=$10k and >=28 delivering days) cuts lapsed to roughly 2,562 by spend alone; the two filters overlap and were not intersected in this run.

Caveats:
- The 2024-01-01 floor is the table's own floor, so the >365d bucket is truncated: an advertiser whose last delivery predates 2024-01-01 is absent entirely. Reaching further back means resolving last-active from `cost_impression_log` (floor 2023-10-01).
- Buckets are last-delivery recency, not churn status. A seasonal advertiser between flights lands in a lapsed bucket.
- Per-advertiser rate metrics are NOT in this run. Sizing only.
