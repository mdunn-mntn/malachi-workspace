# Kindred Bravely (35094) — systematic audience audit

## Where delivery & revenue go, by stage

| Stage | Camps | Impressions | Spend | Conv | Revenue | ROAS |
|---|--:|--:|--:|--:|--:|--:|
| Prospecting | 6 | 7.1M | $159K | 2,563 | $296K | 1.9x |
| Retargeting | 6 | 7.4M | $70K | 15,758 | $1.9M | 26.5x |
| Multi-Touch S2 | 6 | 4.0M | $18K | 2 | $663 | 0.0x |
| Multi-Touch S3 | 6 | 2.0M | $9K | 345 | $38K | 4.3x |
| Ego | 6 | 82 | $1 | 0 | $0 | 0.0x |

**Structural:** each campaign group is a full funnel (stage = `objective_id`); group-level metrics conflate stages. Retargeting (89071) is the engine; prospecting = 62% of spend / 13% of revenue.

## Prospecting audience — targeting + funnel + flags

| Campaign | Geo | Interest | Gate | Reached | HI-share | Coverage | ROAS | Flags |
|---|---|---|---|--:|--:|--:|--:|---|
| 69884 High Pop (base) | 20/210 (top-20) | MM OR 3P | — | — dark | — | — | 2.36x | narrow geo 20/210, dark (F1 stopped) |
| 109926 Mid Pop | 38/210 (mid-38) | MM OR 3P | — | 599K | 85% | 12% | 1.82x | — |
| 96108 Low Pop | 152/210 (low-152) | MM OR 3P | — | 191K | 80% | 4% | 1.47x | thin geo 152/210 |
| 115946 HiPop Mom-Focus | 20/210 (top-20) | MM OR 3P | net-new | 334K | 88% | 4% | 1.25x | net-new gate, narrow geo 20/210 |
| 115943 HiPop Harter | 20/210 (top-20) | MM OR 3P | net-new | 331K | 85% | 4% | 1.49x | net-new gate, narrow geo 20/210 |
| 115945 HiPop Motherhood | 20/210 (top-20) | MM OR 3P | net-new | 332K | 86% | 4% | 1.31x | net-new gate, narrow geo 20/210 |

**Read:** prospecting reaches ~80–88% HI at ~4–12% coverage of the (inflated) addressable — no hard HI ceiling, not scraping low-score users. Variants' worse ROAS = net-new HI converting worse. Base 261318 dark since ~Mar (F1 prospecting stopped; group's later delivery = retargeting).
