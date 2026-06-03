# Returning-advertiser sizing for Victor's score-filter proposal

**Question:** how many advertisers spend substantial $$$, go dark for a long stretch, then resume meaningful spend? (Brian's risk re: Victor's daily score-filter.)

**Source:** `dw-main-silver.summarydata.sum_by_advertiser_by_day`, last 730 days (2024-06-04 → 2026-06-03).

**Method:** for each advertiser, find their longest run of inactive days (no `media_spend`) between two active days. Bucket by gap length and post-gap spend.

## Result

Advertisers with a gap of N+ days, by post-gap spend bucket:

| Gap        | <$1k | $1k–$10k | $10k–$100k | $100k–$1M | $1M+ | **N**  | **Post-gap $$** |
|------------|-----:|---------:|-----------:|----------:|-----:|------:|----------------:|
| <30d       | 1,119|     1,737|         992|        335|    37| 4,220 | $256.6M         |
| 30–59d     |    61|       143|         106|         30|     2|   342 |  $14.3M         |
| 60–89d     |    38|        96|          56|         23|     2|   215 |  $11.6M         |
| 90–179d    |    42|       130|         122|         31|     3|   328 |  $18.8M         |
| 180–364d   |    40|       131|          93|         19|     0|   283 |   $6.8M         |
| 365d+      |    13|        20|          14|          0|     0|    47 |   $0.4M         |

## Headline numbers

- **Gap ≥60d + post-gap spend ≥$10k: 363 advertisers, $35.9M post-gap.**
  Real seasonal/quarterly returners — not zero, but ~7% of the active cohort.
- **Gap ≥180d + post-gap spend ≥$100k: 19 advertisers, $3.6M.** Vanishingly small.
- **Gap ≥365d + post-gap spend ≥$100k: 0 advertisers.** Malachi's "close to 0" holds for true year-long hibernation.
- Big-spender returners (post-gap ≥$1M) sit almost entirely in <90 day gaps (37 + 2 + 2 = 41) — likely planned dark periods between flights, not "we forgot we had this advertiser."

## Implications for Victor's filter

- Victor's rules generate scores when a campaign group is live OR was updated <24h ago. Any returning advertiser who flips a campaign group from off→on will trigger rule 2 (campaign_group.update_time) the same day. So the residual risk is advertisers who resume spend **without touching campaign_groups at all**, which is structurally close to impossible — bidding requires an active campaign group.
- The next-day reconciliation check Victor described bounds worst-case latency to 1 day. For 363 advertisers / 730 days = ~0.5 returns/day, 1-day latency on a fraction of them is tiny.
- The size of the "return" cohort doesn't kill the proposal — it justifies the next-day catch-up check.
