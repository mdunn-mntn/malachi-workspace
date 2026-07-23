---
doc_type: ticket
title: "Ad-hoc: Returning-Advertiser Sizing for Victor's Score-Filter Proposal"
status: done
date: 2026-06-03
summary: "Sizing advertisers who go dark then resume spend — risk for Victor's daily score-filter"
result: "Return/day-1-lag risk tiny; 79% of spend in flights >=15d; next-day catch-up covers it"
keywords: [score-filter, returning advertiser, dormancy, day-1 lag, flight length, campaign_group.update_time, sum_by_advertiser_by_day, cold-start scoring, victor savitskiy, blitz, max gap]
---

## TL;DR

**Q:** How many advertisers spend substantial money, go dark for a long stretch, then resume meaningful spend — sizing the risk for Victor's daily score-filter proposal (plus short-flight / day-1-lag reality checks)?

**A:** The return / day-1-lag risk is small and structurally bounded, so it doesn't kill Victor's daily score-filter proposal — it justifies the next-day catch-up check. Over 730 days (2024-06-04→2026-06-03) from sum_by_advertiser_by_day: advertisers with a gap >=60d and post-gap spend >=$10k = 363 advertisers / $35.9M post-gap (~7% of the active cohort, ~0.5 returns/day). Gap >=180d + >=$100k = 19 advertisers / $3.6M; gap >=365d + >=$100k = 0 (true year-long hibernation ~0). Big-spender returners (post-gap >=$1M) sit almost entirely in <90-day gaps (41 of them), likely planned dark periods between flights. By spend tier, $1M+ spenders never disappear long — all 75 have max gap <=174d, so 180d retention covers 100% of $1M+; the $10k–$100k mid tier has the worst tail (p95=273d, p99=401d); <$10k has a 365d+ tail but only ~$62k at stake over 2 years. Short-flight / blitz checks: max single-day spend for any advertiser in 2 years was $153k (an established advertiser, not a blitz); true 1-day blitzes = 74 in 2 years / $56k combined; first-ever spend-day events = 5,466 over 2 years (~7.5/day, max $42k, p99 $2.7k) — Victor's rule 3 (new advertisers <7d) covers them. No Super Bowl one-day campaigns exist in MNTN. Campaign-length frame: 79% of MNTN's $477M flight spend runs in flights >=15 days, where worst-case day-1 lag hits <=4% of the campaign; the flights where day-1 lag matters most (1–3 days, 32–100% exposure) are only 4.3% of total spend; across the whole population only 6.9% of flight spend lands on a flight day-1. The residual structural risk — an advertiser resuming spend without touching campaign_groups at all — is close to impossible because bidding requires an active campaign group, and flipping a cg off->on triggers Victor's rule 2 (campaign_group.update_time) the same day.

**How:** For each advertiser over the last 730 days of sum_by_advertiser_by_day, found the longest run of inactive days (no media_spend) between two active days; bucketed advertisers by gap length and post-gap spend. Added a tiered max-gap percentile view by spend tier, a flight-grain and advertiser-day-grain short-flight/blitz view (including a "true 1-day blitz = no spend +/-7d" and "first-ever spend day" cohort), and a campaign-length-vs-day1-share view. Day-1 lag reframed as degraded targeting for a fraction of the flight equal to 1/flight_length.

**Tables:** sum_by_advertiser_by_day

**Learned:**
- Gap >=60d + post-gap spend >=$10k = 363 advertisers / $35.9M post-gap (~7% of active cohort, ~0.5 returns/day); gap >=180d + >=$100k = 19 advertisers / $3.6M; gap >=365d + >=$100k = 0.
- $1M+ spenders never disappear long: all 75 have max gap <=174d, so 180d retention covers 100% of them; $10k–$100k mid tier has the worst tail (p95=273d, p99=401d); <$10k has a 365d+ tail worth only ~$62k over 2 years.
- Max single-day advertiser spend in 2 years was $153k (established advertiser, not a blitz); true 1-day blitzes = 74 in 2 years / $56k combined; first-ever spend-day events = 5,466 over 2 years (~7.5/day, max $42k, p99 $2.7k). No Super Bowl one-day campaigns exist in MNTN.
- 79% of MNTN's $477M (last 2 years) flight spend runs in flights >=15 days, where worst-case day-1 lag affects <=4% of the campaign; flights 1–3 days (32–100% day-1 exposure) are only 4.3% of total spend; only 6.9% of flight spend lands on a flight day-1 overall.
- Victor's proposed score-filter generates scores when a campaign group is live OR was updated <24h ago (rule 2 keys off campaign_group.update_time), plus a rule 3 for new advertisers <7d and a next-day reconciliation check bounding worst-case latency to 1 day; a returning advertiser must flip a campaign group off->on to resume bidding, triggering rule 2 the same day.

**Reuse when:**
- sizing dormant/returning-advertiser risk for a scoring or freshness filter
- evaluating day-1 targeting-lag exposure by flight length or spend tier
- questions about MNTN flight-length spend distribution or single-day spend blitzes
- advertiser max-gap / retention percentiles by spend tier

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

## Tiered retention — max-gap by spend tier

| Spend tier  | n     | p50 | p75 | p90 | p95 | p99 | max |
|-------------|------:|----:|----:|----:|----:|----:|----:|
| $10M+       |     2 |   0 |  12 |  12 |  12 |  12 |  12 |
| $1M–$10M    |    73 |   2 |  26 |  70 | 120 | 174 | 174 |
| $100k–$1M   |   664 |   8 |  57 | 137 | 192 | 316 | 518 |
| $10k–$100k  | 1,663 |   5 |  63 | 198 | 273 | 401 | 631 |
| <$10k       | 3,064 |   0 |   7 |  46 | 122 | 324 | 652 |

**Reads:**
- **$1M+ spenders never disappear long.** 75 advertisers, every single one has max gap ≤174 days. **180d retention covers 100% of $1M+.**
- **$100k–$1M: 200d covers p95, 320d covers p99.** 664 advertisers.
- **$10k–$100k actually has the worst tail** (p95=273d, p99=401d) — mid-tier advertisers cycle in/out more than top-tier.
- **<$10k: 3k advertisers; tail of 365d+ returners exists but spend at stake is trivial ($62k over 2 years).**

## Short-flight reality check (Ryan's question)

### Flight-grain view (raw count of short flights)

Last 2 years, flights with `end_time - start_time ≤ 3 days`:

| Duration | n flights | with spend | >$1k | >$10k | Total spend |
|---------:|----------:|-----------:|-----:|------:|------------:|
| 0d (same day) | 9,910 | 9,143 | 905 | 46 | $4.6M |
| 1d            | 8,796 | 7,572 | 1,731 | 163 | $8.9M |
| 2d            | 5,326 | 4,415 | 1,218 | 123 | $7.2M |
| 3d            | 4,710 | 3,929 | 1,492 | 151 | $7.3M |

### Are there massive one-day blitzes? (advertiser-day grain)

Re-framed at advertiser-day level — flight-grain double-counts when an advertiser stacks multiple flights on the same day.

| Cohort | n | p50 | p90 | p99 | max spend | total |
|---|---:|---:|---:|---:|---:|---:|
| All active advertiser-days | 915,965 | $119 | $1,031 | $5,545 | **$152,861** | $453M |
| True 1-day blitz (no spend ±7d) | **74** | $13 | $345 | $26,463 | **$26,463** | $56k |
| First-ever spend day | 5,466 | $56 | $469 | $2,704 | $42,518 | $1.3M |

**Reads:**
- Max single-day spend for *any* advertiser in 2 years was $153k — and that's an established advertiser, not a blitz.
- True 1-day blitzes: **74 in 2 years, $56k combined.** Noise.
- First-ever spend day cohort (the real "no scores cached" risk): 5,466 events over 2 years (~7.5/day), max $42k, p99 $2.7k — and Victor's rule 3 ("new advertisers <7d") covers them.

**No Super Bowl one-day campaigns exist in MNTN.** The untargeted-day-1 risk is empirically tiny.

## The campaign-length story (the real frame)

The risk isn't lost dollars on day 1 — it's **degraded targeting performance for a fraction of the flight equal to 1/flight_length**. So a 1-day score lag eats:

- **100% of a 1-day flight**
- **~3% of a 30-day flight**
- **~1% of a 90-day flight**

Where MNTN's flight spend actually lives (last 2 years, $477M total):

| Flight length | n flights | Bucket spend | % of all spend | Day-1 spend | Day-1 % of bucket |
|--------------|----------:|-------------:|---------------:|------------:|------------------:|
| 1d           |     9,143 |        $4.6M |          0.97% |       $4.6M |          **100%** |
| 2d           |     7,572 |        $8.9M |          1.87% |       $4.6M |             52%   |
| 3d           |     4,415 |        $7.2M |          1.50% |       $2.3M |             32%   |
| 4–7d         |    13,279 |       $32.7M |          6.85% |       $5.6M |             17%   |
| 8–14d        |    16,207 |       $60.3M |         12.65% |       $5.6M |              9%   |
| 15–30d       |    27,304 |      $116.0M |         24.32% |       $4.6M |              4%   |
| 31–60d       |    21,938 |      $156.2M |         32.74% |       $4.4M |              3%   |
| 61–90d       |     2,830 |       $30.8M |          6.47% |       $0.5M |              1.5% |
| 91–180d      |     2,681 |       $41.1M |          8.62% |       $0.4M |              0.9% |
| 181d+        |       850 |       $20.0M |          4.19% |       $0.1M |              0.5% |
| **Total**    |   106,219 |     **$477.9M** |        100% |   **$32.8M** |          **6.9%** |

### Story

1. **79% of MNTN's spend runs in flights ≥15 days.** For those, even worst-case day-1 lag affects ≤4% of each campaign. Structurally bounded.
2. **The flights where day-1 lag matters most (1–3 days, 32–100% exposure) are only 4.3% of total spend.**
3. **Across the whole population, only 6.9% of flight spend lands on a flight day-1** — and that's *before* discounting for advertisers who already had cached scores from prior flights.
4. The actual at-risk population is much smaller: only flights for advertisers who *weren't* already scored. Sized earlier: ~74 isolated 1-day blitzes total, ~7.5 first-ever-day events per day. The 100% column is dominated by tiny tests.

**The campaign-length distribution is structurally protective.** Long campaigns absorb day-1 lag as a small fraction of total performance; short campaigns expose more % but carry tiny absolute dollars.
