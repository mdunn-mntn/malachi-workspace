# Household Score Threshold v4 (HHSTv4): Fangorn
IP addresses are scored for intent (to purchase) by the ML Squad (alyson@mountain.com, rkleck@mountain.com). There are scores for campaign, advertiser, and segment (conquest). A different process ensures that Bidder can look up these scores duing the bidding process. THis DAG calculates how high that score needs to be for a campaign in order for us to bid on an IP.

This is done in order to maximize the quality of IPs while minimizing underspend. If we manage to pace within target while only bidding on the highest quality IPs, MNTN Matched is doing its job!

Example 1:
- Bidder is deciding whether to bid on an IP for a campaign
- The "threshold" for the campaign is 5000
- The score for the IP is 4000.
- Bidder decides not to bid because the IP's score (4000) doesn't meet the threshold (5000).

Example 2:
- Bidder is deciding whether to bid on an IP for a campaign
- The "threshold" for the campaign is 3000.
- The score for the IP is 4000.
- Bidder decides to bid because the IP's score (4000) meets the threshold (3000).


## Bidder's Logic

Bidder uses [the following logic](https://github.com/SteelHouse/rtb-bidder-service/pull/485#issuecomment-3730474187) to determine if a bid should be rejected:
- if HHST is missing: pass
- else:
    - if segment_score exists and segment_score < 24h old:
        - if segment_score >= HHST: pass
        - else: fail with "invalidSegmentScore"
    - elif campaign_score exists:
        - if campaign_score >= HHST: pass
        - else: fail with "invalidCampaignScore"
    - elif advertiser_score exists:
        - if advertiser_score >= HHST: pass
        - else: fail with "invalidAdvertiserScore"
    - else: fail with "missingIntentScore"

**Note: For segment_score, campaign_score, advertiser_score, and HHST, a value of `null`, `-1`, or `0` is same as "missing".**

## HHST's Logic

In general, HHST:
1. Retrieves the last budget for yesterday, the total spend for all of yesterday, and calculates pacing_pct (spend / budget).
2. It then makes decisions based on pacing_pct:
    - If it's within target (between 90% and 95%), HHST uses the current score.
    - If it's above target (above 95%), HHST increases (currently by 5 "buckets"). The assumption here is, if we're pacing well, we want to see if we can keep it up with an even higher threshold.
    - If it's below target (below 90%), HHST attempts to "intelligently" change the threshold based on population estimates. This is where all the complexity lives.

We estimate populations by counting IPs from _relevant_ Bid Events for all of yesterday. The Bid Events conveniently have the intent score of the IP, regardless of whether it did or didn't meet thresholds in previous situations.

Using that data, we construct "buckets":
```
+---------+-----+-----+------------+----------+------------------+
|bucket_id|lower|upper|intent_group|population|population_rolling|
+---------+-----+-----+------------+----------+------------------+
|        0|    0|    0| 4 Max Reach|      1000|              3000|
|        1|    1|  100| 4 Max Reach|        10|              2000|
|        2|  101|  200| 4 Max Reach|        10|              1990|
|        3|  201|  300| 4 Max Reach|        10|              1980|
|        4|  301|  400| 4 Max Reach|        10|              1970|
|        5|  401|  500| 4 Max Reach|        10|              1960|
|        6|  501|  600| 4 Max Reach|        10|              1950|
|        7|  601|  700| 4 Max Reach|        10|              1940|
|        8|  701|  800| 4 Max Reach|        10|              1930|
|        9|  801|  900| 4 Max Reach|        10|              1920|
|       10|  901| 1000| 4 Max Reach|        10|              1910|
|       11| 1001| 1100| 4 Max Reach|        10|              1900|
|       12| 1101| 1200| 4 Max Reach|        10|              1890|
|       13| 1201| 1300| 4 Max Reach|        10|              1880|
|       14| 1301| 1400| 4 Max Reach|        10|              1870|
|       15| 1401| 1500| 4 Max Reach|        10|              1860|
|       16| 1501| 1600| 4 Max Reach|        10|              1850|
|       17| 1601| 1700| 4 Max Reach|        10|              1840|
|       18| 1701| 1800| 4 Max Reach|        10|              1830|
|       19| 1801| 1900| 4 Max Reach|        10|              1820|
|       20| 1901| 2000| 4 Max Reach|        10|              1810|
|       21| 2001| 2100| 4 Max Reach|        10|              1800|
|       22| 2101| 2200| 4 Max Reach|        10|              1790|
|       23| 2201| 2300| 4 Max Reach|        10|              1780|
|       24| 2301| 2400| 4 Max Reach|        10|              1770|
|       25| 2401| 2500| 4 Max Reach|        10|              1760|
|       26| 2501| 2600| 4 Max Reach|        10|              1750|
|       27| 2601| 2700| 4 Max Reach|        10|              1740|
|       28| 2701| 2800| 4 Max Reach|        10|              1730|
|       29| 2801| 2900| 4 Max Reach|        10|              1720|
|       30| 2901| 3000| 4 Max Reach|        10|              1710|
|       31| 3001| 3100| 4 Max Reach|        10|              1700|
|       32| 3101| 3200| 4 Max Reach|        10|              1690|
|       33| 3201| 3300| 4 Max Reach|        10|              1680|
|       34| 3301| 3332| 4 Max Reach|        10|              1670|
|       35| 3333| 3400|       3 Mid|         0|              1660|
|       36| 3401| 3500|       3 Mid|         0|              1660|
|       37| 3501| 3600|       3 Mid|         0|              1660|
|       38| 3601| 3700|       3 Mid|         0|              1660|
|       39| 3701| 3800|       3 Mid|         0|              1660|
|       40| 3801| 3900|       3 Mid|         0|              1660|
|       41| 3901| 4000|       3 Mid|         0|              1660|
|       42| 4001| 4100|       3 Mid|         0|              1660|
|       43| 4101| 4200|       3 Mid|         0|              1660|
|       44| 4201| 4300|       3 Mid|        10|              1660|
|       45| 4301| 4400|       3 Mid|         0|              1650|
|       46| 4401| 4500|       3 Mid|         0|              1650|
|       47| 4501| 4600|       3 Mid|         0|              1650|
|       48| 4601| 4700|       3 Mid|         0|              1650|
|       49| 4701| 4800|       3 Mid|         0|              1650|
|       50| 4801| 4900|       3 Mid|        10|              1650|
|       51| 4901| 5000|       3 Mid|         0|              1640|
|       52| 5001| 5100|       3 Mid|         0|              1640|
|       53| 5101| 5200|       3 Mid|         0|              1640|
|       54| 5201| 5300|       3 Mid|         0|              1640|
|       55| 5301| 5400|       3 Mid|         0|              1640|
|       56| 5401| 5500|       3 Mid|         0|              1640|
|       57| 5501| 5600|       3 Mid|         0|              1640|
|       58| 5601| 5700|       3 Mid|         0|              1640|
|       59| 5701| 5800|       3 Mid|         0|              1640|
|       60| 5801| 5900|       3 Mid|         0|              1640|
|       61| 5901| 6000|       3 Mid|         0|              1640|
|       62| 6001| 6100|       3 Mid|         0|              1640|
|       63| 6101| 6200|       3 Mid|         0|              1640|
|       64| 6201| 6300|       3 Mid|         0|              1640|
|       65| 6301| 6400|       3 Mid|         0|              1640|
|       66| 6401| 6500|       3 Mid|         0|              1640|
|       67| 6501| 6600|       3 Mid|         0|              1640|
|       68| 6601| 6665|       3 Mid|        40|              1600|
|       69| 6666| 6700|      2 Peak|         0|              1500|
|       70| 6701| 6800|      2 Peak|         0|              1500|
|       71| 6801| 6900|      2 Peak|         0|              1500|
|       72| 6901| 7000|      2 Peak|         0|              1500|
|       73| 7001| 7100|      2 Peak|         0|              1500|
|       74| 7101| 7200|      2 Peak|         0|              1500|
|       75| 7201| 7300|      2 Peak|         0|              1500|
|       76| 7301| 7400|      2 Peak|         0|              1500|
|       77| 7401| 7500|      2 Peak|         0|              1500|
|       78| 7501| 7600|      2 Peak|       500|              1500|
|       79| 7601| 7700|      2 Peak|         0|              1000|
|       80| 7701| 7800|      2 Peak|         0|              1000|
|       81| 7801| 7900|      2 Peak|         0|              1000|
|       82| 7901| 8000|      2 Peak|         0|              1000|
|       83| 8001| 8100|      1 High|         0|              1000|
|       84| 8101| 8200|      1 High|         0|              1000|
|       85| 8201| 8300|      1 High|         0|              1000|
|       86| 8301| 8400|      1 High|         0|              1000|
|       87| 8401| 8500|      1 High|         0|              1000|
|       88| 8501| 8600|      1 High|         0|              1000|
|       89| 8601| 8700|      1 High|         0|              1000|
|       90| 8701| 8800|      1 High|         0|              1000|
|       91| 8801| 8900|      1 High|         0|              1000|
|       92| 8901| 9000|      1 High|         0|              1000|
|       93| 9001| 9100|      1 High|         0|              1000|
|       94| 9101| 9200|      1 High|         0|              1000|
|       95| 9201| 9300|      1 High|         0|              1000|
|       96| 9301| 9400|      1 High|         0|              1000|
|       97| 9401| 9500|      1 High|         0|              1000|
|       98| 9501|10000|      1 High|      1000|              1000|
+---------+-----+-----+------------+----------+------------------+
```

Using the pacing_pct and the population buckets above, we do the following math:
- pacing_pct = x
- pacing_target = 0.9
- old_population = y
- new_population = pacing_target / pacing_pct * old_population

We look for the bucket with population_rolling >= new_population, and choose the one with the highest scores.

For example (using the buckets in the table above):
- pacing_pct = 0.8
- pacing_target = 0.9
- old_population = 1730
- new_population = 0.9 / 0.8 * 1730 = 1946

In this case, we would move HHST from wherever it was to 501, since setting that threshold would make 1950 IPs availble to this campaign (satisfies the new_population requirement of 1946).

## Exceptions

There are a ton of extra rules we use to catch edge. For example:
- If a campaign has a preset, we don't change the score.
- If the first HHST score is less than 24 hours old, we don't change the score.
- If the budget is a penny or less, we don't change the score.
- If the campaign is pacing above target, but the bucket to which we want to bump has a rolling population that's less than 60% of the current population, we don't change the score.
- If the flight started more than 24 hours ago and the campaign has spent nothing, we set the score to peak performance (6,666) so it has an audience to bid into (PACE-6966). This one only ever widens: a campaign already at or below peak performance is left to the pacing rules, since setting peak performance there would be walking backwards.

v3 used to have special rules when bumping around the top of mid intent (to avoid blowing through peak performance), but that logic is no longer applicable in v4.

## DAG Design
The work is primarily done using Spark Pipelines that write Parquet files to GCS. The parquet files are also mapped as External Tables in BigQuery for easy access. The work is broken up into separate tables to make debugging and error handling easier. The tables are:
- campaign
- campaign_budget
- campaign_impression_cap
- campaign_cost_and_impression
- campaign_bucket_population
- campaign_threshold

The `campaign` table is the first table run, and is used as a filtering mechanism for the rest of the ETL. `campaign_threshold` is the last table to run, and uses all the other ones to calculate the final scores.

`campaign_bucket_population` does not scan the bid logs itself: a DAG-level `population_histogram` BigQuery job scans 24 h of both bid logs **once for v3 and v4 together** (its campaign filter is the union of both DAGs' `campaign_bucket` tables, so it waits for v3's `campaign` task via the `wait_for_v3_campaign` sensor) and writes a shared score histogram (`camperbid_<env>__hhst_shared__population_histogram`). Each DAG's bucketing step then folds that histogram into its own buckets — v3 waits for the histogram via its `wait_for_v4_histogram` sensor. This halves the nightly bid-log scan and removes the two queries' reservation collision (see DEV-8606 / the Aug 2026 `resourcesExceeded` incident).

Once all the parquet tables are done, a few things happen:
1. A sync process copies all the parquet files into CoreDB as tables in the `camperbid` schema.
1. Tests run. Some are blocking (will stop production from updating), others are nonblocking.
1. Once blocking tests pass, the data is synced into two production tables:
    - `performance.optimized_intent_thresholds`
    - `performance.intent_threshold_buckets`

## MNTN ID (PACE-6846)

Advertisers cut over to MNTN ID (household identity graph) are flagged by `public.advertisers.uses_mntn_id` in CoreDB (added by DPLAT-1335). For campaigns of flagged advertisers, the population query counts `DISTINCT household_id_value` from MNTN bid events (the household UUID the bidder actually gated on) instead of `DISTINCT ip`; all other campaigns keep IP counting. Notes:

- The `campaign` step probes `information_schema.columns` for the flag column, so this DAG works before AND after DPLAT-1335 lands — no coordinated deploy. Until the column exists (or while it is FALSE everywhere), behavior is identical to IP counting; each run logs which state it is in. The probe is transitional: from `USES_MNTN_ID_COLUMN_REQUIRED_FROM` (2026-09-01) the column is referenced unconditionally, so probing stops and a missing/renamed column fails the run loudly instead of silently falling back to IP counting.
- Flagged campaigns are reachable in the bidder ONLY via the mntn-id lookup (no IP fallback), so their events always carry a household UUID. Beeswax events carry no household id and drop out of flagged campaigns' counts by design (rollout advertisers are MNTN-bidder-only).
- The population key additionally requires `household_id_source = 'mntn_id'`: ip-sourced events contribute nothing to a flagged campaign (rather than counting raw device IPs), so a flag set before the bidder actually cuts the advertiser over shows up as zero population — caught by the nonblocking test — instead of a silent miscount.
- The `mntn_id_observability` task group probes the latest hour of bid events: `mntn_id_ingestion_active` shows *skipped* until `household_id_source = 'mntn_id'` events exist, then flips green — the grid row marks the exact date ingestion started.
- A nonblocking test alerts on flagged campaigns whose entire population is zero: with zero population the threshold logic can only hold or raise the threshold (never lower it), which shows up as underdelivery.
- Step up/down tuning for household-scale populations (households are fewer than IPs) is separate, follow-on work: PACE-6840/PACE-6841.

Scope note: this change covers v4 only (per PACE-6846). Production thresholds are stitched per-advertiser by the sync SQL — advertisers NOT in `tpa.fangorn_advertiser_inclusion` get thresholds from `intent_score_threshold_v3`, which still counts IPs. If any `uses_mntn_id` advertiser is not fangorn-included at cutover, v3 needs the same (mechanical) change.
