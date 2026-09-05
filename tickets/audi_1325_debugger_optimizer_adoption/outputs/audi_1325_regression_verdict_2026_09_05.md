# Did the optimizer's fixes make DAGs worse? Verdict — 2026-09-05

**Answer: mixed.**

Five of the six are measurement artifacts and one is real. The published "6 of 15 regressed" is 100% a defect of the Mode SQL: it reproduces exactly (site_network_hourly before 23,062.2 / after 88,476.1) and vanishes under correct aggregation. But the correction does not clear the fleet: ipdsc_ds_49 (PR #1272, maxPartitionBytes 128 -> 64 MiB) shows a real post-fix cost increase on an independent Dataproc meter, 10.1-15.7 DCU-h/day across 16 pre-fix days -> 18.7 on 09-04 -> 24.3 on 09-05, above every pre-fix day and still rising. That is a different regression from the one the dashboard flagged; the dashboard's -0.3 for that DAG was itself a clone artifact.

## Per DAG

### ipdsc_ds_49 — REAL

**Numbers.** Published: 2.9 -> 3.2, which was itself a clone artifact (the entire after-window was one state='applied' row copying its own 09-02 value 3.2). But the genuine post-fix data is worse than the dashboard claimed. Dataproc DCU-hours, one run/day, pulled fresh: 08-20 10.2, 08-21 13.8, 08-22 10.1, 08-23 10.5, 08-24 15.7, 08-25 11.0, 08-26 11.7, 08-27 11.3, 08-28 11.2, 08-29 11.3, 08-30 11.3, 08-31 11.5, 09-01 12.0, 09-02 13.3, 09-03 13.5 -> 09-04 18.7 -> 09-05 24.3. Both post-fix days exceed every one of the 15 pre-fix days, and the trend is up not reverting. Per-run event logs: pre-fix exec_h mean 2.578, sd 0.258, max 3.161 (n=17); post 4.261 (09-04) and 5.350 (09-05), z = +6.5 and +10.7. Input-normalized exec_h/GiB held in [0.0557, 0.0628] across the whole pre-fix window despite input growing 36.8%, then 0.0770 and 0.0897 post-fix (+23%, +43% over the pre-fix max). Ledger agrees: 2.7 / 2.7 / 3.2 -> 5.3.

**Reasoning.** This is the one case where a regression survives every correction and is corroborated by a meter that never touches the ledger, on two consecutive days, with the confounder controlled (input grew 5.8% then 7.6% while cost grew 39% then 80%). The mechanism is deterministic and visible in the logs: PR #1272 halved spark.sql.files.maxPartitionBytes from the 128 MiB default to 67108864, and read-stage tasks per GiB duly doubled (12.58 -> 26.74 and 26.53), with executors scaling 48 -> 107 -> 116 under dynamicAllocation(4,180). Total task run-time per input GiB also rose (+13.2%, +33.6%), so this is real compute, not merely idle held executors. The fix bought a 94% spill reduction and a 28% wall-clock cut with roughly 2x the executor-hours. Caveat: the DAG's cost is driven by site_network_hourly over a 7-day lookback, and that input stepped +43% on 09-01; normalizing by the correct 7-day input still leaves 0.3013 and 0.3643 DCU-h/GiB against a pre-fix band of 0.2138-0.2428 across eleven days. Causation by PR #1272 is plausible and mechanistically supported but not proven by a revert.

### guid_conv_log_pivot_ip_vertical_id — UNMEASURABLE

**Numbers.** Never flagged by the dashboard (published 7.25 -> 6.8, an improvement). Corrected on live data it is now the second-largest riser: before 6.4 / 9.0 / 6.8 / 6.8, after 10.3 on 09-04. n=1 after-day.

**Reasoning.** Included because it inverts under correction, which shows the published set of six was not even the right set. One post-fix observation on a series whose own before-window spans 6.4-9.0. Not assertable in either direction; worth re-checking with ipdsc_ds_49.

### fangorn_score_monitor — UNMEASURABLE

**Numbers.** The only DAG with more than one genuine post-fix sweep-day (applied 2026-08-27). Before [687.7] (n=1), after [668.3, 592.3, 603.1] (n=3), mean 621.2, -9.7%. Welch is undefined at n_before=1 (scipy returns nan); exact permutation over C(4,3)=4 arrangements cannot reach any conventional threshold.

**Reasoning.** An improvement, but with one before-observation it is statistically indistinguishable from noise. Listed because it is the only case in the entire ledger with a real multi-day after-window, and it does not regress.

### site_network_hourly — ARTIFACT

**Numbers.** Published: before 23,062.2 / after 88,476.1, delta -65,413.9. Reproduced exactly. The 88,476.1 is the SUM of 22 finding rows on the applied date, 11 of them state='applied' clones (38,112.6 h); the true DAG-day value is 4,578.5, a 19.3x row-fanout inflation. The 23,062.2 is 92,248.6/4 days of the same defect, and 09-02 alone is 99.3% of that numerator. Corrected ledger on live data: before 970.9 (08-26 73.7, 08-27 101.7, 08-29 55.0, 09-02 3,653.1) vs after 6,581.7 on the single post-fix day 09-04. Independent Dataproc meter, 24 runs/day every day with no schedule change: 7,615 / 6,669 / 9,176 / 18,756 / 16,835 / 20,605 / 9,301 / 11,506 / 25,168 / 21,717 DCU-h on 08-25..09-03, then 23,126 on 09-04 and a 16-run partial 14,922 on 09-05. Normalized 1,929.7 DCU-h per GB of output on 09-04 against a pre-fix band of 1,091.6-2,659.0 and a pre-fix mean of 1,914.5, i.e. +0.8%.

**Reasoning.** The headline magnitude is entirely aggregation defect. The residual corrected rise is coverage, not cost: ledger exec_h is a SUM over however many of the DAG's 24 daily runs the sweep happened to download, and the series rose 66x (55.0 on 08-29 to 3,653.1 on 09-02) BEFORE any fix shipped, on a day when the downloader was repaired (601483d, 2026-09-02T19:27Z, replacing gsutil which was landing ~6 of 200 objects per sweep for six consecutive days). Fleet exec_h stepped 5.3k -> 18.7k h/day at the same boundary with DAGs swept going 34 -> 88. Against the independent meter the DAG is flat once normalized, and its raw rise tracks output volume that grew ~45-60% starting 2026-09-01, two days before the fix.

### advertiser_score_distribution_monitor — ARTIFACT

**Numbers.** Published: 67.1 -> 98.4, delta -31.3, which is exactly 2x the deduplicated gap of +15.63 (row-fanout doubling). Corrected series: 08-26 23.7, 08-27 23.7, 09-02 53.3, 09-03 49.2, 09-04 42.9. Matched event-log pair with the config change visible in each run's own environment block: pre app-20260903080445955-0320 (shuffle.partitions 128) 4.86 min / 26.44 exec_h / 15,118 tasks; post app-20260904073215813-0612 (916) 5.12 min / 22.84 exec_h / 21,757 tasks, exec_h -13.6%, zero spill both sides.

**Reasoning.** The 23.7 -> 53.3 level shift lands between 08-27 and 09-02, before the fix; the ledger's own prev_exec_h on the 09-02 row records 23.7. The 'regression' exists only because the before-mean averages two pre-coverage-shift days against a post-shift day. Comparing like coverage to like coverage (09-02 full sweep vs 09-04 full sweep) gives 53.3 -> 42.9 = 0.80x, an improvement, and the matched per-run event logs agree. Placebo p = 1.000: the real cut point produces the SMALLEST of the three possible shifts in its own series.

### ipdsc_42_monitor — ARTIFACT

**Numbers.** Published: 8.6 -> 9.0. The entire after-window is six state='applied' clone rows, all carrying exec_h 1.5, which mark_applied (ledger.py:383, exec_h=last.get('exec_h')) copied verbatim from the 09-02 pre-fix value. Zero genuine 09-03 rows. Corrected: before 1.475 (1.7 / 1.7 / 1.0 / 1.5) vs after 0.2 on 09-04. Post-fix run app-20260904033841985-0840: 0.13 exec_h, 1.32 min, 1,082 tasks, against 13.7-24.1 min wall on all 11 preceding days. Dataproc compute per unit volume 0.478 post-fix vs a 2.063-8.728 pre-fix band.

**Reasoning.** After-rate 9.0 is literally the before-day value re-stamped six times. Both the ledger and the independent meter show a large improvement post-fix, roughly 7x on exec_h and 4.3x on normalized compute. Nothing here regressed.

### ipdsc_ds_2 — ARTIFACT

**Numbers.** Published: 92.4 -> 94.4. After value 94.4 = 2 genuine rows at 33.1 plus one stale applied clone at 28.2. Corrected: before 30.8 (32.1 / 32.1 / 28.2, where 08-26 and 08-27 are the SAME run app-20260826023738997-0519 stamped twice) vs after 30.4 on 09-04, +/-1%. Per-run event logs: pre-fix exec_h 33.62 / 32.04 / 32.11 / 28.24 (mean 31.50), post 33.08 / 30.42 (mean 31.75), +0.8%. Reduce stage 3 spilled ~221 GiB to disk on every pre-fix run and exactly 0.00 GiB on both post-fix runs; total disk spill 282.97 -> 58.36 GiB (-79%), per-task reduce input 82.9 MiB, inside PR #1273's predicted 80-300 MiB.

**Reasoning.** Cost is flat within run-to-run spread on two independent measures, and the fix hit its stated target exactly. The 58 GiB of residual spill is the MAP stage, whose partitioning is input-split driven and is not governed by spark.sql.shuffle.partitions; it is an untargeted separate finding, not a failure.

### guid_log_pivot_ip_vertical_id — ARTIFACT

**Numbers.** Published: 7.6 -> 8.3. The entire after-window is one state='applied' clone carrying exec_h 8.3, byte-identical to its own 09-02 pre-fix value. Zero genuine 09-03 rows. Raw event-log series (all seven runs, one run/day at ~01:28 UTC): 08-21 10.12, 08-25 9.10, 08-26 7.52, 08-27 7.07, 09-02 8.37, 09-03 8.26 (all ~19 GiB disk spill, stage 33 = 800 tasks), then post-fix 09-04 01:28 = 7.69 exec_h, 4.2 min, 30,612 tasks, 0.00 GiB spill, stage 33 = 4,000 tasks. Corrected ledger shows 09-04 at 10.0 vs a 7.1/7.5/8.3 before-window.

**Reasoning.** The published after value is the before value relabelled. The first genuinely post-fix run is BELOW the pre-fix median (8.32) and mean (8.41) and is the fastest of all seven runs, with disk spill eliminated and the PR's own success test (3,100-4,000 pivot tasks, zero spill) met exactly. The ledger's 09-04 row at 10.0 is n=1 over a wider sweep window and is contradicted by the per-run log; it is not evidence of a regression but it is not resolved either.

## The speculation canary (PR #1271, site_network_hourly)

NOT MET, and the mechanism it warns about is quantitatively excluded. Ryan Kleck's approve on PR #1271 (rkleck-mntn, 2026-09-03T19:08:29Z) and the recorded kill criterion are "kill it if executor-hours rise while wall-clock stays flat, because with skew speculation just adds executors chasing the same long tail." Executor-hours did rise in the ledger, but wall-clock did not stay flat under either reading: pooled across the 17 archived runs straddling the 2026-09-03T20:20:37Z merge, median wall fell 26.5% (48.7 -> 35.8 min); hour-matched (n=3, the only clean comparison since pre runs are 04:51-19:51 UTC and post are 21:51-07:51) it ROSE 1.49x median, sign test p=0.75, permutation p=0.205. The two readings point opposite ways, so the criterion's precondition is not satisfied. Independently, speculation cannot be the cost channel: killed duplicate attempts consumed 11.27 h of 9,214.34 h of slot time across all 19 post-flip runs, 0.12%. The expensive post-flip runs are FetchFailed storms (7,295 / 13,362 / 10,080 FetchFailed), a pattern that also occurs pre-flip with speculation off (2026-09-03 12:51: 0 speculative attempts, 7,104 FetchFailed) and is already tracked separately as IMP-104. And the run with the HIGHEST speculation rate (2026-09-03 21:51, 3,141 speculative attempts, 3.77%) was the cheapest and fastest measured, 660 s / 22.2 exec-h against the last pre-fix run's 3,056 s / 60.3 h. Verdict: do not kill speculation on this evidence. It is not yet measurable on the criterion's own terms (n=3 hour-matched, one day per side), but nothing supports the failure mode Kleck named.

## Discriminating tests

Two tests, different DAGs, different dates.

1. ipdsc_ds_49, the only real regression. Revert or re-tune PR #1272: set spark.sql.files.maxPartitionBytes back to the 128 MiB default (or to 96 MiB) on ipdsc_ds_49 only, leave every other PR #1272/#1273 DAG untouched, and compare Dataproc approximateUsage.milliDcuSeconds per GiB of 7-day site_network_hourly input across three days each side. The pre-fix band is tight (0.2138-0.2428 DCU-h/GiB over eleven days) and the post-fix points are 0.3013 and 0.3643, so three clean days on each side separate them by more than 6 sigma. The DAG runs once daily at ~03:00 UTC, so this is RUNNABLE NOW and returns a verdict on 2026-09-09. If reverting restores the 0.21-0.24 band, PR #1272 caused it; if it does not, the cause is the 43% input step that landed 2026-09-01 and the config change is exonerated.

2. Everything else, including the speculation canary. Re-run the corrected savings() (one exec_h per DAG-day, state='applied' excluded, after-window strictly greater than applied_date) restricted to sweep-days whose backlog Source line reads "newest 200 of 200, 0 failed", and require the upstream gate of at least 3 such days on each side. Today there is exactly one (2026-09-04). The 09-05 and 09-06 sweeps give the third on 2026-09-07, so this becomes RUNNABLE 2026-09-07. Until then every non-ds_49 verdict rests on n=1. For site_network_hourly specifically the ledger cannot settle it at any n, because exec_h is a sum over however many of its 24 daily runs the sweep downloaded; the canary must be judged on Dataproc DCU-hours per GB of output at a fixed 24 runs/day (currently 1,929.7 post-fix against a 1,091.6-2,659.0 pre-fix band) and on hour-matched wall-clock, which needs three full days of paired hours, i.e. 2026-09-07 as well.

Also fix the dashboard before anyone reads it again: the published SQL SUMs exec_h across finding rows (a 19.3x inflation on site_network_hourly), includes state='applied' clones (which mark_applied stamps with the PREVIOUS sweep's exec_h, so the "after" is literally the "before" relabelled for three of the six), and uses an after-window inclusive of the applied date. All three are one-line fixes and all three are load-bearing.

## Corrections to the prior audit chain

The prior audit chain overstated in five places, and I am correcting all of them against the live 1,875-row ledger, not the 1,692-row snapshot it worked from.

1. "0 of 15 DAGs show a corrected regression" and "the after-window is empty for 14 of 15" were true on 2026-09-04 and are false now. The 2026-09-05 sweep appended 183 rows dated 2026-09-04, so every fixed DAG except vertical_size_monitor now has exactly one post-fix day. Under the corrected aggregation 5 of 15 show after > before: site_network_hourly (970.9 -> 6,581.7), guid_conv_log_pivot_ip_vertical_id (7.25 -> 10.3), guid_log_pivot_ip_vertical_id (7.6 -> 10.0), ipdsc_ds_49 (2.9 -> 5.3), advertiser_score_distribution_monitor (33.6 -> 42.9). Four of those are n=1 and three of them evaporate on a like-coverage comparison, but "not one DAG shows a corrected regression" is no longer a defensible sentence.

2. "On an independent meter their real compute is flat or improving on the one post-fix day available" was false for ipdsc_ds_49, and I confirmed that myself: 18.7 and 24.3 DCU-h on 09-04 and 09-05 against a 10.1-15.7 band over 16 pre-fix days. Presenting all six as artifacts would have shipped a real cost regression as a measurement error.

3. "At least 96-98% of site_network_hourly's apparent increase is coverage" overstated the floor. The honest band from the same two date-pairs is 92-98% (98.5% for 08-29 -> 09-03, 92.4% for 08-26 -> 09-02); 96% is only reachable by swapping the volume ratio in for the compute ratio on one of the pairs.

4. The framing premise that "the same exec_h is stamped on every finding row for that DAG that day" is false in the data. 35 of roughly 470 DAG-day cells carry two or three distinct values (site_network_hourly 2026-08-26 holds 9.8, 35.3 and 73.7; 2026-09-02 holds 3,238.8 and 3,653.1), because append() replaces only rows whose (date, dag_id, key) the current sweep re-emits, so earlier narrower sweep generations survive. Every "one value per DAG-day" correction in this audit, mine included, is a MAX pick, and it has to say so. Under MIN, site_network_hourly's 09-02 -> 09-03 ratio is 1.41x rather than 1.25x.

5. Several lanes attributed the six-sweep coverage collapse and the 08-26 fleet expansion to the same cause. They are different: downloads fell to 6 of 200 on 08-27 through 09-01 (repaired by 601483d on 2026-09-02), while the PHS component jumped 22 -> 150 on 08-26 on a day with a full 200-of-200 download. The download collapse DEFLATES exec_h and would mask a regression; the PHS expansion inflates it. Reciting "jobs scanned 214 -> 344" as evidence of acquisition collapse has the mechanism backwards.

On the original audit under test: it was not wrong to flag the numbers, it was wrong to report them as a production outcome. All three of its SQL defects reproduce to the decimal, and correcting them removes every one of the six it named.

## Not verified

- Causation for ipdsc_ds_49. The cost rise is real on two independent meters and the mechanism (read tasks per GiB doubling from 12.58 to 26.74 after maxPartitionBytes halved) is deterministic and visible in the logs, but no revert or A/B has been run, and the DAG's 7-day site_network_hourly input stepped +43% on 2026-09-01. Attribution to PR #1272 rests on mechanism, not on a controlled comparison.
- Whether the 2026-09-02 fleet-wide step (5.3k -> 18.7k exec-h/day, 34 -> 88 DAGs swept) is fully explained by commit 601483d. Three fetch.py commits landed in the same inter-sweep gap (59c81cb 09:58, 6a62038 11:29, 601483d 12:27 PDT) and ledger rows carry no per-row timestamp, so 601483d is identified only as the sole non-gsutil path among the three. The separate PHS 22 -> 150 expansion on 08-26 is a second, unquantified contributor.
- The wall-clock half of the speculation canary. Hour-matched comparison is n=3 (permutation p=0.205, sign test p=0.75) over one day per side, and the archive holds only 10-16 of site_network_hourly's 24 daily runs, by an unverified selection rule, with the post window skewed to overnight hours. Neither the pooled -26.5% nor the matched +49% is trustworthy.
- BigQuery was not queried. .claude/scripts/bq_run.sh fails with 'Reauthentication failed. cannot prompt during non-interactive execution', as do gcloud storage and gsutil. Every figure above comes from the GCS JSON API and the Dataproc REST API using application-default credentials, which did refresh; the ledger I analysed is a live re-download (1,875 rows, md5 4c07e416ffd01f606ac7c715c4235904) matching the local copies byte for byte.
- Whether the corrected before-windows are usable at all. Six of the nine observable sweep-days (2026-08-27 through 09-01) acquired 6 of 200 event logs, and 08-26 carries '+ 0 PHS batch log(s)' against '+ 150 PHS' on 09-02 through 09-04, so even it is not a clean baseline. Only 09-02, 09-03 and 09-04 are known-full sweeps, and 09-03 is the applied date.
- vertical_size_monitor has zero post-fix rows in the live ledger and was not measurable at all.
- Dollar cost. exec_h counts executor-hours held under dynamic allocation, billed whether or not a task runs; dcu_h is null on most of these rows. Dataproc DCU-hours are the closest thing to billing here and were used as such, but no invoice was reconciled.

## Method

49 agents. One clean recompute (one exec_h per DAG-day via MAX, `state='applied'` clones excluded, after-window strictly after the applied date), then four independent confounder lanes each trying to explain the regression away (measurement coverage, input volume, statistics, mechanism), every material claim through a default-refute verifier, then adjudication. Independent meters: Dataproc REST `approximateUsage.milliDcuSeconds` and per-run Spark event logs, neither of which touches the ledger.

