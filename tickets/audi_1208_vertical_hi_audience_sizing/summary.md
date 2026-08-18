---
doc_type: ticket
title: "AUDI-1208: Vertical and MM HI audience sizing (mean + quartiles)"
status: in_progress
date: 2026-08-18
summary: "Mean/quartile sizes for DS13 verticals and the HI subset of MM audiences"
result: "Verticals mean 9.5M IPs / median 6.6M / Q1-Q3 4.0-12.0M (n=148); prospecting-audience HI pool mean 4.8M / median 3.6M / Q1-Q3 1.6-6.0M (n=2,063); exclusions are bid-time, so both cohorts are pre-exclusion"
question: "What are the mean and quartile audience sizes of MNTN verticals, and of the HI subset of MM prospecting audiences, split by audiences with no exclusions vs all MM audiences?"
framing_state: locked
---

# AUDI-1208: Vertical and MM HI audience sizing (mean + quartiles)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1208
**Status:** in_progress
**Date Started:** 2026-08-18
**Assignee:** Malachi

---
## 0. Framing  (locked 2026-08-18)
- **Question (the unknown):** What are the mean and quartile audience sizes of (a) MNTN verticals and (b) the HI subset of MM prospecting audiences, with (b) split into audiences carrying no exclusions vs all MM audiences?
- **Goal (why / the decision):** Paulo (VP Eng) asked in #targeting-squad on 2026-08-18 and flagged it asap. He is sizing the MM addressable universe. Ties to the north star via targeting mastery: "explain the system with data" is the named primary role, and audience supply is the substrate every prospecting-performance argument rests on.
- **Objective (done-when):** A branded `.xlsx` in `My Drive/Tickets/AUDI-1208/` reporting n, mean, median, Q1, Q3, min, max for: DS13 verticals, DS13 buckets, HI-subset size of MM audiences with no exclusions, and HI-subset size of all MM audiences. Each cut states its source table, snapshot date, and counting unit.
- **Approach (how):** Vertical sizes = distinct IPs per DS13 `data_source_category_id` from `dw-main-bronze.external.ipdsc__v1` (dt partition, `data_source_id=13`); 6-digit ids are verticals, 3-digit are buckets (confirmed: 148 verticals / 37 buckets / 1 root in `integrationprod.fpa_categories`). Cross-check against Ryan Kleck's existing `vertical_size_monitor` (reads `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations`). HI subsets = distinct IPs with `household_score` in 8001-10000 per campaign from `gs://household-scoring-prod/output/scoring/prospecting_intent/`; exclusion cohort from `prospecting_active_campaign_categories` where `include = false`.
- **What would change the answer:** (1) If the IPDSC DS13 counts diverge materially from the `ip_vertical_associations` monitor, the monitor is authoritative and the vertical numbers get restated from it. (2) If "size" means households rather than IPs, every number changes; IP is the counting unit in both the monitor and the scoring output, so IP is the stated unit. (3) If Paulo means the advertiser sales vertical (`fpa_advertiser_verticals`) rather than the DS13 targeting vertical, part (a) is a different question — note that both taxonomies share the same 37-parent / 148-child shape.

## 1. Introduction
Paulo Black (VP Engineering) asked in #targeting-squad on 2026-08-18, marked asap:

> The average size of all verticals. Quartiles would be nice.
> The average size of all HI subsets of MM audiences. I'm interested in: HI of audiences with no exclusions; HI of all MM audiences, including those with exclusions.

Two distinct sizing questions on the MNTN Matched (MM) addressable universe:

1. **Vertical sizes.** "Vertical" here is the DS13 (MNTN Vertical Categorization) targeting vertical — the 6-digit `data_source_category_id` subindustry level. Its parent is the 3-digit "bucket" (industry). `integrationprod.fpa_categories` carries exactly 1 root + 37 buckets + 148 verticals for `data_source_id = 13`, matching the known `fpa_advertiser_verticals` type-0 (37) / type-1 (148) split.
2. **HI subset sizes per MM audience.** HI (High Intent) is the top prospecting score band. Per `data_catalog.md` and Ryan Kleck's `audience_intent` DAG page, prospecting HI = in Vertical (DS13) **AND** in Keywords (DS19); the delivered band is `household_score` 8001-10000 (v1 pins at exactly 10000, v2/Fangorn spreads continuously across the band). The two cuts Paulo wants are a **cohort split on the audience config**, not a re-derivation of scores with exclusions applied — confirmed by Ryan Kleck in-thread ("he just wants audiences with exclusions vs no exclusions... so you just take that dataframe and add a column has_exclusion = TRUE/FALSE").

## 2. The Problem
There is no single place that answers either question:

- A daily monitor already emails vertical sizes (`MNTN GCS Vertical Sizes - PROD`, to `targeting-infrastructure@` and `machine-learning-squad@`), but it reports per-vertical IP counts and day-over-day deltas only. It computes **no distributional summary** — no mean, no quartiles. Ryan Kleck, in-thread: "quartile size, i'm not sure exactly how to do that???"
- Nothing reports HI-subset size per audience at all. The household-score distribution monitor deliberately never counts distinct IPs (its own module doc: "never `count_distinct` on IP"), so its row counts cannot answer "how big is this audience's HI pool."
- `dw-main-bronze.external_ddm.data_source_category_sizes` is 3P-only and carries no DS13 or DS19 sizes (recorded in `data_catalog.md` from AUDI-1070).

So both halves need a fresh query against the scoring-pipeline GCS output.

## 3. Plan of Action
1. Confirm the DS13 id shape (bucket vs vertical) in `integrationprod.fpa_categories`. **Done** — 1 root (id 0) + 37 buckets (101-137) + 148 verticals (101000-137001).
2. Size every DS13 category by distinct IPs from `external.ipdsc__v1` at one `dt`; compute mean/median/Q1/Q3/min/max separately for verticals and buckets. **Done.**
3. Read Ryan's `vertical_size_monitor.py` to align the definition, then reconcile our numbers against the monitor's own output for the same day.
4. Size the HI subset per MM prospecting audience from `prospecting_intent`, at campaign / campaign-group grain.
5. Tag each campaign `has_exclusion` from `prospecting_active_campaign_categories` (`include = false`), and `has_mm_incl` (an `include = true` row on DS13/DS19/DS46).
6. Compute the two cuts Paulo asked for and build the branded `.xlsx`.

## 4. Investigation & Findings

### 4.1 DS13 id shape (verified 2026-08-18)
`dw-main-bronze.integrationprod.fpa_categories` where `data_source_id = 13`:

| id length | n | id range | meaning |
|---|---|---|---|
| 1 | 1 | 0 | root |
| 3 | 37 | 101-137 | bucket (industry) |
| 6 | 148 | 101000-137001 | vertical (subindustry) |

This is the authoritative count of "all verticals" = **148**. It matches the `fpa_advertiser_verticals` type-0/type-1 split already recorded in memory, so the DS13 targeting taxonomy and the advertiser sales taxonomy are the same 37/148 tree.

### 4.2 Vertical sizes, distinct IPs, dt = 2026-08-16 (IPDSC DS13)
Query: `queries/audi_1208_ds13_vertical_sizes.sql`. Output: `outputs/audi_1208_ds13_sizes.csv`.

Grain note: within a single (`dt`, `data_source_id`) partition IPDSC is already one row per IP, and `data_source_category_ids` is the array of that IP's categories — so after `UNNEST`, `COUNT(*)` per category **is** the distinct-IP count. No `COUNT(DISTINCT ip)` needed.

| cut | n | mean | median | Q1 | Q3 | min | max |
|---|---|---|---|---|---|---|---|
| verticals (6-digit) | 148 | 9,295,275 | 6,480,734 | 3,879,756 | 11,694,301 | 912,084 | 75,215,006 |
| buckets (3-digit) | 37 | 25,499,975 | 20,475,267 | 11,128,210 | 32,947,240 | 2,514,907 | 87,290,934 |

The distribution is strongly right-skewed: mean 9.30M vs median 6.48M, and the largest vertical (75.2M, `124000 Current Affairs`) is 82x the smallest (0.91M). An "average vertical" is therefore a poor summary on its own — the quartiles Paulo asked for are the number that actually describes the portfolio.

Largest buckets: `104 B2B Software & Services` 87.3M, `101 Apparel` 81.2M, `124 News & Politics` 75.6M.

Cost: 17.28 GB billed, 618s slot / 57s wall over 212,011,373 IP rows in the `dt=2026-08-16` DS13 partition.

**Snapshot date caveat:** `dt = 2026-08-16` is the newest DS13 IPDSC partition available (2026-08-17 had not landed at query time).

### 4.3 Ryan Kleck's existing monitor (source alignment)
`SteelHouse/airflow-ti` `models/monitoring/vertical_size_monitor.py` — a Dataproc batch that emails `MNTN GCS Vertical Sizes - <ENV>` daily and writes parquet to `gs://household-scoring-prod/output/monitoring/vertical_size_monitor/dt=<date>`.

Definition it uses, which our query matches in shape:
- Source is **`gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<date>/*`**, NOT IPDSC. This is the upstream DS13 membership parquet; IPDSC DS13 is downstream of it. **Our numbers must be reconciled against it before sending.**
- Vertical rows = `LENGTH(CAST(CAST(data_source_category_id AS INT) AS STRING)) > 3`; bucket rows = length `<= 3`. Same 6-digit/3-digit rule.
- Metric = `COUNT(DISTINCT ip)` per `data_source_category_id`. Same counting unit.
- Vertical name lookup comes from Postgres `fpa.advertiser_verticals` over JDBC: `type = 1` for verticals, `type = 0` for buckets, both filtered `vertical_name != 'MNTN Matched Audience'`. So the monitor's vertical roster **excludes** an "MNTN Matched Audience" entry, and drops any DS13 category absent from that lookup (inner `JOIN`). Our 148 is the raw `fpa_categories` count and may include rows the monitor's table omits.
- It also reports `bucket_coverage` = a vertical's distinct IPs / distinct IPs on that bucket's own 3-digit rows.

Open item: run the reconciliation for the same `dt` and record the delta. If it is material, restate 4.2 from `ip_vertical_associations`.

### 4.4 HI subset per MM audience — sources located
- **Scores:** `gs://household-scoring-prod/output/scoring/prospecting_intent/year=<y>/month=<mm>/day=<dd>/*.parquet`. Columns: `ip`, `advertiser_id`, `campaign_group_id`, `campaign_id`, `household_score`. One day (2026-08-17) = **251,588,309,448 rows** across 20,000 files.
- **Exclusions + MM membership:** `gs://household-scoring-prod/output/data_aggregation/prospecting_active_campaign_categories/year=/month=/day=/*.parquet`. Columns: `advertiser_id`, `campaign_group_id`, `campaign_id`, `campaign_template_id`, `data_source_id`, `data_source_category_id`, `include` (BOOL), `is_active_campaign`. `include = false` **is** the exclusion clause — this is the `has_exclusion` column Ryan pointed at. Day 2026-08-17 rolls up to **4,907 active prospecting campaigns**.
- HI band = `household_score BETWEEN 8001 AND 10000` (`HIGH_MIN`/`HIGH_MAX` in `household_score_distribution_monitor.py`; PP = 6666-8000, Mid = 3333-6665, Max Reach = 1-3332).

### 4.5 GOTCHA — the registered BQ external table is stale past mid-July
`dw-main-bronze.external.household_scoring__prospecting_intent__v1` is defined with
`sourceUris = ['gs://household-scoring-prod/output/scoring/prospecting_intent/*.parquet']` and hive
partitioning `mode: CUSTOM`, `sourceUriPrefix = 'gs://.../prospecting_intent/'` (no `{key:TYPE}` schema
in the prefix, which CUSTOM mode expects).

Empirically it **cannot see August data**, even though the GCS partitions exist:
- `WHERE year='2026' AND month='08' AND day IN ('16','17')` → **0 rows** (46,537 slot-sec burned, no pruning).
- `SELECT year, month, day ... LIMIT 5` → returns `2026 / 07 / 13`.
- `gcloud storage ls` confirms `year=2026/month=08/day=13..17/` each hold 20,000 `.snappy.parquet` files.

So the partition-value format was never the problem (it is zero-padded STRING, `month='08'`); the
registered table's file discovery is. **Workaround used here: an inline
`--external_table_definition` pointed straight at one day's directory**, which reads correctly:

```
--external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"
```

Two consequences worth carrying forward:
1. Any past analysis that filtered this registered table by a recent date and got few/no rows was silently reading a stale file set. Treat prior results from it with suspicion.
2. Per global CLAUDE.md, an inline `--external_table_definition` over GCS **must** pass `--location=us-central1` explicitly, or BigQuery defaults the job to the US multi-region and bills on-demand (the AUDI-1089 ~$875 footgun). Every query here passed it.

### 4.6 Also worth noting
`_SUCCESS` markers sit alongside the parquet in each day directory; a plain `gcloud storage ls` of the
day prefix shows only `_SUCCESS` and the directory itself, which reads as "empty partition." Use
`ls '<prefix>/**' | grep '\.parquet$'` to actually count files.


### 4.7 Vertical + bucket sizes, distinct IPs, dt = 2026-08-17 (`ip_vertical_associations`, the monitor's own source)
Output: `outputs/audi_1208_vertical_sizes_2026_08_17.csv`. **These are the reported numbers** — same source and same counting rule as Ryan's monitor, and the same day as the HI cut.

| cut | n | mean | median | Q1 | Q3 | min | max |
|---|---|---|---|---|---|---|---|
| verticals (6-digit) | 148 | 9,479,187 | 6,557,786 | 3,959,353 | 12,026,014 | 919,345 | 76,274,119 |
| buckets (3-digit) | 37 | 25,952,755 | 20,852,312 | 11,362,823 | 33,340,029 | 2,546,637 | 88,832,335 |

Largest verticals: `124000 Current Affairs` 76.3M · `101000 Apparel & Accessories` 44.3M · `104014 B2B - Workflow Automation` 42.3M · `114003 Food Products` 34.0M.
Smallest: `101005 Apparel & Accessories - Healthcare` 0.92M · `128000 Home Warranties` 0.98M · `133005 Skiing & Snowboarding` 1.32M.

**Reconciliation against IPDSC DS13 (dt = 2026-08-16) — sources agree.** Median delta +1.90% for verticals (range −3.59% to +9.13%) and +1.77% for buckets (range +0.35% to +8.94%). The `dt`s differ by one day and vertical membership grows daily, so a small positive median delta is the expected shape. No structural divergence: IPDSC DS13 is a faithful downstream copy. Either source answers the question; `ip_vertical_associations` is preferred because it is what the monitor reports and it had the fresher partition.

**Roster gotcha:** `integrationprod.fpa_advertiser_verticals` is one row per ADVERTISER (30,863 rows per `type`), not a vertical dimension. `SELECT DISTINCT vertical_id, vertical_name` gives 37 (`type = 0`) / 148 (`type = 1`) and matches `fpa_categories` exactly. Joining it undeduped fans the size table out — the monitor's JDBC subquery uses `SELECT DISTINCT` for exactly this reason. Also: the monitor filters `vertical_name != 'MNTN Matched Audience'`, but **no such row exists in the BQ copy** (0 rows match `%MNTN%` in either type), so that filter is a no-op here and the roster is the full 148.

### 4.8 HI pool size per MM audience, 2026-08-17
Grain = one active Stage-1 prospecting campaign (4,907 of them; the scoring pipeline's own active set). Every one carries MNTN Matched targeting (`has_mm_incl` TRUE for all 4,907), so there is no non-MM comparison cohort in this population. `prospecting_intent` and PACC join 1:1 with full overlap.

| cohort | audiences | mean | median | Q1 | Q3 | min | max |
|---|---|---|---|---|---|---|---|
| all MM audiences | 4,907 | 18,280,569 | 5,531,138 | 2,663,819 | 21,394,484 | 0 | 178,048,203 |
| no exclusions | 3,211 | 17,040,321 | 5,144,080 | 2,400,946 | 20,516,066 | 0 | 178,048,203 |
| with exclusions | 1,696 | 20,628,706 | 6,201,842 | 3,057,384 | 24,182,556 | 0 | 177,293,701 |

Context, same 4,907 audiences: mean 51,241,847 IPs at ANY score (median 43,223,724), so HI is a mean 18.3M of a mean 51.2M scored pool. `min = 0` is real — some active audiences reached no HI IP that day; they are kept in the counts, not dropped.

Distinct-IP counts use `APPROX_COUNT_DISTINCT` (HLL, ~1% error) because one day is 251.6B rows. That error is negligible against a distribution whose mean is 3.3x its median.

### 4.9 THE FINDING — exclusions are invisible to scoring, so neither cohort is post-exclusion
The counter-intuitive result (audiences WITH exclusions show a **larger** HI median, 6.20M vs 5.14M) is not a data error. Read `models/audience_intent/prospecting_join.py` in `SteelHouse/airflow-ti`:

```python
active_campaign_categories = (
    self.spark.read.parquet(f"{...}/prospecting_active_campaign_categories/year={year}/...")
    .groupBy("advertiser_id", "campaign_group_id", "campaign_id", "campaign_template_id")
    .agg(F.count("*").alias("_c"))
    .drop("_c")
)
```

The job groups PACC down to the campaign key and **throws away `include`, `data_source_id`, and `data_source_category_id`**. PACC is used purely as a dimension to fetch `campaign_template_id` / `funnel_level` (which decide whether a row keeps its pipeline score or is flattened to 10000), then LEFT-joined to the scores. `include = false` **never acts as a filter anywhere in the scoring path.**

So every HI number above is the **pre-exclusion** pool, for both cohorts. Consequences:
1. The no-exclusion vs with-exclusion split is a **cohort correlation, not an exclusion effect.** Audiences that carry exclusions are simply attached to larger/more mature accounts, which have bigger HI pools to begin with. The +1.06M median gap must NOT be read as "exclusions add reach" or as any cost of excluding.
2. This is consistent with the already-recorded serve-time model: exclusions bind in the **bidder**, at bid time, on the DS the bidder evaluates (DS47 since the 2026-07-01 release) — see memory `reference_crm_exclusion_serve_time`. Scoring sizes the addressable universe; the bidder removes the excluded slice afterwards.
3. **Answering Paulo's part 2b as a post-exclusion number is a materially different and much harder query** — it needs the exclusion sets resolved per campaign against IPDSC at the same `dt` and subtracted from the HI pool. Ryan Kleck flagged exactly this in-thread ("you're gonna have to like apply those exclusions somehow.. that's gonna be the worst part 2b") before both of us settled on the cohort reading. If Paulo wants the true post-exclusion HI size, that is a follow-on.

PACC exclusion composition: 1,696 of 4,907 audiences (34.6%) carry at least one `include = false` clause.


### 4.10 CORRECTION to 4.8 — the first HI figures were contaminated by flat-scored later-stage campaigns
**4.8 stands as the record of what the unfiltered query returned; its numbers must not be quoted.** They were ~3.8x too high. Found by challenging the counter-intuitive result rather than shipping it.

**The tell.** The HI/all-scored ratio per campaign is bimodal with an empty middle: 296 campaigns under 1%, 3,185 between 1% and 50%, **zero between 50% and 99%**, then **1,426 campaigns at exactly 100%**. A campaign whose every scored IP is High Intent is not a real audience shape.

**The cause, from `prospecting_join.py`.** The job keeps the pipeline score only when
`campaign_template_id == 10 OR funnel_level IN (1, 2)`; every other campaign has `household_score`
**flattened to `REGULAR_SCORE_WITH_KEYWORD` = 10000**. A flat 10000 sits inside the 8001-10000 HI band, so
those campaigns contribute their **entire** scored IP set as counterfeit High Intent.

**Verified separation** (joining `integrationprod.campaigns.funnel_level` onto the 4,907, output `outputs/audi_1208_campaign_funnel_levels.csv`):

| funnel_level | campaigns | flat at hi = all_ips |
|---|---|---|
| 1 (prospecting) | 2,063 | 0 |
| 2 (MT-S2) | 1,418 | 0 |
| 3 (MT-S3) | 1,426 | **1,426 (all)** |

Perfect separation — every flat campaign is `funnel_level = 3`, and zero funnel-1/2 campaigns are flat. This reproduces the pipeline's own rule exactly, so the filter is not a heuristic. All 4,907 carry `objective_id = 1`, which is why objective_id could not have caught this (consistent with the standing "objective_id UNRELIABLE, use funnel_level" rule).

**CORRECTED figures — `funnel_level = 1`, active MM prospecting audiences, 2026-08-17. These are the reported numbers.**

| cohort | audiences | mean | median | Q1 | Q3 | min | max |
|---|---|---|---|---|---|---|---|
| all MM prospecting | 2,063 | 4,772,375 | 3,553,726 | 1,644,679 | 5,958,157 | 0 | 41,760,550 |
| no exclusions | 1,342 | 4,516,518 | 3,486,590 | 1,310,364 | 5,719,723 | 0 | 34,470,335 |
| with exclusions | 721 | 5,248,601 | 3,725,338 | 2,273,025 | 6,832,458 | 0 | 41,760,550 |

Context, same 2,063: mean 51,321,823 IPs at any score (median 43,120,471); PP band (6666-8000) mean 6,313,960 / median 2,913,153. So HI is a mean 4.8M of a mean 51.3M scored pool.

Adding `funnel_level = 2` barely moves it (n=3,481, mean 4,757,882, median 3,564,058), so the headline is not sensitive to the S1-vs-S1+S2 choice. It is *extremely* sensitive to leaving funnel 3 in.

**4.9 survives unchanged.** With-exclusion audiences still report a higher median (3.73M vs 3.49M) and the pre-exclusion caveat is untouched — the exclusion mechanism finding was never a function of the contamination.

**The builder now asserts this cannot regress:** `artifacts/audi_1208_build_xlsx.py` filters `funnel == 1` and then `assert not [c for c in mm if c["hi"] == c["all_ips"]]`.

### 4.11 Vertical numbers independently verified (2026-08-17, integrity query)
`ip_vertical_associations` at `dt = 2026-08-17`: 2,375,803,803 rows · **214,079,274 distinct IPs** · **185 distinct `data_source_category_id` = exactly 148 verticals + 37 buckets** · **0 null `ip`** · **0 null category id** · every distinct IP sits in at least one 6-digit vertical (`ips_in_any_vertical` = `distinct_ips`).

So the vertical half needs no correction: the counts are exact `COUNT(DISTINCT ip)` (not approximate), the roster is complete with no unmapped ids, and there is no null or orphan contamination. An IP averages ~6.6 verticals, which is why the 148 vertical sizes sum to ~1.40B against a 214.08M IP base — categories overlap by design and must never be added.

## 5. Solution
**Delivered (rebuilt 2026-08-18 with the funnel_level=1 correction):** `My Drive/Tickets/AUDI-1208/AUDI-1208 Vertical and HI Audience Sizes.xlsx` (branded, `lib/mntn_xlsx.py`). Builder: `artifacts/audi_1208_build_xlsx.py`. Tabs: Overview · Vertical sizes · HI pool sizes · All verticals (148, ranked) · All buckets (37, ranked) · Score bands · Read me · Method & caveats · Queries.

The two Method & caveats blocks that lead the tab are the exclusion-mechanism caveat and the skew caveat — the two ways these numbers get misread.

**Not delivered (out of scope, flagged to the requester):** the true post-exclusion HI pool size. See 4.9 item 3.

## 6. Questions Answered
- **Q:** What is the average size of all verticals, with quartiles?
  **A:** Across all 148 DS13 verticals on 2026-08-17: mean 9,479,187 distinct IPs, median 6,557,786, Q1 3,959,353, Q3 12,026,014, min 919,345 (`101005 Apparel & Accessories - Healthcare`), max 76,274,119 (`124000 Current Affairs`). Buckets (37): mean 25,952,755, median 20,852,312, Q1 11,362,823, Q3 33,340,029.

- **Q:** What is the average size of the HI subset of MM audiences, for audiences with no exclusions and for all MM audiences?
  **A:** No exclusions (1,342 audiences): mean 4,516,518 IPs, median 3,486,590, Q1 1,310,364, Q3 5,719,723. All MM prospecting audiences (2,063): mean 4,772,375, median 3,553,726, Q1 1,644,679, Q3 5,958,157. Scope is `funnel_level = 1`; see 4.10 for why that filter is mandatory. **Both are pre-exclusion pools** — see the next question.

- **Q:** Does carrying an exclusion shrink an audience's HI pool?
  **A:** These numbers cannot tell you, and they are not evidence that it does. `prospecting_join` discards the `include` flag before scoring, so exclusions are absent from the scoring path entirely and bind at bid time instead. The with-exclusion cohort actually reports a HIGHER median (3.73M vs 3.49M) purely because exclusions correlate with larger accounts.

- **Q:** Where in BQ do vertical sizes come from?
  **A:** Two equivalent sources. `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<date>/` is what the existing `vertical_size_monitor` reads and is preferred. `dw-main-bronze.external.ipdsc__v1` filtered to `data_source_id = 13` is the downstream copy and agrees to a median 1.9% at one day's lag. `external_ddm.data_source_category_sizes` is 3P-only and does NOT work.

- **Q:** How many verticals are there?
  **A:** 148, plus 37 bucket parents and 1 root. Consistent across `integrationprod.fpa_categories` (`data_source_id = 13`) and `SELECT DISTINCT` on `integrationprod.fpa_advertiser_verticals` (`type = 1` / `type = 0`).

- **Q:** Can the registered BQ external table be used for prospecting scores?
  **A:** No, not for recent dates. `external.household_scoring__prospecting_intent__v1` returns 0 rows for August 2026 while the GCS partitions exist and are full. Use an inline `--external_table_definition` pointed at the day directory, and always pass `--location=us-central1`. See 4.5.

## 7. Data Documentation Updates
Queued for `/capture`:
1. **`data_knowledge.md`** — any query banding `household_score` from `prospecting_intent` MUST scope to `funnel_level = 1` (or `IN (1,2)`) / `campaign_template_id = 10`. Everything else is flattened to 10000 by `prospecting_join` and enters the HI band as its whole audience. 2026-08-17: 1,426 of 4,907 campaigns, inflating the HI mean 3.8x. Tell = HI/all-scored ratio bimodal with an empty 50-99% middle.
2. **`data_knowledge.md`** — exclusions (`include = false` in `prospecting_active_campaign_categories`) are **invisible to scoring**. `prospecting_join` groups PACC to the campaign key and drops `include`; exclusions bind in the bidder at serve time. Any "HI pool" from `prospecting_intent` is pre-exclusion for every campaign. Extends `reference_crm_exclusion_serve_time`.
3. **`data_catalog.md`** — CORRECTION: `external.household_scoring__prospecting_intent__v1` returns **0 rows for Aug 2026** while the GCS partitions are full (hive `mode: CUSTOM` with no `{key:TYPE}` in `sourceUriPrefix`). Use an inline `--external_table_definition` on the day directory + `--location=us-central1`. Existing note says "~35 days active (10-day in BQ)" — the real BQ-visible ceiling was ~5 weeks stale.
4. **`data_catalog.md`** — vertical sizing source of truth: `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<date>/` (what `vertical_size_monitor.py` reads). IPDSC DS13 is the downstream copy, agrees to median +1.9% at one day's lag. Both give 148 verticals / 37 buckets.
5. **`data_catalog.md`** — `integrationprod.fpa_advertiser_verticals` is one row per ADVERTISER (30,863 per `type`), not a vertical dim. `SELECT DISTINCT vertical_id, vertical_name` before joining or size tables fan out.
6. **New memory** — the AUDI-1208 sizing baseline (verticals mean 9.5M / median 6.6M; prospecting HI pool mean 4.8M / median 3.6M, 2026-08-17) so the next "how big is X" ask starts from a number.

## 8. Open Items / Follow-ups
1. **Post-exclusion HI size is NOT answered** and is the one thing Paulo might actually have meant by 2b. Needs each campaign's exclusion sets resolved against IPDSC at the same `dt` and subtracted from its HI pool. Ryan Kleck called this the hard part in-thread. Scope as its own spike if asked.
2. **Tell Ryan the registered `prospecting_intent` external table is blind past mid-July 2026.** It is a live prod table others may be querying and silently getting stale or empty results. Fix is a corrected hive `sourceUriPrefix`. Not hot-patched here.
3. **Offer the quartile summary as a monitor addition.** Ryan said "quartile size, i'm not sure exactly how to do that???" — `vertical_size_monitor.py` already computes every per-vertical count, so a distribution strip (mean/median/Q1/Q3) on the existing email is a few lines of Spark. Would answer this class of ask standing, without a person.
4. **Verify the 148/37 roster against Postgres.** The monitor filters `vertical_name != 'MNTN Matched Audience'`; no such row exists in the BQ mirror. If it exists in Postgres, the monitor reports 147 verticals where we report 148.
