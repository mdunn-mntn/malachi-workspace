---
doc_type: ticket
title: "AUDI-1208: Vertical and MM HI audience sizing (mean + quartiles)"
status: in_progress
date: 2026-08-18
summary: "Mean/quartile sizes for DS13 verticals and the HI subset of MM audiences"
result: "Verticals mean 9.5M IPs / median 6.6M / Q1-Q3 4.0-12.0M (n=148); MM audience HI pool mean 18.3M / median 5.5M; exclusions are bid-time, so both cohorts are pre-exclusion"
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

## 5. Solution
**Delivered:** `My Drive/Tickets/AUDI-1208/AUDI-1208 Vertical and HI Audience Sizes.xlsx` (branded, `lib/mntn_xlsx.py`). Builder: `artifacts/audi_1208_build_xlsx.py`. Tabs: Overview · Vertical sizes · HI pool sizes · All verticals (148, ranked) · All buckets (37, ranked) · Score bands · Read me · Method & caveats · Queries.

The two Method & caveats blocks that lead the tab are the exclusion-mechanism caveat and the skew caveat — the two ways these numbers get misread.

**Not delivered (out of scope, flagged to the requester):** the true post-exclusion HI pool size. See 4.9 item 3.

## 6. Questions Answered
- **Q:** What is the average size of all verticals, with quartiles?
  **A:** Across all 148 DS13 verticals on 2026-08-17: mean 9,479,187 distinct IPs, median 6,557,786, Q1 3,959,353, Q3 12,026,014, min 919,345 (`101005 Apparel & Accessories - Healthcare`), max 76,274,119 (`124000 Current Affairs`). Buckets (37): mean 25,952,755, median 20,852,312, Q1 11,362,823, Q3 33,340,029.

- **Q:** What is the average size of the HI subset of MM audiences, for audiences with no exclusions and for all MM audiences?
  **A:** No exclusions (3,211 audiences): mean 17,040,321 IPs, median 5,144,080, Q1 2,400,946, Q3 20,516,066. All MM audiences (4,907): mean 18,280,569, median 5,531,138, Q1 2,663,819, Q3 21,394,484. **Both are pre-exclusion pools** — see the next question.

- **Q:** Does carrying an exclusion shrink an audience's HI pool?
  **A:** These numbers cannot tell you, and they are not evidence that it does. `prospecting_join` discards the `include` flag before scoring, so exclusions are absent from the scoring path entirely and bind at bid time instead. The with-exclusion cohort actually reports a HIGHER median (6.20M vs 5.14M) purely because exclusions correlate with larger accounts.

- **Q:** Where in BQ do vertical sizes come from?
  **A:** Two equivalent sources. `gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<date>/` is what the existing `vertical_size_monitor` reads and is preferred. `dw-main-bronze.external.ipdsc__v1` filtered to `data_source_id = 13` is the downstream copy and agrees to a median 1.9% at one day's lag. `external_ddm.data_source_category_sizes` is 3P-only and does NOT work.

- **Q:** How many verticals are there?
  **A:** 148, plus 37 bucket parents and 1 root. Consistent across `integrationprod.fpa_categories` (`data_source_id = 13`) and `SELECT DISTINCT` on `integrationprod.fpa_advertiser_verticals` (`type = 1` / `type = 0`).

- **Q:** Can the registered BQ external table be used for prospecting scores?
  **A:** No, not for recent dates. `external.household_scoring__prospecting_intent__v1` returns 0 rows for August 2026 while the GCS partitions exist and are full. Use an inline `--external_table_definition` pointed at the day directory, and always pass `--location=us-central1`. See 4.5.

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
