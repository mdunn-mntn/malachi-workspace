---
doc_type: ticket
title: "AUDI-1207: PS-8614 Site Visitors geography Other bucket"
status: done
date: 2026-08-17
summary: "Apollo.io 89% Other in Site Visitors geography is 86% non-US traffic, not a geo-pipeline regression"
result: "Not a regression. Apollo.io (AID 33129) site visitors are 86.4% non-US (India 57%, Philippines 31%) plus 5.3% unresolvable IPs; only 8.4% land on a US state, and the widget buckets US states only. Platform-wide Other is ~29%. The defect bucket (resolved to a US location but no state) was zero rows for every advertiser, and guid_geos_raw logic is unchanged since 2025-09-23."
question: "Is Apollo.io's 89% Other in the Audience UI Site Visitors geography breakdown a regression in the geo-resolution pipeline, or expected for this advertiser?"
framing_state: "skip: retroactive — PS escalation opened and answered the same day; the question was fully specified by PS-8614 and Richie's comment, so there was nothing to negotiate"
---

# AUDI-1207: PS-8614 Site Visitors geography "Other" bucket

**Jira:** https://mntn.atlassian.net/browse/AUDI-1207
**Status:** done
**Date Started:** 2026-08-17
**Assignee:** Malachi

---
## 0. Framing

The framing gate was skipped as a ceremony (see `framing_state`) because the escalation arrived fully
specified and was closed within the session. The content is recorded here anyway.

- **Question (the unknown):** Is the ~89% "Other" share in Apollo.io's Site Visitors geography breakdown
  caused by a defect in the geo-resolution pipeline, or is it the expected output for this advertiser's
  traffic?
- **Goal (why / the decision):** PS needs to tell the customer either "this is a bug we are fixing" or
  "this is what your traffic looks like." The reporting team had already handed the investigation to
  targeting as a suspected upstream data-quality regression, so the decision also settles whether an
  engineering fix gets scheduled.
- **Objective (done-when):** PS-8614 carries an evidenced verdict that names the mechanism, distinguishes
  defect from expected behavior with data, and states the lookback limit honestly.
- **Approach (how):** decompose NULL `iso_code` in the source parquet into mutually exclusive causes,
  baseline the advertiser against every other advertiser on the same day, and check the pipeline's code
  history for a change that could have introduced a step.
- **What would change the answer:** any non-zero count in the "resolved to a US location but no state
  attached" bucket, or a step change in the daily series that lines up with a deploy. Both were checked
  and both came back clean.

## 1. Introduction

The Audience UI's **Segments > Site Visitors > Geography** panel is served by `gary-ql`'s
`getAudienceTotals` resolver, which reads the Postgres table **`geo.guid_geos_summary`** (ti_core_db, NOT
BigQuery) and buckets each row's `iso_code` against `AUDIENCE_REGIONS`, a fixed list of US state codes in
`src/constants/Audience.ts`. Anything not on that list is rendered as **Other**.

That table is produced by a three-hop pipeline that spans two teams:

1. **dbt python model `guid_geos_raw`** — repo `SteelHouse/dbt`, `ml_squad/models/tpa/guid_geos_raw.py`,
   **ML squad**. Scheduled hourly by the airflow-ti DAG `databricks_guid_geos` (`JobTeamConfig.ML`).
   Reads `gs://mntn-data-archive-prod/guid_log/dt=/hh=`, keeps IPv4 only (`ip LIKE '%.%.%.%'`, so IPv6
   visitors never enter the output at all), masks each IP to a network, joins `network_locations` INNER to
   the postal rows of `location_data` (`location_type_id = 7`) on `(postal_code, geoname_id)`, then LEFT
   joins the US-state rows (`location_type_id = 5 AND country_iso_code = 'US'`) on `region` to attach
   `iso_code`. IPs that fall out of the inner join are re-attached via `leftanti` with NULL
   `location_ids` / `name` / `iso_code`. Writes
   `gs://mntn-data-archive-prod/guid_geos_raw/dt=/hh=`, **8-day GCS retention**.
2. **`spark/targeting/build_guid_geos_summary.py`** on Dataproc, run daily by the airflow-ti DAG
   `guid_geos_summary_to_integration` (**`JobTeamConfig.TGT` — targeting**). Reads **up to 7 days** of
   those hourly partitions, groups by `(advertiser_id, iso_code, dt)` with `countDistinct(ip)`, appends to
   `test.stg_guid_geos_summary`.
3. The same DAG then `TRUNCATE`s `geo.guid_geos_summary` and re-inserts
   `SUM(count) ... GROUP BY advertiser_id, iso_code` stamped `CURRENT_DATE`.

## 2. The Problem

PS-8614 (Katia Podtynov, 2026-08-17) reported Apollo.io (AID 33129, confirmed via
`integrationprod.advertisers.company_name`) showing ~89% of Site Visitors as Unknown/Other under Geography.

Richie Gonzalez investigated the front end and escalated to targeting the same morning. His comment
established, correctly, that:

- `AUDIENCE_REGIONS` and the `AudienceTotals` resolver are unchanged since April 2024 and the arithmetic
  reconciles exactly, so it is not a gary-ql bug.
- 909,157 of ~1,033,157 rows for advertiser 33129 have a blank/NULL `iso_code` (88.0%), more than 7x the
  largest real state (CA at 21,532).

His conclusion, and the ask he handed over, was that `iso_code` NULL at that rate is "a data quality issue
in the geo-resolution pipeline," with three questions: is it expected or a regression, when did it start,
and what in the geo pipeline is stopping most IPs resolving to a state.

Bryce Wagg's read when passing it on was that targeting probably did not need to be involved. That turned
out to be wrong on ownership (hop 2 and 3 are a TGT DAG in our own repo), though right that nothing was
broken.

## 3. Plan of Action

1. Locate `geo.guid_geos_summary`. It is not in BigQuery.
2. Read the three pipeline stages and establish exactly what makes `iso_code` NULL.
3. Check the code history of every hop for a change that could produce a step.
4. Query the source parquet directly to get a per-day series and a platform baseline.
5. Split NULL `iso_code` into mutually exclusive causes so "defect" and "expected" are separable.
6. Reconcile against Richie's Postgres numbers before publishing anything.
7. Name the countries, so PS has something concrete to tell the customer.

## 4. Investigation & Findings

### 4a. Dead end: assuming the table was in BigQuery

First move was `bq show` against `geo.guid_geos_summary` in bronze, silver and gold, then `bq ls` on the
`geo` dataset in each, then an `INFORMATION_SCHEMA.TABLES` sweep for `%guid_geo%`. All empty. Nothing in
`knowledge/_ROUTING.md` mentioned the table either, and the geo keywords in the routing index all pointed
at `reference_aud22_geo_reporting_sync`, which is a **different** pipeline and a different failure mode
(`location_data` metro_id/hierarchy mismatch plus memdb serve-side skew). Several minutes were lost here.

What actually found it was `grep -ril guid_geos` across the local service repos, which hit three files in
`airflow-ti` immediately. **Lesson:** when the routing index returns nothing for a named table, leave the
index and grep the repos, rather than trying more datasets.

Confirmed afterwards via `INFORMATION_SCHEMA` that no BigQuery table exists over `guid_geos_raw` anywhere
in bronze or silver, so the external-table definition below is required, not merely convenient.

### 4b. Mechanism: `iso_code` is populated only for US states, by construction

From `guid_geos_raw.py`, `build_geo()`:

```python
state_df = (locdata.filter(F.col("location_type_id") == F.lit(5))
                   .filter(F.col('country_iso_code') == F.lit('US'))
            .select(F.col('location').alias('region'), F.col('iso_code'), ...))
...
netloc.join(postal_df, ['postal_code', 'geoname_id'], "inner") \
      .join(state_df, 'region', "left")
```

The `iso_code` column can therefore only ever hold a **US state code**. A NULL means one of exactly three
things, and they are distinguishable in the raw output because `location_ids` (the `location_data.hierarchy`
chain) survives independently of the state join:

| `iso_code` NULL and… | meaning | is it a defect? |
|---|---|---|
| `location_ids` empty/NULL | IP matched nothing in `network_locations` | no, unresolvable |
| `237` present in `location_ids` | resolved to a **US** location but no state attached | **yes** |
| `237` absent | non-US visitor | no, expected |

`237` is the `location_id` for the United States (`location_type_id = 2`), consistent with
`feedback_geo_axes`, which documents US = 237 as the country-level geo id.

### 4c. Code history: no change that could have introduced a step

| file | repo | last change |
|---|---|---|
| `ml_squad/models/tpa/guid_geos_raw.py` | `SteelHouse/dbt` | 2025-09-23 |
| `dags/targeting/databricks_guid_geos.py` | `airflow-ti` | 2025-09-17 |
| `spark/targeting/build_guid_geos_summary.py` | `airflow-ti` | 2026-06-16 (vault path refactor) |
| `dags/targeting/guid_geos_summary_to_integration.py` | `airflow-ti` | 2026-06-24 (a revert) |

The geo-resolution logic itself is untouched for roughly eleven months. The two 2026 commits are a Vault
credential refactor and its revert, neither of which touches the joins.

GCS shows all 24 hourly partitions present for every retained day and `dt=2026-08-17` current through the
hour the check ran, so the hourly producer is healthy.

### 4d. Querying the source

`geo.guid_geos_summary` is truncated on every run, so it holds one snapshot and cannot answer a
"when did it start" question at all. The only substrate with any history is the raw parquet, which retains
8 days. Two footguns building the external table:

- BigQuery permits **one wildcard per source URI**, so `dt=*/hh=*/*.parquet` is rejected outright.
- The path contains Databricks `_started_*` / `_committed_*` marker files, which fail Parquet parsing when
  swept in by a broader glob.

Both are handled by enumerating the hour directories and globbing `*.parquet` inside each. See
`queries/audi_1207_repro.sh`, which builds the definition and runs all three queries, taking
`<advertiser_id> <date>` as arguments.

Full result tables: `outputs/audi_1207_results_2026_08_16.md`. Headlines:

- **Per-day series (all 8 retained days).** Advertiser 33129 sits between 86.07% and 91.65% with no step.
  Platform-wide sits between 28.31% and 30.27%. Both series are flat.
- **Cause decomposition (2026-08-16).** Advertiser 33129: 8.35% US state / 5.30% no geo match /
  **86.35% non-US**. All other advertisers: 71.45% / 10.60% / 17.94%. The defect bucket
  (`3_us_but_no_state`) returned **zero rows for every advertiser on the platform**.
- **Country mix.** India 57.44%, Philippines 31.39%, Vietnam 2.12%, then a long tail. Consistent with a
  sales-intelligence product used heavily by offshore SDR teams.

### 4e. Reconciliation before publishing

Before quoting any of this externally, the external read was checked against the number Richie had already
published from Postgres. His snapshot: 88.0% blank (909,157 of ~1,033,157). Sum of the per-day pair counts
over the equivalent 7-day shape: 88.5% (976,901 of 1,103,524). Agreement to 0.5pp, with the residual
explained by the window offset.

That reconciliation is also what confirmed the hop-3 semantics: **`count` is a SUM across seven days of
per-day distinct IPs, not distinct IPs over seven days.** A visitor active on five days is counted five
times, so the ~1.03M "users" in the widget overstates unique visitors by the repeat-visit factor.

## 5. Solution

No code change. The pipeline is behaving correctly and the reported percentage is a true description of
Apollo.io's traffic.

Delivered:

- Slack answer to Bryce, and a comment on PS-8614 (posted 2026-08-17 15:32) carrying the verdict, the
  ownership correction, and both caveats.
- `knowledge/data_knowledge.md` § "Audience UI Site Visitors > Geography" — the full mechanism, the
  decomposition table, and the measured baselines (commit `fa106d0e`).
- `knowledge/memory/reference_guid_geos_summary_pipeline.md` — the five facts a future session needs first
  (commit `fa106d0e`).
- `knowledge/bq/query_cookbook.md` §A9 — the three queries plus the external-table recipe
  (commit `99b4774e`).
- `improvements_backlog.md` **IMP-045** — the two structural gaps this exposed.

## 6. Questions Answered

- **Q:** Is the NULL `iso_code` rate for advertiser 33129 expected or a regression?
  **A:** Expected. 86.4% of their site visitors are non-US, and `iso_code` is a US-state-only field, so
  those rows are correctly NULL. Another 5.3% are IPs with no geo match at all. Only 8.4% are US-state
  visitors. Platform-wide the same measure is ~29%, so a large Other bucket is normal; Apollo.io is an
  outlier on audience composition, not on pipeline health.

- **Q:** If a regression, when did it start?
  **A:** Not applicable, and the question is only answerable 8 days back regardless. `guid_geos_raw` has
  8-day GCS retention and `geo.guid_geos_summary` is truncated and rebuilt on every run, so no longer
  history exists anywhere. Across the 8 days that do exist the rate is flat at 86-92%.

- **Q:** What in the geo pipeline is causing most IPs not to resolve to a state?
  **A:** Nothing. The IPs resolve fine, just not to **US** states. The premise that resolution is failing
  is what the decomposition disproves: the bucket that would represent a genuine resolution failure on US
  traffic (`3_us_but_no_state`) is zero rows across every advertiser.

- **Q:** Does targeting need to be involved?
  **A:** Yes, on ownership, though nothing needs fixing here. `guid_geos_summary_to_integration` (Dataproc
  plus the Postgres writes) is `JobTeamConfig.TGT` in `airflow-ti`. The upstream model is ML squad. Future
  escalations on this metric should not be routed away from targeting.

- **Q:** Is this the same problem as the recurring aud22 geo audit?
  **A:** No. That is `location_data` metro_id/hierarchy inconsistency plus memdb serve-side version skew,
  affecting geo **targeting** compliance. See `reference_aud22_geo_reporting_sync`. Different pipeline,
  different tables, different failure mode.

## 7. Data Documentation Updates

- `knowledge/data_knowledge.md` — new Business Logic section covering the three-hop pipeline, the NULL
  `iso_code` decomposition table, the 8-day lookback ceiling, the 7-day-sum `count` semantics, and the
  measured platform baseline.
- `knowledge/memory/reference_guid_geos_summary_pipeline.md` — new reference memory; routed via
  `_ROUTING.md` on rebuild.
- `knowledge/bq/query_cookbook.md` §A9 — the reusable diagnostic, including the one-wildcard-per-URI and
  Databricks-marker-file gotchas that cost time here.
- `improvements_backlog.md` IMP-045.

## 8. Open Items / Follow-ups

- **IMP-045 (logged, not scheduled).** Two structural gaps in `guid_geos_summary_to_integration`:
  (1) `count` sums seven days of daily distinct IPs, so the widget overstates unique visitors;
  (2) the daily `TRUNCATE` destroys all history, so no onset question about this metric is answerable
  beyond the 8-day raw retention. Both are targeting-owned.
- **Not raised as a defect, worth knowing:** IPv6 visitors are filtered out of `guid_geos_raw` entirely
  (`ip LIKE '%.%.%.%'`), so they are absent from the widget rather than counted as Other. Not
  investigated here; no evidence it is material to this escalation.
- The `2_no_geo_match_at_all` bucket runs 10.6% platform-wide. Not examined. It is the plausible next
  question if anyone asks why Other is ~29% for a US-only advertiser base.
