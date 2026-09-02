# PR 1194 — status after Sean's fixes (57c4c44 + 5d2ca0f)

Reviewed head: 375b717. Fix head: 5d2ca0f. Baseline: audi_1049_pr1194_review_baseline.md

## The 7 blocking items

| # | Finding | Status | Note |
|---|---|---|---|
| 1 | hhdsc_geo biased representative IP | Fixed | `%.0` filter, `ipdsc_icloud_ips` anti-join, and numeric `min(ip_num)` all added |
| 2 | Three models hardcoded to prod HHDSC | Fixed | Now `read_model("hhdsc_ds_13.HHDSC13")`; `optional=self.runtime_env == "dev"` so prod raises on a missing partition |
| 3 | hh_fangorn_prospecting_scoring empty guard | Fixed | `isEmpty()` raise added |
| 4 | soft_fail sensors skip the DAG silently | Fixed | DAG rewritten (+98); new `tests/dags/test_hh_audience_intent.py` |
| 5 | Resolution: staleness, coverage, crediting | Partial | Staleness and coverage fixed; crediting still absent |
| 6 | hh_vertical_mid two graph vintages | Partial | Root cause fixed; a deploy-window edge remains |
| 7 | DS19 unguarded feeds the prod export | Fixed | `isEmpty()` raise added to hhdsc_ds_19 (and to hhdsc_ds_13) |

## What #5 and #6 still leave open

**Crediting.** `utils_model/household_resolution.py` at the fix head contains no `log_translation`
call. The precedent is `models/feature_store/feature_group_2_derived/guid_log_derived_household_id_vertical_id.py:206`,
which credits the same guid_log -> IP -> household hop. These translations will not appear in
`graph_translation_signal`.

**Coverage has no floor.** `_validate_graph_resolution` (household_resolution.py:28-50) now prints
`matched_ids/input_ids` and raises on cardinality expansion. The denominator is correct: the
library's resolve mode left-joins, so unmatched inputs survive with a null `household_id`. But the
function only prints. A day at 5% coverage logs 5% and the job continues.

**hh_recency deploy window.** `most_recent_view` is new in this commit (`hh_page_views.py:79-92`).
`hh_recency` reads a 7-day window of `hh_page_views` with `.option("mergeSchema", "true")`
(hh_recency.py:99-101), so for six days after deploy the pre-57c4c44 partitions backfill that column
as NULL. `F.max` skips nulls, so this only bites a household whose entire 7-day window is
old-schema: it gets `recency = NULL`, fails the gate in `hh_vertical_mid`, and lands in the
`without_activity` random-score branch. Transient and narrow, but it is the same failure mode #6
set out to remove. A one-time backfill of the 7 days before cutover closes it.

## Change to live production code

`hhdsc_ds_13` and `hhdsc_ds_19` already run in prod. This commit switches both from
`resolve_households` (reads the graph parquet directly) to `attach_ipv4_households` /
`attach_ip_households` (delegates to the mntn_graph library). Two consequences:

1. **Household assignments will shift on the first prod run.** The old tiebreak was
   `max(struct(confidence_score, household_id))`, i.e. highest `household_id`. The library orders
   `confidence_score DESC, household_id ASC`, i.e. lowest. For any IP with two equal-confidence
   edges the two rules pick differently, so some households gain or lose category ids and
   `hhdsc_geo` / `tpa_hh_export` inherit the shift. This moves toward ID-Service parity, so it is
   the right direction, but it is an unannounced output change to a running job.
2. **The detailed coverage breakdown is gone.** Both files dropped
   `coverage_metrics(resolved).show()` (status, resolved_from, shared share, avg confidence) in
   favour of the single-line print. `coverage_metrics()` now has no callers repo-wide.

Both files also gained an `isEmpty()` guard on the source, which is an improvement.

## Checked and refuted

A review agent claimed `from mntn_graph import GraphConfig, IdType as MntnGraphIdType`
(household_resolution.py:85) would raise ImportError on Dataproc and take down hhdsc_ds_13/19.
False: `mntn_graph/__init__.py` re-exports `IdType` from `.id_types` and lists it in `__all__`.
Verified against the deployed zip.

## Notable

- The staleness fix is better than what was asked for. `attach_ip_households` calls
  `load_graph(spark, GraphConfig(as_of=..., max_graph_staleness_days=MAX_GRAPH_STALENESS_DAYS), ...)`
  and passes the result in as `graph_df`, which is the only way to reach the guard given
  `ids_to_households` does not expose it.
- The two resolvers were collapsed into one. `utils_model/mntn_graph_resolution.py` is deleted and
  `household_resolution.py` is the single chokepoint again, which is what AUDI-1167 asked for.
- `.persist(StorageLevel.MEMORY_AND_DISK)` added on the per-IP lookup.
- `optional=self.runtime_env == "dev"` is a real improvement on the coverage-floor point: prod now
  refuses a partial lookback window instead of scoring against one. It is all-or-nothing, so a
  single missing day in a 31-day HHDSC window fails the job.
