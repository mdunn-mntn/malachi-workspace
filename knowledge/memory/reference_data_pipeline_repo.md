---
name: reference_data_pipeline_repo
description: SteelHouse/data-pipeline = the PySpark reporting-pipelines repo; home of the enriched_impressions builder (impression_enrichment.py)
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [data-pipeline, SteelHouse/data-pipeline, impression_enrichment.py, enriched_impressions, pyspark_pipelines, UI Audience Segment Reporting, category_facts, DDP meter, backfill_enriched_impressions, biglake]
domain: [repos, data-catalog]
lifecycle: active
last_verified: 2026-07-29
---

`SteelHouse/data-pipeline` is the **PySpark reporting-pipelines repo** (separate from airflow-ti / sqlmesh;
found 2026-07-29 via GitHub org code-search, not cloned locally). It builds the persisted reporting
intermediates the DDP meter + UI Audience Segment Reporting consume.

Key paths:
- `pyspark_pipelines/impression_enrichment.py` — **builds `summarydata.enriched_impressions`** (materialized to
  `mntn-analytics-prod-01`). Prod config `conf/impression_enrichment/prod/config.yaml`: `lookback=2` (impression
  days), `ipdsc_lookback=35`, `dsid_block_list=[2,14,42]`. Inputs all `dw-main-silver` (`logdata.cost_impression_log`,
  `public.campaigns`/`advertisers`, `summarydata.v_campaign_group_segment_history`, `ber_stg.category_facts__domain_x_publisher_types`)
  + ipdsc from `gs://mntn-data-archive-prod/ipdsc`. The `data_source_id` tag = what the campaign TARGETED
  (segment history), not a column on the impression log; ipdsc join is a **35-day BACKWARD** window
  (`ipdsc_dt BETWEEN to_date(time)-35d AND time`). Writes bucketed by `ad_served_id` (600), partitioned `dt,hh`,
  dynamic-overwrite on a rolling 2-day window.
- `scripts/backfill_enriched_impressions.py`, `bigquery/ddl/biglake/enriched_impressions_biglake_ext.def.json`,
  `tests/integration/test_impression_enrichment.py`, `conf/impression_enrichment/{dev,local,prod}/config.yaml`.
- Also has `pyspark_pipelines/constants/category_facts/` (source-column constants) + `extensions/category_facts_ext.py`.

Consumers/related: DDP usage meter (`SteelHouse/bae-sql-utility/ddp/`), Bombora S2S pushback
(`SteelHouse/mntn-gar-at-astro/dags/bombora_impression_pushback.py`). See on-call INC-001 for the DS51
skip-day → serving-side-zero mechanism proven from this code. Related: [[reference_airflow_ti]], [[reference_sqlmesh_repo]].
