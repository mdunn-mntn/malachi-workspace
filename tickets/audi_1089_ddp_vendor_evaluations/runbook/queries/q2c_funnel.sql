-- ============================================================================
-- DDP quality-score runbook, STEP 2c: the survival funnel — raw feed to DS13/DS19
-- Claim: per source, follow the rows/IPs/domains through every filter stage to
-- what is actually eligible for MM consumption (DS13 verticals, DS19 product
-- categories) — the "filter criteria" that decide whether we ever use (and pay for) a row.
--
-- Stages (row-level flags, filters mirror the internal ingestion-pipeline code (airflow-ti repo)):
--   raw            — everything delivered that day
--   kept           — survives HARD filters: url non-empty, NET.REG_DOMAIN parses,
--                    not infra (steelhouse/googlesyndication/gtm)
--   ds13_input     — kept minus BLOCKED_DOMAIN_NAMES (yahoo/aol/easybrain)
--   ds13_class     — ds13_input AND reg domain IN website_crawl_verticals (wcv)
--   ds19_cat       — url non-empty, not infra, composite_key (url minus query + "_1")
--                    IN product_categorization with dsc_id >= 900000  (NOTE: no reg-domain
--                    parse gate and no blocklist on this path — mirrors the feature model,
--                    which is exactly how garbage hosts / yahoo reach billing)
--   used           — ds13_class OR ds19_cat (eligible for MM -> creditable)
-- Plus unique IPs at raw/used and unique domains at raw/classified.
--
-- Sample: ONE full day (SAMPLE_DT). Billed reality (June) joins in the chart from q1d.
--
-- Run (from workspace root; ~290 GB scan + two small external classifier tables):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q2c funnel" \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/*.parquet" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q2c_funnel.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q2c_funnel.csv
--
-- Parameters (in the external-table URI above, not in the SQL):
--   SAMPLE_DT = 2026-07-01   any complete day inside the signal window
-- ============================================================================

WITH wcv_d AS (
  SELECT DISTINCT domain_name FROM wcv
),

pc_k AS (
  SELECT DISTINCT composite_key
  FROM pc
  WHERE (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

base AS (
  SELECT
    CAST(data_source_id AS INT64) AS ds,
    ip,
    NET.REG_DOMAIN(url) AS dom,
    (url IS NULL OR url = '') AS empty_url,
    (url LIKE '%steelhouse.com%' OR url LIKE '%googlesyndication.com%'
     OR url LIKE '%gtm-msr.appspot.com/render%') AS infra_url,
    CONCAT(SPLIT(url, '?')[SAFE_OFFSET(0)], '_1') AS composite_key
  FROM svs
),

flagged AS (
  SELECT
    b.ds,
    b.ip,
    b.dom,
    NOT (b.empty_url OR b.infra_url OR b.dom IS NULL) AS kept,
    (NOT (b.empty_url OR b.infra_url OR b.dom IS NULL)
     AND b.dom NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')) AS ds13_input,
    (NOT (b.empty_url OR b.infra_url OR b.dom IS NULL)
     AND b.dom NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')
     AND w.domain_name IS NOT NULL) AS ds13_class,
    (NOT b.empty_url AND NOT b.infra_url AND p.composite_key IS NOT NULL) AS ds19_cat
  FROM base b
  LEFT JOIN wcv_d w ON b.dom = w.domain_name
  LEFT JOIN pc_k p ON b.composite_key = p.composite_key
)

SELECT
  ds,
  COUNT(*) AS rows_raw,
  COUNTIF(kept) AS rows_kept,
  COUNTIF(ds13_input) AS rows_ds13_input,
  COUNTIF(ds13_class) AS rows_ds13_class,
  COUNTIF(ds19_cat) AS rows_ds19_cat,
  COUNTIF(ds13_class OR ds19_cat) AS rows_used,
  APPROX_COUNT_DISTINCT(ip) AS ips_raw,
  APPROX_COUNT_DISTINCT(IF(ds13_class OR ds19_cat, ip, NULL)) AS ips_used,
  APPROX_COUNT_DISTINCT(dom) AS domains_raw,
  APPROX_COUNT_DISTINCT(IF(ds13_class, dom, NULL)) AS domains_classified
FROM flagged
GROUP BY ds
ORDER BY rows_raw DESC;
