-- TI-1027: 5x5 (DS25) data evaluation — analysis queries
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=/hh=/data_source_id=N/*.parquet
-- Queried via BQ temp external-table defs (read-only; zzz_temp.site_visit_signal is manual/stale).
-- NET.REG_DOMAIN(url) = registered domain (matches consumer's tldextract eTLD+1).
-- Run pattern:
--   URIS=""; for d in 09 10 11 12 13 14 15; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-06-${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--            --external_table_definition='wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet' ...

-- ============================================================
-- Phase 0: vendor billing/usage registry (cost structure + MM roster)
-- ============================================================
SELECT data_source_id, data_partner_name, billing_type, fixed_cpm, enabled, is_current,
       used_in_mntn_match, used_in_interests, type, go_live_date, valid_from, valid_to, notes
FROM `dw-main-silver.tpa.direct_data_partners`
WHERE is_current = true
ORDER BY CAST(data_source_id AS INT64);

-- ============================================================
-- Phase 1: SCALE — per-vendor rows/IPs/domains + URL-path % (1 day external table: dt=2026-06-15)
-- ============================================================
SELECT
  data_source_id,
  COUNT(*) AS n_rows,
  APPROX_COUNT_DISTINCT(ip) AS approx_ips,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS approx_domains,
  COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) AS rows_with_path,
  ROUND(100*COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*"))/COUNT(*),1) AS pct_with_path
FROM svs
GROUP BY data_source_id
ORDER BY n_rows DESC;

-- ============================================================
-- Phase 3: DOMAIN overlap — DS25 unique vs internal(23,30) vs other DDPs (7-day svs)
-- ============================================================
WITH dsd AS (
  SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain
  FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL
),
dom AS (
  SELECT domain, COUNT(DISTINCT data_source_id) AS n_ds,
         LOGICAL_OR(data_source_id = 25) AS in_5x5,
         LOGICAL_OR(data_source_id IN (23,30)) AS in_internal,
         LOGICAL_OR(data_source_id IN (24,26,28,33,36,39,40)) AS in_other_ddp
  FROM dsd GROUP BY domain
)
SELECT COUNT(*) AS universe_domains,
       COUNTIF(in_5x5) AS d_5x5_total,
       COUNTIF(in_5x5 AND n_ds=1) AS d_5x5_only,
       COUNTIF(in_5x5 AND in_internal) AS d_5x5_also_internal,
       COUNTIF(in_5x5 AND in_other_ddp) AS d_5x5_also_other_ddp,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNTIF(in_5x5),1) AS pct_of_5x5_unique,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNT(*),2) AS unique_share_of_universe
FROM dom;

-- ============================================================
-- Phase 2x3: 5x5 domain classification rate (total vs unique vs universe) — svs(7d) JOIN wcv
-- ============================================================
WITH dsd AS (
  SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL
),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) AS n_ds, LOGICAL_OR(data_source_id=25) AS in_5x5 FROM dsd GROUP BY domain),
j AS (SELECT d.domain, d.n_ds, d.in_5x5, (w.domain_name IS NOT NULL) AS classified
      FROM dom d LEFT JOIN wcv w ON w.domain_name = d.domain)
SELECT COUNTIF(in_5x5) AS d_5x5_total,
       COUNTIF(in_5x5 AND classified) AS d_5x5_classified,
       ROUND(100*COUNTIF(in_5x5 AND classified)/COUNTIF(in_5x5),1) AS pct_5x5_classified,
       COUNTIF(in_5x5 AND n_ds=1) AS d_5x5_unique,
       COUNTIF(in_5x5 AND n_ds=1 AND classified) AS d_5x5_unique_classified,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1 AND classified)/NULLIF(COUNTIF(in_5x5 AND n_ds=1),0),1) AS pct_5x5_unique_classified,
       ROUND(100*COUNTIF(classified)/COUNT(*),1) AS pct_universe_classified
FROM j;

-- ============================================================
-- Phase 3: IP overlap — is 5x5 unique on reach? (1 day svs)
-- ============================================================
WITH dsi AS (SELECT DISTINCT data_source_id, ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE "%:%"),
ipm AS (SELECT ip, COUNT(DISTINCT data_source_id) n_ds,
               LOGICAL_OR(data_source_id=25) in_5x5,
               LOGICAL_OR(data_source_id IN (23,30)) in_internal,
               LOGICAL_OR(data_source_id IN (24,26,28,33,36,39,40)) in_other_ddp
        FROM dsi GROUP BY ip)
SELECT COUNT(*) ip_universe, COUNTIF(in_5x5) ip_5x5, COUNTIF(in_5x5 AND n_ds=1) ip_5x5_only,
       COUNTIF(in_5x5 AND in_internal) ip_5x5_also_internal,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNTIF(in_5x5),1) pct_5x5_ip_unique,
       ROUND(100*COUNTIF(in_5x5 AND in_internal)/COUNTIF(in_5x5),1) pct_5x5_ip_in_internal
FROM ipm;

-- ============================================================
-- Phase 4: vertical dependence on 5x5-unique domains (7d svs JOIN wcv)
-- ============================================================
WITH dsd AS (SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) n_ds, LOGICAL_OR(data_source_id=25) in_5x5 FROM dsd GROUP BY domain),
j AS (SELECT w.bucket_id, w.vertical_name, (d.in_5x5 AND d.n_ds=1) AS is_5x5_unique FROM dom d JOIN wcv w ON w.domain_name=d.domain)
SELECT bucket_id, vertical_name, COUNT(*) AS classified_domains, COUNTIF(is_5x5_unique) AS d_5x5_unique,
       ROUND(100*COUNTIF(is_5x5_unique)/COUNT(*),1) AS pct_dependent_on_5x5
FROM j GROUP BY 1,2 HAVING classified_domains >= 500 ORDER BY pct_dependent_on_5x5 DESC LIMIT 25;

-- ============================================================
-- Phase 3: per-vendor uniqueness comparison (answers "compare to other DDPs") — 7d svs JOIN wcv
-- ============================================================
WITH dsd AS (SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) n_ds FROM dsd GROUP BY domain),
dsd2 AS (SELECT d.data_source_id, d.domain, m.n_ds, (w.domain_name IS NOT NULL) AS classified
         FROM dsd d JOIN dom m USING(domain) LEFT JOIN wcv w ON w.domain_name=d.domain)
SELECT data_source_id, COUNT(*) AS total_domains, COUNTIF(classified) AS classified_domains,
       COUNTIF(n_ds=1) AS unique_domains, COUNTIF(n_ds=1 AND classified) AS unique_classified,
       ROUND(100*COUNTIF(n_ds=1)/COUNT(*),1) AS pct_unique
FROM dsd2 GROUP BY data_source_id ORDER BY unique_classified DESC;

-- ============================================================
-- Phase 3b: vendor IP score-tier mix (delivered MM household_score) — 7d svs + cost_impression_log
-- cost_impression_log = delivered scores (1.75 GB/day). Full universe (prospecting_intent_daily) is 19.4 TB/day.
-- ============================================================
WITH scored AS (
  SELECT ip, MAX(household_score) AS sc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-06-09' AND '2026-06-15'
  GROUP BY ip
),
vip AS (SELECT DISTINCT data_source_id, ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'),
j AS (SELECT v.data_source_id, v.ip, s.sc FROM vip v LEFT JOIN scored s USING(ip))
SELECT data_source_id,
  COUNT(*) AS vendor_ips,
  COUNTIF(sc IS NOT NULL) AS delivered_ips,
  ROUND(100*COUNTIF(sc IS NOT NULL)/COUNT(*),1) AS pct_delivered,
  COUNTIF(sc = 10000) AS hi_10000, COUNTIF(sc = 8000) AS pp_8000,
  COUNTIF(sc BETWEEN 6666 AND 9999 AND sc <> 8000) AS high_grad,
  COUNTIF(sc BETWEEN 3333 AND 6665) AS mid, COUNTIF(sc BETWEEN 1 AND 3332) AS maxreach,
  COUNTIF(sc <= 0) AS unscored_delivered,
  ROUND(100*COUNTIF(sc>=6666)/NULLIF(COUNTIF(sc IS NOT NULL),0),1) AS pct_of_delivered_high
FROM j GROUP BY data_source_id ORDER BY vendor_ips DESC;

-- ============================================================
-- PHASE 2 (data valuation): cardinality — events/IPs/domains/(IP×domain)/(IP×url) per vendor (1 day svs)
-- ============================================================
SELECT data_source_id, COUNT(*) AS events,
  APPROX_COUNT_DISTINCT(ip) AS ips,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS domains,
  APPROX_COUNT_DISTINCT(CONCAT(ip,'|',IFNULL(NET.REG_DOMAIN(url),''))) AS ip_domain_pairs,
  APPROX_COUNT_DISTINCT(CONCAT(ip,'|',IFNULL(url,''))) AS ip_url_pairs
FROM svs GROUP BY data_source_id ORDER BY events DESC;

-- PHASE 2: layered uniqueness — 5x5 (IP×domain) pair uniqueness vs all vendors (1 day svs)
WITH p AS (SELECT DISTINCT data_source_id, ip, NET.REG_DOMAIN(url) AS domain
           FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL),
pm AS (SELECT ip, domain, COUNT(DISTINCT data_source_id) AS n_ds,
              LOGICAL_OR(data_source_id IN (23,30)) AS in_internal, LOGICAL_OR(data_source_id=25) AS in_5x5
       FROM p GROUP BY ip, domain)
SELECT COUNT(*) AS universe_pairs, COUNTIF(in_5x5) AS p_5x5, COUNTIF(in_5x5 AND n_ds=1) AS p_5x5_unique,
       COUNTIF(in_5x5 AND in_internal) AS p_5x5_also_internal,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNTIF(in_5x5),1) AS pct_5x5_pairs_unique
FROM pm;

-- PHASE 2 (WTP anchor): impressions + media/data spend + high-intent for 5x5 IPs (all + unique), CIL × svs (1 day)
WITH ipm AS (
  SELECT ip, LOGICAL_OR(data_source_id=25) AS in_5x5, COUNT(DISTINCT data_source_id) AS n_ds
  FROM (SELECT DISTINCT data_source_id, ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%') GROUP BY ip),
imp AS (
  SELECT ip, COUNT(*) AS impressions, SUM(media_spend) AS media_spend, SUM(data_spend) AS data_spend, MAX(household_score) AS sc
  FROM `dw-main-silver.logdata.cost_impression_log` WHERE DATE(time) = '2026-06-15' GROUP BY ip)
SELECT COUNTIF(m.in_5x5) AS ips_5x5, COUNTIF(m.in_5x5 AND m.n_ds=1) AS ips_5x5_unique,
  SUM(IF(m.in_5x5,i.impressions,0)) AS impr_5x5, SUM(IF(m.in_5x5 AND m.n_ds=1,i.impressions,0)) AS impr_5x5_unique,
  ROUND(SUM(IF(m.in_5x5,i.media_spend,0)),0) AS media_spend_5x5, ROUND(SUM(IF(m.in_5x5,i.data_spend,0)),0) AS data_spend_5x5,
  SUM(IF(m.in_5x5 AND i.sc>=6666,i.impressions,0)) AS impr_5x5_high
FROM ipm m JOIN imp i USING(ip);

-- ============================================================
-- PHASE 3 (recency/spend): per-vendor touched impressions + media/data spend (1 day, CIL × svs; heavy IP overlap → relative only)
-- ============================================================
WITH ipm AS (SELECT ip, ARRAY_AGG(DISTINCT data_source_id) AS ds_list
             FROM (SELECT DISTINCT data_source_id, ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%') GROUP BY ip),
imp AS (SELECT ip, COUNT(*) AS impr, SUM(media_spend) AS media, SUM(data_spend) AS data
        FROM `dw-main-silver.logdata.cost_impression_log` WHERE DATE(time)='2026-06-15' GROUP BY ip)
SELECT ds AS data_source_id, SUM(i.impr) AS impr_touched, ROUND(SUM(i.media),0) AS media_spend_day, ROUND(SUM(i.data),0) AS data_spend_day
FROM imp i JOIN ipm m USING(ip), UNNEST(m.ds_list) AS ds GROUP BY ds ORDER BY impr_touched DESC;

-- PHASE 3: 5x5 30-day recency — sole-in-window vs freshest (targeting window = 30d). svs over 30 daily partitions.
WITH p AS (
  SELECT ip, NET.REG_DOMAIN(url) AS domain,
         MAX(IF(data_source_id=25, dt, NULL)) AS dt_5x5,
         MAX(IF(data_source_id<>25, dt, NULL)) AS dt_other
  FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL
  GROUP BY ip, domain)
SELECT COUNTIF(dt_5x5 IS NOT NULL) AS pairs_5x5_30d,
       COUNTIF(dt_5x5 IS NOT NULL AND dt_other IS NULL) AS pairs_5x5_sole_30d,
       ROUND(100*COUNTIF(dt_5x5 IS NOT NULL AND dt_other IS NULL)/COUNTIF(dt_5x5 IS NOT NULL),1) AS pct_sole_30d,
       ROUND(100*COUNTIF(dt_5x5 IS NOT NULL AND (dt_other IS NULL OR dt_5x5>=dt_other))/COUNTIF(dt_5x5 IS NOT NULL),1) AS pct_sole_or_freshest
FROM p;

-- ============================================================
-- PHASE 4 (per-IP depth): per-vendor UNIQUE (IP,domain) pairs (1 day svs). Combine with cardinality for
-- visits/IP, domains/IP, unique-domains/IP. Shows raw volume != value (33Across huge but shallow/redundant).
-- ============================================================
WITH p AS (SELECT DISTINCT data_source_id, ip, NET.REG_DOMAIN(url) AS domain
           FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL),
pm AS (SELECT ip, domain, COUNT(DISTINCT data_source_id) AS n_ds, MIN(data_source_id) AS sole_ds FROM p GROUP BY ip, domain)
SELECT sole_ds AS data_source_id, COUNT(*) AS unique_ip_domain_pairs
FROM pm WHERE n_ds=1 GROUP BY sole_ds ORDER BY unique_ip_domain_pairs DESC;

-- ============================================================
-- PHASE 5 (additivity): are vendors additive or sharing the same (IP,domain)? (1 day svs)
-- Q1 pair multiplicity: what % of distinct (ip,domain) pairs come from a single vendor.
WITH p AS (SELECT DISTINCT data_source_id, ip, NET.REG_DOMAIN(url) AS domain
           FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL),
pm AS (SELECT ip, domain, COUNT(DISTINCT data_source_id) AS n_ds FROM p GROUP BY ip, domain)
SELECT COUNT(*) total_distinct_pairs, COUNTIF(n_ds=1) pairs_1_vendor,
       ROUND(100*COUNTIF(n_ds=1)/COUNT(*),1) pct_single_vendor, ROUND(AVG(n_ds),3) avg_vendors_per_pair
FROM pm;
-- Q2 per-IP additivity: union domains vs best-single vendor, by # vendors seeing the IP.
WITH p AS (SELECT DISTINCT data_source_id, ip, NET.REG_DOMAIN(url) AS domain
           FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL),
ipv AS (SELECT ip, data_source_id, COUNT(DISTINCT domain) dom_v FROM p GROUP BY ip, data_source_id),
ipagg AS (SELECT ip, COUNT(*) n_vendors, SUM(dom_v) sum_dom, MAX(dom_v) max_dom FROM ipv GROUP BY ip),
ipu AS (SELECT ip, COUNT(DISTINCT domain) union_dom FROM p GROUP BY ip)
SELECT a.n_vendors, COUNT(*) n_ips, ROUND(AVG(u.union_dom),2) avg_union_domains,
       ROUND(AVG(a.max_dom),2) avg_best_single, ROUND(AVG(u.union_dom)/AVG(a.max_dom),2) lift_vs_best,
       ROUND(100*(1-AVG(u.union_dom)/AVG(a.sum_dom)),1) pct_overlap
FROM ipagg a JOIN ipu u USING(ip) WHERE a.n_vendors BETWEEN 1 AND 10 GROUP BY a.n_vendors ORDER BY a.n_vendors;

-- ============================================================
-- PHASE 6 (free baseline): 5x5 (IP,domain) pairs net-new vs FREE internal logs (augmentor DS30 + guid DS23) + classifiable.
-- The right value test for a PAID vendor. (DS30 augmentor added to site_visit_signal ~Apr 2026 — recent partitions only.)
-- ============================================================
WITH p AS (SELECT DISTINCT data_source_id, ip, NET.REG_DOMAIN(url) AS domain
           FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL),
pm AS (SELECT ip, domain, LOGICAL_OR(data_source_id=25) AS in_5x5, LOGICAL_OR(data_source_id IN (23,30)) AS in_free_internal
       FROM p GROUP BY ip, domain),
j AS (SELECT pm.ip, pm.domain, in_5x5, in_free_internal, (w.domain_name IS NOT NULL) AS classified
      FROM pm LEFT JOIN wcv w ON w.domain_name = pm.domain)
SELECT COUNTIF(in_5x5) p_5x5,
       COUNTIF(in_5x5 AND in_free_internal) p_5x5_also_free,
       COUNTIF(in_5x5 AND NOT in_free_internal) p_5x5_netnew_vs_free,
       COUNTIF(in_5x5 AND NOT in_free_internal AND classified) p_5x5_netnew_classified,
       ROUND(100*COUNTIF(in_5x5 AND NOT in_free_internal)/COUNTIF(in_5x5),1) pct_netnew_vs_free,
       ROUND(100*COUNTIF(in_5x5 AND NOT in_free_internal AND classified)/COUNTIF(in_5x5),1) pct_netnew_classified
FROM j;
