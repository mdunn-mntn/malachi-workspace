-- AUDI-1142 blast radius: Shopper Graph /vertical shared-domain miss population.
-- All run 2026-08-24 via .claude/scripts/bq_run.sh, --project_id=dw-main-bronze unless noted.
-- Domain normalization: lowercase, trim, strip protocol + leading www., cut at first /?#: or space,
-- then require a "." (drops 105 malformed rows). Validated on 10 literal URL cases before use;
-- an earlier double-backslash regex draft was discarded (details in outputs/audi_1142_blast_radius.md).
-- Results per query are on the one-line RESULT comments; full table + caveats in the outputs doc.

-- D1 RESULT 2026-08-24: url=37,801 valid=37,696 domains=30,503 shared_domains=2,018 aids_on_shared=9,211 max=955 (youtube.com)
WITH active AS (
  SELECT advertiser_id,
         REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(company_url)), "^https?://", ""), "^www[.]", ""), "^([^/?#: ]+)") AS domain
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE AND is_test = FALSE
    AND company_url IS NOT NULL AND TRIM(company_url) != ""
), valid AS (
  SELECT advertiser_id, domain FROM active WHERE domain IS NOT NULL AND domain LIKE "%.%"
), by_domain AS (
  SELECT domain, COUNT(DISTINCT advertiser_id) AS n_aids
  FROM valid GROUP BY domain
)
SELECT
  (SELECT COUNT(*) FROM active) AS active_aids_nonempty_url,
  (SELECT COUNT(*) FROM valid) AS active_aids_valid_domain,
  (SELECT COUNT(*) FROM by_domain) AS distinct_domains,
  (SELECT COUNT(*) FROM by_domain WHERE n_aids > 1) AS shared_domains,
  (SELECT SUM(n_aids) FROM by_domain WHERE n_aids > 1) AS aids_on_shared_domains,
  (SELECT MAX(n_aids) FROM by_domain) AS max_aids_one_domain;

-- D1b RESULT 2026-08-24 (top shared domains): youtube.com 955, google.com 326, mountain.com 294, instagram.com 259, facebook.com 251, gmail.com 238, tiktok.com 229, auth.mountain.com 203, orangetheory.com 149, youtu.be 104, metalsupermarkets.com 103, amazon.com 79, linkedin.com 75, example.com 70, linktr.ee 51
WITH active AS (
  SELECT advertiser_id,
         REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(company_url)), "^https?://", ""), "^www[.]", ""), "^([^/?#: ]+)") AS domain
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE AND is_test = FALSE
    AND company_url IS NOT NULL AND TRIM(company_url) != ""
)
SELECT domain, COUNT(DISTINCT advertiser_id) AS n_aids
FROM active WHERE domain IS NOT NULL AND domain LIKE "%.%"
GROUP BY domain HAVING n_aids > 1
ORDER BY n_aids DESC LIMIT 15;

-- D2 RESULT 2026-08-24: shared_domain_aids=9,211; shared_aids_no_vertical_row=2,740; shared_aids_no_type1_row=2,740 (identical, 2-rows-per-AID grain); all_active_aids=37,802; active_aids_no_vertical_row=8,025
WITH active AS (
  SELECT advertiser_id,
         REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(company_url)), "^https?://", ""), "^www[.]", ""), "^([^/?#: ]+)") AS domain
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE deleted = FALSE AND is_test = FALSE
    AND company_url IS NOT NULL AND TRIM(company_url) != ""
), valid AS (
  SELECT advertiser_id, domain FROM active WHERE domain IS NOT NULL AND domain LIKE "%.%"
), shared AS (
  SELECT advertiser_id
  FROM valid
  QUALIFY COUNT(DISTINCT advertiser_id) OVER (PARTITION BY domain) > 1
), vert AS (
  SELECT DISTINCT advertiser_id FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
), vert1 AS (
  SELECT DISTINCT advertiser_id FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals` WHERE type = 1
), all_active AS (
  SELECT advertiser_id FROM `dw-main-bronze.integrationprod.advertisers` WHERE deleted = FALSE AND is_test = FALSE
)
SELECT
  (SELECT COUNT(*) FROM shared) AS shared_domain_aids,
  (SELECT COUNT(*) FROM shared s LEFT JOIN vert v USING (advertiser_id) WHERE v.advertiser_id IS NULL) AS shared_aids_no_vertical_row,
  (SELECT COUNT(*) FROM shared s LEFT JOIN vert1 v USING (advertiser_id) WHERE v.advertiser_id IS NULL) AS shared_aids_no_type1_row,
  (SELECT COUNT(*) FROM all_active) AS all_active_aids,
  (SELECT COUNT(*) FROM all_active a LEFT JOIN vert v USING (advertiser_id) WHERE v.advertiser_id IS NULL) AS active_aids_no_vertical_row;

-- D3/D4 BLOCKED 2026-08-24: mm_domain_map has no BQ mirror (this search, repeated per project x region over dw-main-bronze/silver/gold in us-central1 and US, returns nothing; only fpa_advertiser_verticals + fpa_categories are replicated from the fpa schema); reconfirming the ~561 mismatches needs Postgres fpa.mm_domain_map (knowledge/data_catalog.md:2944).
SELECT table_schema, table_name, table_type
FROM `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.TABLES`
WHERE LOWER(table_name) LIKE "%domain_map%" LIMIT 50;

-- xref RESULT 2026-08-24 (found while searching; run with --project_id=dw-main-silver): dw-main-gold.bae.v_aid_flagged_dup_domain = 823 rows / 823 AIDs / 312 domains, BAE's curated dup-domain subset.
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT advertiser_id) AS n_aids, COUNT(DISTINCT domain) AS n_domains
FROM `dw-main-gold.bae.v_aid_flagged_dup_domain`;
