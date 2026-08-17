#!/usr/bin/env bash
# PS-8614 — reproduce the Site Visitors "Other" finding from the guid_geos_raw source files.
#
# geo.guid_geos_summary is Postgres and holds one truncated-daily snapshot, so the proof has to
# run against the hourly parquet that feeds it. Needs read on gs://mntn-data-archive-prod and
# BigQuery job-create on any project. No tables are created; the external definition is in-memory.
#
#   ./ps_8614_repro.sh [ADVERTISER_ID] [YYYY-MM-DD]      defaults: 33129, yesterday
set -euo pipefail

ADV="${1:-33129}"
DAY="${2:-$(date -u -v-1d +%F 2>/dev/null || date -u -d yesterday +%F)}"
BUCKET="gs://mntn-data-archive-prod/guid_geos_raw"
PROJECT="${BQ_PROJECT:-dw-main-silver}"
BQ_CMD="${BQ_CMD:-bq query}"
DEF="$(mktemp -t ggr_def).json"

# BigQuery allows ONE wildcard per source URI and chokes on Databricks _started_/_committed_
# marker files, so enumerate the hour directories and glob only *.parquet inside each.
gsutil ls -d "$BUCKET/dt=*/hh=*" 2>/dev/null \
  | sed 's:/*$::' \
  | python3 -c '
import json,sys
uris=[l.strip()+"/*.parquet" for l in sys.stdin if l.startswith("gs://")]
json.dump({"sourceFormat":"PARQUET","sourceUris":uris,
  "hivePartitioningOptions":{"mode":"AUTO",
    "sourceUriPrefix":"'"$BUCKET"'/"}}, sys.stdout)
' > "$DEF"
echo "external definition: $(python3 -c 'import json,sys;print(len(json.load(open(sys.argv[1]))["sourceUris"]))' "$DEF") hour partitions available"

run() { $BQ_CMD --location=us-central1 --project_id="$PROJECT" \
        --external_table_definition="ggr::$DEF" --use_legacy_sql=false \
        --format=pretty --max_rows=100 "$1"; }

echo; echo "== 1. Is it a regression? NULL iso_code rate per day, advertiser $ADV vs platform =="
run "
WITH pairs AS (
  SELECT dt, advertiser_id, ip, MAX(IF(iso_code IS NULL OR iso_code = '', 1, 0)) AS null_iso
  FROM ggr GROUP BY dt, advertiser_id, ip
)
SELECT dt, scope, pairs, null_pairs, ROUND(100 * null_pairs / pairs, 2) AS pct_other FROM (
  SELECT dt, 'platform' AS scope, COUNT(*) AS pairs, SUM(null_iso) AS null_pairs FROM pairs GROUP BY dt
  UNION ALL
  SELECT dt, 'adv_$ADV', COUNT(*), SUM(null_iso) FROM pairs WHERE advertiser_id = $ADV GROUP BY dt
) ORDER BY scope, dt"

echo; echo "== 2. Why is iso_code NULL? Three causes, $DAY. Only 'us_but_no_state' is a defect =="
# location_ids is the location_data.hierarchy chain; location_id 237 = United States.
run "
WITH r AS (
  SELECT advertiser_id, ip, iso_code,
         ARRAY_LENGTH(location_ids.list) AS n_loc,
         EXISTS(SELECT 1 FROM UNNEST(location_ids.list) e WHERE e.element = '237') AS has_us
  FROM ggr WHERE dt = '$DAY'
)
SELECT IF(advertiser_id = $ADV, 'adv_$ADV', 'all_others') AS scope,
       CASE WHEN iso_code IS NOT NULL AND iso_code <> '' THEN '1_us_state_resolved'
            WHEN IFNULL(n_loc, 0) = 0                    THEN '2_no_geo_match_at_all'
            WHEN has_us                                  THEN '3_us_but_no_state (DEFECT)'
            ELSE                                              '4_non_us (expected Other)'
       END AS cause,
       COUNT(DISTINCT CONCAT(CAST(advertiser_id AS STRING), '|', ip)) AS ip_adv_pairs
FROM r GROUP BY scope, cause ORDER BY scope, cause"

echo; echo "== 3. Where are the non-US visitors? Country mix for advertiser $ADV, $DAY =="
run "
WITH r AS (
  SELECT ip, location_ids.list[SAFE_OFFSET(0)].element AS country_loc_id
  FROM ggr
  WHERE dt = '$DAY' AND advertiser_id = $ADV
    AND (iso_code IS NULL OR iso_code = '') AND ARRAY_LENGTH(location_ids.list) > 0
  GROUP BY ip, country_loc_id
)
SELECT COALESCE(c.location, CONCAT('location_id ', r.country_loc_id)) AS country,
       c.country_iso_code,
       COUNT(*) AS distinct_ips,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_non_us
FROM r
LEFT JOIN (SELECT DISTINCT location_id, location, country_iso_code
           FROM \`dw-main-bronze.geo.location_data\` WHERE location_type_id = 2) c
  ON CAST(r.country_loc_id AS INT64) = c.location_id
GROUP BY 1, 2 ORDER BY distinct_ips DESC LIMIT 15"

rm -f "$DEF"
