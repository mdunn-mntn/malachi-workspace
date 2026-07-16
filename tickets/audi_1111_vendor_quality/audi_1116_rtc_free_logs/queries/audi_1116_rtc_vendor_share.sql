-- ============================================================================
-- AUDI-1116 Q2: RTC-fired impressions — free-log vs vendor dependence
--
-- Claim: ONE 37d svs membership scan + ONE CIL valuation-week scan splits
-- RTC-FIRED won impressions (model_params token realtime_conquest_score=10000
-- — the token KEY is on ~100% of rows, only VALUE 10000 = RTC fired) by which
-- svs sources delivered the impression IP:
--   rec='path'    : guid_realtime (guid_log delivered the IP — the Kafka
--                   streaming pipeline could have qualified it), vs
--                   hourly_batch_only (svs membership WITHOUT guid_log — only
--                   the TI hourly batch could have qualified it), vs
--                   no_svs_membership (RTC fired but the IP has no 37d svs
--                   row — qualification predates the window or another feed).
--   rec='renewal' : free_covered (guid OR augmentor delivered the IP) vs
--                   vendor_only (only paid vendors did) vs no_svs_membership —
--                   vendor_only = the RTC volume at risk if all 8 vendors drop.
--   rec='source'  : per-source touched RTC imps (overlapping, non-additive).
--   rec='total'   : all RTC-fired imps/IPs in the week.
--
-- Context (2026-07-16 readout): RTC = two pipelines — guid_log via Kafka
-- (~real-time, Zach S.) + TI-run HOURLY batch over svs-minus-guid. So vendors
-- CAN drive RTC firings; this measures how much of realized RTC actually
-- depends on them.
--
-- Grain/hygiene: RAW 37d svs membership at IP grain (IPv4 both sides, NO
-- usable-domain gate) — the serving-cohort convention shared with deck_d2/
-- q6/q8b/q15. Membership window 2026-06-02..2026-07-08 covers the valuation
-- week + 30d lookback (matches the documented 30-day RTC/site-visit scoring
-- lookback).
--
-- ARCHITECTURE NOTE (cost): externals + CIL are each read EXACTLY ONCE —
-- mask histogram collapsed to one ARRAY-carrying row (hist), all output rows
-- derived by array arithmetic (house single-pass pattern).
--
-- BIG-ISH SCAN (svs 37d + CIL week incl model_params) — dry-run, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod; BQ read on dw-main-silver):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1116_rtc_vendor_share.sql)" \
--     > audi_1116_rtc_vendor_share.csv
--
-- Parameters: MEMBERSHIP = 2026-06-02..2026-07-08 (37d), VALUE WEEK =
-- 2026-07-02..2026-07-08. Bit order: ds 23,24,25,26,28,30,33,36,39,40 =
-- bits 0..9; free mask 33; paid mask 990.
-- ============================================================================

WITH mem AS (
  SELECT ip,
         BIT_OR(1 << (CASE CAST(data_source_id AS INT64)
                        WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                        WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                        WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

rtc AS (
  SELECT ip, COUNT(*) AS imps
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
    AND REGEXP_CONTAINS(COALESCE(model_params, ''), r'realtime_conquest_score=10000')
  GROUP BY ip
),

mhist AS (
  SELECT IFNULL(m.m, 0) AS m, COUNT(*) AS ips, SUM(r.imps) AS imps
  FROM rtc r
  LEFT JOIN mem m USING (ip)
  GROUP BY 1
),

hist AS (
  -- single reference to mhist; totals derived from the array downstream so
  -- the externals/CIL are scanned exactly once (house re-read gotcha)
  SELECT ARRAY_AGG(STRUCT(m, ips, imps)) AS h
  FROM mhist
),

tot AS (
  SELECT h,
         (SELECT SUM(x.imps) FROM UNNEST(h) x) AS tot_imps,
         (SELECT SUM(x.ips) FROM UNNEST(h) x) AS tot_ips
  FROM hist
)

SELECT o.rec, o.key, o.rtc_ips, o.rtc_imps,
       ROUND(100 * o.rtc_imps / r.tot_imps, 2) AS pct_rtc_imps
FROM tot r,
UNNEST(ARRAY_CONCAT(
  [
    STRUCT('path' AS rec, 'guid_realtime' AS key,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE (x.m & 1) != 0) AS rtc_ips,
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE (x.m & 1) != 0) AS rtc_imps),
    ('path', 'hourly_batch_only',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE (x.m & 1) = 0 AND x.m != 0),
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE (x.m & 1) = 0 AND x.m != 0)),
    ('path', 'no_svs_membership',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE x.m = 0),
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE x.m = 0)),
    ('renewal', 'free_covered',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE (x.m & 33) != 0),
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE (x.m & 33) != 0)),
    ('renewal', 'vendor_only',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE (x.m & 33) = 0 AND (x.m & 990) != 0),
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE (x.m & 33) = 0 AND (x.m & 990) != 0)),
    ('total', 'all_rtc_fired', r.tot_ips, r.tot_imps)
  ],
  ARRAY(
    SELECT AS STRUCT 'source' AS rec, CAST(s.ds AS STRING) AS key,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE ((x.m >> s.bit) & 1) = 1) AS rtc_ips,
      (SELECT SUM(x.imps) FROM UNNEST(r.h) x WHERE ((x.m >> s.bit) & 1) = 1) AS rtc_imps
    FROM UNNEST([STRUCT(23 AS ds, 0 AS bit), (24, 1), (25, 2), (26, 3), (28, 4),
                 (30, 5), (33, 6), (36, 7), (39, 8), (40, 9)]) s
  )
)) o
ORDER BY o.rec, o.rtc_imps DESC
