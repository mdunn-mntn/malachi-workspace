-- ============================================================================
-- AUDI-1117 Q3: svs vs DS14 gate — per-source biddable share + pool expansion
--
-- Claim: ONE 30d svs scan (2026-06-02..2026-07-01) sizes, at IP grain, the
-- meeting's two questions ("query what's in site_visit_signal that's not in
-- DS14" / "add these IPs to DS14 to make a larger pool"):
--   per source (rec='source', 10 rows): ips_total = distinct IPv4 IPs the
--     source delivered in 30d; ips_in_gate = those satisfying the DS14 proxy
--     at window end (aug_log within 1d OR guid_log within 4d of 2026-07-01);
--     pct_in_gate = the share of the source's delivered IPs that are even
--     BIDDABLE under today's gate.
--   pool rows (rec='pool'): gate_pool_today = all svs IPs in-gate;
--     expansion_all = svs IPs OUT of gate (the "add svs to DS14" candidates);
--     expansion_free_stale = out-of-gate IPs the free logs delivered anyway
--     in the 30d window (pool growth WITHOUT vendors — just widen the free
--     windows); expansion_vendor_only = out-of-gate IPs ONLY vendors
--     delivered in 30d (growth that actually requires paying vendors).
--   rec='universe' = all svs IPs in the window.
--
-- Gate proxy caveat: DS14 windows are the DOCUMENTED aug(1d)/guid(4d),
-- partition-day grain; Q2 showed the realized CTV edge is softer (graph
-- expansion / churn candidates) — this measures against the documented gate,
-- which is the right benchmark for the pool-expansion decision.
--
-- ARCHITECTURE NOTE (cost): svs read EXACTLY ONCE — per-IP aggregate
-- (mask, in_gate) collapsed to a histogram, then array arithmetic (house
-- single-pass pattern).
--
-- BIG SCAN (svs 30d, IP-grain GROUP BY) — dry-run, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1117_ds14_overlap_sizing.sql)" \
--     > audi_1117_ds14_overlap_sizing.csv
--
-- Parameters: WINDOW = 2026-06-02..2026-07-01 (30d); GATE_REF = 2026-07-01,
-- aug window 1d (last_aug >= 2026-06-30), guid window 4d (last_guid >=
-- 2026-06-27). Bit order: ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9;
-- free mask 33; paid mask 990.
-- ============================================================================

WITH ipg AS (
  SELECT
    ip,
    BIT_OR(1 << (CASE CAST(data_source_id AS INT64)
                   WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                   WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                   WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m,
    MAX(IF(CAST(data_source_id AS INT64) = 30, CAST(dt AS DATE), NULL)) AS last_aug,
    MAX(IF(CAST(data_source_id AS INT64) = 23, CAST(dt AS DATE), NULL)) AS last_guid
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

mhist AS (
  SELECT
    m,
    (last_aug >= DATE '2026-06-30' OR last_guid >= DATE '2026-06-27') AS in_gate,
    COUNT(*) AS ips
  FROM ipg
  GROUP BY 1, 2
),

hist AS (
  SELECT ARRAY_AGG(STRUCT(m, IFNULL(in_gate, FALSE) AS ok, ips)) AS h
  FROM mhist
)

SELECT o.rec, o.key, o.ips_total, o.ips_in_gate,
       ROUND(100 * o.ips_in_gate / o.ips_total, 2) AS pct_in_gate
FROM hist r,
UNNEST(ARRAY_CONCAT(
  ARRAY(
    SELECT AS STRUCT 'source' AS rec, CAST(s.ds AS STRING) AS key,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE ((x.m >> s.bit) & 1) = 1) AS ips_total,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE ((x.m >> s.bit) & 1) = 1 AND x.ok) AS ips_in_gate
    FROM UNNEST([STRUCT(23 AS ds, 0 AS bit), (24, 1), (25, 2), (26, 3), (28, 4),
                 (30, 5), (33, 6), (36, 7), (39, 8), (40, 9)]) s
  ),
  [
    STRUCT('pool' AS rec, 'gate_pool_today' AS key,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE x.ok) AS ips_total,
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE x.ok) AS ips_in_gate),
    ('pool', 'expansion_all_out_of_gate',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE NOT x.ok),
      0),
    ('pool', 'expansion_free_stale',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE NOT x.ok AND (x.m & 33) != 0),
      0),
    ('pool', 'expansion_vendor_only',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE NOT x.ok AND (x.m & 33) = 0),
      0),
    ('universe', 'all_svs_ips_30d',
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x),
      (SELECT SUM(x.ips) FROM UNNEST(r.h) x WHERE x.ok))
  ]
)) o
ORDER BY o.rec, o.ips_total DESC
