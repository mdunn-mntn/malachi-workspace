-- AUDI-1175: scored universe (31d MM/vertical) vs DS14-addressable set (8d TTL)
-- Table: dw-main-bronze.external.ipdsc__v1 (hive-partitioned by dt + data_source_id).
-- MUST filter dt with LITERALS (dynamic MAX(dt) subquery scans 164B rows; literal prunes to ~70-105M/partition).
-- HLL distinct-IP (~1.5% err) to avoid a heavy DISTINCT+JOIN shuffle. Run via .claude/scripts/bq_run.sh, --location=us-central1.
-- Intersection = |A| + |B| - |A ∪ B|  (inclusion-exclusion on the merged sketches).
--
-- Result 2026-07-28 (scored 31d = 2026-06-27..07-27; addressable = DS14 8d = 2026-07-20..27):
--   DS13 scored 31d = 269.8M | DS14 8d = 259.2M | DS14 1d = 149.4M
--   DS19 scored 31d = 499.4M
--   DS13 ∩ DS14_8d = 164.7M (61%)  -> non-addressable 105.1M (39%)
--   DS13 ∩ DS14_1d = 118.7M (44%)  -> non-addressable 151.2M (56%)
--   DS19 ∩ DS14_8d = 156.6M (31%)  -> non-addressable 342.8M (69%)

WITH s AS (
  SELECT "ds13_31" AS grp, HLL_COUNT.INIT(ip) AS sk FROM `dw-main-bronze.external.ipdsc__v1` WHERE data_source_id=13 AND dt BETWEEN "2026-06-27" AND "2026-07-27"
  UNION ALL SELECT "ds19_31", HLL_COUNT.INIT(ip) FROM `dw-main-bronze.external.ipdsc__v1` WHERE data_source_id=19 AND dt BETWEEN "2026-06-27" AND "2026-07-27"
  UNION ALL SELECT "ds14_8",  HLL_COUNT.INIT(ip) FROM `dw-main-bronze.external.ipdsc__v1` WHERE data_source_id=14 AND dt BETWEEN "2026-07-20" AND "2026-07-27"
  UNION ALL SELECT "ds14_1",  HLL_COUNT.INIT(ip) FROM `dw-main-bronze.external.ipdsc__v1` WHERE data_source_id=14 AND dt = "2026-07-27"
)
SELECT
  (SELECT HLL_COUNT.EXTRACT(sk) FROM s WHERE grp="ds13_31") AS scored_ds13_31d,
  (SELECT HLL_COUNT.EXTRACT(sk) FROM s WHERE grp="ds19_31") AS scored_ds19_31d,
  (SELECT HLL_COUNT.EXTRACT(sk) FROM s WHERE grp="ds14_8")  AS addr_ds14_8d,
  (SELECT HLL_COUNT.EXTRACT(sk) FROM s WHERE grp="ds14_1")  AS addr_ds14_1d,
  (SELECT HLL_COUNT.MERGE(sk) FROM s WHERE grp IN ("ds13_31","ds14_8")) AS union_13_14_8d,
  (SELECT HLL_COUNT.MERGE(sk) FROM s WHERE grp IN ("ds13_31","ds14_1")) AS union_13_14_1d,
  (SELECT HLL_COUNT.MERGE(sk) FROM s WHERE grp IN ("ds19_31","ds14_8")) AS union_19_14_8d;
