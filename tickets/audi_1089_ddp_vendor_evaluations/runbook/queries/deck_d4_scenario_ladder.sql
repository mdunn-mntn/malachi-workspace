-- ============================================================================
-- AUDI-1089 DECK QUERY D4 of 7: the 9 drop-scenarios — kept coverage + HI/PP
-- FILLS: deck workbook SCENARIO table (block 4, identified by column titles —
--        row positions shift as the sheet evolves): "Total triples kept (of N
--        possible today)", "Coverage (% of today)", "Triples LOST vs today"
--        (= the today row's trips_kept minus the scenario's — sheet arithmetic
--        over this query's output), "HI triples kept", "HI-IP coverage %"
--        (IP-GRAIN — see grain note), "PP triples kept", "PP-IP coverage %".
--
-- Claim: one scan builds BOTH histograms every scenario needs:
--   triple grain — (ip x domain x date) holder masks, split by the IP's score
--     tier: fills C/D (all triples) and E/G (triples on HI/PP IPs);
--   IP grain — per-IP holder masks (BIT_OR of the IP's triple masks), split by
--     tier: fills F/H = % of HI (PP) member IPs a scenario still covers.
-- A triple/IP survives a scenario iff at least one KEPT source holds it.
-- "Today" = all 8 paid + both free logs = the whole universe (100% row).
--
-- GRAIN NOTE (do not mix in the deck): E/G count TRIPLES on HI/PP IPs — under
-- free-logs-only they drop to roughly the all-triple retention (~60%). F/H count
-- IPs — they stay >= 99% because virtually every scored IP is free-covered
-- (the workbook's q3d result: free-only keeps 99.94% of HI / 99.25% of PP IPs
-- on the 37d window; this query's 30d window lands within ~0.1pp). Audience
-- membership is IP-grain — F/H are the "does the audience shrink" columns;
-- E/G measure signal volume on those IPs, not audience size. (The 30d window
-- reads HI/PP IP coverage ~0.2-0.8pp below q3d's 37d figures — see the
-- reconciliation note.)
--
-- HI/PP tier is assigned at IP grain: per-IP MAX(household_score) over the CIL
-- valuation week (HI = 10000 exactly, PP = 8000 exactly — the score pipeline
-- pins these tier values). An IP not served that week (or scored below) counts
-- in neither. Windows differ on purpose (triples/IP-masks 30d, tiers the CIL
-- valuation week 2026-07-02..08).
--
-- Scenario keep-set masks (bit order ds 23,24,25,26,28,30,33,36,39,40 = bits
-- 0..9; free logs = bits 0+5 = 33, kept in every paid scenario):
--   1023 today: all 8 paid + free            831 drop Sovrn(33)+Cybba(36)
--    575 + drop Klickly(39)                  573 + drop Justuno(24) = the k=4 knee
--    561 33Across(28)+33A API(40) only        45 flat-fee survivors: 5x5(25)+
--        Predactiv(26) (Klickly is also flat-fee but already dropped at step 3)
--     33 free logs only                        32 augmentor ds30 alone
--      1 guid_log ds23 alone
--
-- ARCHITECTURE NOTE (cost): CTE re-references RE-READ temp external tables
-- (house-measured trap). The chain trips -> trip_g -> ipx -> ipg -> flat ->
-- hist -> final references each svs-reading CTE EXACTLY ONCE; both histograms
-- come out of ONE GROUPING SETS aggregation. Externals scanned once.
--
-- Expected reconciliation vs the landed run (deck_d4_scenario_ladder.csv,
-- 2026-07-16): today 13,286,674,041 triples (== q3c mask universe within
-- 0.00003% live-snapshot drift); free_logs_only trips_kept 7,887,062,821
-- (59.36%), hi_ip_coverage 99.7579, pp 98.4920; k4 hi 99.9975. WINDOW NOTE:
-- these IP coverages sit slightly below q3d's 37d-window numbers (HI 99.94,
-- PP 99.25) because this query's IP masks use the 30d window — a real,
-- expected gap, same conclusion (every paid-drop scenario keeps 99.9%+ of HI).
--
-- BIG SCAN (svs 30d + wcv + pc single pass + CIL week; shuffle-heavy; ~1.5-2h)
-- — dry-run, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=20 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d4_scenario_ladder.sql)" \
--     > deck_d4_scenario_ladder.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30; VALUE week 2026-07-02..08
-- ============================================================================

WITH usable_dom AS (
  SELECT DISTINCT domain_name AS dom
  FROM wcv
  WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')
  UNION DISTINCT
  SELECT DISTINCT NET.REG_DOMAIN(composite_key) AS dom
  FROM pc
  WHERE NET.REG_DOMAIN(composite_key) IS NOT NULL
    AND (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

trips AS (
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    s.dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3, 4
),

trip_g AS (
  SELECT ip, dom, dt,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask
  FROM trips
  GROUP BY ip, dom, dt
),

-- per-IP compaction: triple-mask histogram per IP (ipx), then one row per IP
-- carrying its overall holder mask + its compact histogram (ipg)
ipx AS (
  SELECT ip, tmask, COUNT(*) AS n
  FROM trip_g
  GROUP BY ip, tmask
),

ipg AS (
  SELECT ip, BIT_OR(tmask) AS ip_mask, ARRAY_AGG(STRUCT(tmask AS m, n)) AS h
  FROM ipx
  GROUP BY ip
),

ip_tier AS (
  SELECT ip,
         CASE MAX(household_score) WHEN 10000 THEN 'hi' WHEN 8000 THEN 'pp' END AS tier
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

flat AS (
  SELECT g.ip, g.ip_mask, t.tier, x.m AS tmask, x.n
  FROM ipg g
  LEFT JOIN ip_tier t USING (ip),
  UNNEST(g.h) AS x
),

-- ONE aggregation, two grouping sets:
--   (tmask, tier)   -> triple histogram  (g_ipmask = 1): use SUM(n)
--   (ip_mask, tier) -> IP histogram      (g_tmask = 1): use COUNT(DISTINCT ip)
hist AS (
  SELECT tmask, ip_mask, tier,
         SUM(n) AS trips,
         COUNT(DISTINCT ip) AS ips,
         GROUPING(tmask) AS g_tmask,
         GROUPING(ip_mask) AS g_ipmask
  FROM flat
  GROUP BY GROUPING SETS ((tmask, tier), (ip_mask, tier))
),

scen AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS ord, 'today_all_8_paid' AS scenario, 1023 AS keepmask),
    (2, 'drop_sovrn_cybba', 831),
    (3, 'plus_drop_klickly', 575),
    (4, 'plus_drop_justuno_k4', 573),
    (5, '33across_combined_only', 561),
    (6, 'flat_fee_only_5x5_predactiv', 45),
    (7, 'free_logs_only', 33),
    (8, 'augmentor_ds30_only', 32),
    (9, 'guid_log_ds23_only', 1)
  ])
)

SELECT
  s.ord,
  s.scenario,
  SUM(IF(h.g_ipmask = 1 AND (h.tmask & s.keepmask) != 0, h.trips, 0)) AS trips_kept,
  ROUND(100 * SUM(IF(h.g_ipmask = 1 AND (h.tmask & s.keepmask) != 0, h.trips, 0))
        / SUM(IF(h.g_ipmask = 1, h.trips, 0)), 2) AS pct_of_today,
  SUM(IF(h.g_ipmask = 1 AND h.tier = 'hi' AND (h.tmask & s.keepmask) != 0, h.trips, 0)) AS hi_trips_kept,
  ROUND(100 * SUM(IF(h.g_tmask = 1 AND h.tier = 'hi' AND (h.ip_mask & s.keepmask) != 0, h.ips, 0))
        / SUM(IF(h.g_tmask = 1 AND h.tier = 'hi', h.ips, 0)), 4) AS hi_ip_coverage_pct,
  SUM(IF(h.g_ipmask = 1 AND h.tier = 'pp' AND (h.tmask & s.keepmask) != 0, h.trips, 0)) AS pp_trips_kept,
  ROUND(100 * SUM(IF(h.g_tmask = 1 AND h.tier = 'pp' AND (h.ip_mask & s.keepmask) != 0, h.ips, 0))
        / SUM(IF(h.g_tmask = 1 AND h.tier = 'pp', h.ips, 0)), 4) AS pp_ip_coverage_pct
FROM scen s
CROSS JOIN hist h
GROUP BY s.ord, s.scenario
ORDER BY s.ord;
