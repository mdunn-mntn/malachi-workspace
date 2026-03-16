-- TI-650 v22: Segment Membership Trace for IP 216.126.34.185
-- Purpose: Trace HOW this IP entered S3 targeting for campaign_group 93957 (adv 37775)
-- The v21 proof showed the IP has zero S1/S2 impressions in cg 93957.
-- This query checks segment membership tables to understand the targeting path.
--
-- Context:
--   VV: ad_served_id = 80207c6e-1fb9-427b-b019-29e15fb3323c
--   VV date: 2026-02-04, impression date: 2026-01-27
--   VV campaign: 450300 (cg 93957, adv 37775, funnel_level=3, S3 CTV)
--   Campaign group name: "7 2025 Wedding CRM"
--   IP: 216.126.34.185
--
-- Key facts:
--   - tmul_daily: DS 2 and DS 3 only (14-day TTL). DS 4 (CRM) is NOT in tmul_daily.
--   - tpa_membership_update_log: has segment updates, longer retention
--   - ipdsc__v1: CRM IP resolution (data_source_id=4)
--
-- Results (2026-03-16):
--
-- Q1: tmul_daily, adv 37775 (14-day TTL, only recent data Mar 3-16)
--   - 11 segments per day, ALL data_source_id=2 (MNTN First Party / OPM)
--   - Campaign IDs: 394579, 443864, 324472, 443847, 443863, 260989, 443867
--   - ALL are retargeting campaigns (objective_id=4) from OTHER campaign groups
--   - ZERO campaigns from cg 93957 (450300-450305)
--   - Conclusion: IP has no DS 2/3 targeting membership for cg 93957

-- Q1: tmul_daily — advertiser 37775 segments (14-day window only)
SELECT
  td.id AS ip,
  DATE(td.time) AS dt,
  td.data_source_id,
  isl.element.segment_id,
  isl.element.advertiser_id,
  isl.element.campaign_id
FROM `dw-main-bronze.raw.tmul_daily` td,
  UNNEST(td.in_segments.list) AS isl
WHERE td.id = '216.126.34.185'
  AND td.time >= TIMESTAMP('2026-03-01')
  AND isl.element.advertiser_id = 37775
ORDER BY dt, isl.element.segment_id
LIMIT 100;

-- Q2: tpa_membership_update_log — full retention, advertiser 37775
SELECT
  td.id AS ip,
  DATE(td.time) AS dt,
  td.data_source_id,
  isl.segment_id,
  isl.advertiser_id,
  isl.campaign_id
FROM `dw-main-bronze.raw.tpa_membership_update_log` td,
  UNNEST(td.in_segments.segments) AS isl
WHERE td.id = '216.126.34.185'
  AND td.time >= TIMESTAMP('2025-07-01')
ORDER BY dt, isl.segment_id
LIMIT 100;

-- Q3: ipdsc__v1 — CRM IP resolution check
SELECT *
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE ip = '216.126.34.185'
ORDER BY dt DESC
LIMIT 50;
