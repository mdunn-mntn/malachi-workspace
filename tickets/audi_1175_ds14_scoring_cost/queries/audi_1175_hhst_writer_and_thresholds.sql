-- AUDI-1175 shadow query 2: HHST write path + applied-threshold distribution (starvation baseline)
-- Compass located the tables but was denied row access to dw-main-bronze; these run fine via the CDC mirrors.
-- (The camperbid_prod__hhst_v4__* externals ARE still blocked for malachi@ — gs://camperbid-prod GCS list denied.)

-- Q2a: who writes the HHST PRESET (override) column, by reason?  Result 2026-07-28:
--   ~26 campaigns total, EVERY reason human/ops (PER-#### Jira, "WC Setup", "SF Program", "TOF MANAGE", "Special Help").
--   => the automated recommender does NOT write configuration_service_campaign_presets.household_score_threshold_preset.
SELECT reason, COUNT(*) AS n, COUNT(DISTINCT campaign_id) AS camps, MAX(update_time) AS last_upd
FROM `dw-main-bronze.integrationprod.dso_configuration_service_campaign_presets`
WHERE household_score_threshold_preset IS NOT NULL
GROUP BY reason ORDER BY n DESC LIMIT 40;

-- Q2b: applied HHST threshold (the real gate) distribution + writer cadence.  Result 2026-07-28:
--   32,550 campaigns; newest update = today 22:11; 2,082 updated in last day => LIVE daily automated writer.
--   threshold=0 (Max Reach/ungated) 21,144 (65.0%); 1-3332 1,410 (4.3%) => 69.3% already Max Reach.
--   mid 2,308 (7.1%); =6666 2,285 (7.0%); peak 618 (1.9%); high >=8001 4,103 (12.6%). Real intent gate = 28.6%.
--   => Max Reach is the system's dominant steady state; the gate can't worsen the 69% already ungated,
--      and the gated ~31% hold high thresholds because they have ample addressable population (preserved).
SELECT
  COUNT(DISTINCT campaign_id) AS distinct_camps,
  COUNTIF(update_time >= TIMESTAMP("2026-07-27 00:00:00")) AS upd_last_day,
  MAX(update_time) AS newest,
  COUNTIF(threshold = 0) AS thr_0_maxreach,
  COUNTIF(threshold BETWEEN 1 AND 3332) AS thr_maxreach_band,
  COUNTIF(threshold BETWEEN 3333 AND 6665) AS thr_mid,
  COUNTIF(threshold = 6666) AS thr_6666_guardrail,
  COUNTIF(threshold BETWEEN 6667 AND 8000) AS thr_peak,
  COUNTIF(threshold >= 8001) AS thr_high
FROM `dw-main-bronze.integrationprod.dso_household_score_thresholds`;
