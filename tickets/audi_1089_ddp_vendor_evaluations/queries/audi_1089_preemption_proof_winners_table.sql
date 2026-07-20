-- PREEMPTION PROOF — does the meter skip paid credit when a free log already covers the impression?
-- Groups June won-impression rows by whether a FREE log (23=guid, 30=augmentor) and/or a PAID
-- vendor (24/28/33/36/40) is among the credited winners, and shows the tv_cpm charged.
--
-- RESULT (verified 2026-07-20, dw-main-gold, 18 GB, $0 reserved):
--   free-only winner   → tv_cpm $0    on 100%   (165.7M imps)  -- free never bills (correct)
--   free + paid winner → tv_cpm $0.50 on 100%   (268.9M imps)  -- PAID STILL BILLS on free-covered imps
--   paid-only winner   → tv_cpm $0.50 on 100%   ( 38.2M imps)
--   neither            → tv_cpm $0    on 100%   ( 20.7M imps)
-- Conclusion: tv_cpm is a pure function of "did any paid vendor win" — free co-presence is IGNORED.
-- That is the definition of NO preemption.
--
-- NOTE: impression_cnt here OVER-COUNTS the final meter (usage_reporting_data) because one impression
-- has multiple path-rows. This query proves the RATE behavior only. The DOLLAR recoverable ($273.7K/yr)
-- comes from each vendor's free-co-held SHARE x its ACTUAL meter bill (see q3c + q0), NOT from these counts.

SELECT
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (23,30))          AS has_free_winner,
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (24,28,33,36,40)) AS has_paid_winner,
  COUNT(*)                                                       AS imp_rows,
  ROUND(SUM(impression_cnt), 0)                                  AS impressions,
  ROUND(SUM(IF(tv_cpm = 0.50, 1, 0)) / COUNT(*) * 100, 1)        AS pct_rows_billed_050,
  ROUND(AVG(tv_cpm), 4)                                          AS avg_tv_cpm
FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606`
GROUP BY 1, 2
ORDER BY 1, 2;
