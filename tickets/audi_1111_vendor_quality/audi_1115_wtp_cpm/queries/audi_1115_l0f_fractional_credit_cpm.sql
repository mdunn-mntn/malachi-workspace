-- ============================================================================
-- AUDI-1115 L0f: fractional-credit fair CPM per vendor
--   BAE winners (dw-main-gold.reporting.ddp_mm_winners_imp_202606, June 2026)
--   JOINED to the media we actually made (cost_impression_log, same month).
--
-- The question this answers (Malachi, 2026-07-17): "our CPM should be based on
-- the money we actually made — and we don't remove other-vendor overlap, we
-- give FRACTIONAL credit." So every won impression's credit (and the media it
-- earned) is SPLIT equally across the paid vendors that co-won it, and free
-- logs preempt (an impression a free log also won earns the vendor $0 — the
-- AUDI-1093 rule). This puts the numerator (money made) and the denominator
-- (credited impressions) on the SAME fractional basis — unlike L0/L0p, which
-- pair a full-attribution numerator with the real meter.
--
-- MODEL (documented, one candidate — the exact BAE downstream rule is the
-- 2026-07-20 billing-sync question):
--   * winners of an impression = DISTINCT UNION of mm_dsids_winner across its
--     rows (the table has one row per matched path/slot; free logs 23/30 DO
--     appear in the array).
--   * FREE PREEMPTION: any impression with a free-log winner (23 or 30) earns
--     paid vendors $0 (excluded).
--   * FRACTIONAL SPLIT: on a remaining impression, each of its n_paid distinct
--     paid winners (24,25,26,28,33,36,39,40) gets 1/n_paid of the credit AND
--     1/n_paid of the media. A sole paid winner gets full credit.
--
-- Per vendor output:
--   imps_any_winner      = impressions the vendor co-won (full, incl. free-covered)
--   imps_free_preempted  = of those, ones a free log also won (dropped)
--   imps_paid_eligible   = vendor won, no free log won (integer count)
--   frac_credit_imps     = SUM(1/n_paid) over eligible = fractional credited imps
--                          (invariant: SUM over all vendors == # eligible imps)
--   media_frac_usd       = SUM(media/n_paid) over eligible = fractionally-
--                          attributed MNTN media $ (June)
--   media_cpm_frac       = media_frac_usd / frac_credit_imps x 1000 = the media
--                          revenue per fractionally-credited impression.
--   media_cpm_elig_full  = full-attribution media CPM on the same eligible
--                          cohort (diagnostic: ~= media_cpm_frac proves the
--                          fractional weights cancel in the RATIO — only total
--                          $ shrink, not the per-unit rate).
--
-- BREAK-EVEN VENDOR CPM = media_cpm_frac x internal margin band (10-30%) —
-- applied in the workbook, NOT here (margin parameters stay out of shared SQL).
-- media/data_spend lenses are the shareable ones; this query emits only those.
--
-- Reconciliation: frac_credit_imps is the fractional-credit meter — compare vs
-- coredw.usage_reporting_data June billed_imps (33Across 70.3M; equal-split
-- earlier landed 81.8M full-union / this is post-free-preemption so LOWER).
-- imps_any_winner vs the BAE full-credit sums (audi_1115_l0b, 33Across 282M).
--
-- CHEAP-ISH: BAE 72GB + CIL June ~127GB, one shuffle join on ad_served_id.
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1115 --label "l0f fractional credit cpm" \
--     --use_legacy_sql=false --format=csv --max_rows=40 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1111_vendor_quality/audi_1115_wtp_cpm/queries/audi_1115_l0f_fractional_credit_cpm.sql)"
--
-- Parameters: MONTH = 202606; PAID = {24,25,26,28,33,36,39,40}; FREE = {23,30}.
-- ============================================================================

WITH flat AS (
  SELECT ad_served_id, w
  FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606`, UNNEST(mm_dsids_winner) AS w
  GROUP BY 1, 2
),

imp AS (
  SELECT
    ad_served_id,
    ARRAY_AGG(w) AS winners,
    COUNTIF(w IN (24, 25, 26, 28, 33, 36, 39, 40)) AS n_paid,
    LOGICAL_OR(w IN (23, 30)) AS has_free
  FROM flat
  GROUP BY ad_served_id
),

med AS (
  SELECT ad_served_id, SUM(media_spend) AS media
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-06-01' AND '2026-06-30'
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
),

ex AS (
  SELECT i.n_paid, i.has_free, IFNULL(m.media, 0) AS media, w
  FROM imp i
  LEFT JOIN med m USING (ad_served_id)
  CROSS JOIN UNNEST(i.winners) AS w
  WHERE w IN (24, 25, 26, 28, 33, 36, 39, 40)
)

SELECT
  w AS vendor_ds,
  COUNT(*) AS imps_any_winner,
  COUNTIF(has_free) AS imps_free_preempted,
  COUNTIF(NOT has_free) AS imps_paid_eligible,
  ROUND(SUM(IF(NOT has_free, 1.0 / n_paid, 0)), 0) AS frac_credit_imps,
  ROUND(SUM(IF(NOT has_free, media / n_paid, 0)), 2) AS media_frac_usd,
  ROUND(SAFE_DIVIDE(SUM(IF(NOT has_free, media / n_paid, 0)),
                    SUM(IF(NOT has_free, 1.0 / n_paid, 0))) * 1000, 4) AS media_cpm_frac,
  ROUND(SAFE_DIVIDE(SUM(IF(NOT has_free, media, 0)),
                    COUNTIF(NOT has_free)) * 1000, 4) AS media_cpm_elig_full
FROM ex
GROUP BY 1
ORDER BY frac_credit_imps DESC
