-- BAE-4923 review: reproduce Sherwin's free-log preemption savings and correct the denominator.
-- Claim under test: shifting usage-based-DDP impression credits to free sources (guid 23 /
-- augmentor 30) on winner sets that contain both saves ~$43K/mo (Sherwin, BAE-4923 comment 602686).
-- Result: his 6 months reproduce to the cent; the denominator is wrong; the run-rate is higher.

-- Free = guid_log 23, mntn_augmentor_log 30. Metered paid = 33Across 28, 33Across API 40,
-- Justuno 24, Sovrn 33, Cybba 36. Flat-fee 25/26/39 never meter, so they are neither.
-- mm_dsid_count (native column) = ARRAY_LENGTH(mm_dsids_winner) - 1 exactly when BOTH 28 and 40
-- are present: the pipeline treats 33Across + 33Across API as ONE vendor. Sherwin shadowed this
-- column with a recomputed ARRAY_LENGTH, which double-counts 33Across on 34.2% of rows.

WITH base AS (
  SELECT _TABLE_SUFFIX AS month,
         impression_cnt AS imp,
         mm_dsid_count  AS native_n,
         ARRAY_LENGTH(mm_dsids_winner) AS array_n,
         (SELECT COUNTIF(x IN (23,30))          FROM UNNEST(mm_dsids_winner) x) AS free_cnt,
         (SELECT COUNTIF(x IN (40,28,24,36,33)) FROM UNNEST(mm_dsids_winner) x) AS paid_arr
  FROM `dw-main-gold.reporting.ddp_mm_winners_imp_*`
  WHERE _TABLE_SUFFIX BETWEEN '202601' AND '202607'
)
SELECT month,
       ROUND(SUM(imp), 0) AS mixed_imps,
       ROUND(SUM(imp / array_n  * paid_arr) / 1000 * 0.50, 2)                        AS sherwin_usd,
       ROUND(SUM(imp / native_n * (paid_arr - (array_n - native_n))) / 1000 * 0.50, 2) AS corrected_usd
FROM base
WHERE free_cnt > 0 AND paid_arr > 0
GROUP BY 1
ORDER BY 1;

-- Proof the denominator gap is exactly the 33Across dedup (two buckets, zero exceptions):
-- SELECT (28 IN UNNEST(mm_dsids_winner)) AND (40 IN UNNEST(mm_dsids_winner)) AS has_both_33across,
--        ARRAY_LENGTH(mm_dsids_winner) - mm_dsid_count AS diff, COUNT(*) AS rows_
-- FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606` GROUP BY 1,2;
-- -> (false, 0, 349,175,512) and (true, 1, 181,514,444)
