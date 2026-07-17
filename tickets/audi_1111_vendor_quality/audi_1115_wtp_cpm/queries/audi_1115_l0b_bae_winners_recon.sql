-- ============================================================================
-- AUDI-1115 L0b: BAE billing table reconciliation —
--                dw-main-gold.reporting.ddp_mm_winners_imp_202606
--
-- Source: Alyson Lefkowitz 2026-07-17 ("the table BAE uses for billing; moved
-- in the BQ migration; double-check your numbers against it").
--
-- MEASURED SEMANTICS (2026-07-17 probes, documented in summary §4f):
--   * Grain: (ad_served_id x consumer data_source_id x and_seq x or_seq x
--     data_source_category_id). data_source_id is the CONSUMER (13/19 only);
--     the vendor(s) live in mm_dsids_winner ARRAY<INT64>. 530.7M rows for
--     June 2026.
--   * impression_cnt: ~90% exactly 1.0 regardless of winner count (NOT 1/N of
--     the row's winners). ~10% fractional (e.g. 0.5 on a single-winner row —
--     denominator NOT visible in this table; open question for the billing
--     sync).
--   * tv_cpm: the billing rate APPLIED to the row. tv_cpm=0 on 100% of rows
--     whose winners are ONLY free logs (23/30) -> free logs never bill.
--     Rows with a free log AND a paid vendor co-winning carry tv_cpm=0.5 on
--     91.7% -> paid vendors bill even when the free logs had the signal =
--     the AUDI-1093 preemption gap, visible in the billing table itself
--     (291.1M imps/mo on mixed rows).
--   * NO simple aggregation reproduces coredw.usage_reporting_data June
--     billed_imps exactly (33Across 70,337,329): equal-split-by-winners
--     81.8M (+16%), DS19-only split 75.6M (+7.5%), first-array-element 81.8M,
--     union-dedupe-per-impression 84.9M (+21%); directions vary by vendor
--     (Sovrn/Justuno/Cybba come out LOW). The exact downstream aggregation
--     lives in BAE/Sherwin's compute — agenda item for the 2026-07-20 billing
--     sync (with the 0.5-fraction single-winner sample as the exhibit:
--     ad_served_id f05c2bac-e547-4eb0-b49d-1abe16d3955c).
--
-- Claim: ONE scan emits (a) rec='winner' — per winner-vendor credit under the
-- candidate rules + dollars at tv_cpm, for comparison against the actual June
-- bills; (b) rec='mix' — the winner-mix x tv_cpm split that proves the
-- free-never-bills / paid-bills-on-overlap encoding.
--
-- CHEAP-ISH (one 72GB native-table scan, reservation) — console-friendly.
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1115 --label "l0b bae winners recon" \
--     --use_legacy_sql=false --format=csv --max_rows=40 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1111_vendor_quality/audi_1115_wtp_cpm/queries/audi_1115_l0b_bae_winners_recon.sql)"
--
-- Parameters: MONTH TABLE = ddp_mm_winners_imp_202606 (June 2026)
-- ============================================================================

WITH base AS (
  SELECT ad_served_id, data_source_id AS consumer, impression_cnt, tv_cpm,
         mm_dsids_winner,
         ARRAY_LENGTH(mm_dsids_winner) AS nw,
         NOT EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w NOT IN (23, 30)) AS free_only,
         EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (23, 30)) AS has_free
  FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606`
),

winner AS (
  SELECT w,
         ROUND(SUM(b.impression_cnt), 0) AS full_cnt,
         ROUND(SUM(b.impression_cnt / b.nw), 0) AS split_cnt,
         ROUND(SUM(IF(b.consumer = 19, b.impression_cnt / b.nw, 0)), 0) AS split_ds19_cnt,
         ROUND(SUM(b.impression_cnt / b.nw * b.tv_cpm / 1000), 2) AS usd_split
  FROM base b, UNNEST(b.mm_dsids_winner) AS w
  GROUP BY w
),

mix AS (
  SELECT
    CASE WHEN free_only THEN 'free_only_winners'
         WHEN has_free THEN 'mixed_free_and_paid'
         ELSE 'paid_only_winners' END AS grp,
    COUNT(*) AS rows_,
    ROUND(SUM(impression_cnt), 0) AS imps,
    ROUND(AVG(tv_cpm), 4) AS avg_cpm,
    ROUND(COUNTIF(tv_cpm = 0) / COUNT(*), 3) AS share_cpm_zero
  FROM base
  GROUP BY 1
)

SELECT * FROM (
  SELECT 'winner' AS rec, CAST(w AS STRING) AS key, full_cnt AS v1, split_cnt AS v2,
         split_ds19_cnt AS v3, usd_split AS v4
  FROM winner
  UNION ALL
  SELECT 'mix', grp, CAST(rows_ AS FLOAT64), imps, avg_cpm, share_cpm_zero
  FROM mix
)
ORDER BY rec, v2 DESC
