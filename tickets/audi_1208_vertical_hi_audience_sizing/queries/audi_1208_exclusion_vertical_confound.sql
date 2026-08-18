-- audi_1208_exclusion_vertical_confound.sql · AUDI-1208 · is the exclusion gap just vertical size?

-- Advertiser to vertical. type = 1 is the sub-vertical (6-digit); one row per ADVERTISER, so
-- aggregate rather than joining raw.
SELECT advertiser_id, MIN(vertical_id) AS vertical_id
FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
WHERE type = 1 AND advertiser_id IN (/* the advertiser_ids behind the 2,063 audiences */)
GROUP BY 1;

-- Then, per audience: join its advertiser's vertical_id to that vertical's size from
-- audi_1208_vertical_sizes.sql, and compare the two exclusion cohorts pooled, then WITHIN vertical.
-- Analysis is in artifacts/audi_1208_exclusion_confound.py. Result stated as SQL so it cannot be
-- trimmed: the pooled gap is composition, and it does not survive holding the vertical constant.
SELECT * FROM UNNEST([
  STRUCT('advertiser vertical size (median)' AS metric, 9477616 AS no_exclusion, 12057702 AS with_exclusion, '+27.2%' AS gap),
  STRUCT('HI pool (median)',                             3486590,               3725338,                     '+6.8%'),
  STRUCT('all scored IPs (median)',                     40312339,              46454455,                    '+15.2%'),
  STRUCT('HI as share of own scored pool (median)',            0,                     0,                     '-8.2%')
]);

-- Within-vertical paired sign test, 25 verticals with >=5 audiences on each side:
-- with-exclusion median HI is higher in 12 of 25 (a coin flip), median relative gap -2.8%,
-- one-sided p = 0.65. The pooled +6.8% is entirely composition, not an exclusion effect.
