-- ti_1313_fcap_in_window.sql: the frequency cap each prospecting campaign group actually ran during
-- the measurement window, from the archive rather than current state.
--
-- dso.frequency_caps holds only today's value and rewrote 12,197 of 125,672 rows in one day, so it
-- mislabels 144 of 433 campaign groups here. archives.frequency_cap_archives is versioned and covers
-- 433 of 433; take the latest row on or before the window end.

WITH pc AS (
  SELECT c.campaign_id, c.campaign_group_id
  FROM `dw-main-silver.public.campaigns` c
  WHERE c.deleted = FALSE AND c.is_test = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
arch AS (
  SELECT
    a.campaign_id,
    COALESCE(a.secondary_cap, a.dsp_cap) AS fcap_impressions,
    COALESCE(a.secondary_duration, a.dsp_duration) AS fcap_duration_seconds,
    a.dsp_cap IS NOT NULL AS fcap_manual_override,
    ROW_NUMBER() OVER (PARTITION BY a.campaign_id ORDER BY a.update_time DESC) AS rn
  FROM `dw-main-silver.archives.frequency_cap_archives` a
  JOIN pc USING (campaign_id)
  WHERE a.update_time <= TIMESTAMP('2026-08-31 23:59:59')
)
SELECT
  pc.campaign_group_id,
  ANY_VALUE(a.fcap_impressions) AS fcap_impressions_in_window,
  ANY_VALUE(a.fcap_duration_seconds) AS fcap_duration_seconds_in_window,
  LOGICAL_OR(a.fcap_manual_override) AS fcap_manual_override_in_window
FROM pc
JOIN (SELECT * FROM arch WHERE rn = 1) a USING (campaign_id)
GROUP BY 1
