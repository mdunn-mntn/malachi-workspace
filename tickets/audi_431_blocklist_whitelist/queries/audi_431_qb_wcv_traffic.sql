-- AUDI-431 Query B: traffic-ranked wcv-classified domains (corrections leg).
-- ddp = external table over ddp_url_verticals (hive-partitioned on dt, def in audi_431_ddp_external_def.json).
-- Single external reference; dt filter prunes to the 7-day closed window.
-- Keeps top 60 per vertical + global top 3000 server-side (QUALIFY) to bound output.
SELECT
  domain,
  vertical_id,
  vertical_name,
  bucket_id,
  COUNT(*) AS n_urls,
  COUNT(DISTINCT dt) AS days_seen
FROM ddp
WHERE dt BETWEEN '2026-08-02' AND '2026-08-08'
  AND is_in_vertical_mapping
GROUP BY domain, vertical_id, vertical_name, bucket_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY vertical_id ORDER BY COUNT(*) DESC) <= 60
     OR RANK() OVER (ORDER BY COUNT(*) DESC) <= 3000
ORDER BY n_urls DESC
