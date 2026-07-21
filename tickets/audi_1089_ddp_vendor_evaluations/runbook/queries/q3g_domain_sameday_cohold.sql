-- ============================================================================
-- DDP runbook STEP 3g: DOMAIN cohold — did a free log have the SAME (ip, domain, date)?
--
-- The clean, literal redundancy measure (user, 2026-07-21): out of all the (ip x domain x date)
-- signals a vendor is billed for, how many did a FREE log (23 guid, 30 augmentor) ALSO deliver on
-- the SAME (ip, domain, date)? That share x the vendor's bill = the domain-grain preemptable $.
-- No prior-day / recency logic — just "did we capture that exact visit for free". Reproduces the q3c
-- same-day cohold (33Across = 52.9%). NOT tautological for augmentor: the IP-grain cross-check shows
-- 52.9% same-domain vs 88.5% any-domain, so the 52.9% is genuine same-visit overlap.
--
-- Grain (ip, REG_DOMAIN(url), dt), 30d 2026-06-02..07-01, usable domains (wcv OR pc), IPv4. BIG (~20-30m).
--
-- Run (from workspace root): build 30d URIS as in q3c, then bq_run.sh with svs+wcv+pc external defs.
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
  SELECT DISTINCT
    CAST(s.data_source_id AS INT64) AS ds, s.ip, NET.REG_DOMAIN(s.url) AS dom, s.dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
),

free AS (SELECT DISTINCT ip, dom, dt FROM trips WHERE ds IN (23, 30)),   -- free (ip, domain, date)
vt   AS (SELECT ds, ip, dom, dt FROM trips WHERE ds NOT IN (23, 30))     -- paid vendor triples

SELECT
  v.ds,
  COUNT(*)                                                  AS triples,
  COUNTIF(f.ip IS NOT NULL)                                 AS free_cohold,   -- free had the SAME (ip,domain,date)
  ROUND(COUNTIF(f.ip IS NOT NULL) / COUNT(*) * 100, 1)      AS pct_free_cohold
FROM vt v
LEFT JOIN free f USING (ip, dom, dt)
GROUP BY v.ds
ORDER BY v.ds;
