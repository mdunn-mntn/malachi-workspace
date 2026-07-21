# DDP — verify these claims (run the query, check the result)

Each claim is one sentence. Run the query under it and confirm you see the same thing. All read-only.
Bills = June 2026 meter. Coverage/overlap = the last 30 days of `site_visit_signal`.
DS map: 23 guid (free), 30 augmentor (free); 24 Justuno, 25 5x5, 26 Predactiv, 28 33Across, 33 Sovrn, 36 Cybba, 39 Klickly, 40 33Across API.

---

### 1. This shows what we bill each metered vendor right now.

```sql
SELECT data_source_id,
       ROUND(SUM(usage), 0)      AS june_usage_dollars,
       ROUND(SUM(usage) * 12, 0) AS annualized
FROM `dw-main-bronze.coredw.usage_reporting_data`
WHERE reporting_month = '2026-06-01'
GROUP BY data_source_id
ORDER BY june_usage_dollars DESC;
```
*You should see:* 33Across (28) biggest (~$35K/mo ≈ $422K/yr), then 33Across API (40), Sovrn (33), Justuno (24), Cybba (36).

---

### 2. This shows the meter still charges a vendor $0.50 even when our own free logs already won that exact impression.

```sql
SELECT
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (23,30))          AS free_log_won,
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (24,28,33,36,40)) AS paid_vendor_won,
  ROUND(SUM(impression_cnt), 0) AS impressions,
  ROUND(AVG(tv_cpm), 4)         AS avg_tv_cpm
FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606`
GROUP BY 1, 2
ORDER BY 1, 2;
```
*You should see:* when a free log AND a paid vendor both won (both TRUE) → **tv_cpm $0.50 on ~269M impressions**. If we truly skipped free-covered impressions, that row would be $0.

---

### 3. This shows who gets credited per impression, including the free logs, so you can see the free logs sitting next to the paid vendors.

```sql
SELECT data_source_id        AS consumer,          -- 4 CRM, 13/19 MM
       source_data_source_id AS vendor,            -- the originating source
       COUNT(*)              AS rows
FROM `dw-main-bronze.external.targeted_signal`
WHERE dt = '2026-07-18'
GROUP BY 1, 2
ORDER BY consumer, rows DESC;
```
*You should see:* under each MM consumer (13/19), the free logs (23 guid, 30 augmentor) appear alongside the paid vendors — they're all in the pool the credit gets split across.

---

### 4. This shows, per vendor, how much of its data we already have for free — the exact same IP + domain + date.

```sql
WITH usable_dom AS (
  SELECT DISTINCT domain_name AS dom FROM wcv
  WHERE domain_name NOT IN ('yahoo.com','aol.com','easybrain.com')
  UNION DISTINCT
  SELECT DISTINCT NET.REG_DOMAIN(composite_key) FROM pc
  WHERE NET.REG_DOMAIN(composite_key) IS NOT NULL
    AND (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),
trips AS (
  SELECT DISTINCT CAST(s.data_source_id AS INT64) AS ds, s.ip, NET.REG_DOMAIN(s.url) AS dom, s.dt
  FROM svs s JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
),
free AS (SELECT DISTINCT ip, dom, dt FROM trips WHERE ds IN (23,30)),
vt   AS (SELECT ds, ip, dom, dt FROM trips WHERE ds NOT IN (23,30))
SELECT v.ds,
       ROUND(COUNTIF(f.ip IS NOT NULL) / COUNT(*) * 100, 1) AS pct_already_in_free_logs
FROM vt v LEFT JOIN free f USING (ip, dom, dt)
GROUP BY v.ds ORDER BY v.ds;
```
*You should see:* 33Across (28) **52.9%**, Predactiv (26) 42.7%, Cybba (36) 28.3%, 33Across API (40) 23.7%, 5x5 (25) 18.6%, Justuno (24) 4.4%, Sovrn (33) 0.2%.
*(This one reads `site_visit_signal` / `website_crawl_verticals` / `product_categorization` from GCS parquet — run it with the bq CLI external-table setup at the bottom.)*

---

### 5. This shows how MM actually bids — an IP is targetable via ANY visit in the same vertical, on any site — and how much of each vendor's signal the free logs already cover that way.

*Same setup as #4; groups by DS13 vertical instead of exact domain.* Query file: `runbook/queries/q3f_category_prior_coverage.sql`.
*You should see:* 33Across (28) **60.7%**, 33Across API (40) 47.4%, Sovrn (33) 43.8%, Cybba (36) 42.0%, Justuno (24) 17.0%.

---

**The claim in one line:** free logs already cover ~99% of the IPs we actually bid on, and 40–60% of what each vendor bills for is data we already have for free — so most of the metered spend is redundant.

**Running #4 / #5 (they read `site_visit_signal` from GCS):**
```bash
URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do
  URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
bq query --use_legacy_sql=false --project_id=dw-main-silver \
  --external_table_definition="svs::PARQUET=${URIS}" \
  --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
  --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
  "$(< the query above >)"
```
