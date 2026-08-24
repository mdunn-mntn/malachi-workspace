#!/usr/bin/env python3
"""AUDI-1074 Q1: repurchase cadence at item and category level.

Usage: python3 02_cadence.py  (writes outputs/q1_cadence.json + csv detail)
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs"

con = duckdb.connect(str(ROOT / "outputs" / "proxima.duckdb"), read_only=True)

con.execute("""
CREATE TEMP TABLE cust_day AS
SELECT b.customer_id, i.product_taxonomy_level1 AS category,
       i.line_item_product_id AS product_id, i.brand_id,
       CAST(b.order_created_at AS DATE) AS order_date
FROM basket b JOIN items i ON CAST(b.order_id AS VARCHAR) = i.order_id
WHERE b.customer_id IS NOT NULL
GROUP BY ALL
""")

results = {}


def cadence(key):
    gaps = con.execute(f"""
    WITH dd AS (
      SELECT DISTINCT customer_id, {key} AS k, order_date
      FROM cust_day WHERE {key} IS NOT NULL
    ),
    g AS (
      SELECT customer_id, k, order_date,
             order_date - LAG(order_date) OVER (PARTITION BY customer_id, k ORDER BY order_date) AS gap
      FROM dd
    ),
    per_cust AS (
      SELECT customer_id, k, MEDIAN(gap) AS cust_median
      FROM g WHERE gap IS NOT NULL GROUP BY 1, 2
    )
    SELECT
      (SELECT MEDIAN(gap) FROM g WHERE gap IS NOT NULL) AS pooled_median,
      (SELECT QUANTILE_CONT(gap, 0.25) FROM g WHERE gap IS NOT NULL) AS p25,
      (SELECT QUANTILE_CONT(gap, 0.75) FROM g WHERE gap IS NOT NULL) AS p75,
      (SELECT MEDIAN(cust_median) FROM per_cust) AS median_of_cust_medians,
      (SELECT COUNT(*) FROM g WHERE gap IS NOT NULL) AS n_gaps
    """).fetchone()
    repurch = con.execute(f"""
    SELECT 100.0*SUM(CASE WHEN n > 1 THEN 1 ELSE 0 END)/COUNT(*) FROM (
      SELECT customer_id, {key}, COUNT(DISTINCT order_date) n
      FROM cust_day WHERE {key} IS NOT NULL GROUP BY 1, 2)
    """).fetchone()[0]
    return {"pooled_median_days": gaps[0], "p25": gaps[1], "p75": gaps[2],
            "median_of_customer_medians": gaps[3], "n_gaps": gaps[4],
            "repurchaser_pct": repurch}


results["category_level"] = cadence("category")
results["item_level_products_50plus"] = con.execute("""
WITH dd AS (
  SELECT DISTINCT customer_id, product_id, order_date
  FROM cust_day WHERE product_id IS NOT NULL
),
g AS (
  SELECT customer_id, product_id, order_date,
         order_date - LAG(order_date) OVER (PARTITION BY customer_id, product_id ORDER BY order_date) AS gap
  FROM dd
),
eligible AS (
  SELECT product_id FROM g WHERE gap IS NOT NULL
  GROUP BY 1 HAVING COUNT(DISTINCT customer_id) >= 50
)
SELECT COUNT(DISTINCT g.product_id) AS n_products,
       MEDIAN(gap) AS pooled_median_days,
       QUANTILE_CONT(gap, 0.25) AS p25, QUANTILE_CONT(gap, 0.75) AS p75
FROM g JOIN eligible USING (product_id) WHERE gap IS NOT NULL
""").fetchdf().to_dict("records")[0]

per_cat = con.execute("""
WITH dd AS (
  SELECT DISTINCT customer_id, category, order_date
  FROM cust_day WHERE category IS NOT NULL
),
g AS (
  SELECT customer_id, category, order_date,
         order_date - LAG(order_date) OVER (PARTITION BY customer_id, category ORDER BY order_date) AS gap
  FROM dd
)
SELECT category, COUNT(*) AS n_gaps, MEDIAN(gap) AS median_days,
       QUANTILE_CONT(gap, 0.25) AS p25, QUANTILE_CONT(gap, 0.75) AS p75
FROM g WHERE gap IS NOT NULL
GROUP BY category ORDER BY n_gaps DESC
""").fetchdf()
per_cat.to_csv(OUT / "q1_cadence_by_category.csv", index=False)
results["per_category_csv"] = "q1_cadence_by_category.csv"

(OUT / "q1_cadence.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
