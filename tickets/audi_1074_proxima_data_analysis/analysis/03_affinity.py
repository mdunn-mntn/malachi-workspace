#!/usr/bin/env python3
"""AUDI-1074 Q2: cross-category affinity (lift matrix) + first-purchase transitions.

Usage: python3 03_affinity.py  (writes outputs/q2_affinity_*.csv + json)
Censoring-safe: X-anchors restricted to t <= max_date - N so every anchor has a full window.
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs"
WINDOWS = [30, 60, 90]

con = duckdb.connect()
con.execute(f"CREATE VIEW basket AS SELECT * FROM read_parquet('{DATA}/basket/*')")
con.execute(f"CREATE VIEW items AS SELECT * FROM read_parquet('{DATA}/items/*')")

con.execute("""
CREATE TEMP TABLE purch AS
SELECT b.customer_id, i.product_taxonomy_level1 AS category,
       CAST(b.order_created_at AS DATE) AS d
FROM basket b JOIN items i ON CAST(b.order_id AS VARCHAR) = i.order_id
WHERE b.customer_id IS NOT NULL AND i.product_taxonomy_level1 IS NOT NULL
GROUP BY ALL
""")
max_date = con.execute("SELECT MAX(d) FROM purch").fetchone()[0]

results = {"max_date": str(max_date)}
for n in WINDOWS:
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE anchors AS
    SELECT customer_id, category, MIN(d) AS first_d
    FROM purch WHERE d <= DATE '{max_date}' - INTERVAL {n} DAY
    GROUP BY 1, 2
    """)
    base = con.execute(f"""
    SELECT 100.0 * COUNT(DISTINCT CASE WHEN f.customer_id IS NOT NULL THEN a.customer_id || a.category END)
           / COUNT(DISTINCT a.customer_id || a.category)
    FROM anchors a
    LEFT JOIN purch f ON f.customer_id = a.customer_id
      AND f.d > a.first_d AND f.d <= a.first_d + INTERVAL {n} DAY
    """).fetchone()[0]
    mat = con.execute(f"""
    WITH x AS (SELECT customer_id, category AS cx, first_d FROM anchors),
    hits AS (
      SELECT x.cx, p.category AS cy, COUNT(DISTINCT x.customer_id) AS buyers
      FROM x JOIN purch p ON p.customer_id = x.customer_id
        AND p.d > x.first_d AND p.d <= x.first_d + INTERVAL {n} DAY
        AND p.category != x.cx
      GROUP BY 1, 2
    ),
    anchor_n AS (SELECT cx, COUNT(*) AS n FROM x GROUP BY 1)
    SELECT h.cx AS from_cat, h.cy AS to_cat, h.buyers, a.n AS anchor_customers,
           100.0 * h.buyers / a.n AS affinity_pct
    FROM hits h JOIN anchor_n a USING (cx)
    ORDER BY affinity_pct DESC
    """).fetchdf()
    mat["base_rate_any_pct"] = base
    mat.to_csv(OUT / f"q2_affinity_{n}d.csv", index=False)
    results[f"window_{n}d"] = {
        "base_rate_any_followup_pct": base,
        "top5": mat.head(5)[["from_cat", "to_cat", "affinity_pct"]].to_dict("records"),
    }

trans = con.execute("""
WITH firsts AS (
  SELECT customer_id, category, MIN(d) AS first_d FROM purch GROUP BY 1, 2
),
seq AS (
  SELECT customer_id, category,
         LEAD(category) OVER (PARTITION BY customer_id ORDER BY first_d) AS next_cat
  FROM firsts
)
SELECT category AS from_cat, next_cat AS to_cat, COUNT(*) AS customers
FROM seq WHERE next_cat IS NOT NULL
GROUP BY 1, 2 ORDER BY customers DESC
""").fetchdf()
trans.to_csv(OUT / "q2_first_purchase_transitions.csv", index=False)
results["top10_first_purchase_transitions"] = trans.head(10).to_dict("records")
results["single_category_customer_pct"] = con.execute("""
SELECT 100.0*SUM(CASE WHEN n = 1 THEN 1 ELSE 0 END)/COUNT(*) FROM (
  SELECT customer_id, COUNT(DISTINCT category) n FROM purch GROUP BY 1)
""").fetchone()[0]

(OUT / "q2_affinity.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
