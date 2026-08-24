#!/usr/bin/env python3
"""AUDI-1074 Q6: new-to-brand identification + lookback adequacy.

Usage: python3 07_ntb.py  (writes outputs/q6_ntb.json + monthly csv)
NTB_m is left-censored: month 1 reads ~100% by construction; effective lookback =
months until the curve plateaus (delta < 1pp/month).
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs"

con = duckdb.connect(str(ROOT / "outputs" / "proxima.duckdb"), read_only=True)

results = {}
results["guest_checkout_excluded_pct"] = con.execute(
    "SELECT 100.0*SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM basket"
).fetchone()[0]

monthly = con.execute("""
WITH firsts AS (
  SELECT customer_id, brand_id, MIN(order_created_at) AS first_ts
  FROM basket WHERE customer_id IS NOT NULL GROUP BY 1, 2
)
SELECT DATE_TRUNC('month', b.order_created_at) AS m,
       COUNT(*) AS orders,
       SUM(CASE WHEN b.order_created_at = f.first_ts THEN 1 ELSE 0 END) AS ntb_orders,
       100.0*SUM(CASE WHEN b.order_created_at = f.first_ts THEN 1 ELSE 0 END)/COUNT(*) AS ntb_pct
FROM basket b
JOIN firsts f ON f.customer_id = b.customer_id AND f.brand_id = b.brand_id
GROUP BY m ORDER BY m
""").fetchdf()
monthly.to_csv(OUT / "q6_ntb_monthly.csv", index=False)

curve = monthly["ntb_pct"].tolist()
plateau = next(
    (i for i in range(1, len(curve)) if abs(curve[i] - curve[i - 1]) < 1.0), None)
results["ntb_pct_by_month"] = {str(m)[:7]: round(p, 2) for m, p in zip(monthly["m"], curve)}
results["plateau_month_index"] = plateau
results["final_month_ntb_pct"] = curve[-1] if curve else None

results["flag_history_beyond_file"] = con.execute("""
SELECT 100.0 * COUNT(DISTINCT CASE WHEN category_beauty_buyer_36mo AND obs_beauty = 0
                                   THEN customer_id END)
       / COUNT(DISTINCT CASE WHEN category_beauty_buyer_36mo THEN customer_id END)
FROM (
  SELECT customer_id, BOOL_OR(category_beauty_buyer_36mo) AS category_beauty_buyer_36mo,
         SUM(CASE WHEN brand_category = 'Beauty' THEN 1 ELSE 0 END) AS obs_beauty
  FROM basket WHERE customer_id IS NOT NULL GROUP BY 1)
""").fetchone()[0]

(OUT / "q6_ntb.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
