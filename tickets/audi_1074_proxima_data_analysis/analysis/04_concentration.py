#!/usr/bin/env python3
"""AUDI-1074 Q3: brand roster, volume and GMV concentration.

Usage: python3 04_concentration.py  (writes outputs/q3_concentration.json + brand csv)
GMV = SUM(order_total) on USD orders only; refunded/voided excluded, share reported.
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs"

con = duckdb.connect(str(ROOT / "outputs" / "proxima.duckdb"), read_only=True)

results = {}
results["excluded"] = con.execute("""
SELECT 100.0*SUM(CASE WHEN currency != 'USD' OR currency IS NULL THEN 1 ELSE 0 END)/COUNT(*) AS non_usd_pct,
       100.0*SUM(CASE WHEN financial_status IN ('refunded','voided') THEN 1 ELSE 0 END)/COUNT(*) AS refunded_pct
FROM basket
""").fetchdf().to_dict("records")[0]

con.execute("""
CREATE TEMP TABLE per_brand AS
SELECT brand_id, ANY_VALUE(brand_category) AS brand_category,
       COUNT(*) AS orders, COUNT(DISTINCT customer_id) AS customers,
       SUM(order_total) AS gmv
FROM basket
WHERE currency = 'USD' AND (financial_status IS NULL OR financial_status NOT IN ('refunded','voided'))
GROUP BY brand_id
""")

brands = con.execute(
    "SELECT * FROM per_brand ORDER BY gmv DESC").fetchdf()
brands.to_csv(OUT / "q3_brand_roster.csv", index=False)

total_gmv = brands["gmv"].sum()
total_orders = brands["orders"].sum()
cum_gmv = brands["gmv"].cumsum() / total_gmv
by_orders = brands.sort_values("orders", ascending=False).reset_index(drop=True)
cum_orders = by_orders["orders"].cumsum() / total_orders

results["roster"] = {
    "distinct_brands": len(brands),
    "total_usd_gmv": float(total_gmv),
    "total_orders": int(total_orders),
    "monthly_active_brands": {str(r[0])[:7]: r[1] for r in con.execute("""
        SELECT DATE_TRUNC('month', order_created_at) m, COUNT(DISTINCT brand_id)
        FROM basket GROUP BY m ORDER BY m""").fetchall()},
}
results["concentration"] = {
    "gmv_share_top10_pct": float(100 * cum_gmv.iloc[9]) if len(brands) > 9 else None,
    "gmv_share_top50_pct": float(100 * cum_gmv.iloc[49]) if len(brands) > 49 else None,
    "gmv_share_top100_pct": float(100 * cum_gmv.iloc[99]) if len(brands) > 99 else None,
    "orders_share_top10_pct": float(100 * cum_orders.iloc[9]) if len(brands) > 9 else None,
    "orders_share_top100_pct": float(100 * cum_orders.iloc[99]) if len(brands) > 99 else None,
    "hhi_gmv": float(((brands["gmv"] / total_gmv) ** 2).sum()),
    "long_tail_brands_lt100_orders_pct": float(100 * (brands["orders"] < 100).mean()),
}
results["by_brand_category"] = con.execute("""
SELECT brand_category, COUNT(*) brands, SUM(orders) orders, SUM(gmv) gmv
FROM per_brand GROUP BY 1 ORDER BY gmv DESC
""").fetchdf().to_dict("records")

(OUT / "q3_concentration.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
