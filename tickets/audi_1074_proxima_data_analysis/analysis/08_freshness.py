#!/usr/bin/env python3
"""AUDI-1074 Q7: freshness — purchase-to-delivery lag vs the 2026-07-17 drop date.

Usage: python3 08_freshness.py  (writes outputs/q7_freshness.json)
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
DELIVERY = "DATE '2026-07-17'"

con = duckdb.connect()
con.execute(f"CREATE VIEW basket AS SELECT * FROM read_parquet('{DATA}/basket/*')")

row = con.execute(f"""
SELECT MAX(order_created_at) AS max_ts,
       {DELIVERY} - CAST(MAX(order_created_at) AS DATE) AS nominal_lag_days,
       CAST(TO_TIMESTAMP(QUANTILE_CONT(EPOCH(order_created_at), 0.999)) AS DATE) AS p999_date,
       MIN(order_created_at) AS min_ts
FROM basket
""").fetchdf().to_dict("records")[0]

daily = con.execute(f"""
SELECT CAST(order_created_at AS DATE) d, COUNT(*) orders
FROM basket
WHERE order_created_at >= (SELECT MAX(order_created_at) FROM basket) - INTERVAL 60 DAY
GROUP BY d ORDER BY d
""").fetchdf()
daily.to_csv(ROOT / "outputs" / "q7_daily_volume_tail60.csv", index=False)

med = daily["orders"][:40].median()
full = daily[daily["orders"] >= 0.95 * med]
row["trailing_median_daily_orders"] = float(med)
row["last_full_volume_date"] = str(full["d"].max()) if len(full) else None
row["ramp_down_days_below_95pct"] = int((daily["d"] > full["d"].max()).sum()) if len(full) else None

out = {k: str(v) for k, v in row.items()}
(ROOT / "outputs" / "q7_freshness.json").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
