#!/usr/bin/env python3
"""AUDI-1074 Q4: Proxima IP overlap vs DS14 gate and CIL served, recency-bucketed.

Usage: python3 05_identity_overlap.py  (needs outputs/ds14_ip_sample_1pct.csv and
outputs/cil_served_ips_30d_1pct.csv; writes outputs/q4_overlap.json)

Estimator: BQ samples are the k=1% FARM_FINGERPRINT bucket of each denominator, so
share_in_denominator = matches * (100/k) / N_proxima_ipv4, binomial CI on matches.
"""
import json
import math
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs"
K_PCT = 1

con = duckdb.connect()
con.execute(f"""
CREATE VIEW ipmap AS SELECT _COL_0 AS customer_id, _COL_1 AS ip_address
FROM read_parquet('{DATA}/ip_mapping/*')""")
con.execute(f"CREATE VIEW basket AS SELECT * FROM read_parquet('{DATA}/basket/*')")
con.execute(f"CREATE TABLE ds14 AS SELECT ip FROM read_csv('{OUT}/ds14_ip_sample_1pct.csv')")
con.execute(f"""
CREATE TABLE cil AS SELECT ip, ever_hi, ever_scored
FROM read_csv('{OUT}/cil_served_ips_30d_1pct.csv')""")

con.execute("""
CREATE TEMP TABLE prox_ip AS
SELECT m.ip_address AS ip, MAX(CAST(b.order_created_at AS DATE)) AS last_order_d
FROM ipmap m
LEFT JOIN basket b ON b.customer_id = m.customer_id
WHERE m.ip_address SIMILAR TO '\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}'
GROUP BY 1
""")
max_d = con.execute("SELECT MAX(last_order_d) FROM prox_ip").fetchone()[0]


def wilson(k, n):
    if n == 0:
        return (0.0, 0.0)
    p, z = k / n, 1.96
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (100 * (c - h), 100 * (c + h))


def overlap(where):
    n = con.execute(f"SELECT COUNT(*) FROM prox_ip WHERE {where}").fetchone()[0]
    row = {"n_proxima_ipv4": n}
    for denom, extra in (("ds14", ""), ("cil", ""),
                         ("cil", " AND t.ever_scored = 0"),
                         ("cil", " AND t.ever_hi = 1")):
        label = denom + ("_unscored" if "ever_scored" in extra else "_hi" if "ever_hi" in extra else "")
        m = con.execute(f"""
        SELECT COUNT(*) FROM prox_ip p JOIN {denom} t ON t.ip = p.ip
        WHERE {where}{extra}""").fetchone()[0]
        lo, hi = wilson(m, n)
        row[label] = {"matches_in_sample": m,
                      "share_pct": round(m * (100 / K_PCT) * 100 / n, 2) if n else None,
                      "ci95_pct": (round(lo * 100 / K_PCT, 2), round(hi * 100 / K_PCT, 2))}
    return row


results = {"reference_max_order_date": str(max_d), "estimator_k_pct": K_PCT}
results["all_ips"] = overlap("TRUE")
for name, days in (("last_30d", 30), ("d31_90", 90), ("d91_365", 365)):
    lo = f"DATE '{max_d}' - INTERVAL {days} DAY"
    if days == 30:
        w = f"last_order_d > {lo}"
    elif days == 90:
        w = f"last_order_d <= DATE '{max_d}' - INTERVAL 30 DAY AND last_order_d > {lo}"
    else:
        w = f"last_order_d <= DATE '{max_d}' - INTERVAL 90 DAY"
    results[name] = overlap(w)
results["ips_with_no_basket_order"] = con.execute(
    "SELECT COUNT(*) FROM prox_ip WHERE last_order_d IS NULL").fetchone()[0]

(OUT / "q4_overlap.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
