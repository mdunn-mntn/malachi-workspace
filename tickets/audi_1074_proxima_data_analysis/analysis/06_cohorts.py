#!/usr/bin/env python3
"""AUDI-1074 Q5: can Proxima features separate Fangorn ever-HI vs never-HI served IPs?

Usage: python3 06_cohorts.py  (needs outputs/cil_served_ips_30d_1pct.csv + proxima.duckdb;
writes outputs/q5_cohorts.json)
Cohorts: CIL 30d served IPv4, ever_hi=1 vs ever_hi=0 (never reached HI band in window).
Proxima side restricted to last-90d orders. AUC = 5-fold CV logistic on Proxima-only features.
"""
import json
from pathlib import Path

import duckdb
import numpy as np
from scipy.spatial.distance import jensenshannon
from scipy.stats import chi2_contingency, mannwhitneyu
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"

con = duckdb.connect()
con.execute(f"ATTACH '{OUT / 'proxima.duckdb'}' AS px (READ_ONLY)")
con.execute(f"""
CREATE TEMP TABLE cohort AS
SELECT ip, ever_hi FROM read_csv('{OUT}/cil_served_ips_30d_1pct.csv')
""")

con.execute("""
CREATE TEMP TABLE feat AS
WITH cust AS (
  SELECT m.ip_address AS ip, MAX(c.ever_hi) AS ever_hi, m.customer_id
  FROM px.ipmap m JOIN cohort c ON c.ip = m.ip_address
  GROUP BY 1, 3
),
recent AS (
  SELECT b.*, cu.ip, cu.ever_hi
  FROM px.basket b JOIN cust cu ON cu.customer_id = b.customer_id
  WHERE b.order_created_at >= TIMESTAMP '2026-07-15 10:54:04' - INTERVAL 90 DAY
)
SELECT ip, MAX(ever_hi) AS ever_hi,
  COUNT(*) AS orders_90d,
  MEDIAN(order_total) AS med_order_total,
  SUM(order_total) AS gmv_90d,
  COUNT(DISTINCT brand_id) AS brands_90d,
  MAX(order_created_at) AS last_order_ts,
  AVG(CASE WHEN brand_category = 'Fashion & Accessories' THEN 1.0 ELSE 0 END) AS sh_fashion,
  AVG(CASE WHEN brand_category = 'Health' THEN 1.0 ELSE 0 END) AS sh_health,
  AVG(CASE WHEN brand_category = 'Home' THEN 1.0 ELSE 0 END) AS sh_home,
  AVG(CASE WHEN brand_category = 'Beauty' THEN 1.0 ELSE 0 END) AS sh_beauty,
  AVG(CASE WHEN brand_category = 'Food & Drink' THEN 1.0 ELSE 0 END) AS sh_food,
  AVG(CASE WHEN brand_category IN ('Pets','Children','Travel','Hobbies & Leisure','Other')
      THEN 1.0 ELSE 0 END) AS sh_rest,
  MAX(CASE WHEN category_beauty_buyer_12mo THEN 1 ELSE 0 END) AS f_beauty12,
  MAX(CASE WHEN category_fashion_buyer_12mo THEN 1 ELSE 0 END) AS f_fashion12,
  MAX(CASE WHEN category_health_buyer_12mo THEN 1 ELSE 0 END) AS f_health12,
  MAX(CASE WHEN category_home_buyer_12mo THEN 1 ELSE 0 END) AS f_home12
FROM recent GROUP BY ip
""")

results = {"cohort_sample_sizes": dict(con.execute(
    "SELECT ever_hi, COUNT(*) FROM cohort GROUP BY 1").fetchall())}
results["matched_ips_with_90d_orders"] = dict(con.execute(
    "SELECT ever_hi, COUNT(*) FROM feat GROUP BY 1").fetchall())

catmix = {}
for hi in (0, 1):
    catmix[hi] = con.execute(f"""
    SELECT SUM(sh_fashion), SUM(sh_health), SUM(sh_home), SUM(sh_beauty), SUM(sh_food), SUM(sh_rest)
    FROM feat WHERE ever_hi = {hi}
    """).fetchone()
p0 = np.array(catmix[0]) / sum(catmix[0])
p1 = np.array(catmix[1]) / sum(catmix[1])
results["category_mix"] = {
    "labels": ["fashion", "health", "home", "beauty", "food", "rest"],
    "never_hi": [round(x, 4) for x in p0], "ever_hi": [round(x, 4) for x in p1],
    "js_divergence": float(jensenshannon(p0, p1) ** 2),
}

df = con.execute("SELECT * FROM feat").fetchdf()
a = df[df.ever_hi == 1]
b = df[df.ever_hi == 0]
results["behavior"] = {}
for col in ("orders_90d", "med_order_total", "gmv_90d", "brands_90d"):
    u = mannwhitneyu(a[col].dropna(), b[col].dropna())
    results["behavior"][col] = {
        "ever_hi_median": float(a[col].median()), "never_hi_median": float(b[col].median()),
        "mannwhitney_p": float(u.pvalue),
    }

feats = ["orders_90d", "med_order_total", "gmv_90d", "brands_90d",
         "sh_fashion", "sh_health", "sh_home", "sh_beauty", "sh_food", "sh_rest",
         "f_beauty12", "f_fashion12", "f_health12", "f_home12"]
X = df[feats].fillna(0).to_numpy(dtype=float)
y = df["ever_hi"].to_numpy(dtype=int)
auc = cross_val_score(
    make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
    X, y, cv=5, scoring="roc_auc")
results["classifier"] = {
    "n": int(len(y)), "pos_rate": float(y.mean()),
    "cv5_auc_mean": float(auc.mean()), "cv5_auc_std": float(auc.std()),
    "features": feats,
}

(OUT / "q5_cohorts.json").write_text(json.dumps(results, indent=2, default=str))
print(json.dumps(results, indent=2, default=str))
