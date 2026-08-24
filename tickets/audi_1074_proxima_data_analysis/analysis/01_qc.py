#!/usr/bin/env python3
"""AUDI-1074 QC battery: grain, customer_id scope, dates, nulls, IPs, vendor flags.

Usage: python3 01_qc.py  (run from the ticket root; reads outputs/proxima_20260717/,
writes outputs/qc_results.json and prints the report)
"""
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
OUT = ROOT / "outputs" / "qc_results.json"

CATEGORIES = [
    "beauty", "fashion", "health", "fooddrink", "home",
    "pets", "children", "hobbiesleisure", "travel", "other",
]

IPV4_RE = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
BOGON_RE = (
    r"^(10\.|127\.|169\.254\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|"
    r"100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.|0\.)"
)

results = {}


def q(sql):
    return con.execute(sql).fetchall()


def q1(sql):
    return con.execute(sql).fetchone()[0]


def section(name, data):
    results[name] = data
    print(f"\n== {name} ==")
    print(json.dumps(data, indent=2, default=str))


DICT_BASKET = [
    "order_id", "brand_id", "brand_category", "order_created_at", "order_subtotal",
    "order_discount_percent", "order_total_tax", "order_total_shipping_price",
    "order_total", "currency", "financial_status", "email_marketing_consent",
    "sms_marketing_consent", "browser_ip", "customer_id",
    *[f"{k}_address_{f}" for k in ("billing", "shipping", "default")
      for f in ("city", "state", "zipcode", "country")],
    *[f"category_{c}_buyer_{w}" for c in CATEGORIES for w in ("6mo", "12mo", "36mo")],
]

con = duckdb.connect()
con.execute(f"CREATE VIEW basket AS SELECT * FROM read_parquet('{DATA}/basket/*')")
con.execute(f"CREATE VIEW items AS SELECT * FROM read_parquet('{DATA}/items/*')")
con.execute(
    f"CREATE VIEW ipmap AS SELECT _COL_0 AS customer_id, _COL_1 AS ip_address "
    f"FROM read_parquet('{DATA}/ip_mapping/*')")

delivered = [r[0] for r in q("DESCRIBE basket")]
section("dictionary_vs_delivery", {
    "basket_missing_vs_dictionary": [c for c in DICT_BASKET if c not in delivered],
    "basket_extra_vs_dictionary": [c for c in delivered if c not in DICT_BASKET],
    "ipmap_positional_headers": True,
})

section("manifest", {
    d.name: {
        "files": sum(1 for f in d.iterdir() if f.is_file()),
        "bytes": sum(f.stat().st_size for f in d.iterdir() if f.is_file()),
    }
    for d in sorted(DATA.iterdir()) if d.is_dir()
})

rows = {t: q1(f"SELECT COUNT(*) FROM {t}") for t in ("basket", "items", "ipmap")}
section("row_counts", rows)

section("schemas", {
    t: {r[0]: r[1] for r in q(f"DESCRIBE {t}")} for t in ("basket", "items", "ipmap")
})

section("grain", {
    "basket_order_id_dup_rate": 1 - q1("SELECT COUNT(DISTINCT order_id)/COUNT(*) FROM basket"),
    "items_line_item_id_dup_rate": 1 - q1("SELECT COUNT(DISTINCT line_item_id)/COUNT(*) FROM items"),
    "items_orphan_rate": q1(
        "SELECT COUNT(*)/(SELECT COUNT(*) FROM items) FROM items i "
        "WHERE NOT EXISTS (SELECT 1 FROM basket b WHERE b.order_id = i.order_id)"),
    "basket_orders_without_items_rate": q1(
        "SELECT COUNT(*)/(SELECT COUNT(*) FROM basket) FROM basket b "
        "WHERE NOT EXISTS (SELECT 1 FROM items i WHERE i.order_id = b.order_id)"),
    "ipmap_rows": rows["ipmap"],
    "ipmap_distinct_customers": q1("SELECT COUNT(DISTINCT customer_id) FROM ipmap"),
    "ipmap_distinct_ips": q1("SELECT COUNT(DISTINCT ip_address) FROM ipmap"),
    "ipmap_dup_pair_rate": 1 - q1(
        "SELECT COUNT(DISTINCT customer_id || '|' || ip_address)/COUNT(*) FROM ipmap"),
})

scope = q(
    "SELECT n_brands, COUNT(*) AS customers FROM ("
    "  SELECT customer_id, COUNT(DISTINCT brand_id) AS n_brands FROM basket"
    "  WHERE customer_id IS NOT NULL GROUP BY customer_id"
    ") GROUP BY n_brands ORDER BY n_brands LIMIT 20")
total_cust = sum(r[1] for r in scope)
section("customer_id_scope", {
    "distinct_customers": total_cust,
    "brands_per_customer_hist": {str(r[0]): r[1] for r in scope},
    "pct_multi_brand": 100 * sum(r[1] for r in scope if r[0] > 1) / total_cust,
})

section("dates", {
    "min": q1("SELECT MIN(order_created_at) FROM basket"),
    "max": q1("SELECT MAX(order_created_at) FROM basket"),
    "p999": q1("SELECT QUANTILE_CONT(EPOCH(order_created_at), 0.999) FROM basket"),
    "monthly_orders": {str(r[0])[:7]: r[1] for r in q(
        "SELECT DATE_TRUNC('month', order_created_at) m, COUNT(*) FROM basket "
        "GROUP BY m ORDER BY m")},
})

nulls = {}
for t in ("basket", "items", "ipmap"):
    cols = [r[0] for r in q(f"DESCRIBE {t}")]
    exprs = ", ".join(f"100.0*SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END)/COUNT(*)" for c in cols)
    vals = q(f"SELECT {exprs} FROM {t}")[0]
    nulls[t] = {c: round(v, 3) for c, v in zip(cols, vals) if v > 0}
section("null_pct_nonzero_only", nulls)


def ip_profile(table, col):
    return {
        "null_pct": q1(f"SELECT 100.0*SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM {table}"),
        "distinct": q1(f"SELECT COUNT(DISTINCT {col}) FROM {table}"),
        "ipv4_pct_of_nonnull": q1(
            f"SELECT 100.0*SUM(CASE WHEN regexp_matches({col}, '{IPV4_RE}') THEN 1 ELSE 0 END)/COUNT(*) "
            f"FROM {table} WHERE {col} IS NOT NULL"),
        "ipv6_pct_of_nonnull": q1(
            f"SELECT 100.0*SUM(CASE WHEN {col} LIKE '%:%' THEN 1 ELSE 0 END)/COUNT(*) "
            f"FROM {table} WHERE {col} IS NOT NULL"),
        "bogon_pct_of_ipv4": q1(
            f"SELECT 100.0*SUM(CASE WHEN regexp_matches({col}, '{BOGON_RE}') THEN 1 ELSE 0 END)/COUNT(*) "
            f"FROM {table} WHERE regexp_matches({col}, '{IPV4_RE}')"),
        "distinct_ipv4": q1(
            f"SELECT COUNT(DISTINCT {col}) FROM {table} WHERE regexp_matches({col}, '{IPV4_RE}')"),
    }


section("ip_ipmap_ip_address", ip_profile("ipmap", "ip_address"))
section("ipmap_join", {
    "basket_customers_with_ipmap_pct": q1(
        "SELECT 100.0*COUNT(DISTINCT b.customer_id)/(SELECT COUNT(DISTINCT customer_id) FROM basket) "
        "FROM basket b WHERE EXISTS (SELECT 1 FROM ipmap m WHERE m.customer_id = b.customer_id)"),
    "ipmap_customers_in_basket_pct": q1(
        "SELECT 100.0*COUNT(DISTINCT m.customer_id)/(SELECT COUNT(DISTINCT customer_id) FROM ipmap) "
        "FROM ipmap m WHERE EXISTS (SELECT 1 FROM basket b WHERE b.customer_id = m.customer_id)"),
    "ips_per_customer_p50_p90_max": q(
        "SELECT QUANTILE_CONT(n, 0.5), QUANTILE_CONT(n, 0.9), MAX(n) FROM ("
        "  SELECT customer_id, COUNT(DISTINCT ip_address) n FROM ipmap GROUP BY 1)")[0],
    "customers_per_ip_p50_p99_max": q(
        "SELECT QUANTILE_CONT(n, 0.5), QUANTILE_CONT(n, 0.99), MAX(n) FROM ("
        "  SELECT ip_address, COUNT(DISTINCT customer_id) n FROM ipmap GROUP BY 1)")[0],
    "mega_ip_gt10_customers_pct": q1(
        "SELECT 100.0*SUM(CASE WHEN n > 10 THEN 1 ELSE 0 END)/COUNT(*) FROM ("
        "  SELECT ip_address, COUNT(DISTINCT customer_id) n FROM ipmap GROUP BY 1)"),
})

mono = {}
for c in CATEGORIES:
    mono[c] = {
        "6mo_gt_12mo_pct": q1(
            f"SELECT 100.0*SUM(CASE WHEN category_{c}_buyer_6mo AND NOT category_{c}_buyer_12mo "
            f"THEN 1 ELSE 0 END)/COUNT(*) FROM basket"),
        "12mo_gt_36mo_pct": q1(
            f"SELECT 100.0*SUM(CASE WHEN category_{c}_buyer_12mo AND NOT category_{c}_buyer_36mo "
            f"THEN 1 ELSE 0 END)/COUNT(*) FROM basket"),
    }
section("flag_monotonicity_violation_pct", mono)

flag_consistency = q(
    "SELECT AVG(CASE WHEN n_variants > 1 THEN 1.0 ELSE 0 END) FROM ("
    "  SELECT customer_id, COUNT(DISTINCT category_beauty_buyer_12mo) AS n_variants"
    "  FROM basket WHERE customer_id IS NOT NULL GROUP BY customer_id)")
section("flag_customer_consistency", {
    "pct_customers_with_conflicting_beauty12mo_flag": 100 * flag_consistency[0][0]})

section("distributions", {
    "financial_status": {r[0]: r[1] for r in q(
        "SELECT financial_status, COUNT(*) FROM basket GROUP BY 1 ORDER BY 2 DESC LIMIT 10")},
    "currency": {r[0]: r[1] for r in q(
        "SELECT currency, COUNT(*) FROM basket GROUP BY 1 ORDER BY 2 DESC LIMIT 10")},
    "shipping_country_top10": {r[0]: r[1] for r in q(
        "SELECT shipping_address_country, COUNT(*) FROM basket GROUP BY 1 ORDER BY 2 DESC LIMIT 10")},
    "brand_category": {r[0]: r[1] for r in q(
        "SELECT brand_category, COUNT(*) FROM basket GROUP BY 1 ORDER BY 2 DESC LIMIT 20")},
    "distinct_brands": q1("SELECT COUNT(DISTINCT brand_id) FROM basket"),
    "guest_checkout_pct": q1(
        "SELECT 100.0*SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM basket"),
})

OUT.write_text(json.dumps(results, indent=2, default=str))
print(f"\nwrote {OUT}", file=sys.stderr)
