#!/usr/bin/env python3
"""AUDI-1074: build the Proxima Sample Evaluation workbook from outputs/*.json + csv."""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
Q = ROOT / "queries"


def j(name):
    return json.load(open(OUT / name))


q1, q2, q3 = j("q1_cadence.json"), j("q2_affinity.json"), j("q3_concentration.json")
q4, q5, q6 = j("q4_overlap.json"), j("q5_cohorts.json"), j("q6_ntb.json")
q7, qc = j("q7_freshness.json"), j("qc_results.json")

wb = MntnWorkbook(
    title="Proxima Sample Evaluation",
    ticket="AUDI-1074",
    subtitle="1 year of DTC Shopify transactions: 80.0M orders, 1,163 brands, 33.5M customer-IP mappings",
    period="2025-07-16 to 2026-07-15 (delivered 2026-07-17)",
    generated="2026-08-24",
)

score = pd.DataFrame([
    ["1. Repurchase cadence", "Yes, measurable", "30-day median gap at item and category level", "Q1 Cadence"],
    ["2. Adjacent-bucket purchases", "Yes, real structure", "Software buyers reach Health & Beauty at 43% in 30d vs 24% baseline", "Q2 Category affinity"],
    ["3. Brand roster and volume", "Broad, low concentration", "$10.1B GMV/yr, 1,112 brands, top 10 = 25% of GMV", "Q3 Brands"],
    ["4. IP and brand overlap", "High addressability", "92% of 23.5M IPs in the MNTN bidding gate; 40% served last 30d", "Q4 Identity and overlap"],
    ["5. Cohort separability", "No", "Purchase features cannot tell high-intent from other served households (AUC 0.51)", "Q5 Cohort test"],
    ["6. First-time buyers", "Yes, identifiable", "~44% of orders are new-to-brand at steady state", "Q6 New-to-brand"],
    ["7. Data freshness", "~2 days", "Newest order is 2 days before delivery; volume full through day -3", "Q7 Freshness"],
], columns=["Evaluation question", "Answer", "Supporting number", "Detail tab"])
wb.table("Answers to the 7 questions", score,
         finding="6 of 7 questions answer in Proxima's favor; the exception: purchases do not mirror the intent score (AUC 0.51)",
         method="One row per AUDI-929 evaluation question. Each answer's basis is on the named detail tab; definitions on Read me.",
         kind="headline", toc="The verdict, one row per question")

gaps = pd.DataFrame([
    ["browser_ip column", "Missing; redelivery agreed", "Dictionary and delivery email promise it; vendor offered redelivery 2026-08-25"],
    ["Address columns (12)", "Deliberately omitted", "Vendor understood zip-only was needed from a prior call; redelivery with zip offered 2026-08-25"],
    ["ip_mapping headers", "No column names", "Files arrive as unnamed _COL_0 / _COL_1"],
    ["Customer IP coverage", "45.4% of customers", "AUDI-935 scoping said 60-80% IP fill on transactions"],
    ["Buyer-flag windows", "Disjoint bands; dictionary says cumulative", "Vendor 2026-08-25: 12mo flag = purchases 6-12 months ago, 36mo = 12-36. Dictionary wording contradicts the computation"],
    ["Item taxonomy", "24% unclassified", "product_taxonomy_level1 NULL on 23.6% of line items; levels 3-5 mostly NULL"],
    ["Item ID types", "VARCHAR, dictionary says BIGINT", "items order_id/line_item_id need a cast to join basket"],
    ["Monthly active brands", "Declined 19% over the year", "1,082 brands active Jul-2025 down to 878 Jul-2026"],
], columns=["Delivery item", "What we received", "Basis"])
wb.table("Delivery gaps", gaps,
         finding="The sample is clean at grain (0% duplicate orders); the 13 missing identity columns have an agreed redelivery path",
         method="Delivered files vs the vendor dictionary and AUDI-935 scoping. All items raised with Proxima 2026-08-24; their answers folded in 2026-08-25.",
         toc="Where delivery fell short of the dictionary")

cad = pd.read_csv(OUT / "q1_cadence_by_category.csv").sort_values("n_gaps", ascending=False).head(15)
cad.columns = ["Category (Shopify level 1)", "Repurchase gaps observed", "Median days between purchases", "25th percentile days", "75th percentile days"]
wb.table("Q1 Cadence", cad,
         finding="Median repurchase gap is 30 days pooled; per-category medians in the 20-60 day range",
         method="Gap = days between a customer's consecutive distinct purchase days in the same category. 1-yr window censors long cycles. See Read me.",
         formats={"Repurchase gaps observed": FMT.INT, "Median days between purchases": FMT.INT},
         heat={"Median days between purchases": "low"},
         toc="How often people repurchase, by category")

aff = pd.read_csv(OUT / "q2_affinity_30d.csv").sort_values("affinity_pct", ascending=False).head(20)
aff = aff[["from_cat", "to_cat", "anchor_customers", "buyers", "affinity_pct"]]
aff["affinity_pct"] = aff["affinity_pct"] / 100
aff.columns = ["First purchase in", "Then bought from", "Customers with first purchase", "Customers who crossed", "Crossed within 30 days"]
wb.table("Q2 Category affinity", aff,
         finding="Cross-category purchase paths are strong and specific; baseline for any follow-up purchase in 30d is 24.2%",
         method="Share of customers whose first purchase in category X is followed by any category-Y purchase within 30 days. Anchors end 30d before file close. See Read me.",
         formats={"Crossed within 30 days": FMT.PCT1, "Customers with first purchase": FMT.INT, "Customers who crossed": FMT.INT},
         heat={"Crossed within 30 days": "high"},
         toc="Which categories lead to which")

bc = pd.DataFrame(q3["by_brand_category"])
bc = bc.sort_values("gmv", ascending=False)
bc.columns = ["Brand category", "Brands", "Orders (USD, 1 yr)", "GMV (USD, 1 yr)"]
wb.table("Q3 Brands", bc,
         finding="$10.1B annual GMV over 1,112 USD brands; top 10 brands = 25% of GMV, top 100 = 70% (low concentration)",
         method="USD orders only (99.7%), refunded/voided excluded (1.3%). GMV = order totals incl. shipping and tax. Roster csv in the ticket repo.",
         formats={"Brands": FMT.INT, "Orders (USD, 1 yr)": FMT.INT, "GMV (USD, 1 yr)": FMT.USD0},
         heat={"GMV (USD, 1 yr)": "high"},
         toc="Brand categories, volume, concentration")

rows = []
for key, label in (("all_ips", "All Proxima IPs"), ("last_30d", "Ordered in last 30 days"),
                   ("d31_90", "Ordered 31-90 days ago"), ("d91_365", "Ordered 91-365 days ago")):
    r = q4[key]
    rows.append([label, r["n_proxima_ipv4"], r["ds14"]["share_pct"] / 100,
                 r["ds14"]["ci95_pct"][0] / 100, r["ds14"]["ci95_pct"][1] / 100,
                 r["cil"]["share_pct"] / 100, r["cil_unscored"]["share_pct"] / 100])
ov = pd.DataFrame(rows, columns=["Customer order recency", "Proxima IPv4 addresses",
                                 "In MNTN addressable gate", "CI 95 low", "CI 95 high",
                                 "Served by MNTN, last 30d", "Served but never scored"])
wb.table("Q4 Identity and overlap", ov,
         finding="92% of Proxima IPv4 sit in MNTN's addressable gate, and the share barely moves with order recency",
         method="Overlap vs 1% deterministic samples of the addressable gate (201M IPs) and served IPs (49.7M, 30d). Estimator and sampling on Method & caveats.",
         formats={"Proxima IPv4 addresses": FMT.INT, "In MNTN addressable gate": FMT.PCT1,
                  "CI 95 low": FMT.PCT1, "CI 95 high": FMT.PCT1,
                  "Served by MNTN, last 30d": FMT.PCT1, "Served but never scored": FMT.PCT1},
         heat={"In MNTN addressable gate": "high"},
         toc="Can MNTN reach these households")

beh = q5["behavior"]
coh = pd.DataFrame([
    ["Orders in 90 days (median)", beh["orders_90d"]["ever_hi_median"], beh["orders_90d"]["never_hi_median"], beh["orders_90d"]["mannwhitney_p"]],
    ["Order value USD (median)", beh["med_order_total"]["ever_hi_median"], beh["med_order_total"]["never_hi_median"], beh["med_order_total"]["mannwhitney_p"]],
    ["90-day spend USD (median)", beh["gmv_90d"]["ever_hi_median"], beh["gmv_90d"]["never_hi_median"], beh["gmv_90d"]["mannwhitney_p"]],
    ["Brands bought (median)", beh["brands_90d"]["ever_hi_median"], beh["brands_90d"]["never_hi_median"], beh["brands_90d"]["mannwhitney_p"]],
], columns=["Purchase behavior", "High-intent households", "Other served households", "Mann-Whitney p"])
wb.table("Q5 Cohort test", coh,
         finding="Purchase behavior is identical across intent cohorts; a classifier on 14 purchase features scores AUC 0.506 (chance)",
         method="18,205 high-intent vs 21,654 other served IPs matched into Proxima, last-90d orders. Cohort construction and AUC detail on Method & caveats.",
         formats={"High-intent households": FMT.NUM2, "Other served households": FMT.NUM2, "Mann-Whitney p": FMT.NUM2},
         toc="Does purchase history mirror the intent score")

ntb = pd.DataFrame([(m, p / 100) for m, p in q6["ntb_pct_by_month"].items()],
                   columns=["Month", "Orders that are first-time at brand"])
wb.table("Q6 New-to-brand", ntb,
         finding="New-to-brand share settles at ~44-46% of orders after a 5-month burn-in; a 6-month lookback suffices",
         method="First observed order per customer x brand. Early months read high by construction (left censoring). 0% guest checkout. See Read me.",
         
         formats={"Orders that are first-time at brand": FMT.PCT1},
         toc="Can first-time buyers be identified")

fresh = pd.DataFrame([
    ["Newest order in file", str(q7["max_ts"])],
    ["Delivery date", "2026-07-17"],
    ["Purchase-to-delivery lag", f"{q7['nominal_lag_days']} days"],
    ["Last date at full daily volume", str(q7["last_full_volume_date"])[:10]],
    ["Median daily orders (trailing)", f"{float(q7['trailing_median_daily_orders']):,.0f}"],
    ["Refresh cadence", "Monthly, sometimes weekly, in production (vendor 2026-08-25); eval is a one-time drop"],
    ["Purchase age at actionability", "~17 days mean at monthly refresh (cadence midpoint + 2-day lag)"],
    ["Refresh semantics", "Orders restated each delivery (full refresh, not append-only)"],
], columns=["Freshness measure", "Observed"])
wb.table("Q7 Freshness", fresh,
         finding="Purchases reach the delivered file ~2 days after they happen; daily volume is full through 3 days before delivery",
         method="Newest order timestamp and trailing daily-volume ramp vs the 2026-07-17 drop date. Timestamps are timezone-naive; read as UTC.",
         toc="How fast a purchase becomes usable")

wb.glossary("Read me", intro="Definitions for every term the tabs use.", rows=[
    ("The sample", ""),
    ("Basket", "Order-level file: 80.0M orders, one row per order_id, with customer_id and 30 category buyer flags."),
    ("Items", "Line-item file: 162.0M rows, Shopify product taxonomy, joins basket on order_id."),
    ("ip_mapping", "Customer-to-IP file: 33.5M rows, 14.8M customers, 24.7M distinct IPs (96% IPv4)."),
    ("Category (level 1)", "Top level of the Shopify product taxonomy, e.g. Health & Beauty. NULL on 24% of line items."),
    ("Buyer flag", "Vendor-computed boolean over DISJOINT windows: 6mo = past 6 months, 12mo = 6-12 months ago, 36mo = 12-36 months ago. A past-12-months audience is 6mo OR 12mo."),
    ("MNTN terms", ""),
    ("Addressable gate", "The set of IPs MNTN can currently bid on: seen in its logs within the serving window. 201M IPs on the sample day."),
    ("Served IPs", "Distinct IPs that received at least one MNTN impression in the trailing 30 days: 49.7M."),
    ("High-intent household", "A served IP whose intent score reached the top band (8000-10000) at least once in the 30-day window."),
    ("Intent score", "MNTN's household purchase-intent model output, 0-10000; -1 means the impression was unscored."),
    ("Measures", ""),
    ("New-to-brand", "A customer's first observed order at a brand within the 1-year file."),
    ("AUC", "Probability a classifier ranks a random high-intent household above a random other one; 0.5 = coin flip, 1.0 = perfect."),
    ("CI 95", "Range that would contain the true value in 95% of repeated samples."),
])

wb.notes("Method & caveats", blocks=[
    ("Overlap estimator", "MNTN-side sets are sampled with a deterministic 1% hash (FARM_FINGERPRINT mod 100). Overlap share = matches x 100 / (1% x Proxima IPs), Wilson 95% CI. Both denominators sampled 2026-08-23/24."),
    ("IP recency is customer-level", "The promised per-order browser_ip was not delivered, so IP recency uses the customer's most recent order date via ip_mapping. Purchase-time IP recency is not measurable on this sample."),
    ("Unscored sentinel", "Served-impression scores mark unscored as -1, not NULL. First pass read scored coverage as 100%; corrected same day. 46.7% of served sample IPs never scored in the window."),
    ("Cohort test construction", "Cohorts from the served-IP sample: ever high-intent (n=201,246) vs never (n=296,232, mixes unscored and low-scored). Matched into Proxima via ip_mapping; last-90d orders; 5-fold CV logistic on 14 purchase-only features."),
    ("What AUC 0.51 does and does not say", "Purchase history cannot reproduce the intent score's bands. It does not rule out incremental lift on outcomes; that needs the follow-up offline model test with leakage controls."),
    ("Censoring", "1-year window biases repurchase medians short (long cycles truncated) and inflates early-month new-to-brand shares (left censoring; first 5 months are burn-in)."),
    ("Exclusions", "Brand/GMV tab: USD orders only (99.65%), refunded and voided excluded (1.28%). Overlap tabs: IPv4 only; IPv6 is 3.9% of mapped IPs and outside MNTN's current feature pipeline."),
    ("Timezones", "order_created_at is timezone-naive; all dates read as UTC. Freshness lag carries a plus/minus 1 day sensitivity."),
    ("Flag band semantics", "The 5-10% of rows reading 6mo=true, 12mo=false are legal under the vendor-confirmed band windows, and 36mo-flagged customers without in-file purchases are expected (12-36mo is pre-file). Both were first read as defects; reframed 2026-08-25."),
])

wb.sql_dir("Queries", str(Q),
           note="BigQuery SQL for the MNTN-side samples. Local transforms run in DuckDB; scripts live in the ticket repo under analysis/.")

wb.cover(takeaways=[
    "92% of Proxima's 23.5M purchase-linked IPs are MNTN-addressable, and purchases reach the file ~2 days after they happen",
    "$10.1B/yr across 1,112 DTC brands, 30-day median repurchase cycle, ~44% of orders new-to-brand: strong measurement and seeding raw material",
    "Purchase history does not mirror MNTN's intent score (AUC 0.51): feature value for the model is unproven pending an offline lift test",
])
print(wb.save_drive("AUDI-1074", "Proxima Sample Evaluation"))
