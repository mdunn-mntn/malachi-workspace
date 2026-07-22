#!/usr/bin/env python3
"""Gruns — CTV prospecting campaign (CGID 126905, excludes high intent):
performance metrics + ghost-bid incremental visit lift.  Shareable .xlsx for Kirsa
(incrementality team).  Built on the INCR-75 ghost-bid measurement + reporting layer.

Lift window = Jun 24 – Jul 14, 2026 (the active incrementality-data range; matches the
measurement pipeline exactly — treat VR 0.0010273, control VR 0.0008916, rel CI [-32%,+63%]).
All numbers verified via bq_run.sh on 2026-07-22 (see the "Queries" sheet).  Sources:
  * Delivery / spend  -> logdata.cost_impression_log            (flight 2026-06-08..07-22)
  * Reporting visits  -> summarydata.all_facts (industry-std = last_touch + competing)
  * Conversions/rev   -> summarydata.ui_conversions             (distinct converting IPs)
  * Incremental lift  -> enriched.lift__ghost_bid_visits         (entry-cohort, Jun 24..Jul 14)
"""
import math
import os
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

GEN = "2026-07-22"
OUT = "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers/outputs"

# ---------------------------------------------------------------------------
# 1. Incremental lift — prospecting campaign 626276, entry-cohort method,
#    active incrementality window Jun 24 – Jul 14 (matches the pipeline exactly).
# ---------------------------------------------------------------------------
T_N, T_V = 207323, 213                   # treatment (ad served-eligible)
H_N, H_V = 21309, 19                     # holdout (ghost bid, never served)
WIN_RATE = 0.373                         # treatment served (won) fraction, ~stable across days

p1, p2 = T_V / T_N, H_V / H_N
abs_lift = p1 - p2
rel_lift = abs_lift / p2
se = math.sqrt(p1 * (1 - p1) / T_N + p2 * (1 - p2) / H_N)
z = abs_lift / se
p_val = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
ci_abs = (abs_lift - 1.96 * se, abs_lift + 1.96 * se)
ci_rel = (ci_abs[0] / p2, ci_abs[1] / p2)
ghost_frac = H_N / (T_N + H_N)

print(f"treat VR={p1:.7f}  holdout VR={p2:.7f}  abs={abs_lift:.7f}  "
      f"rel={rel_lift:+.1%}  z={z:.2f}  p={p_val:.3f}  "
      f"CI_abs=[{ci_abs[0]:.6f},{ci_abs[1]:.6f}]  CI_rel=[{ci_rel[0]:+.0%},{ci_rel[1]:+.0%}]  gf={ghost_frac:.3f}")

lift_df = pd.DataFrame([
    {"Group": "Ad served (treatment)", "Prospects reached": T_N, "Visits": T_V,
     "Visit rate": p1, "Lift vs holdout": rel_lift},
    {"Group": "Holdout (no ad shown)", "Prospects reached": H_N, "Visits": H_V,
     "Visit rate": p2, "Lift vs holdout": None},
])

# ---------------------------------------------------------------------------
# 2. Performance — flight-to-date 2026-06-08 .. 2026-07-22.
#    Visits = MNTN view-through visits, industry-standard lens (last_touch + competing).
# ---------------------------------------------------------------------------
perf_rows = [
    # campaign, stage, imps, reach, spend, visits, conv
    ("Prospecting — excludes high intent (626276)", "Prospecting", 440919, 348769, 9575.63, 835, 21),
    ("Multi-Touch (626275)", "Exposed to Prior Ad", 1069040, 179478, 23461.21, 543, 7),
    ("Multi-Touch Plus (626274)", "Has a Prior VV", 9510, 695, 214.76, 165, 1),
]
perf = []
for name, stage, imps, reach, spend, visits, conv in perf_rows:
    perf.append({
        "Campaign": name, "Stage": stage, "Impressions": imps, "Households reached": reach,
        "Spend": spend, "Visits": visits, "Visit rate": visits / imps,
        "CPV": spend / visits, "Conv.": conv,
    })
g_imps = sum(r[2] for r in perf_rows); g_spend = sum(r[4] for r in perf_rows)
g_vis = sum(r[5] for r in perf_rows); g_conv = sum(r[6] for r in perf_rows)
perf.append({
    "Campaign": "Campaign group total (126905)", "Stage": "All (CTV)", "Impressions": g_imps,
    "Households reached": None, "Spend": g_spend, "Visits": g_vis,
    "Visit rate": g_vis / g_imps, "CPV": g_spend / g_vis, "Conv.": g_conv,
})
perf_df = pd.DataFrame(perf)

os.makedirs(OUT, exist_ok=True)
lift_df.to_csv(os.path.join(OUT, "incr_75_gruns_cgid126905_lift.csv"), index=False)
perf_df.to_csv(os.path.join(OUT, "incr_75_gruns_cgid126905_performance.csv"), index=False)

# ---------------------------------------------------------------------------
# 4. Build the workbook.
# ---------------------------------------------------------------------------
wb = MntnWorkbook(
    title="Gruns — CTV Prospecting Incrementality",
    ticket="INCR-75",
    subtitle="Campaign group 126905 (“CTV Prospecting TOFU High DMA”, excludes high intent) · Gruns, advertiser 42097",
    period="Performance: flight to date (Jun 8 – Jul 22, 2026) · Lift: active window Jun 24 – Jul 14",
    generated=GEN,
)

SIGN_PCT = "+0.0%;-0.0%"

wb.table(
    "Incremental lift", lift_df,
    finding="Incremental visit lift is +15%, but not distinguishable from zero on this campaign",
    method=("Served 0.103% vs holdout 0.089% = +15% relative, 95% CI [−32%, +63%], p = 0.53. Holdout = ~9% of prospects bid on but "
            "never served; visit = within 7 days of first bid; window Jun 24 – Jul 14. Only 19 holdout visits, so underpowered. "
            "Matches the ghost-bid pipeline and the gold rollup exactly."),
    formats={"Prospects reached": FMT.INT, "Visits": FMT.INT, "Visit rate": FMT.PCT3,
             "Lift vs holdout": SIGN_PCT},
    kind="headline",
    toc="Headline: treatment vs holdout visit rate (why one campaign can't confirm it)",
)

wb.table(
    "Performance", perf_df,
    finding="A 44-day, $33K CTV flight; the prospecting stage ran a 0.19% visit rate — low by design",
    method=("Flight to date Jun 8 – Jul 22, 2026. Spend = media + data + platform. Visits = MNTN view-through (industry-standard: "
            "last-touch + competing). CPV = spend ÷ visits ($2.50 goal). Stage = who the campaign targets: Prospecting = new "
            "audience; Exposed to Prior Ad = already saw an ad; Has a Prior VV = already visited. The high-intent exclusion and "
            "the holdout apply to Prospecting only."),
    formats={"Impressions": FMT.INT, "Households reached": FMT.INT, "Spend": FMT.USD0,
             "Visits": FMT.INT, "Visit rate": FMT.PCT2, "CPV": FMT.USD2, "Conv.": FMT.INT},
    heat={"Visit rate": "high"},
    kind="headline",
    toc="Delivery, spend, visits, visit rate and CPV by campaign in the group",
)

wb.notes(
    "Read me",
    intro="Plain-English guide: what the campaign is, what the lift does and doesn't show, and the platform-wide answer.",
    blocks=[
        ("What this campaign is",
         "Gruns (advertiser 42097) ran a CTV prospecting campaign group (126905) that excludes high intent — top-of-funnel, "
         "high-population DMAs, live Jun 8 to Aug 1, 2026, $2.50 cost-per-visit goal. CTV-only."),
        ("The incremental read",
         "Served group visited at 0.103% vs 0.089% for the ~9% holdout (bid on, never shown an ad) = +15% relative, over Jun 24 to "
         "Jul 14."),
        ("Why it is not conclusive",
         "Not significant: 95% CI −32% to +63%, p = 0.53. The holdout produced only 19 visits and grows ~1/day, so even by the Aug 1 "
         "flight end it reaches only ~29. A fixed ~10% holdout on a 0.1%-visit-rate campaign can't resolve a few-percent lift."),
        ("Why ~0 lift is the expected result here",
         "The audience is almost entirely UNSCORED (\"no-score\") households — it excludes high AND mid intent, not just high (of 207K "
         "prospects, 5 are high-intent and 30 mid-intent; the rest have no score). Across MNTN, unscored / reach audiences show "
         "essentially no incremental lift — they visit anyway. The audiences that DO lift are mid-intent, which this campaign excludes. "
         "So ~0 is the expected result; the +15% is noise consistent with zero."),
        ("Reading the performance numbers",
         "The ~0.2% visit rate and ~$11 CPV (vs the $2.50 goal) are by design: a cold, intent-excluded audience visits less than warm "
         "retargeting. Visits are MNTN view-through (TV ads are rarely clicked); conversions are sparse, so visit rate is the KPI."),
    ],
)

SQL = """-- SIMPLEST PATH (gold layer, already time-boxed): reproduces the aggregate lift below exactly.
SELECT * FROM `dw-main-gold.reporting.lift__ghost_bid_rollup`  WHERE entity_id = 126905;          -- aggregate
SELECT * FROM `dw-main-gold.reporting.lift__ghost_bid_results` WHERE campaign_group_id = 126905;  -- per score_band / bid_count stratum (confirms High+Mid excluded -> ~100% no_score)

-- Incremental lift, from silver (entry-cohort; active window Jun 24 - Jul 14, 2026)
WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited, won,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE campaign_group_id = 126905
),
entry AS (SELECT arm, visited, won, dt AS entry_dt FROM e WHERE rn = 1)
SELECT arm, COUNT(*) n_ip, COUNTIF(won) n_won, COUNTIF(visited) visits,
       100*SAFE_DIVIDE(COUNTIF(visited), COUNT(*)) AS vr_pct
FROM entry WHERE entry_dt BETWEEN '2026-06-24' AND '2026-07-14'
GROUP BY arm;

-- Delivery + spend by campaign (flight to date)
SELECT campaign_id, COUNT(*) impressions, COUNT(DISTINCT ip) reach_ips,
       SUM(CAST(media_spend AS FLOAT64)+CAST(data_spend AS FLOAT64)+CAST(platform_spend AS FLOAT64)) total_spend
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) BETWEEN '2026-06-08' AND CURRENT_DATE()
  AND advertiser_id = 42097 AND campaign_id IN (626274,626275,626276)
GROUP BY campaign_id;

-- Visits (industry-standard = last_touch + competing) from the reporting layer
SELECT campaign_id, SUM(last_touch_views)+SUM(competing_views) AS visits_industry_std
FROM `dw-main-silver.summarydata.all_facts`
WHERE CAST(hour AS DATE) BETWEEN '2026-06-08' AND CURRENT_DATE()
  AND advertiser_id = 42097 AND campaign_group_id = 126905
GROUP BY campaign_id;"""
wb.sql("Queries", SQL, note="Run 2026-07-22. The gold rollup reproduces the silver entry-cohort calc to the digit (+15.2%, CI, z) — cross-validated.")

wb.cover(takeaways=[
    "Incremental visit lift is +15% but not significant (95% CI −32% to +63%, only 19 holdout visits) — the campaign is too small to resolve it.",
    "The audience is ~100% unscored (\"no-score\") households: it excludes high AND mid intent. Unscored / reach audiences don't drive incremental lift, so ~0 is the expected result here, not +15%.",
    "Raw visit rate ~0.2% and CPV ~$11 (vs the $2.50 goal) are low by design for a cold, intent-excluded audience.",
])

local = wb.save_local(os.path.join(OUT, "incr_75_gruns_cgid126905_incrementality.xlsx"))
print("local:", local)
try:
    drive = wb.save_drive("INCR-75", "Gruns Incrementality CGID 126905")
    print("drive:", drive)
except Exception as ex:
    print("drive save skipped:", ex)
