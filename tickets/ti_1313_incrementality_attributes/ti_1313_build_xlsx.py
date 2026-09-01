#!/usr/bin/env python3
"""Build the TI-1313 incrementality-attributes workbook from the query CSV."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace")
sys.path.insert(0, str(ROOT))
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

TICKET = ROOT / "tickets/ti_1313_incrementality_attributes"
df = pd.read_csv(TICKET / "outputs/ti_1313_campaign_lift_attributes.csv")

df = df[df["n_holdout"] >= 100].copy()
df["visit_lift"] = df["visit_lift_pct"] / 100.0
df["se_lift"] = (df["visit_ci_high_pct"] - df["visit_ci_low_pct"]) / (2 * 1.96 * 100.0)
df["significant"] = df["visit_significant"].astype(str).str.lower() == "true"

for c in ["pct_ctv_chan", "pct_display_chan", "pct_high_intent",
          "pct_peak_intent", "pct_mid_intent", "pct_max_reach"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df["primary_channel"] = np.where(
    df["pct_ctv_chan"].fillna(0) >= 0.5, "CTV",
    np.where(df["pct_display_chan"].fillna(0) >= 0.5, "Display", "Mixed / unknown"))
df.loc[df["impression_count"].isna(), "primary_channel"] = None

df["dominant_intent"] = df[["pct_high_intent", "pct_peak_intent",
                            "pct_mid_intent", "pct_max_reach"]].idxmax(axis=1).map({
    "pct_high_intent": "High Intent", "pct_peak_intent": "Peak Performance",
    "pct_mid_intent": "Mid Intent", "pct_max_reach": "Max Reach"})
df.loc[df["impression_count"].isna(), "dominant_intent"] = None


def pooled(g):
    """Inverse-variance-weighted mean lift; campaigns with no usable SE fall out of the weighting."""
    ok = g[(g["se_lift"] > 0) & g["se_lift"].notna() & g["visit_lift"].notna()]
    if len(ok) == 0:
        return pd.Series({"campaigns": len(g), "pooled_lift": np.nan, "pooled_se": np.nan,
                          "pct_significant": np.nan, "median_lift": np.nan,
                          "total_incremental_visits": g["incremental_visits"].sum(),
                          "total_spend": g["total_spend_usd"].sum()})
    w = 1.0 / ok["se_lift"] ** 2
    pooled_lift = (ok["visit_lift"] * w).sum() / w.sum()
    return pd.Series({
        "campaigns": len(g),
        "pooled_lift": pooled_lift,
        "pooled_se": np.sqrt(1.0 / w.sum()),
        "pct_significant": g["significant"].mean(),
        "median_lift": g["visit_lift"].median(),
        "total_incremental_visits": g["incremental_visits"].sum(),
        "total_spend": g["total_spend_usd"].sum(),
    })


def summarize(by, label):
    s = df.groupby(by, dropna=True).apply(pooled, include_groups=False).reset_index()
    s = s[s["campaigns"] >= 5]
    s["ci_low"] = s["pooled_lift"] - 1.96 * s["pooled_se"]
    s["ci_high"] = s["pooled_lift"] + 1.96 * s["pooled_se"]
    s = s.rename(columns={by: label, "campaigns": "Campaigns", "pooled_lift": "Pooled lift",
                          "ci_low": "CI low", "ci_high": "CI high",
                          "pct_significant": "% significant", "median_lift": "Median lift",
                          "total_incremental_visits": "Incremental visits",
                          "total_spend": "Spend"})
    cols = [label, "Campaigns", "Pooled lift", "CI low", "CI high", "% significant",
            "Median lift", "Incremental visits", "Spend"]
    return s[cols].sort_values("Pooled lift", ascending=False)


by_vertical = summarize("vertical_name", "Vertical")
by_channel = summarize("primary_channel", "Primary channel")
by_intent = summarize("dominant_intent", "Dominant intent band")

raw = df[[
    "campaign_group_id", "campaign_group_name", "company_name", "vertical_name", "product",
    "visit_lift", "visit_ci_low_pct", "visit_ci_high_pct", "z_stat", "significant",
    "baseline_visit_rate", "incremental_visits", "primary_channel", "pct_ctv_chan", "pct_display_chan",
    "dominant_intent", "avg_household_score", "pct_high_intent", "pct_peak_intent",
    "pct_mid_intent", "pct_max_reach", "frequency_cap_impressions", "has_audience", "budget",
    "impression_count", "total_spend_usd", "cost_per_incremental_visit", "account_health",
    "monthly_muv", "n_treatment", "n_holdout",
]].copy()
raw["visit_ci_low_pct"] = raw["visit_ci_low_pct"] / 100.0
raw["visit_ci_high_pct"] = raw["visit_ci_high_pct"] / 100.0
raw = raw.rename(columns={
    "campaign_group_id": "CG id", "campaign_group_name": "Campaign group", "company_name": "Advertiser",
    "vertical_name": "Vertical", "product": "Product", "visit_lift": "Visit lift",
    "visit_ci_low_pct": "CI low", "visit_ci_high_pct": "CI high", "z_stat": "z",
    "significant": "Significant", "baseline_visit_rate": "Baseline rate",
    "incremental_visits": "Incremental visits", "primary_channel": "Primary channel",
    "pct_ctv_chan": "% CTV", "pct_display_chan": "% Display",
    "dominant_intent": "Dominant intent", "avg_household_score": "Avg score",
    "pct_high_intent": "% High", "pct_peak_intent": "% Peak", "pct_mid_intent": "% Mid",
    "pct_max_reach": "% Max reach", "frequency_cap_impressions": "Freq cap",
    "has_audience": "Has audience", "budget": "Budget", "impression_count": "Impressions",
    "total_spend_usd": "Spend", "cost_per_incremental_visit": "Cost per inc visit",
    "account_health": "Account health", "monthly_muv": "Monthly MUV",
    "n_treatment": "Treated IPs", "n_holdout": "Holdout IPs",
}).sort_values("Incremental visits", ascending=False)

n = len(df)
n_sig = int(df["significant"].sum())
n_imp = int(df["impression_count"].notna().sum())

wb = MntnWorkbook(
    title="Campaign incrementality by attribute",
    ticket="AUDI-1313",
    subtitle=f"Ghost-bid visit lift for {n:,} campaign groups, paired with delivery attributes",
    period="Lift: all-time. Delivery attributes: Jul-Aug 2026",
)

SUM_FMT = {"Pooled lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
           "% significant": FMT.PCT0, "Median lift": FMT.PCT1,
           "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Campaigns": FMT.INT}

wb.table(
    "By vertical", by_vertical,
    finding="Pooled visit lift by advertiser vertical",
    method="Inverse-variance-weighted mean of per-campaign relative lift. Verticals with under 5 campaigns excluded. Observational: verticals differ in more than vertical.",
    formats=SUM_FMT, signal={"Pooled lift": {}}, kind="headline",
    toc="Pooled lift for each vertical, ranked",
    query="ti_1313_main_query.sql")

wb.table(
    "By channel", by_channel,
    finding="Pooled visit lift by primary delivery channel",
    method="Primary channel = the campaign channel holding 50% or more of impressions, from campaigns.channel_id (8 = CTV, 1 = Display). Same inverse-variance weighting.",
    formats=SUM_FMT, signal={"Pooled lift": {}}, kind="headline",
    toc="Pooled lift by CTV, Display, Mobile or mixed",
    query="ti_1313_main_query.sql")

wb.table(
    "By intent band", by_intent,
    finding="Pooled visit lift by the intent band holding most impressions",
    method="Band assigned from the largest share of scored impressions. Scores are null before 2025-06-01, so older campaigns are unbanded.",
    formats=SUM_FMT, signal={"Pooled lift": {}}, kind="data",
    toc="Pooled lift by High, Peak, Mid or Max Reach",
    query="ti_1313_main_query.sql")

wb.table(
    "Campaign detail", raw,
    finding=f"{n:,} campaign groups with at least 100 holdout IPs, {n_sig:,} significant at 95%",
    method="One row per campaign group. Lift is all-time relative intent-to-treat on the ghost-bid holdout. Delivery attributes cover Jul-Aug 2026 only.",
    formats={"Visit lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
             "Baseline rate": FMT.PCT2, "Incremental visits": FMT.INT, "% CTV": FMT.PCT0,
             "% Display": FMT.PCT0, "% High": FMT.PCT0, "% Peak": FMT.PCT0,
             "% Mid": FMT.PCT0, "% Max reach": FMT.PCT0, "Avg score": FMT.INT,
             "Impressions": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2,
             "Budget": FMT.USD0, "Treated IPs": FMT.INT, "Holdout IPs": FMT.INT, "z": FMT.NUM2},
    signal={"Visit lift": {"sig": "Significant"}}, kind="data",
    toc="Every campaign group with its lift and its attributes",
    query="ti_1313_main_query.sql")

wb.glossary(
    "Read me",
    intro="How these numbers were produced and what they can and cannot support.",
    rows=[
        ("Visit lift", "Relative intent-to-treat lift in visit rate, treated versus ghost-bid holdout."),
        ("Ghost-bid holdout", "10% of IPs are withheld from bidding. Lift compares served IPs against them."),
        ("Pooled lift", "Inverse-variance-weighted mean across campaigns. Precise campaigns count more."),
        ("Significant", "95% confidence interval on the lift excludes zero."),
        ("Baseline rate", "Visit rate in the holdout arm."),
        ("Dominant intent", "The intent band holding the largest share of the campaign's scored impressions."),
        ("Primary channel", "From the campaign channel on each impression: 8 is CTV, 1 is Display. A campaign under 50% either way reads as mixed."),
        ("Observational", "Attributes are advertiser-chosen, not assigned. Differences between groups are confounded. Treat every pattern here as a hypothesis to test, not an effect."),
        ("Time mismatch", "Lift is all-time. Delivery attributes are Jul-Aug 2026. A campaign whose mix changed is described by its recent mix."),
        ("Score coverage", f"{n_imp:,} of {n:,} campaigns matched impression data. Household scores are null before 2025-06-01."),
        ("Excluded", "Partner 79, zero-variance rows, low-coverage rows, and campaigns under 100 holdout IPs."),
    ])

wb.sql_dir("Queries", str(TICKET / "queries"),
           ignore=["ti_1313_main_query_draft.sql"],
           note="BigQuery SQL behind every sheet.")
wb.cover(takeaways=[
    f"{n:,} campaign groups clear the 100-holdout-IP bar. {n_sig:,} show significant visit lift at 95%.",
    "Pooled lift is reported by vertical, by primary channel and by dominant intent band, each weighted by precision.",
    "Attributes are advertiser-chosen, so every difference here is a hypothesis for a designed test, not a measured effect.",
])

out = wb.save_drive("AUDI-1313", "Campaign Incrementality by Attribute")
print(out)
print(f"campaigns={n} significant={n_sig} with_impressions={n_imp}")
