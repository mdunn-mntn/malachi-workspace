#!/usr/bin/env python3
"""Build the TI-1313 incrementality-attributes workbook from the three query CSVs."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace")
sys.path.insert(0, str(ROOT))
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

TICKET = ROOT / "tickets/ti_1313_incrementality_attributes"
OUT = TICKET / "outputs"

base = pd.read_csv(OUT / "ti_1313_campaign_base.csv")
bands = pd.read_csv(OUT / "ti_1313_score_bands.csv")
freq = pd.read_csv(OUT / "ti_1313_bid_counts.csv")

BAND_ORDER = ["High", "PP", "Mid", "MaxReach", "no_score"]
BAND_LABEL = {"High": "High Intent", "PP": "Peak Performance", "Mid": "Mid Intent",
              "MaxReach": "Max Reach", "no_score": "Unscored"}
FREQ_ORDER = ["1", "2-3", "4-10", "11+"]


def log_rr(df):
    """Log risk ratio and its variance; the pooling basis data_catalog.md requires for relative lift."""
    pt, ph = df["rate_treatment"], df["rate_holdout"]
    nt, nh = df["n_treatment"], df["n_holdout"]
    ok = (pt > 0) & (ph > 0) & (nt > 0) & (nh > 0)
    y = np.where(ok, np.log(pt.where(ok, 1) / ph.where(ok, 1)), np.nan)
    v = np.where(ok, (1 - pt) / (pt * nt) + (1 - ph) / (ph * nh), np.nan)
    return pd.Series(y, index=df.index), pd.Series(v, index=df.index)


def pool(df):
    """DerSimonian-Laird random-effects pool of the log risk ratio."""
    y, v = log_rr(df)
    m = y.notna() & v.notna() & (v > 0)
    k = int(m.sum())
    if k == 0:
        return None
    y, v = y[m].to_numpy(), v[m].to_numpy()
    w = 1.0 / v
    fixed = (w * y).sum() / w.sum()
    q = float((w * (y - fixed) ** 2).sum())
    tau2 = max(0.0, (q - (k - 1)) / (w.sum() - (w**2).sum() / w.sum())) if k > 1 else 0.0
    wr = 1.0 / (v + tau2)
    est = float((wr * y).sum() / wr.sum())
    se = float(np.sqrt(1.0 / wr.sum()))
    i2 = max(0.0, (q - (k - 1)) / q) if (k > 1 and q > 0) else 0.0
    return {"k": k, "lift": np.expm1(est), "lo": np.expm1(est - 1.96 * se),
            "hi": np.expm1(est + 1.96 * se), "i2": i2}


def summarize(df, by, label, order=None, min_k=5, extras=True):
    rows = []
    for key, g in df.groupby(by, dropna=True):
        p = pool(g)
        if p is None or p["k"] < min_k:
            continue
        r = {label: key, "Campaigns": p["k"], "Pooled lift": p["lift"],
             "CI low": p["lo"], "CI high": p["hi"],
             "% significant": g["significant_95"].mean(),
             "Heterogeneity": p["i2"],
             "Incremental visits": g["incremental_visits"].sum()}
        if extras:
            r["Spend"] = g["prospecting_spend"].sum()
            r["Cost per inc visit"] = (g["prospecting_spend"].sum() / g["incremental_visits"].sum()
                                       if g["incremental_visits"].sum() > 0 else np.nan)
        rows.append(r)
    out = pd.DataFrame(rows)
    if order:
        out[label] = pd.Categorical(out[label], categories=order, ordered=True)
        out = out.sort_values(label)
    else:
        out = out.sort_values("Pooled lift", ascending=False)
    return out.reset_index(drop=True)


by_vertical = summarize(base, "vertical_name", "Vertical")

bands["band"] = bands["score_band"].map(BAND_LABEL)
by_band = summarize(bands, "band", "Intent band",
                    order=[BAND_LABEL[b] for b in BAND_ORDER], extras=False)

freq["band"] = pd.Categorical(freq["bid_count_band"], categories=FREQ_ORDER, ordered=True)
by_freq = summarize(freq, "band", "Bids per household", order=FREQ_ORDER, extras=False)

base["mt_bucket"] = pd.cut(
    base["pct_spend_multitouch"].fillna(0),
    bins=[-0.001, 0.001, 0.05, 0.15, 1.0],
    labels=["None", "Under 5%", "5 to 15%", "Over 15%"])
by_mt = summarize(base, "mt_bucket", "Multi-touch share of spend",
                  order=["None", "Under 5%", "5 to 15%", "Over 15%"])

base["freq_bucket"] = pd.qcut(base["avg_frequency"], 4,
                              labels=["Lowest quartile", "Second", "Third", "Highest quartile"])
by_freq_q = summarize(base, "freq_bucket", "Average frequency",
                      order=["Lowest quartile", "Second", "Third", "Highest quartile"])

settings = []
for col, lab in [("product", "Product"),
                 ("has_audience", "Uses audience targeting"),
                 ("account_health", "Account health")]:
    t = summarize(base, col, "Value", min_k=5)
    if len(t) < 2:
        continue
    t = t.rename(columns={"Value": "Setting"})
    t.insert(0, "Attribute", lab)
    settings.append(t)
by_setting = pd.concat(settings, ignore_index=True)

detail = base[[
    "campaign_group_id", "campaign_group_name", "advertiser_name", "vertical_name", "product",
    "rel_itt", "p_value", "significant_95", "rate_holdout", "rate_treatment",
    "incremental_visits", "prospecting_spend", "cost_per_incremental_visit",
    "avg_frequency", "pct_spend_multitouch", "has_audience", "budget",
    "conv_rel_itt", "conv_significant_95", "ntb_rel_itt",
    "account_health", "monthly_muv", "ip_compliance",
    "n_treatment", "n_holdout", "vis_treatment", "vis_holdout",
]].rename(columns={
    "campaign_group_id": "CG id", "campaign_group_name": "Campaign group",
    "advertiser_name": "Advertiser", "vertical_name": "Vertical", "product": "Product",
    "rel_itt": "Visit lift", "p_value": "p value", "significant_95": "Significant",
    "rate_holdout": "Holdout visit rate", "rate_treatment": "Treated visit rate",
    "incremental_visits": "Incremental visits", "prospecting_spend": "Spend",
    "cost_per_incremental_visit": "Cost per inc visit", "avg_frequency": "Avg frequency",
    "pct_spend_multitouch": "Multi-touch spend", "has_audience": "Uses audience",
    "budget": "Budget", "conv_rel_itt": "Conversion lift",
    "conv_significant_95": "Conv significant", "ntb_rel_itt": "New-to-brand lift",
    "account_health": "Account health", "monthly_muv": "Monthly visitors",
    "ip_compliance": "Share of bid households reached",
    "n_treatment": "Treated households", "n_holdout": "Holdout households",
    "vis_treatment": "Treated visits", "vis_holdout": "Holdout visits",
}).sort_values("Incremental visits", ascending=False)

n = len(base)
n_sig = int(base["significant_95"].sum())
n_adv = base["advertiser_id"].nunique()
spend = base["prospecting_spend"].sum()
inc = base["incremental_visits"].sum()

SUMF = {"Pooled lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
        "% significant": FMT.PCT0, "Heterogeneity": FMT.PCT0, "Campaigns": FMT.INT,
        "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2}

wb = MntnWorkbook(
    title="Campaign incrementality by attribute",
    ticket="AUDI-1313",
    subtitle=f"Ghost-bid visit lift for {n:,} powered campaign groups across {n_adv:,} advertisers",
    period="Ghost-bid measurement window: 22 Jun to 31 Aug 2026",
)

wb.table(
    "By vertical", by_vertical,
    finding="Pooled visit lift by advertiser vertical",
    method="Random-effects pool of the log risk ratio per campaign. Verticals under 5 campaigns excluded. Advertisers pick their vertical, so this compares populations, not a setting anyone can change.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="headline",
    toc="Pooled lift for each vertical, ranked",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By intent band", by_band,
    finding="Pooled visit lift by the intent band the household was scored into",
    method="From the sanctioned score-band strata, pooled across campaigns. A campaign appears in every band it delivered to, so these are within-campaign comparisons.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="headline",
    toc="Lift by High, Peak, Mid, Max Reach and Unscored",
    query="ti_1313_score_band_strata.sql")

wb.table(
    "By frequency", by_freq,
    finding="Pooled visit lift by how many times a household was bid on",
    method="From the sanctioned bid-count strata, pooled across campaigns. Households are not randomised into these bands, so heavier exposure also marks a more responsive household.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="headline",
    toc="Lift at 1, 2 to 3, 4 to 10 and 11+ bids per household",
    query="ti_1313_bid_count_strata.sql")

wb.table(
    "By campaign frequency", by_freq_q,
    finding="Pooled visit lift by the campaign's own average frequency",
    method="Campaigns split into quartiles on prospecting impressions divided by distinct households reached over the measurement window.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by the campaign's average frequency quartile",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By multi-touch mix", by_mt,
    finding="Pooled visit lift by how much of the group's spend went to multi-touch",
    method="Prospecting runs on CTV and the multi-touch stages run on display, so this is the stage mix, not a channel choice. The lift itself is prospecting only.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by share of spend outside prospecting",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By setting", by_setting,
    finding="Pooled visit lift by product, audience targeting and account health",
    method="Each block pools the same campaigns a different way. Values with under 5 campaigns are dropped.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by product, audience targeting and account health",
    query="ti_1313_campaign_base.sql")

wb.table(
    "Campaign detail", detail,
    finding=f"{n:,} campaign groups clear the power and quality gates, {n_sig:,} show significant visit lift",
    method="One row per campaign group. Every row has at least 100 holdout visits and passes the full ghost-bid quality gate.",
    formats={"Visit lift": FMT.PCT1, "p value": FMT.NUM2, "Holdout visit rate": FMT.PCT2,
             "Treated visit rate": FMT.PCT2, "Incremental visits": FMT.INT, "Spend": FMT.USD0,
             "Cost per inc visit": FMT.USD2, "Avg frequency": FMT.NUM1,
             "Multi-touch spend": FMT.PCT0, "Budget": FMT.USD0, "Conversion lift": FMT.PCT1,
             "New-to-brand lift": FMT.PCT1, "Monthly visitors": FMT.INT,
             "Share of bid households reached": FMT.PCT0, "Treated households": FMT.INT,
             "Holdout households": FMT.INT, "Treated visits": FMT.INT, "Holdout visits": FMT.INT},
    signal={"Visit lift": {"sig": "Significant"}}, kind="data",
    toc="Every powered campaign group with its lift and its attributes",
    query="ti_1313_campaign_base.sql")

wb.glossary(
    "Read me",
    intro="What these numbers are, how they were pooled, and what they cannot support.",
    rows=[
        ("What this measures", "Ghost-bid holdout lift. About 10% of eligible households are withheld from bidding; lift compares the households we bid on against the ones we held back."),
        ("Visit lift", "Relative increase in the share of households that visited the site, treated against holdout."),
        ("Pooled lift", "Random-effects pool of the log risk ratio across campaigns. Random effects because campaigns disagree far more than their own error bars allow."),
        ("Heterogeneity", "Share of variation between campaigns that is real disagreement, not sampling noise. Above roughly 75% the pooled number is the centre of a wide spread."),
        ("Significant", "The campaign's own 95% interval excludes zero."),
        ("Who is in this", f"{n:,} campaign groups across {n_adv:,} advertisers, each with at least 100 holdout visits and passing the full quality gate on holdout validity, sample size, compliance and arm balance."),
        ("Who is not", "Campaign groups under 100 holdout visits. Gating instead on holdout households would admit about 1,300 more campaigns whose lift is too noisy to rank on."),
        ("Prospecting only", "The holdout is prospecting-only by construction, so every lift number here is prospecting. Delivery attributes are restricted to prospecting campaigns to match. Multi-touch appears only as a spend share."),
        ("Intent bands", "From the platform's own score-band strata, not re-derived from impression scores. Band ordering is contested internally and depends on the pooling basis."),
        ("Frequency", "Bids per household, not impressions per household. A bid does not always win."),
        ("Spend", "Media spend on prospecting over the measurement window. It excludes data and platform cost, so cost per incremental visit is a floor, not a full loaded cost."),
        ("Not causal", "Every attribute here was chosen by the advertiser, not assigned. Verticals, budgets and frequencies differ in many things at once. Read each row as a hypothesis worth a designed test, never as an effect."),
        ("Not measured", "Creative length, attribution window, CRM exclusion and geography are not in this workbook. Nothing here speaks to them either way."),
    ])

wb.sql_dir("Queries", str(TICKET / "queries"),
           note="BigQuery SQL behind every sheet.")

wb.cover(takeaways=[
    f"{n:,} campaign groups are powered enough to rank. {n_sig:,} show significant visit lift.",
    f"They carry {inc:,.0f} incremental visits on ${spend:,.0f} of prospecting media spend.",
    "Every attribute is advertiser-chosen, so the rankings are hypotheses to test, not proven levers.",
])

print(wb.save_drive("AUDI-1313", "Campaign Incrementality by Attribute"))
print(f"campaigns={n} advertisers={n_adv} significant={n_sig} spend={spend:,.0f} inc_visits={inc:,.0f}")
