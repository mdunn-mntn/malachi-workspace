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

inband = base[base["in_validity_band"]].copy()

BAND_LABEL = {"High": "High Intent", "PP": "Peak Performance", "Mid": "Mid Intent",
              "MaxReach": "Max Reach", "no_score": "Unscored"}
BAND_ORDER = [BAND_LABEL[b] for b in ["High", "PP", "Mid", "MaxReach", "no_score"]]
FREQ_ORDER = ["1", "2-3", "4-10", "11+"]


def log_rr(df):
    """Log risk ratio and its variance, the pooling basis data_catalog.md requires for relative lift."""
    pt, ph = df["rate_treatment"], df["rate_holdout"]
    nt, nh = df["n_treatment"], df["n_holdout"]
    ok = (pt > 0) & (ph > 0) & (nt > 0) & (nh > 0)
    y = pd.Series(np.where(ok, np.log(pt.where(ok, 1) / ph.where(ok, 1)), np.nan), index=df.index)
    v = pd.Series(np.where(ok, (1 - pt) / (pt * nt) + (1 - ph) / (ph * nh), np.nan), index=df.index)
    return y, v


def pool(df):
    """DerSimonian-Laird random-effects pool of the log risk ratio, with the surviving row mask."""
    y, v = log_rr(df)
    m = y.notna() & v.notna() & (v > 0)
    k = int(m.sum())
    if k == 0:
        return None
    yv, vv = y[m].to_numpy(), v[m].to_numpy()
    w = 1.0 / vv
    fixed = (w * yv).sum() / w.sum()
    q = float((w * (yv - fixed) ** 2).sum())
    tau2 = max(0.0, (q - (k - 1)) / (w.sum() - (w**2).sum() / w.sum())) if k > 1 else 0.0
    wr = 1.0 / (vv + tau2)
    est = float((wr * yv).sum() / wr.sum())
    se = float(np.sqrt(1.0 / wr.sum()))
    i2 = max(0.0, (q - (k - 1)) / q) if (k > 1 and q > 0) else 0.0
    return {"k": k, "mask": m, "lift": np.expm1(est), "lo": np.expm1(est - 1.96 * se),
            "hi": np.expm1(est + 1.96 * se), "i2": i2}


def summarize(df, by, label, order=None, min_k=5, extras=True):
    rows = []
    for key, g in df.groupby(by, dropna=True, observed=True):
        p = pool(g)
        if p is None or p["k"] < min_k:
            continue
        gp = g[p["mask"]]
        r = {label: key, "Campaigns": p["k"], "Pooled lift": p["lift"],
             "CI low": p["lo"], "CI high": p["hi"],
             "% significant": gp["significant_95"].mean(),
             "Heterogeneity": p["i2"],
             "Incremental visits": gp["incremental_visits"].sum()}
        if extras:
            sp, iv = gp["prospecting_spend"].sum(), gp["incremental_visits"].sum()
            r["Spend"] = sp
            r["Cost per inc visit"] = sp / iv if iv > 0 else np.nan
        rows.append(r)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    if order:
        out[label] = pd.Categorical(out[label], categories=order, ordered=True)
        out = out.sort_values(label)
    else:
        out = out.sort_values("Pooled lift", ascending=False)
    return out.reset_index(drop=True)


by_vertical = summarize(inband, "vertical_name", "Vertical")

bands["band"] = bands["score_band"].map(BAND_LABEL)
by_band = summarize(bands, "band", "Intent band", order=BAND_ORDER, extras=False)
band_multi = int((bands.groupby("campaign_group_id").size() > 1).sum())
band_single = int(bands["campaign_group_id"].nunique() - band_multi)

freq["band"] = pd.Categorical(freq["bid_count_band"], categories=FREQ_ORDER, ordered=True)
by_freq = summarize(freq, "band", "Bids per household", order=FREQ_ORDER, extras=False)

inband["freq_bucket"] = pd.qcut(inband["avg_frequency"], 4,
                                labels=["Lowest quartile", "Second", "Third", "Highest quartile"])
by_freq_q = summarize(inband, "freq_bucket", "Average frequency",
                      order=["Lowest quartile", "Second", "Third", "Highest quartile"])

inband["mt_bucket"] = pd.cut(inband["pct_spend_multitouch"].fillna(0),
                             bins=[-0.001, 0.001, 0.05, 0.15, 1.0],
                             labels=["None", "Under 5%", "5 to 15%", "Over 15%"])
by_mt = summarize(inband, "mt_bucket", "Multi-touch share of spend",
                  order=["None", "Under 5%", "5 to 15%", "Over 15%"])

settings = []
for col, lab in [("product", "Product"), ("has_audience", "Uses audience targeting")]:
    t = summarize(inband, col, "Value", min_k=5)
    if len(t) < 2:
        continue
    t = t.rename(columns={"Value": "Setting"})
    t.insert(0, "Attribute", lab)
    settings.append(t)
by_setting = pd.concat(settings, ignore_index=True) if settings else pd.DataFrame()

base["gf_bin"] = pd.cut(base["ghost_frac"], bins=[0, 0.08, 0.09, 0.10, 0.11, 1.0],
                        labels=["Under 8%", "8 to 9%", "9 to 10%", "10 to 11%", "Over 11%"])
GF_ORDER = ["Under 8%", "8 to 9%", "9 to 10%", "10 to 11%", "Over 11%"]
sens = summarize(base, "gf_bin", "Holdout share of households", order=GF_ORDER, extras=False)
sens["Inside validity band"] = sens["Holdout share of households"].isin(["9 to 10%", "10 to 11%"])

base["In validity band"] = base["in_validity_band"]
detail = base[[
    "campaign_group_id", "campaign_group_name", "advertiser_name", "vertical_name", "product",
    "rel_itt", "p_value", "significant_95", "In validity band", "ghost_frac",
    "rate_holdout", "rate_treatment", "incremental_visits", "prospecting_spend",
    "cost_per_incremental_visit", "avg_frequency", "pct_spend_multitouch", "has_audience", "budget",
    "conv_rel_itt", "conv_significant_95", "ntb_rel_itt", "ntb_significant_95",
    "ip_compliance", "n_treatment", "n_holdout", "vis_treatment", "vis_holdout",
]].rename(columns={
    "campaign_group_id": "CG id", "campaign_group_name": "Campaign group",
    "advertiser_name": "Advertiser", "vertical_name": "Vertical", "product": "Product",
    "rel_itt": "Visit lift", "p_value": "p value", "significant_95": "Significant",
    "ghost_frac": "Holdout share", "rate_holdout": "Holdout visit rate",
    "rate_treatment": "Treated visit rate", "incremental_visits": "Incremental visits",
    "prospecting_spend": "Spend", "cost_per_incremental_visit": "Cost per inc visit",
    "avg_frequency": "Avg frequency", "pct_spend_multitouch": "Multi-touch spend",
    "has_audience": "Uses audience", "budget": "Budget", "conv_rel_itt": "Conversion lift",
    "conv_significant_95": "Conv significant", "ntb_rel_itt": "New-to-brand lift",
    "ntb_significant_95": "New-to-brand significant",
    "ip_compliance": "Share of bid households reached",
    "n_treatment": "Treated households", "n_holdout": "Holdout households",
    "vis_treatment": "Treated visits", "vis_holdout": "Holdout visits",
}).sort_values("Incremental visits", ascending=False)

n_all, n_in = len(base), len(inband)
n_sig = int(inband["significant_95"].sum())
n_adv = inband["advertiser_id"].nunique()
head = pool(inband)
gf_lo = sens.loc[sens["Holdout share of households"] == "Under 8%", "Pooled lift"]
gf_hi = sens.loc[sens["Holdout share of households"] == "Over 11%", "Pooled lift"]

SUMF = {"Pooled lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
        "% significant": FMT.PCT0, "Heterogeneity": FMT.PCT0, "Campaigns": FMT.INT,
        "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2}

wb = MntnWorkbook(
    title="Campaign incrementality by attribute",
    ticket="AUDI-1313",
    subtitle=f"Ghost-bid visit lift for {n_in:,} campaign groups across {n_adv:,} advertisers",
    period="Ghost-bid window: 22 Jun to 31 Aug 2026",
)

wb.table(
    "By frequency", by_freq,
    finding="Pooled visit lift by how many times a household was bid on",
    method="From the platform's bid-count strata. Households are not randomised into these bands, so a household bid on more often is also one the system judged more promising. A pattern, not a dose response.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="headline",
    toc="Lift at 1, 2 to 3, 4 to 10 and 11+ bids per household",
    query="ti_1313_bid_count_strata.sql")

wb.table(
    "By intent band", by_band,
    finding="Pooled visit lift by the intent band the household was scored into",
    method=f"From the platform's own score-band strata. {band_multi} campaigns contribute more than one band and {band_single} contribute a single band, so this is only partly a within-campaign contrast.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by High, Peak, Mid, Max Reach and Unscored",
    query="ti_1313_score_band_strata.sql")

wb.table(
    "By vertical", by_vertical,
    finding="Pooled visit lift by advertiser vertical",
    method="Advertisers pick their own vertical, so this compares populations rather than a setting anyone can change. Verticals under 5 campaigns are dropped.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Pooled lift for each vertical, ranked",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By campaign frequency", by_freq_q,
    finding="Pooled visit lift by the campaign's own average frequency",
    method="Campaigns split into quartiles on prospecting impressions divided by households reached. A different question than the bid-count sheet, which splits households inside a campaign.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by the campaign's average frequency quartile",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By multi-touch mix", by_mt,
    finding="Pooled visit lift by how much of the group's spend went to multi-touch",
    method="Prospecting runs on connected TV and the multi-touch stages run on display, so this is stage mix and not a channel choice. The lift itself is prospecting only.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by share of spend outside prospecting",
    query="ti_1313_campaign_base.sql")

if not by_setting.empty:
    wb.table(
        "By setting", by_setting,
        finding="Pooled visit lift by product",
        method="Audience targeting carries one value across every campaign here, so it supports no comparison and is left out.",
        formats=SUMF, signal={"Pooled lift": {}}, kind="data",
        toc="Lift by product", query="ti_1313_campaign_base.sql")

wb.table(
    "Holdout depth check", sens,
    finding="Measured lift climbs as the holdout thins, which is why the summary sheets use part of the population",
    method="Every campaign on the validated bidder leg, binned on the share of households held back. The estimator is documented as reliable only between 9 and 11%.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="detail",
    toc=f"Why the summary sheets use {n_in:,} campaigns and not {n_all:,}",
    query="ti_1313_campaign_base.sql")

wb.table(
    "Campaign detail", detail,
    finding=f"{n_all:,} campaign groups pass the power and quality gates, {n_in:,} sit inside the holdout validity band",
    method="One row per campaign group, at least 100 holdout visits each. Only rows inside the validity band feed the summary sheets.",
    formats={"Visit lift": FMT.PCT1, "p value": FMT.NUM2, "Holdout share": FMT.PCT1,
             "Holdout visit rate": FMT.PCT2, "Treated visit rate": FMT.PCT2,
             "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2,
             "Avg frequency": FMT.NUM1, "Multi-touch spend": FMT.PCT0, "Budget": FMT.USD0,
             "Conversion lift": FMT.PCT1, "New-to-brand lift": FMT.PCT1,
             "Share of bid households reached": FMT.PCT0, "Treated households": FMT.INT,
             "Holdout households": FMT.INT, "Treated visits": FMT.INT, "Holdout visits": FMT.INT},
    signal={"Visit lift": {"sig": "Significant"}}, kind="data",
    toc="Every campaign group with its lift and its attributes",
    query="ti_1313_campaign_base.sql")

wb.glossary(
    "Read me",
    intro="What these numbers are, how they were pooled, and what they cannot support.",
    rows=[
        ("What this measures", "Ghost-bid holdout lift. A slice of eligible households is withheld from bidding, and lift compares the households we bid on against the ones we held back."),
        ("Visit lift", "Relative increase in the share of households that visited the site, treated against holdout."),
        ("Pooled lift", "Random-effects pool of the log risk ratio across campaigns. Random effects because campaigns disagree far more than their own error bars allow."),
        ("Heterogeneity", "Share of variation between campaigns that is real disagreement, not sampling noise. Above roughly 75% the pooled number is the centre of a wide spread."),
        ("Significant", "The campaign's own 95% interval excludes zero."),
        ("Holdout share", "Share of a campaign's households held back. The estimator is documented as reliable only between 9 and 11%."),
        ("Why not every campaign", f"Measured lift climbs as the holdout thins, from {gf_hi.iloc[0]:.0%} above an 11% holdout to {gf_lo.iloc[0]:.0%} under 8%. Summary sheets use the {n_in:,} campaigns inside the reliable band. The Holdout depth check sheet shows the gradient."),
        ("Who is in this", f"{n_in:,} campaign groups across {n_adv:,} advertisers, each with at least 100 holdout visits, passing the quality gate, on the validated bidder leg, inside the holdout band."),
        ("One bidder leg only", "MNTN runs two bidders. The second has no trustworthy holdout, so it is excluded. That removes almost every Select campaign, and no product comparison here should be read as a product effect."),
        ("Prospecting only", "The holdout is prospecting-only by construction, so every lift number here is prospecting. Delivery attributes are restricted to prospecting to match. Multi-touch appears only as a spend share."),
        ("Intent bands", "From the platform's own score-band strata. The ordering here reproduces the result recorded under AUDI-1209 independently."),
        ("Frequency", "Bids per household, not impressions per household. A bid does not always win."),
        ("Spend and cost", "Prospecting media spend scaled to the measured households. It excludes data and platform cost, so cost per incremental visit is a floor. Blank where a campaign measured no incremental visits."),
        ("Not causal", "Every attribute here was chosen by the advertiser, not assigned. Read each row as a hypothesis worth a designed test, never as an effect."),
        ("Not measured", "Creative length, attribution window, CRM exclusion and geography are not in this workbook. Nothing here speaks to them either way."),
    ])

wb.sql_dir("Queries", str(TICKET / "queries"), note="BigQuery SQL behind every sheet.")

wb.cover(takeaways=[
    f"{n_in:,} campaign groups are measurable enough to rank. {n_sig:,} show significant visit lift, pooling to {head['lift']:.1%}.",
    "Bid frequency separates them most. Households bid on once show almost no lift; households bid on 4 to 10 times show the most.",
    "Every attribute is advertiser-chosen, so these are hypotheses to test, not proven levers.",
])

print(wb.save_drive("AUDI-1313", "Campaign Incrementality by Attribute"))
print(f"detail={n_all} summary={n_in} advertisers={n_adv} sig={n_sig} pooled={head['lift']:.4f}")
