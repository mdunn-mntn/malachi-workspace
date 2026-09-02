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

base["primary"] = base["in_validity_band"] & base["meets_75pct_days_live"]
pop = base[base["primary"]].copy()
keep = set(pop["campaign_group_id"])
bands = bands[bands["campaign_group_id"].isin(keep)].copy()
freq = freq[freq["campaign_group_id"].isin(keep)].copy()

BAND_LABEL = {"High": "High Intent", "PP": "Peak Performance", "Mid": "Mid Intent",
              "MaxReach": "Max Reach", "no_score": "Unscored"}
BAND_ORDER = [BAND_LABEL[b] for b in ["High", "PP", "Mid", "MaxReach", "no_score"]]
FREQ_ORDER = ["1", "2-3", "4-10", "11+"]
GEO_LABEL = {"national": "National", "local_radius": "Local radius", "dma": "DMA",
             "state": "State", "zip": "Zip", "city": "City", "national_plus": "National plus"}


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


def pool_conv(df):
    """Random-effects pool of the log risk ratio on the conversion outcome."""
    d = df[(df["conv_rate_treatment"] > 0) & (df["conv_rate_holdout"] > 0)]
    if len(d) < 5:
        return None
    return pool(pd.DataFrame({
        "rate_treatment": d["conv_rate_treatment"].to_numpy(),
        "rate_holdout": d["conv_rate_holdout"].to_numpy(),
        "n_treatment": d["n_treatment"].to_numpy(),
        "n_holdout": d["n_holdout"].to_numpy(),
        "significant_95": d["conv_significant_95"].fillna(False).to_numpy(),
        "incremental_visits": d["incremental_conversions"].to_numpy(),
        "prospecting_spend": d["prospecting_spend"].to_numpy(),
    }))


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


pop["creative"] = pop["creative_length_mix"]
pop["geo"] = pop["geo_targeting_class"].map(GEO_LABEL)
pop["crm"] = np.where(pop["crm_file_excluded"], "Excludes customer file", "Does not exclude")
pop["mt"] = np.where(pop["mt_display_access_enrolled"], "Enrolled", "Not enrolled")
pop["mt_bucket"] = pd.cut(pop["pct_spend_multitouch"].fillna(0), bins=[-0.001, 0.001, 0.05, 0.15, 1.0],
                          labels=["None", "Under 5%", "5 to 15%", "Over 15%"])
_fq = pd.qcut(pop["avg_frequency"], 4)
_edges = [round(c.left, 1) for c in _fq.cat.categories] + [round(_fq.cat.categories[-1].right, 1)]
FREQ_Q_LABELS = [f"{_edges[i]} to {_edges[i + 1]}" for i in range(4)]
pop["freq_bucket"] = pd.qcut(pop["avg_frequency"], 4, labels=FREQ_Q_LABELS)
pop["dma_bucket"] = pd.cut(pop["n_dma_delivered"], bins=[-1, 10, 100, 200, 999],
                           labels=["Under 10 DMAs", "10 to 100", "100 to 200", "Over 200"])

bands["band"] = bands["score_band"].map(BAND_LABEL)
freq["fband"] = pd.Categorical(freq["bid_count_band"], categories=FREQ_ORDER, ordered=True)

by_freq = summarize(freq, "fband", "Bids per household", order=FREQ_ORDER, extras=False)
by_band = summarize(bands, "band", "Intent band", order=BAND_ORDER, extras=False)
by_creative = summarize(pop, "creative", "Creative length mix",
                        order=["15s only", "Mixed, 15s-led", "Mixed, 30s-led", "30s only"])
by_geo = summarize(pop, "geo", "Geographic targeting")
by_vertical = summarize(pop, "vertical_name", "Vertical")
by_freq_q = summarize(pop, "freq_bucket", "Average frequency", order=FREQ_Q_LABELS)

others = []
for col, lab in [("crm", "Customer-file exclusion"), ("mt", "Multi-touch display access"),
                 ("mt_bucket", "Multi-touch share of spend"), ("dma_bucket", "Delivered DMA footprint")]:
    t = summarize(pop, col, "Value", min_k=5)
    if len(t) < 2:
        continue
    t = t.rename(columns={"Value": "Setting"})
    t.insert(0, "Attribute", lab)
    others.append(t)
by_other = pd.concat(others, ignore_index=True) if others else pd.DataFrame()

RANKED_SRC = [
    ("Bids per household", by_freq, len(keep)), ("Intent band", by_band, len(keep)),
    ("Creative length mix", by_creative, None), ("Geographic targeting", by_geo, None),
    ("Vertical", by_vertical, None), ("Average frequency", by_freq_q, None),
]
rank_rows = []
for name, tbl, fixed_n in RANKED_SRC:
    if tbl.empty or len(tbl) < 2:
        continue
    t = tbl.sort_values("Pooled lift", ascending=False)
    best, worst = t.iloc[0], t.iloc[-1]
    rank_rows.append({
        "Attribute": name,
        "Levels": len(t),
        "Campaigns": int(fixed_n if fixed_n else t["Campaigns"].sum()),
        "Smallest level": int(t["Campaigns"].min()),
        "Best level": str(best[t.columns[0]]),
        "Best lift": best["Pooled lift"],
        "Worst level": str(worst[t.columns[0]]),
        "Worst lift": worst["Pooled lift"],
        "Spread": best["Pooled lift"] - worst["Pooled lift"],
        "Intervals separate": bool(best["CI low"] > worst["CI high"]),
    })
ranked = (pd.DataFrame(rank_rows)
          .sort_values(["Intervals separate", "Spread"], ascending=[False, False])
          .reset_index(drop=True))

conv = pop[pop["conv_rate_holdout"] > 0].copy()
conv_rows = []
for name, tbl_col, order in [("Creative length mix", "creative", None),
                             ("Geographic targeting", "geo", None),
                             ("Customer-file exclusion", "crm", None)]:
    for key, g in conv.groupby(tbl_col, dropna=True, observed=True):
        if len(g) < 5:
            continue
        conv_rows.append({
            "Attribute": name, "Setting": key, "Campaigns": len(g),
            "Median conversion lift": g["conv_rel_itt"].median(),
            "% significant": g["conv_significant_95"].fillna(False).mean(),
            "Baseline conv rate": g["conv_rate_holdout"].median(),
            "Incremental conversions": g["incremental_conversions"].sum(),
            "Cost per inc conversion": (g["prospecting_spend"].sum() / g["incremental_conversions"].sum()
                                        if g["incremental_conversions"].sum() > 0 else np.nan),
        })
conv_tbl = pd.DataFrame(conv_rows)

infl_rows = []
for name, col in [("Creative length mix", "creative"), ("Geographic targeting", "geo"),
                  ("Customer-file exclusion", "crm"), ("Vertical", "vertical_name")]:
    for key, g in pop.groupby(col, dropna=True, observed=True):
        pc = pool_conv(g)
        if pc is None:
            continue
        share = pc["lift"] / (1 + pc["lift"])
        gg = g[g["attributed_conversions"] > 0]
        att_conv = gg["attributed_conversions"].sum()
        spend = gg["reporting_total_spend"].sum()
        if att_conv <= 0 or share <= 0:
            continue
        estimable = pc["lo"] > 0
        infl_rows.append({
            "Attribute": name, "Setting": key, "Campaigns": pc["k"],
            "Pooled conversion lift": pc["lift"],
            "CI low": pc["lo"], "CI high": pc["hi"],
            "Lift clears zero": bool(estimable),
            "Attributed conversions": att_conv,
            "Incremental conversions": att_conv * share if estimable else np.nan,
            "% attributed that is incremental": share if estimable else np.nan,
            "Attributed per incremental": 1.0 / share if estimable else np.nan,
            "Attributed CPA": spend / att_conv,
            "Incremental CPA": spend / (att_conv * share) if estimable else np.nan,
        })
infl_tbl = (pd.DataFrame(infl_rows)
            .sort_values(["Lift clears zero", "Attributed per incremental"], ascending=[False, False])
            .reset_index(drop=True))

gates = []
for lab, sub in [("Everything that passes power and quality", base),
                 ("Plus 75% days live", base[base["meets_75pct_days_live"]]),
                 ("Plus holdout validity band", base[base["in_validity_band"]]),
                 ("Both filters (this workbook)", base[base["primary"]])]:
    p = pool(sub)
    if p is None:
        continue
    gates.append({"Population": lab, "Campaigns": p["k"], "Pooled lift": p["lift"],
                  "CI low": p["lo"], "CI high": p["hi"],
                  "% significant": sub[p["mask"]]["significant_95"].mean(),
                  "Heterogeneity": p["i2"]})
gate_tbl = pd.DataFrame(gates)

base["gf_bin"] = pd.cut(base["ghost_frac"], bins=[0, 0.08, 0.09, 0.10, 0.11, 1.0],
                        labels=["Under 8%", "8 to 9%", "9 to 10%", "10 to 11%", "Over 11%"])
sens = summarize(base, "gf_bin", "Holdout share of households",
                 order=["Under 8%", "8 to 9%", "9 to 10%", "10 to 11%", "Over 11%"], extras=False)

base["In this workbook"] = base["primary"]
detail = base[[
    "campaign_group_id", "campaign_group_name", "advertiser_name", "vertical_name",
    "In this workbook", "in_validity_band", "meets_75pct_days_live", "ghost_frac", "days_delivered",
    "rel_itt", "p_value", "significant_95", "rate_holdout", "rate_treatment",
    "incremental_visits", "prospecting_spend", "cost_per_incremental_visit",
    "conv_rel_itt", "conv_p_value", "conv_significant_95", "conv_rate_holdout",
    "incremental_conversions", "cost_per_incremental_conversion",
    "attributed_visits", "attributed_conversions", "attributed_ivr", "attributed_cpa",
    "incremental_cpa", "attribution_inflation_ratio",
    "pct_attributed_visits_incremental", "pct_attributed_conv_incremental",
    "creative_length_mix", "share_15s", "n_creatives",
    "geo_targeting_class", "n_dma_delivered", "crm_file_excluded", "mt_display_access_enrolled",
    "avg_frequency", "pct_spend_multitouch", "budget",
    "n_treatment", "n_holdout", "vis_treatment", "vis_holdout",
]].rename(columns={
    "campaign_group_id": "CG id", "campaign_group_name": "Campaign group",
    "advertiser_name": "Advertiser", "vertical_name": "Vertical",
    "in_validity_band": "Holdout in band", "meets_75pct_days_live": "75% days live",
    "ghost_frac": "Holdout share", "days_delivered": "Days delivered",
    "rel_itt": "Visit lift", "p_value": "p value", "significant_95": "Significant",
    "rate_holdout": "Holdout visit rate", "rate_treatment": "Treated visit rate",
    "incremental_visits": "Incremental visits", "prospecting_spend": "Spend",
    "cost_per_incremental_visit": "Cost per inc visit", "conv_rel_itt": "Conversion lift",
    "conv_p_value": "Conv p value", "conv_significant_95": "Conv significant",
    "conv_rate_holdout": "Baseline conv rate", "incremental_conversions": "Incremental conversions",
    "cost_per_incremental_conversion": "Cost per inc conversion",
    "attributed_visits": "Attributed visits", "attributed_conversions": "Attributed conversions",
    "attributed_ivr": "Attributed IVR", "attributed_cpa": "Attributed CPA",
    "incremental_cpa": "Incremental CPA", "attribution_inflation_ratio": "Attribution inflation",
    "pct_attributed_visits_incremental": "% attr visits incremental",
    "pct_attributed_conv_incremental": "% attr conversions incremental",
    "creative_length_mix": "Creative length", "share_15s": "Share 15s", "n_creatives": "Creatives",
    "geo_targeting_class": "Geo targeting", "n_dma_delivered": "DMAs delivered",
    "crm_file_excluded": "Excludes customer file", "mt_display_access_enrolled": "Multi-touch access",
    "avg_frequency": "Avg frequency", "pct_spend_multitouch": "Multi-touch spend", "budget": "Budget",
    "n_treatment": "Treated households", "n_holdout": "Holdout households",
    "vis_treatment": "Treated visits", "vis_holdout": "Holdout visits",
}).sort_values("Incremental visits", ascending=False)

n_all, n_pop = len(base), len(pop)
n_adv = pop["advertiser_id"].nunique()
n_sig = int(pop["significant_95"].sum())
n_conv = len(conv)
head = pool(pop)

SUMF = {"Pooled lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
        "% significant": FMT.PCT0, "Heterogeneity": FMT.PCT0, "Campaigns": FMT.INT,
        "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2}

wb = MntnWorkbook(
    title="Campaign incrementality by attribute",
    ticket="AUDI-1313",
    subtitle=f"Ghost-bid visit lift for {n_pop:,} campaign groups across {n_adv:,} advertisers",
    period="Ghost-bid window: 22 Jun to 31 Aug 2026",
)

wb.table(
    "Ranked hypotheses", ranked,
    finding="Attributes ranked by how far they separate campaign lift",
    method="Spread is the gap between the attribute's best and worst level. Intervals separate means the two do not overlap, which makes the hypothesis worth testing first. Nothing here is causal.",
    formats={"Best lift": FMT.PCT1, "Worst lift": FMT.PCT1, "Spread": FMT.PCT1,
             "Campaigns": FMT.INT, "Levels": FMT.INT},
    signal={"Spread": {"sig": "Intervals separate"}}, kind="headline",
    toc="Which attribute separates lift most, ranked",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By frequency", by_freq,
    finding="Pooled visit lift by how many times a household was bid on",
    method="From the platform's bid-count strata. Households are not randomised into these bands, so one bid on more often is also one the system judged more promising.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift at 1, 2 to 3, 4 to 10 and 11+ bids per household",
    query="ti_1313_bid_count_strata.sql")

wb.table(
    "By creative length", by_creative,
    finding="Pooled visit lift by the mix of 15 and 30 second creative delivered",
    method="Impression-weighted over prospecting delivery. Nearly half of campaigns run both lengths, so a clean 15 against 30 split is not available in the data.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by 15s only, mixed, or 30s only",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By geography", by_geo,
    finding="Pooled visit lift by the geographic targeting the advertiser chose",
    method="From stored targeting rather than delivered footprint, so it is the advertiser's choice and not an outcome of pacing. Class order is radius, zip, city, state, DMA.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by national, DMA, state, city, zip or local radius",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By intent band", by_band,
    finding="Pooled visit lift by the intent band the household was scored into",
    method="From the platform's own score-band strata. A campaign appears in every band it delivered to, so this is only partly a within-campaign contrast.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by High, Peak, Mid, Max Reach and Unscored",
    query="ti_1313_score_band_strata.sql")

wb.table(
    "By vertical", by_vertical,
    finding="Pooled visit lift by advertiser vertical",
    method="Advertisers pick their own vertical, so this compares populations rather than a setting. Verticals under 5 campaigns are dropped.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Pooled lift for each vertical, ranked",
    query="ti_1313_campaign_base.sql")

wb.table(
    "By campaign frequency", by_freq_q,
    finding="Pooled visit lift by the campaign's own average frequency",
    method="Quartiles on prospecting impressions divided by households reached. A different question than the bid-count sheet, which splits households inside a campaign.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="data",
    toc="Lift by the campaign's average frequency quartile",
    query="ti_1313_campaign_base.sql")

if not by_other.empty:
    wb.table(
        "Other attributes", by_other,
        finding="Pooled visit lift by customer-file exclusion, multi-touch and delivered footprint",
        method="Each block pools the same campaigns a different way. Multi-touch access is an advertiser setting, not a campaign one. Values under 5 campaigns are dropped.",
        formats=SUMF, signal={"Pooled lift": {}}, kind="data",
        toc="Lift by customer-file exclusion, multi-touch and footprint",
        query="ti_1313_campaign_base.sql")

if not conv_tbl.empty:
    wb.table(
        "Conversion outcomes", conv_tbl,
        finding=f"Conversion lift for the {n_conv:,} campaign groups whose holdout recorded conversions",
        method="Medians, not pooled, because conversion counts are thin. Baseline is the holdout conversion rate. Cost per incremental conversion uses prospecting media spend only.",
        formats={"Median conversion lift": FMT.PCT1, "% significant": FMT.PCT0,
                 "Baseline conv rate": FMT.PCT2, "Incremental conversions": FMT.INT,
                 "Cost per inc conversion": FMT.USD2, "Campaigns": FMT.INT},
        signal={"Median conversion lift": {}}, kind="data",
        toc="Conversion lift and cost per incremental conversion",
        query="ti_1313_campaign_base.sql")

if not infl_tbl.empty:
    wb.table(
        "Attribution inflation", infl_tbl,
        finding="Reporting credits far more conversions than the holdout says the ads caused",
        method="Incremental conversions are attributed conversions scaled by the pooled conversion lift. Where that lift does not clear zero the multiple is unbounded and left blank.",
        formats={"Pooled conversion lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
                 "Attributed conversions": FMT.INT,
                 "Incremental conversions": FMT.INT, "% attributed that is incremental": FMT.PCT0,
                 "Attributed per incremental": FMT.MULT, "Attributed CPA": FMT.USD2,
                 "Incremental CPA": FMT.USD2, "Campaigns": FMT.INT},
        kind="headline",
        toc="How far reported cost per acquisition sits below the true cost",
        query="ti_1313_campaign_base.sql")

wb.table(
    "Population choices", gate_tbl,
    finding="What each population filter costs, and what it does to the headline",
    method="The workbook uses the last row. The holdout band matters most: measured lift climbs as the holdout thins, which is an artifact of the estimator rather than a real effect.",
    formats={"Pooled lift": FMT.PCT1, "CI low": FMT.PCT1, "CI high": FMT.PCT1,
             "% significant": FMT.PCT0, "Heterogeneity": FMT.PCT0, "Campaigns": FMT.INT},
    signal={"Pooled lift": {}}, kind="detail",
    toc="Headline lift under each combination of filters",
    query="ti_1313_campaign_base.sql")

wb.table(
    "Holdout depth check", sens,
    finding="Measured lift climbs as the holdout thins, which is why the band filter exists",
    method="Every campaign on the validated bidder leg, binned on the share of households held back. The estimator is documented as reliable only between 9 and 11%.",
    formats=SUMF, signal={"Pooled lift": {}}, kind="detail",
    toc="The gradient behind the holdout band filter",
    query="ti_1313_campaign_base.sql")

wb.table(
    "Campaign detail", detail,
    finding=f"{n_all:,} campaign groups pass power and quality, {n_pop:,} clear both further filters",
    method="One row per campaign group. Only rows where In this workbook is TRUE feed the summary sheets. Every attribute and outcome the ticket asks for is a column here.",
    formats={"Visit lift": FMT.PCT1, "p value": FMT.NUM2, "Holdout share": FMT.PCT1,
             "Holdout visit rate": FMT.PCT2, "Treated visit rate": FMT.PCT2,
             "Incremental visits": FMT.INT, "Spend": FMT.USD0, "Cost per inc visit": FMT.USD2,
             "Conversion lift": FMT.PCT1, "Conv p value": FMT.NUM2, "Baseline conv rate": FMT.PCT2,
             "Incremental conversions": FMT.NUM1, "Cost per inc conversion": FMT.USD2,
             "Attributed visits": FMT.INT, "Attributed conversions": FMT.INT,
             "Attributed IVR": FMT.PCT2, "Attributed CPA": FMT.USD2, "Incremental CPA": FMT.USD2,
             "Attribution inflation": FMT.MULT, "% attr visits incremental": FMT.PCT0,
             "% attr conversions incremental": FMT.PCT0, "Share 15s": FMT.PCT0,
             "Creatives": FMT.INT, "DMAs delivered": FMT.INT, "Days delivered": FMT.INT,
             "Avg frequency": FMT.NUM1, "Multi-touch spend": FMT.PCT0, "Budget": FMT.USD0,
             "Treated households": FMT.INT, "Holdout households": FMT.INT,
             "Treated visits": FMT.INT, "Holdout visits": FMT.INT},
    signal={"Visit lift": {"sig": "Significant"}}, kind="data",
    toc="Every campaign group with every attribute and outcome",
    query="ti_1313_campaign_base.sql")

wb.glossary(
    "Read me",
    intro="What these numbers are, how they were built, and what they cannot support.",
    rows=[
        ("What this measures", "Ghost-bid holdout lift. A slice of eligible households is withheld from bidding, and lift compares the households we bid on against the ones we held back."),
        ("Visit lift", "Relative increase in the share of households that visited the site, treated against holdout."),
        ("Pooled lift", "Random-effects pool of the log risk ratio across campaigns. Random effects because campaigns disagree far more than their own error bars allow."),
        ("Heterogeneity", "Share of variation between campaigns that is real disagreement, not sampling noise. Above roughly 75% the pooled number is the centre of a wide spread."),
        ("Attributed against incremental", "Attributed counts every visit or conversion reporting credits to the ads. Incremental counts only what the holdout says the ads caused."),
        ("Attributed per incremental", "How many reported conversions stand behind one genuine one. Computed from the pooled conversion lift, not from per-campaign ratios, which are unstable where lift is near zero."),
        ("Holdout share", "Share of a campaign's households held back. The estimator is documented as reliable only between 9 and 11%, and measured lift climbs as it thins."),
        ("Who is in this", f"{n_pop:,} campaign groups across {n_adv:,} advertisers: 100+ holdout visits, full quality gate, validated bidder leg, holdout inside the band, and live at least 75% of days."),
        ("Days live", "Distinct days the group delivered prospecting impressions in the window. Measured from delivery, not config dates, so a mid-window pause is caught."),
        ("One bidder leg only", "MNTN runs two bidders. The second has no trustworthy holdout, so it is excluded. That removes almost every Select campaign, and no product comparison is possible here."),
        ("Creative length", "Impression-weighted mix of 15 and 30 second creative on prospecting delivery. Nearly half of campaigns run both, so no clean binary split exists."),
        ("Geography", "The advertiser's stored targeting choice, not the delivered footprint. Delivered DMA count is carried separately as an outcome measure."),
        ("Customer-file exclusion", "The prospecting audience suppresses the advertiser's CRM file. Read from live audience config, which nearly half of these advertisers edited mid-window."),
        ("Conversions are thin", f"Only {n_conv:,} of {n_pop:,} campaign groups recorded holdout conversions, so conversion sheets use medians rather than pooled estimates."),
        ("Not causal", "Every attribute here was chosen by the advertiser, not assigned. Read each row as a hypothesis worth a designed test, never as an effect."),
        ("Not measured", "Attribution window does not vary across these advertisers, and audience size is not stored. Neither is in this workbook."),
    ])

wb.sql_dir("Queries", str(TICKET / "queries"), note="BigQuery SQL behind every sheet.")

top = ranked.iloc[0] if not ranked.empty else None
wb.cover(takeaways=[
    f"{n_pop:,} campaign groups clear every filter the ticket asks for. {n_sig:,} show significant visit lift, pooling to {head['lift']:.1%}.",
    (f"{top['Attribute']} separates lift most, from {top['Worst lift']:.1%} to {top['Best lift']:.1%}."
     if top is not None else "See the ranked hypotheses sheet."),
    "Every attribute is advertiser-chosen, so these are hypotheses to test, not proven levers.",
])

print(wb.save_drive("AUDI-1313", "Campaign Incrementality by Attribute"))
print(f"detail={n_all} summary={n_pop} adv={n_adv} sig={n_sig} conv={n_conv} pooled={head['lift']:.4f}")
