#!/usr/bin/env python3
"""AUDI-1141 aggregation. Rates match TI-999 Pass 26 (all over impressions), stored as DECIMALS.
Buckets: MM (gated) / MM (no gate) / MM restricted / 3P.
  MM restricted = MM narrowed by an AND-include 3P clause or by sub-DMA geo (zip/city/radius).
  A 3P joined by OR is additive and stays MM. HHST gate (threshold>0) splits broad MM.
Weighting: advertiser-weighted (median headline, whale-robust) + impression/spend pooled cross-check."""
import pandas as pd, numpy as np

SRC = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/audi_1141_campaign_grain.csv"
OUT = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"
ADV_MIN_IMPS = 20000
BUCKET_ORDER = ["MM (gated)", "MM (no gate)", "MM restricted", "3P"]
BUCKET2_ORDER = ["MM (all)", "3P"]

df = pd.read_csv(SRC).dropna(subset=["vertical_id"])
df["gated_frac"] = np.where(df.hhst_writes > 0, df.hhst_writes_gated / df.hhst_writes, 0.0)
df["capped"] = df.gated_frac >= 0.5
df = df[df.bucket != "Neither"].copy()
def detail(r):
    if r.bucket == "MM":
        return "MM (gated)" if r.capped else "MM (no gate)"
    return r.bucket
df["bucket_detail"] = df.apply(detail, axis=1)
# blended two-group view: any MM signal (gated/no-gate/restricted) vs pure 3P
df["bucket2"] = np.where(df.bucket == "3P", "3P", "MM (all)")

def scorecard(frame, keys):
    rows = []
    for gkey, g in frame.groupby(keys):
        imps = g.imps.sum(); visits = g.visits.sum(); clicks = g.clicks.sum()
        conv = g.conv.sum(); rev = g.revenue.sum(); spend = g.spend.sum()
        pooled = dict(
            IVR_pooled = visits/imps if imps else np.nan,
            CVR_pooled = conv/imps if imps else np.nan,
            CTR_pooled = clicks/imps if imps else np.nan,
            CPV_pooled = spend/visits if visits else np.nan,
            CPM_pooled = 1000*spend/imps if imps else np.nan,
            CPA_pooled = spend/conv if conv else np.nan,
            ROAS_pooled = rev/spend if spend else np.nan)
        adv = g.groupby("advertiser_id").agg(imps=("imps","sum"),visits=("visits","sum"),
                clicks=("clicks","sum"),conv=("conv","sum"),revenue=("revenue","sum"),spend=("spend","sum"))
        adv = adv[adv.imps >= ADV_MIN_IMPS]
        ivr = adv.visits/adv.imps; cvr = adv.conv/adv.imps; ctr = adv.clicks/adv.imps
        cpv = (adv.spend/adv.visits).replace([np.inf,-np.inf],np.nan)
        cpa = (adv.spend/adv.conv).replace([np.inf,-np.inf],np.nan)
        roas = (adv.revenue/adv.spend).replace([np.inf,-np.inf],np.nan)
        rows.append({**({k:v for k,v in zip(keys,(gkey if isinstance(gkey,tuple) else (gkey,)))}),
            "n_adv": g.advertiser_id.nunique(), "n_adv_qual": len(adv), "n_camp": len(g),
            "spend": spend, "imps": imps,
            # advertiser-weighted MEDIAN (headline)
            "IVR_med": ivr.median(), "CVR_med": cvr.median(), "CTR_med": ctr.median(),
            "CPV_med": cpv.median(), "CPA_med": cpa[adv.conv>0].median(),
            "ROAS_med": roas[adv.revenue>0].median(), "n_adv_roas": int((adv.revenue>0).sum()),
            # advertiser-weighted MEAN
            "IVR_mean": ivr.mean(), "CPV_mean": cpv.mean(),
            **pooled})
    d = pd.DataFrame(rows)
    bcol = "bucket_detail" if "bucket_detail" in keys else "bucket2"
    order = {b:i for i,b in enumerate(BUCKET_ORDER if bcol=="bucket_detail" else BUCKET2_ORDER)}
    d["_o"] = d[bcol].map(order)
    sort_keys = (["sales_vertical","_o"] if "sales_vertical" in keys else ["_o"])
    return d.sort_values(sort_keys).drop(columns="_o")

# 4-bucket detail
overall = scorecard(df, ["bucket_detail"]); overall.to_csv(OUT+"audi_1141_scorecard_overall.csv", index=False)
byvert = scorecard(df, ["sales_vertical","bucket_detail"]); byvert.to_csv(OUT+"audi_1141_scorecard_by_vertical.csv", index=False)
# blended two-group: MM (all) vs 3P
overall2 = scorecard(df, ["bucket2"]); overall2.to_csv(OUT+"audi_1141_scorecard2_overall.csv", index=False)
byvert2 = scorecard(df, ["sales_vertical","bucket2"]); byvert2.to_csv(OUT+"audi_1141_scorecard2_by_vertical.csv", index=False)

# ---------------------------------------------------------------------------
# CPA cohort cut (AUDI-1141 follow-up, 2026-08-20): does the CPA advantage hold for
# advertisers that drive no revenue (B2B and similar)? ROAS is undefined for them, so CPA
# is the only efficiency metric the deck can use, and it is the one that does not hold up.
# A CPA median is taken over advertisers with >=1 conversion only, so the qualifying and the
# CPA-eligible advertiser counts are BOTH reported: they differ a lot by group.
# ---------------------------------------------------------------------------
NONREV_OUT = OUT + "audi_1141_scorecard_nonrevenue.csv"
B2B_VERTICAL = "B2B Software & Services"
adv_revenue = df.groupby("advertiser_id").revenue.sum()
no_revenue = set(adv_revenue[adv_revenue <= 0].index)
df["is_non_revenue"] = df.advertiser_id.isin(no_revenue)

def cpa_cut(frame, cohort, gcol):
    rows = []
    for grp, g in frame.groupby(gcol):
        adv = g.groupby("advertiser_id").agg(imps=("imps","sum"), conv=("conv","sum"),
                                             spend=("spend","sum"))
        adv = adv[adv.imps >= ADV_MIN_IMPS]
        if adv.empty:
            continue
        elig = adv[adv.conv > 0]
        rows.append({"cohort": cohort, "group": grp, "n_adv_qual": len(adv),
            "n_adv_cpa": len(elig), "share_with_conv": len(elig)/len(adv) if len(adv) else np.nan,
            "CPA_med": (elig.spend/elig.conv).median() if len(elig) else np.nan,
            "CPA_pooled": elig.spend.sum()/elig.conv.sum() if elig.conv.sum() else np.nan,
            "spend": adv.spend.sum()})
    return rows

cohorts = [("All advertisers", df),
           ("No revenue tracked", df[df.is_non_revenue]),
           (B2B_VERTICAL, df[df.vertical_name == B2B_VERTICAL]),
           ("B2B and no revenue", df[(df.vertical_name == B2B_VERTICAL) & df.is_non_revenue])]
nr_rows = []
for cohort, frame in cohorts:
    nr_rows += cpa_cut(frame, cohort, "bucket2")
    nr_rows += cpa_cut(frame, cohort, "bucket_detail")
nonrev = pd.DataFrame(nr_rows).drop_duplicates(subset=["cohort","group"])
nonrev["_c"] = nonrev.cohort.map({c:i for i,(c,_) in enumerate(cohorts)})
nonrev["_g"] = nonrev.group.map({g:i for i,g in enumerate(BUCKET2_ORDER[:1] + BUCKET_ORDER)})
nonrev = nonrev.sort_values(["_c","_g"]).drop(columns=["_c","_g"])
nonrev.to_csv(NONREV_OUT, index=False)
print("\n=== CPA by cohort (median advertiser; CPA-eligible advertisers only) ===")
print(nonrev.to_string(index=False, float_format=lambda v: f"{v:,.2f}"))

pd.set_option("display.width",240,"display.max_columns",40)
def pct(x): return f"{x*100:.2f}%" if pd.notna(x) else "-"
print("bucket mix:", dict(df.bucket_detail.value_counts()))
print("\n=== OVERALL (advertiser-weighted median; IVR/CVR over impressions) ===")
show = overall.copy()
for c in ["IVR_med","CVR_med","CTR_med"]: show[c]=show[c].map(pct)
show["CPV_med"]=show.CPV_med.map(lambda v:f"${v:,.2f}"); show["ROAS_med"]=show.ROAS_med.round(2)
print(show[["bucket_detail","n_adv","n_adv_qual","n_camp","spend","IVR_med","CVR_med","CTR_med","CPV_med","ROAS_med","n_adv_roas"]].to_string(index=False))
print("\n=== BY VERTICAL - IVR median (visits/imps %) ===")
piv=byvert.pivot(index="sales_vertical",columns="bucket_detail",values="IVR_med")[BUCKET_ORDER]
print((piv*100).round(2).astype(str).replace('nan','-').to_string())
print("\nSaved scorecards.")
