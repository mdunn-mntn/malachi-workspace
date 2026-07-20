#!/usr/bin/env python3
"""AUDI-1141 — aggregate campaign-grain cohort into the MM vs 3P vs Mixed scorecard.

Decisions (from Malachi, 2026-07-20):
  - Full scorecard: Visit Rate, CPV, CVR, ROAS
  - Buckets: MM / 3P / Mixed ; MM split into (gated) vs (no gate)  [the HHST-cap confound]
  - Advertiser-weighted primary (each advertiser = one vote so WGU can't dominate);
    impression/spend-pooled shown as secondary
  - Drop zip-narrowed campaigns EXCEPT Auto,Travel&Hospitality and ProServ (per Jon)
  - 8 sales verticals (+ Other/Unmapped) via the 37->8 crosswalk (done in SQL)
"""
import pandas as pd, numpy as np

SRC = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/audi_1141_campaign_grain.csv"
OUT = "tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"
ADV_MIN_IMPS = 20000          # advertiser-in-cell floor for advertiser-weighted rates
ZIP_KEEP = {"Auto, Travel & Hospitality", "ProServ"}

df = pd.read_csv(SRC)
df = df.dropna(subset=["vertical_id"])                      # 1 orphan row w/ no vertical

# --- cap definition: gate ON for the majority of in-window threshold writes ---
df["gated_frac"] = np.where(df.hhst_writes > 0, df.hhst_writes_gated / df.hhst_writes, 0.0)
df["capped"] = df.gated_frac >= 0.5                          # HHST intent-gate active

# --- zip filter: drop zip-narrowed campaigns except in Auto & ProServ ---
before = len(df)
df = df[~(df.zip_narrow & ~df.sales_vertical.isin(ZIP_KEEP))].copy()
print(f"zip filter: dropped {before-len(df)} zip-narrowed campaigns (kept in {ZIP_KEEP})")

# --- drop Neither (CRM / 1P / geo-only — not part of the MM vs 3P question) ---
df = df[df.bucket != "Neither"].copy()

# --- bucket_detail: split MM by gate; 3P and Mixed whole ---
def detail(r):
    if r.bucket == "MM":
        return "MM (gated)" if r.capped else "MM (no gate)"
    return r.bucket
df["bucket_detail"] = df.apply(detail, axis=1)

BUCKET_ORDER = ["MM (gated)", "MM (no gate)", "Mixed", "3P"]

def scorecard(frame, keys):
    """Pooled + advertiser-weighted KPIs per group."""
    rows = []
    for gkey, g in frame.groupby(keys):
        imps, visits = g.imps.sum(), g.visits.sum()
        conv, rev, spend = g.conv.sum(), g.revenue.sum(), g.spend.sum()
        # pooled (impression/spend weighted)
        pooled = dict(
            VR_pooled=1000*visits/imps if imps else np.nan,          # visits per 1k imps
            CPV_pooled=spend/visits if visits else np.nan,
            CVR_pooled=100*conv/visits if visits else np.nan,        # % of visits -> conv
            ROAS_pooled=rev/spend if spend else np.nan)
        # advertiser-weighted (each advertiser one vote, min-volume floor)
        adv = g.groupby("advertiser_id").agg(imps=("imps","sum"),visits=("visits","sum"),
                conv=("conv","sum"),revenue=("revenue","sum"),spend=("spend","sum"))
        adv = adv[adv.imps >= ADV_MIN_IMPS]
        adv_vr = 1000*adv.visits/adv.imps
        adv_cpv = (adv.spend/adv.visits).replace([np.inf,-np.inf],np.nan)
        adv_cvr = (100*adv.conv/adv.visits).replace([np.inf,-np.inf],np.nan)
        adv_roas = (adv.revenue/adv.spend).replace([np.inf,-np.inf],np.nan)
        rows.append({**({k:v for k,v in zip(keys,(gkey if isinstance(gkey,tuple) else (gkey,)))}),
            "n_adv": g.advertiser_id.nunique(), "n_adv_qual": len(adv), "n_camp": len(g),
            "spend": spend, "imps": imps,
            # advertiser-weighted MEAN
            "VR_advw": adv_vr.mean(), "CPV_advw": adv_cpv.mean(),
            "CVR_advw": adv_cvr[adv.conv>0].mean(), "ROAS_advw": adv_roas[adv.revenue>0].mean(),
            # advertiser-weighted MEDIAN (whale-robust headline)
            "VR_med": adv_vr.median(), "CPV_med": adv_cpv.median(),
            "ROAS_med": adv_roas[adv.revenue>0].median(), "n_adv_roas": int((adv.revenue>0).sum()),
            **pooled})
    return pd.DataFrame(rows)

# ---- overall (all verticals) ----
overall = scorecard(df, ["bucket_detail"])
overall["_o"] = overall.bucket_detail.map({b:i for i,b in enumerate(BUCKET_ORDER)})
overall = overall.sort_values("_o").drop(columns="_o")
overall.to_csv(OUT+"audi_1141_scorecard_overall.csv", index=False)

# ---- by vertical ----
byvert = scorecard(df, ["sales_vertical","bucket_detail"])
byvert["_o"] = byvert.bucket_detail.map({b:i for i,b in enumerate(BUCKET_ORDER)})
byvert = byvert.sort_values(["sales_vertical","_o"]).drop(columns="_o")
byvert.to_csv(OUT+"audi_1141_scorecard_by_vertical.csv", index=False)

pd.set_option("display.width",220,"display.max_columns",40,"display.float_format",lambda x:f"{x:,.2f}")
print("\n================ OVERALL (all verticals) — MEDIAN advertiser is whale-robust headline ================")
print(overall[["bucket_detail","n_adv","n_adv_qual","n_camp","spend",
               "VR_med","VR_advw","CPV_med","CPV_advw","ROAS_med","n_adv_roas","VR_pooled","ROAS_pooled"]].to_string(index=False))

print("\n================ BY VERTICAL — Visit Rate, MEDIAN advertiser (visits per 1k imps); (n qualifying advs) ================")
piv = byvert.pivot(index="sales_vertical", columns="bucket_detail", values="VR_med")[BUCKET_ORDER]
pivn = byvert.pivot(index="sales_vertical", columns="bucket_detail", values="n_adv_qual")[BUCKET_ORDER]
show = piv.round(1).astype(str) + "  (" + pivn.fillna(0).astype(int).astype(str) + ")"
print(show.to_string())
print("\n================ BY VERTICAL — CPV, MEDIAN advertiser ($ per visit) ================")
print(byvert.pivot(index="sales_vertical", columns="bucket_detail", values="CPV_med")[BUCKET_ORDER].round(0).to_string())

# diagnose the ProServ ROAS outlier
print("\n================ DIAGNOSTIC: ProServ MM(gated) top advertisers by ROAS ================")
ps = df[(df.sales_vertical=="ProServ") & (df.bucket_detail=="MM (gated)")]
psa = ps.groupby("advertiser_id").agg(spend=("spend","sum"),revenue=("revenue","sum"),visits=("visits","sum"),imps=("imps","sum"))
psa = psa[psa.imps>=ADV_MIN_IMPS]; psa["ROAS"]=psa.revenue/psa.spend
print(psa.sort_values("ROAS",ascending=False).head(5).round(1).to_string())
print("\nSaved: audi_1141_scorecard_overall.csv, audi_1141_scorecard_by_vertical.csv")
