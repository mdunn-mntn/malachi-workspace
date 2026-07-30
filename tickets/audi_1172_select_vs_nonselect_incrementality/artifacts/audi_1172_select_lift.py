#!/usr/bin/env python3
"""Select vs non-Select ghost-bid incrementality for Kirsa's 93-AID cohort.

Source: gold reporting.lift__ghost_bid_rollup (campaign_group grain, time-boxed,
entry-cohort + drop-left-censored applied upstream; AUDI-1148). Clean gate here
= se>0 AND NOT low_coverage. Cross-CG -> advertiser x product pooled by
inverse-variance weights (the captured cross-campaign rule; NOT a naive count pool).
"""
import sys, math, json
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
import pandas as pd
from google.cloud import bigquery
from lib.mntn_xlsx import MntnWorkbook, FMT, rag_threshold

GEN = "2026-07-29"
TICKET = "AUDI-1172"
TDIR = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1172_select_vs_nonselect_incrementality"
client = bigquery.Client(project="dw-main-silver")

AIDS = [
 37983,47347,46722,33760,31276,35821,45550,33768,41034,40521,51095,40535,34185,33617,34094,
 39149,40236,36232,33666,41545,53341,41426,36743,34421,40807,37893,42097,37676,53308,45458,
 50413,45921,38363,31357,38579,53656,49868,40598,59241,40601,32863,47228,37798,33389,31460,
 53749,36794,33179,37775,35086,58469,32769,62938,33950,39207,61583,59584,36678,30238,33270,
 31441,32404,36583,37423,57418,38799,38800,33316,36507,33448,34862,34585,54196,47209,65217,
 37085,48875,34472,39834,56494,37316,62689,32153,33467,59460,32040,66784,37880,40002,44054,
 40563,39225,44339,44419]

def _wrap_aids(aids, per=10, indent="    "):
    """Render the AID list wrapped ~per-line so the SQL tab never cuts off horizontally."""
    rows = [", ".join(str(a) for a in aids[i:i+per]) for i in range(0, len(aids), per)]
    return "\n".join(indent + r + ("," if i < len(rows)-1 else "") for i, r in enumerate(rows))

AIDS_SQL = _wrap_aids(AIDS)

SQL = f"""
WITH aids AS (SELECT advertiser_id FROM UNNEST([
{AIDS_SQL}
  ]) AS advertiser_id),
cg AS (
  SELECT r.advertiser_id,
         r.entity_id AS campaign_group_id,
         CASE WHEN pcg.product_id = 2 THEN 'Select' ELSE 'non_Select' END AS product,
         r.abs_itt, r.se, r.n_treatment, r.n_holdout, r.vis_treatment, r.vis_holdout,
         r.conv_treatment, r.conv_holdout
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg
    ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group'
    AND r.advertiser_id IN (SELECT advertiser_id FROM aids)
    AND r.se > 0 AND NOT r.low_coverage
)
SELECT cg.advertiser_id, adv.company_name, cg.product,
       COUNT(*) AS n_cg,
       SUM(cg.n_treatment) AS n_treatment, SUM(cg.n_holdout) AS n_holdout,
       SUM(cg.vis_treatment) AS vis_treatment, SUM(cg.vis_holdout) AS vis_holdout,
       SUM(cg.conv_treatment) AS conv_treatment, SUM(cg.conv_holdout) AS conv_holdout,
       SUM(cg.vis_holdout)/SUM(cg.n_holdout) AS base_holdout_vr,
       SUM(cg.abs_itt / POW(cg.se,2)) / SUM(1.0/POW(cg.se,2)) AS ivw_abs_itt,
       SQRT(1.0 / SUM(1.0/POW(cg.se,2))) AS ivw_se
FROM cg
LEFT JOIN `dw-main-silver.public.advertisers` adv USING (advertiser_id)
GROUP BY 1,2,3
"""

df = client.query(SQL).to_dataframe()

# derived per advertiser x product
import numpy as np
df["visit_lift_pp"] = df["ivw_abs_itt"] * 100.0
# relative lift undefined where holdout visit rate is 0 (no baseline) -> NaN, don't let inf sort to top
df["rel_lift"] = np.where(df["base_holdout_vr"] > 0,
                          df["ivw_abs_itt"] / df["base_holdout_vr"], np.nan)  # decimal
df["z"] = df["ivw_abs_itt"] / df["ivw_se"]
df["sig95"] = df["z"].abs() >= 1.96
df["ci_low_pp"] = (df["ivw_abs_itt"] - 1.96*df["ivw_se"]) * 100.0
df["ci_high_pp"] = (df["ivw_abs_itt"] + 1.96*df["ivw_se"]) * 100.0
df["treated_vr"] = df["vis_treatment"] / df["n_treatment"]
df["holdout_vr"] = df["base_holdout_vr"]

def pool(rows):
    """IVW pool a set of advertiser x product rows -> dict."""
    w = 1.0 / (rows["ivw_se"]**2)
    abs_itt = (w * rows["ivw_abs_itt"]).sum() / w.sum()
    se = math.sqrt(1.0 / w.sum())
    nh = rows["n_holdout"].sum(); vh = rows["vis_holdout"].sum()
    nt = rows["n_treatment"].sum(); vt = rows["vis_treatment"].sum()
    base = vh / nh
    return {
        "n_advertisers": rows["advertiser_id"].nunique(),
        "n_cg": int(rows["n_cg"].sum()),
        "n_treatment": int(nt), "n_holdout": int(nh),
        "treated_vr": vt/nt, "holdout_vr": base,
        "visit_lift_pp": abs_itt*100.0,
        "ci_low_pp": (abs_itt-1.96*se)*100.0, "ci_high_pp": (abs_itt+1.96*se)*100.0,
        "rel_lift": abs_itt/base, "z": abs_itt/se, "sig95": abs(abs_itt/se) >= 1.96,
    }

# advertisers running BOTH (in the clean set)
prod_by_adv = df.groupby("advertiser_id")["product"].agg(set)
both_ids = sorted([a for a, s in prod_by_adv.items() if {"Select","non_Select"} <= s])
sel_only = sorted([a for a, s in prod_by_adv.items() if s == {"Select"}])
ns_only  = sorted([a for a, s in prod_by_adv.items() if s == {"non_Select"}])

both = df[df["advertiser_id"].isin(both_ids)]
pooled = pd.DataFrame([
    {"Cohort": "Advertisers running BOTH", "Product": "Select",      **pool(both[both["product"]=="Select"])},
    {"Cohort": "Advertisers running BOTH", "Product": "non-Select",  **pool(both[both["product"]=="non_Select"])},
    {"Cohort": "All in cohort",            "Product": "Select",      **pool(df[df["product"]=="Select"])},
    {"Cohort": "All in cohort",            "Product": "non-Select",  **pool(df[df["product"]=="non_Select"])},
])

# paired within-advertiser (both): Select rel - non-Select rel
piv = both.pivot_table(index=["advertiser_id","company_name"], columns="product",
                       values=["rel_lift","visit_lift_pp","z","sig95","n_treatment","n_holdout",
                               "holdout_vr","treated_vr"], aggfunc="first")
piv.columns = [f"{a}__{b}" for a,b in piv.columns]
piv = piv.reset_index()
piv["rel_gap_pp"] = (piv["rel_lift__Select"] - piv["rel_lift__non_Select"]) * 100.0  # pp of rel-lift
n_sel_higher = int((piv["rel_lift__Select"] > piv["rel_lift__non_Select"]).sum())

# ---- console summary ----
print("=== POOLED (advertisers running BOTH; n_adv={}) ===".format(len(both_ids)))
for _, r in pooled[pooled["Cohort"]=="Advertisers running BOTH"].iterrows():
    print(f"  {r['Product']:>11}: {r['visit_lift_pp']:+.3f}pp  rel {r['rel_lift']*100:+.1f}%  "
          f"[{r['ci_low_pp']:+.3f},{r['ci_high_pp']:+.3f}]pp z={r['z']:.1f} sig={r['sig95']}  "
          f"(treated {r['treated_vr']*100:.2f}% vs holdout {r['holdout_vr']*100:.2f}%, "
          f"nT={r['n_treatment']:,} nH={r['n_holdout']:,})")
print(f"\ncohort: both={len(both_ids)}  select_only={len(sel_only)}  nonselect_only={len(ns_only)}")
print(f"paired: Select rel-lift > non-Select in {n_sel_higher}/{len(both_ids)} advertisers")
print(f"median paired rel-gap (Select - nonSelect): {piv['rel_gap_pp'].median():+.1f} pp")

# dynamic headline scalars (so findings/cover/notes never drift from the tabs as data accumulates)
_pb = pooled[pooled["Cohort"] == "Advertisers running BOTH"]
SEL_REL = _pb[_pb.Product == "Select"].iloc[0]["rel_lift"] * 100
NS_REL = _pb[_pb.Product == "non-Select"].iloc[0]["rel_lift"] * 100
RATIO = SEL_REL / NS_REL
N_BOTH, N_SEL_HI = len(both_ids), n_sel_higher
MED_GAP = piv["rel_gap_pp"].median()

# save intermediates + query
OUT = f"{TDIR}/outputs"
df.to_csv(f"{OUT}/audi_1172_lift_by_adv_product.csv", index=False)
pooled.to_csv(f"{OUT}/audi_1172_lift_pooled.csv", index=False)
piv.to_csv(f"{OUT}/audi_1172_lift_paired.csv", index=False)
with open(f"{TDIR}/queries/audi_1172_select_lift.sql", "w") as f:
    f.write(SQL.strip() + "\n")
print("\nwrote CSVs + SQL to", TDIR)

# ======================================================================
# BUILD .xlsx
# ======================================================================
YESNO = lambda b: "Yes" if bool(b) else "—"
# absolute visit lift is in PERCENTAGE POINTS, not a rate -> show "pp" so it never reads as a % rate.
# rule for the whole workbook: "%" = a rate or a RELATIVE lift; "pp" = an ABSOLUTE point difference.
PP3 = '0.000"pp"'
PP2 = '0.00"pp"'

# --- Sheet: pooled headline (both cohort) ---
ph = pooled[pooled["Cohort"] == "Advertisers running BOTH"].copy()
# Holdout VR (baseline) is the observed count-pooled rate; Abs/Rel lift are IVW-pooled and are
# defined relative to that baseline (rel = abs/holdout), so the three reconcile. The raw count-pooled
# Treated VR does NOT subtract to the IVW Abs lift, so it's dropped here to keep the headline consistent.
head_df = pd.DataFrame({
    "Product":        ph["Product"].values,
    "Advertisers":    ph["n_advertisers"].values,
    "Campaign groups": ph["n_cg"].values,
    "Holdout VR":     ph["holdout_vr"].values,               # observed baseline visit rate
    "Abs lift":       ph["visit_lift_pp"].values,            # percentage points (IVW)
    "Rel lift":       ph["rel_lift"].values,                 # relative (%) = abs/holdout
    "CI low":         ph["ci_low_pp"].values,                # pp
    "CI high":        ph["ci_high_pp"].values,               # pp
    "Sig 95%":        [YESNO(b) for b in ph["sig95"].values],
    "Treated bids":   ph["n_treatment"].values,
    "Holdout bids":   ph["n_holdout"].values,
})

# --- Sheet: per-advertiser, running both (side by side) ---
pv = piv.copy()
pv["adv"] = pv["company_name"].fillna(pv["advertiser_id"].astype(str))
both_df = pd.DataFrame({
    "Advertiser":       pv["adv"].values,
    "AID":              pv["advertiser_id"].values,
    "Select rel lift":  pv["rel_lift__Select"].values,           # % over baseline
    "non-Sel rel lift": pv["rel_lift__non_Select"].values,       # % over baseline
    "Select edge":      (pv["rel_lift__Select"] - pv["rel_lift__non_Select"]).values,  # % difference of rel lifts
    "Select abs pp":    pv["visit_lift_pp__Select"].values,      # percentage points
    "non-Sel abs pp":   pv["visit_lift_pp__non_Select"].values,  # percentage points
    "Select sig":       [YESNO(b) for b in pv["sig95__Select"].values],
    "non-Sel sig":      [YESNO(b) for b in pv["sig95__non_Select"].values],
    "Select bids":      pv["n_treatment__Select"].values.astype("int64"),
    "non-Sel bids":     pv["n_treatment__non_Select"].values.astype("int64"),
}).sort_values("Select bids", ascending=False)

# --- Sheet: all advertisers by product (detail) ---
det = df.copy()
det["adv"] = det["company_name"].fillna(det["advertiser_id"].astype(str))
det["prod"] = det["product"].map({"Select": "Select", "non_Select": "non-Select"})
detail_df = pd.DataFrame({
    "Advertiser":   det["adv"].values,
    "AID":          det["advertiser_id"].values,
    "Product":      det["prod"].values,
    "Camp groups":  det["n_cg"].values,
    "Treated VR":   det["treated_vr"].values,
    "Holdout VR":   det["holdout_vr"].values,
    "Abs lift":     det["visit_lift_pp"].values,       # percentage points
    "Rel lift":     det["rel_lift"].values,            # relative (%)
    "z":            det["z"].values,
    "Sig 95%":      [YESNO(b) for b in det["sig95"].values],
    "Treated bids": det["n_treatment"].values,
    "Holdout bids": det["n_holdout"].values,
}).sort_values(["Product", "Rel lift"], ascending=[True, False])

wb = MntnWorkbook(
    title="MNTN Select vs Non-Select Incrementality",
    ticket=TICKET,
    subtitle="Ghost-bid holdout visit lift, prospecting, advertisers running both products",
    period="2026-06-22 to 2026-07-27",   # neutral date range: accurate for every tab (the 7d-window detail is on the Read me/Method tabs)
    generated=GEN,
)

wb.table(
    "Headline", head_df,
    finding=f"Select prospecting drives ~{RATIO:.0f}x the relative visit lift of non-Select ({SEL_REL:+.0f}% vs {NS_REL:+.0f}%)",
    method="Pooled across 35 advertisers running both, inverse-variance-weighted; ghost-bid holdout, prospecting. "
           "See Read me for definitions.",
    formats={"Holdout VR": FMT.PCT2, "Abs lift": PP3,
             "Rel lift": FMT.PCT1, "CI low": PP3, "CI high": PP3,
             "Treated bids": FMT.INT, "Holdout bids": FMT.INT},
    signal={"Rel lift": {"sig": "Sig 95%"}, "Abs lift": {"sig": "Sig 95%"}},
    kind="headline",
    toc="The headline: pooled Select vs non-Select visit lift for advertisers running both.",
)

wb.table(
    "By advertiser (both)", both_df,
    finding=f"Select out-lifts non-Select in {N_SEL_HI} of {N_BOTH} advertisers running both (median edge +{MED_GAP:.0f}pp of relative lift)",
    method="One row per advertiser running both, ranked by Select bid volume. "
           "Edge = Select minus non-Select relative lift. See Read me for definitions.",
    formats={"Select rel lift": FMT.PCT1, "non-Sel rel lift": FMT.PCT1, "Select edge": FMT.PCT1,
             "Select abs pp": PP2, "non-Sel abs pp": PP2,
             "Select bids": FMT.INT, "non-Sel bids": FMT.INT},
    signal={"Select rel lift": {"sig": "Select sig"}, "non-Sel rel lift": {"sig": "non-Sel sig"},
            "Select edge": {}},
    kind="data", first_col_width=30,
    toc="Per-advertiser Select vs non-Select, side by side, ranked by Select's edge.",
)

wb.table(
    "All by product", detail_df,
    finding="Full per-advertiser readout: 43 Select and 66 non-Select advertiser rows",
    method="One row per advertiser x product, usable-holdout groups only, sorted by product then relative lift. "
           "See Read me for definitions.",
    formats={"Treated VR": FMT.PCT2, "Holdout VR": FMT.PCT2, "Abs lift": PP3,
             "Rel lift": FMT.PCT1, "z": FMT.NUM1, "Treated bids": FMT.INT, "Holdout bids": FMT.INT},
    signal={"Rel lift": {"sig": "Sig 95%"}},
    kind="detail", first_col_width=30,
    toc="Every advertiser x product row behind the pooled and paired numbers.",
)

# --- Sheet: cost per incremental (client Verified-Visit basis) ---
# Numbers from artifacts/audi_1172_cpiv_vv_compute.py -> outputs/audi_1172_cpiv_vv_pooled.csv.
cpiv = pd.read_csv(f"{OUT}/audi_1172_cpiv_vv_pooled.csv")
_pm = {"Select": "Select", "non_Select": "non-Select"}
cpiv_df = pd.DataFrame({
    "Product":         cpiv["product"].map(_pm).values,
    "Spend":           cpiv["spend"].values,
    "Verified visits": cpiv["vv_reported"].values,
    "Incr. visits":    cpiv["incr_vv"].values,
    "CPIV":            cpiv["cpiv_vv"].values,
    "Incr. conv":      cpiv["incr_conv"].values,
    "CPIA":            cpiv["cpia_vv"].values,
}).sort_values("CPIV")

wb.table(
    "Cost per incremental", cpiv_df,
    finding="Select is cheaper per incremental outcome: ~1.6x per visit, ~3x per conversion",
    method="Spend (metered, prospecting) / incremental, where incremental = Reporting Verified Visits (or "
           "conversions) x lift / (1 + lift), lift = the ghost-bid relative lift. See Read me for the method.",
    formats={"Spend": FMT.USD0, "Verified visits": FMT.INT, "Incr. visits": FMT.INT,
             "CPIV": FMT.USD, "Incr. conv": FMT.INT, "CPIA": FMT.USD0},
    heat={"CPIV": "low", "CPIA": "low"},   # cost: lower is greener
    kind="data", first_col_width=18,
    toc="Cost per incremental visit and conversion, on the basis advertisers see in Reporting.",
)

# --- Sheet: Cost by advertiser (per advertiser x product, both cohort) ---
# Per advertiser x product from queries/audi_1172_cpiv_vv_by_adv.sql; incremental = VV x lift/(1+lift);
# 'n/a' where the advertiser's lift is <=0 or holdout empty (not net-incremental / not measurable).
cpa = pd.DataFrame(json.loads(open(f"{OUT}/audi_1172_cpiv_vv_by_adv.json").read()))
for c in ["rel_lift_raw", "conv_rel_lift_raw", "vv_reported", "conv_reported", "spend"]:
    cpa[c] = pd.to_numeric(cpa[c], errors="coerce")
cpa["advertiser_id"] = cpa["advertiser_id"].astype(int)

def _cost(vv, lift, spend):
    if pd.isna(lift) or pd.isna(vv) or pd.isna(spend) or lift <= 0:
        return None
    incr = vv * lift / (1.0 + lift)
    return spend / incr if incr and incr > 0 else None

cpa["cpiv"] = [_cost(v, l, s) for v, l, s in zip(cpa["vv_reported"], cpa["rel_lift_raw"], cpa["spend"])]
cpa["cpia"] = [_cost(v, l, s) for v, l, s in zip(cpa["conv_reported"], cpa["conv_rel_lift_raw"], cpa["spend"])]
sigmap = {(int(r.advertiser_id), r["product"]): bool(r.sig95) for _, r in df.iterrows()}
NA = lambda x: x if (isinstance(x, (int, float)) and pd.notna(x)) else "n/a"
by = {}
for _, r in cpa.iterrows():
    by.setdefault(int(r["advertiser_id"]), {})[r["product"]] = r
cost_recs = []
for aid in both_ids:
    aid = int(aid)
    s, n = by.get(aid, {}).get("Select"), by.get(aid, {}).get("non_Select")
    if s is None and n is None:
        continue
    name = (s if s is not None else n)["company_name"]
    cost_recs.append({
        "Advertiser":   name if (isinstance(name, str) and name) else str(aid),
        "AID":          aid,
        "Select CPIV":  NA(s["cpiv"]) if s is not None else "n/a",
        "Select CPIA":  NA(s["cpia"]) if s is not None else "n/a",
        "Select sig":   YESNO(sigmap.get((aid, "Select"), False)),
        "non-Sel CPIV": NA(n["cpiv"]) if n is not None else "n/a",
        "non-Sel CPIA": NA(n["cpia"]) if n is not None else "n/a",
        "non-Sel sig":  YESNO(sigmap.get((aid, "non_Select"), False)),
    })
cost_adv_df = pd.DataFrame(cost_recs).sort_values("AID")

wb.table(
    "Cost by advertiser", cost_adv_df,
    finding="Cost per incremental visit and conversion, per advertiser (filter Sig to Yes)",
    method="Per advertiser x product, same method as Cost per incremental. Filter the Sig columns to Yes; "
           "'n/a' = the advertiser's lift was not net-incremental or too small to measure (tiny holdout).",
    formats={"Select CPIV": FMT.USD, "Select CPIA": FMT.USD0,
             "non-Sel CPIV": FMT.USD, "non-Sel CPIA": FMT.USD0},
    heat={"Select CPIV": "low", "Select CPIA": "low", "non-Sel CPIV": "low", "non-Sel CPIA": "low"},
    kind="data", first_col_width=30,
    toc="CPIV/CPIA per advertiser (both cohort); filter the Sig columns to significant rows.",
)

# --- Sheet: AID-level overall incrementality, 3 product-mix groups (all MNTN advertisers) ---
grp = pd.read_csv(f"{OUT}/audi_1172_aid_group_pooled.csv")
gorder = {"Both": 0, "Select-only": 1, "PTV-only": 2}
grp = grp.sort_values("group", key=lambda col: col.map(gorder)).reset_index(drop=True)
vsig = grp["vis_sig"].astype(str).str.lower().isin(["true", "1", "yes"])
CN = lambda x, n: (x if (pd.notna(x) and n >= 5) else "n/a")   # hide conv lift on thin groups (Select-only n=2)
group_df = pd.DataFrame({
    "Group":                    grp["group"].values,
    "Advertisers":              grp["n_adv"].values,
    "Visit lift (volume-wtd)":  grp["vis_ivw"].values,
    "Visit lift (typical adv)": grp["vis_ew_med"].values,
    "Vis sig":                  [YESNO(b) for b in vsig],
    "Conv lift (volume-wtd)":   [CN(x, n) for x, n in zip(grp["conv_ivw"], grp["n_conv"])],
    "Adv w/ conv":              grp["n_conv"].values,
})
_b = grp[grp["group"] == "Both"].iloc[0]
_p = grp[grp["group"] == "PTV-only"].iloc[0]
wb.table(
    "AID-level lift by group", group_df,
    finding=(f"Select-running advertisers are more incremental overall than PTV-only "
             f"(volume-weighted {_b.vis_ivw*100:+.1f}% vs {_p.vis_ivw*100:+.1f}%; "
             f"typical advertiser {_b.vis_ew_med*100:+.0f}% vs {_p.vis_ew_med*100:+.0f}%)"),
    method="Overall advertiser-level visit lift (all of an advertiser's prospecting, both products), across ALL "
           "MNTN advertisers. Volume-weighted = precision-pooled (big advertisers drive it); typical adv = median "
           "advertiser. Observational, not causal. Test accounts + WGU excluded. See Read me / Method.",
    formats={"Visit lift (volume-wtd)": FMT.PCT1, "Visit lift (typical adv)": FMT.PCT1,
             "Conv lift (volume-wtd)": FMT.PCT1, "Advertisers": FMT.INT, "Adv w/ conv": FMT.INT},
    signal={"Visit lift (volume-wtd)": {"sig": "Vis sig"}, "Visit lift (typical adv)": {}},
    kind="data", first_col_width=14,
    toc="Overall incrementality by product mix: Both / Select-only / PTV-only (all advertisers).",
)

wb.glossary(
    "Read me",
    max_entries=16,   # 6 data tabs now; two extra rows for the per-advertiser cost + group-comparison tabs
    intro="How the Select vs non-Select incrementality numbers were produced and how to read them.",
    rows=[
        ("How to read this", ""),
        ("% vs pp", "'%' = a rate or a RELATIVE lift (% over baseline); 'pp' = an absolute percentage-point gap. "
            "A 0.27pp gap on a 1.2% baseline is a +22% relative lift."),
        ("What this measures", "Incremental visit lift from MNTN's ghost-bid holdout: ~10% of prospecting IPs are held out "
            "(evaluated as-if-served). Treated minus holdout visit rate = the incremental effect."),
        ("Select vs non-Select", "Product on the campaign group: Select = product_id 2 (MNTN/Rust bidder); non-Select = PTV "
            "(product_id 1, Beeswax). All rows are prospecting (objective_id 1)."),
        ("Rel lift (%)", "Abs lift / holdout rate = % over the no-ad baseline. +22% = treated visited 22% more than holdout, "
            "NOT 22% of people. The fair cross-product comparison; negatives are usually noise."),
        ("Bid-grain ITT", "The unit is a bid, not a served user; treatment bids win ~10% of auctions, so absolute pp is "
            "diluted. Relative lift is the comparable metric."),
        ("Method & cost", ""),
        ("Pooling (IVW)", "Campaign groups combine by inverse-variance weights (1/SE^2), not a raw count pool (which gives "
            "Simpson's-paradox artifacts). Significance at bid-grain N is a floor."),
        ("Cost (CPIV/CPIA)", "Spend / incremental. Incremental = Reporting Verified Visits (view+click+competing visits, the "
            "client-UI number) x lift / (1 + lift), lift = the ghost-bid relative lift (÷(1+lift) strips out the organic baseline)."),
        ("Why the lift looks bigger there", "CPIV uses the volume-weighted lift (right for a total-cost metric); the lift tabs "
            "use the average-campaign lift (IVW). Same data, different question."),
        ("Cost by advertiser", "Per-customer CPIV/CPIA (both cohort). Filter the Sig columns to Yes; 'n/a' = that advertiser's "
            "lift was not net-incremental or too small to measure (tiny holdout)."),
        ("AID-level lift by group", "Overall advertiser incrementality (all their prospecting), ALL MNTN advertisers, split "
            "PTV-only / Select-only / Both. Volume-weighted (big advertisers drive it) vs typical (median) advertiser. Observational, not causal."),
        ("Coverage & window", ""),
        ("Window", "2026-06-22 to 07-27; each IP's visits count within 7 days of its first bid (fixed per-IP window). "
            "Trailing ~7 days still maturing. No pre-6/22 data (no backfill)."),
        ("Coverage", "Both bidder legs are in (Select = MNTN/Rust, non-Select = Beeswax). Select is a subset only from the "
            "6/22 floor and dropping groups without a usable holdout."),
        ("Cohort", f"93 AIDs requested; with usable-holdout data: {len(both_ids)} run both, "
            f"{len(sel_only)} Select-only, {len(ns_only)} non-Select-only."),
    ],
)

SCOPING_SQL = """-- SCOPING QUERY (validation only) - confirms every campaign group is prospecting (objective_id=1).
WITH aids AS (SELECT advertiser_id FROM UNNEST([ /* the same 93 AIDs as the main query */ ]) AS advertiser_id),
cg AS (
  SELECT r.advertiser_id, pcg.product_id, pcg.objective_id, r.n_treatment, r.n_holdout
  FROM `dw-main-gold.reporting.lift__ghost_bid_rollup` r
  JOIN `dw-main-silver.public.campaign_groups` pcg ON r.entity_id = pcg.campaign_group_id
  WHERE r.level = 'campaign_group' AND r.advertiser_id IN (SELECT advertiser_id FROM aids)
)
SELECT CASE WHEN product_id = 2 THEN 'Select' ELSE 'non-Select(PTV)' END AS product,
       objective_id, COUNT(DISTINCT advertiser_id) AS n_adv, COUNT(*) AS n_cg,
       SUM(n_treatment) AS tot_treatment, SUM(n_holdout) AS tot_holdout
FROM cg GROUP BY 1, 2 ORDER BY 1, n_cg DESC;
-- Result 2026-07-28: Select obj=1 -> 43 adv / 111 cg;  non-Select obj=1 -> 66 adv / 175 cg.
-- Only objective_id = 1 present -> the cohort is 100% prospecting, as expected."""

import re
# CPIV/CPIA query behind the Cost per incremental tab. Strip its own leading comment block (we add one
# short header below) and collapse its 93-AID list like the scoping query.
CPIV_SQL = open(f"{TDIR}/queries/audi_1172_cpiv_vv_correct.sql").read().strip()
CPIV_SQL = re.sub(r"\A(\s*--[^\n]*\n)+", "", CPIV_SQL)                       # drop the file's leading comments
CPIV_SQL = re.sub(r"UNNEST\(\[.*?\]\)", "UNNEST([ /* the same 93 AIDs as the main query */ ])", CPIV_SQL, flags=re.S)

# One short header per query (<=3 lines; the sql() tab enforces this cap and warns otherwise).
QUERY_TAB = (
    "-- LIFT QUERY - drives Headline / By advertiser / All by product (one row per advertiser x product).\n\n"
    + SQL.strip() + "\n\n\n"
    "-- COST QUERY - drives Cost per incremental (CPIV/CPIA).\n\n"
    + CPIV_SQL.strip() + "\n\n\n"
    + SCOPING_SQL + "\n"
)

wb.sql("Query", QUERY_TAB, note="The SQL behind every number, kept for validation. "
                                "Sources: lift__ghost_bid_rollup, campaign_groups, all_facts.")

wb.notes(
    "Method & caveats",
    intro="What to trust, what not to over-read.",
    blocks=[
        ("Headline", f"Among {N_BOTH} advertisers running both Select and non-Select prospecting, Select shows {SEL_REL:+.1f}% relative "
            f"visit lift vs non-Select's {NS_REL:+.1f}% - roughly {RATIO:.0f}x. Both are significant. The gap holds per-advertiser: Select "
            f"beats non-Select in {N_SEL_HI} of {N_BOTH}, median edge +{MED_GAP:.0f}pp of relative lift, so the pooled result is not driven by one large advertiser."),
        ("Why relative, not absolute", "Numbers are ghost-bid ITT at bid grain. Treatment bids win ~10% of auctions, so the "
            "absolute pp lift is diluted by win rate roughly equally across products. Relative lift normalizes this and is the fair comparison."),
        ("Prospecting only", "Every row is objective_id 1. The ghost-bid holdout is a prospecting mechanism (held-out IPs never "
            "win, so they never leave the prospecting pool). Retargeting is out of scope by construction."),
        ("Date range + the 7-day window", "2026-06-22 to 2026-07-27; first day dropped upstream (left-censored). Each IP's "
            "visits are counted over a FIXED 7-day window from its own first bid - not the full calendar period - so early and "
            "late entrants are measured on equal footing (removes an entry-time bias). Tradeoff: visits >7 days after an IP's first "
            "bid aren't attributed, and the trailing ~7 days are still maturing (right-censored). 'IP' ~ a household. No pre-6/22 data (no backfill)."),
        ("Coverage", "Both bidder legs are in: Select = the MNTN (Rust) bidder, non-Select/PTV = Beeswax (the leg tracks the "
            "product). 43 of 93 requested advertisers have Select lift data, 66 have non-Select; 35 have both once campaign "
            "groups without a usable holdout are excluded, plus the 6/22 data floor (no backfill)."),
        ("Cost per incremental (CPIV/CPIA)", "Spend / incremental, where incremental = Reporting Verified Visits (or "
            "conversions) x lift / (1 + lift), lift = the ghost-bid relative lift (÷(1+lift) removes the organic baseline; method "
            "confirmed by Matt Brorby, matches the customer-facing dashboard). On this client basis Select is $5.23/incr visit vs "
            "$8.23 (1.6x) and $84 vs $256/incr conversion (3.0x) - cheaper on both, but a narrower gap than the lift ratio because "
            "non-Select delivers far more total visits. Uses the volume-weighted lift (total-cost basis), not the IVW lift on the other tabs."),
        ("AID-level lift by group (observational)", "The 3-group comparison (PTV-only / Select-only / Both, all MNTN "
            "advertisers) is OBSERVATIONAL, not causal: advertisers self-select into Select, so a higher Both/Select lift shows "
            "association, not that Select caused it. Volume-weighted pools by precision (a few big advertisers dominate; PTV-only "
            "lands near 0% because its largest spenders are barely incremental), while the typical (median) advertiser is higher - "
            "read both. Test accounts + WGU (an extreme outlier) excluded. Select-only n is small (wide interval); its conversion "
            "lift is n/a (too few advertisers with holdout conversions)."),
        ("Do not over-read", "Individual low-volume campaigns have wide intervals; a single small Select campaign is not a verdict. "
            "The pooled and paired advertiser-level reads are the defensible outputs."),
    ],
)

_cs = cpiv[cpiv["product"] == "Select"].iloc[0]
_cn = cpiv[cpiv["product"] == "non_Select"].iloc[0]
wb.cover(takeaways=[
    f"Select prospecting drives {SEL_REL:+.0f}% relative visit lift vs {NS_REL:+.0f}% for non-Select - roughly {RATIO:.0f}x, both significant.",
    f"The edge is consistent: Select beats non-Select in {N_SEL_HI} of {N_BOTH} advertisers running both (median +{MED_GAP:.0f}pp of relative lift).",
    f"Cost: on the basis advertisers see in Reporting, Select runs ${_cs.cpiv_vv:.2f} per incremental visit vs ${_cn.cpiv_vv:.2f} "
    f"({_cn.cpiv_vv/_cs.cpiv_vv:.1f}x) and ${_cs.cpia_vv:.0f} vs ${_cn.cpia_vv:.0f} per incremental conversion ({_cn.cpia_vv/_cs.cpia_vv:.1f}x).",
    f"Overall (all advertisers, observational): the typical Select-running advertiser is more incremental than PTV-only "
    f"(+{_b.vis_ew_med*100:.0f}% vs +{_p.vis_ew_med*100:.0f}% median advertiser).",
    "Window is 2026-06-22 onward (no earlier data); numbers are ghost-bid ITT, compared relatively, prospecting only.",
])

local_path = wb.save_local(f"{TDIR}/artifacts/{TICKET} Select vs Non-Select Incrementality.xlsx")
drive_path = wb.save_drive(TICKET, "Select vs Non-Select Incrementality")
print("wrote xlsx (local):", local_path)
print("wrote xlsx (drive):", drive_path)

