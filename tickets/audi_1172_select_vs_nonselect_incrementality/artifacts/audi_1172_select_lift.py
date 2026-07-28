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

GEN = "2026-07-28"
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
head_df = pd.DataFrame({
    "Product":        ph["Product"].values,
    "Advertisers":    ph["n_advertisers"].values,
    "Campaign groups": ph["n_cg"].values,
    "Treated VR":     ph["treated_vr"].values,
    "Holdout VR":     ph["holdout_vr"].values,
    "Abs lift":       ph["visit_lift_pp"].values,            # percentage points
    "Rel lift":       ph["rel_lift"].values,                 # relative (%)
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
    period="2026-06-22 to 2026-07-27 (7-day per-user window)",
    generated=GEN,
)

wb.table(
    "Headline", head_df,
    finding="Select prospecting drives ~5x the relative visit lift of non-Select (+22% vs +4%)",
    method="Pooled across 35 advertisers running both, inverse-variance-weighted; ghost-bid holdout, prospecting. "
           "See Read me for definitions.",
    formats={"Treated VR": FMT.PCT2, "Holdout VR": FMT.PCT2, "Abs lift": PP3,
             "Rel lift": FMT.PCT1, "CI low": PP3, "CI high": PP3,
             "Treated bids": FMT.INT, "Holdout bids": FMT.INT},
    signal={"Rel lift": {"sig": "Sig 95%"}, "Abs lift": {"sig": "Sig 95%"}},
    kind="headline",
    toc="The headline: pooled Select vs non-Select visit lift for advertisers running both.",
)

wb.table(
    "By advertiser (both)", both_df,
    finding="Select out-lifts non-Select in 27 of 35 advertisers running both (median edge +46pp of relative lift)",
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

wb.glossary(
    "Read me",
    intro="How the Select vs non-Select incrementality numbers were produced and how to read them.",
    rows=[
        ("Reading the numbers", ""),
        ("Reading % vs pp", "Throughout this workbook: '%' means a rate (Treated VR, Holdout VR) or a RELATIVE lift "
            "(Rel lift = percent over baseline); 'pp' means an ABSOLUTE percentage-point gap (Abs lift). They are different - "
            "e.g. Zazzle Select: Abs lift 2.76pp, Rel lift 95% (the 2.76pp gap is 95% of the ~2.9% holdout baseline)."),
        ("What this measures", "Incremental visit lift from MNTN's ghost-bid holdout: ~10% of prospecting IPs are "
            "held out (evaluated 'as if served' but not served). Treated visit rate minus holdout visit rate = "
            "the incremental effect of serving. No holdout = no causal read."),
        ("Select vs non-Select", "Product on the campaign group. Select = product_id 2; non-Select = PTV (product_id 1). "
            "All rows here are prospecting (objective_id 1) - the ghost-bid holdout only exists on the prospecting pool."),
        ("Abs lift (pp)", "Treated visit rate minus holdout visit rate, in percentage points. The absolute incremental effect. "
            "Small here because it is bid-grain (see below), so it is not the headline comparison."),
        ("Rel lift (%)", "Abs lift divided by the holdout visit rate = the percent increase over the no-ad baseline. "
            "A value of 95% means the treated visit rate is 95% higher than holdout (nearly double), NOT that 95% of people visited. "
            "This is the fair cross-product comparison because it normalizes for each product's baseline rate. "
            "Baselines are small (~1-3%), which is why relative lifts look large. Negative values (e.g. -14%) = treated visited "
            "less than holdout = no incremental effect, usually would-visit-anyway noise, not 'ads hurt'."),
        ("Blank Rel lift but Sig = Yes?", "If the holdout group recorded zero visits, the relative % is undefined (can't divide "
            "by a zero baseline) so it shows blank - but the absolute (pp) lift is still measurable and can clear significance. "
            "Treat these cautiously: a result built on a zero-visit holdout is fragile."),
        ("Bid-grain ITT", "The unit is a bid, not a served user. Treatment bids win only ~10% of auctions, so the "
            "absolute pp numbers are diluted. Relative lift is the comparable metric; do not read the pp as a served-user rate."),
        ("Method", ""),
        ("Pooling (IVW)", "Campaign groups are combined by inverse-variance weights (weight = 1/SE^2), not a raw "
            "visit count pool - a count pool produces Simpson's-paradox artifacts across heterogeneous campaigns."),
        ("Sig 95%", "The 95% confidence interval excludes zero. At bid-grain N, z is inflated, so treat significance "
            "as a floor and rank on relative magnitude."),
        ("Which rows are included", "Only campaign groups with a usable holdout: a real holdout group, enough of it to "
            "compute a visit rate, and not flagged low-coverage. Groups without a usable holdout are dropped - there is no "
            "baseline to compare against. Upstream the pipeline also anchors each IP to its entry cohort and drops the "
            "left-censored first window day."),
        ("Coverage & window", ""),
        ("Window", "Data covers 2026-06-22 to 2026-07-27. Each user's visit is counted over a 7-day window from its "
            "first bid, so the most recent ~7 days are not fully mature and fill in over time. No data exists before 6/22 (no backfill)."),
        ("Coverage caveat", "These views are the Beeswax bidder leg. The MNTN Rust-bidder leg is not folded in yet, "
            "which is why Select coverage is a subset of all live Select advertisers."),
        ("Cohort", f"93 AIDs requested. With usable-holdout lift data: {len(both_ids)} run both products, "
            f"{len(sel_only)} Select-only, {len(ns_only)} non-Select-only."),
    ],
)

SCOPING_SQL = """-- SCOPING QUERY (validation only - does NOT feed any number in this workbook).
-- Purpose: confirm every campaign group in scope is prospecting (objective_id = 1) and see the
-- Select vs non-Select split. Ghost-bid holdout only exists on the prospecting pool, so this
-- verifies no retargeting/other funnel rows slipped in.
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

QUERY_TAB = (
    "-- MAIN QUERY - drives every number in this workbook. Returns one row per advertiser x product;\n"
    "-- the pooled comparison, the 27/35 paired test, CIs, z-scores and rel/abs lift are all computed\n"
    "-- in Python from this single result set (see the .py in artifacts/).\n\n"
    + SQL.strip() + "\n\n\n" + SCOPING_SQL + "\n"
)

wb.sql("Query", QUERY_TAB, note="Every figure comes from the main query below; the scoping query is validation only. "
                                "Source: reporting.lift__ghost_bid_rollup x campaign_groups.product_id.")

wb.notes(
    "Method & caveats",
    intro="What to trust, what not to over-read.",
    blocks=[
        ("Headline", "Among 35 advertisers running both Select and non-Select prospecting, Select shows +22.0% relative "
            "visit lift vs non-Select's +4.3% - roughly 5x. Both are significant. The gap holds per-advertiser: Select "
            "beats non-Select in 27 of 35, median edge +46pp of relative lift, so the pooled result is not driven by one large advertiser."),
        ("Why relative, not absolute", "Numbers are ghost-bid ITT at bid grain. Treatment bids win ~10% of auctions, so the "
            "absolute pp lift is diluted by win rate roughly equally across products. Relative lift normalizes this and is the fair comparison."),
        ("Prospecting only", "Every row is objective_id 1. The ghost-bid holdout is a prospecting mechanism (held-out IPs never "
            "win, so they never leave the prospecting pool). Retargeting is out of scope by construction."),
        ("Date range", "2026-06-22 to 2026-07-27. First day dropped upstream as left-censored. 7-day per-user visit window means "
            "the trailing ~7 days are still maturing. No pre-6/22 data (ghost-bid lift pipeline has no backfill)."),
        ("Coverage", "Beeswax bidder leg only; MNTN Rust-bidder leg not yet folded in. 43 of 93 requested advertisers have Select "
            "lift data, 66 have non-Select; 35 have both once campaign groups without a usable holdout are excluded."),
        ("Do not over-read", "Individual low-volume campaigns have wide intervals; a single small Select campaign is not a verdict. "
            "The pooled and paired advertiser-level reads are the defensible outputs."),
    ],
)

wb.cover(takeaways=[
    "Select prospecting drives +22% relative visit lift vs +4% for non-Select - roughly 5x, both significant.",
    "The edge is consistent: Select beats non-Select in 27 of 35 advertisers running both (median +46pp of relative lift).",
    "Window is 2026-06-22 onward (no earlier data); numbers are ghost-bid ITT, compared relatively, prospecting only.",
])

local_path = wb.save_local(f"{TDIR}/artifacts/{TICKET} Select vs Non-Select Incrementality.xlsx")
drive_path = wb.save_drive(TICKET, "Select vs Non-Select Incrementality")
print("wrote xlsx (local):", local_path)
print("wrote xlsx (drive):", drive_path)

