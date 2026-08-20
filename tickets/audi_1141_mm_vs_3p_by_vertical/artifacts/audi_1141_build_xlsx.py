#!/usr/bin/env python3
"""AUDI-1141 shareable .xlsx, built on the shared MntnWorkbook format.

Rebuilt 2026-08-20: moved off the hand-rolled openpyxl builder onto lib/mntn_xlsx, added CPA to the
two by-vertical tabs, and added the non-revenue / B2B CPA cohort tab that the pitch-deck ask needs.
Run artifacts/audi_1141_aggregate.py first — this reads its CSVs.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

TICKET = "AUDI-1141"
OUT = ROOT / "tickets/audi_1141_mm_vs_3p_by_vertical/outputs"
SQLFILE = ROOT / "tickets/audi_1141_mm_vs_3p_by_vertical/queries/audi_1141_cohort_scorecard.sql"
DEST = OUT / "audi_1141_mm_vs_3p_scorecard.xlsx"
BO = ["MM (gated)", "MM (no gate)", "MM restricted", "3P"]
ADV_MIN_IMPS = 20000

ov = pd.read_csv(OUT / "audi_1141_scorecard_overall.csv")
bv = pd.read_csv(OUT / "audi_1141_scorecard_by_vertical.csv")
ov2 = pd.read_csv(OUT / "audi_1141_scorecard2_overall.csv")
bv2 = pd.read_csv(OUT / "audi_1141_scorecard2_by_vertical.csv")
nr = pd.read_csv(OUT / "audi_1141_scorecard_nonrevenue.csv")

det = pd.read_csv(OUT / "audi_1141_campaign_grain.csv").dropna(subset=["vertical_id"])
names = pd.read_csv(OUT / "audi_1141_advertiser_names.csv").drop_duplicates("advertiser_id")
det = det.merge(names, on="advertiser_id", how="left")
det = det[det.bucket != "Neither"].copy()
gated_frac = np.where(det.hhst_writes > 0, det.hhst_writes_gated / det.hhst_writes, 0.0)
det["bucket_detail"] = np.where(
    det.bucket == "MM", np.where(gated_frac >= 0.5, "MM (gated)", "MM (no gate)"), det.bucket
)

wb = MntnWorkbook(
    title="MNTN Matched vs 3P Segment Scorecard",
    ticket=TICKET,
    subtitle="Stage 1 prospecting performance by sales vertical, trailing 6 months",
    period="2026-01-21 to 2026-07-20",
    generated="2026-08-20",
)


# --------------------------------------------------------------------------- headline comparisons
def compare(df_by_vert, df_overall, gcol, a, b, name, finding, method, toc, kind):
    """One MM-group-vs-3P table: visit rate, cost per visit, cost per action, ROAS, by vertical."""
    bvi = df_by_vert.set_index(["sales_vertical", gcol])
    ovi = df_overall.set_index(gcol)

    def row(label, get):
        d = {"Sales vertical": label}
        for tag, col, fmtcols in (("IVR", "IVR_med", None), ("CPV", "CPV_med", None),
                                  ("CPA", "CPA_med", None), ("ROAS", "ROAS_med", None)):
            d[f"MM {tag}"], d[f"3P {tag}"] = get(col, a), get(col, b)
        # advantage: higher-is-better for a rate, lower-is-better for a cost (so invert)
        for tag, invert in (("IVR", False), ("CPA", True), ("ROAS", False)):
            hi, lo = d[f"MM {tag}"], d[f"3P {tag}"]
            num, den = (lo, hi) if invert else (hi, lo)
            d[f"{tag} advantage"] = num / den if den else np.nan
        d["MM advertisers"] = int(get("n_adv", a) or 0)
        d["3P advertisers"] = int(get("n_adv", b) or 0)
        return d

    def cell(frame, key):
        def g(col, grp):
            try:
                return frame.loc[(key, grp) if isinstance(key, str) else grp, col]
            except KeyError:
                return np.nan
        return g

    verts = sorted(df_by_vert.sales_vertical.dropna().unique())
    rows = [row("All verticals", lambda c, g: ovi.loc[g, c] if g in ovi.index else np.nan)]
    rows += [row(v, cell(bvi, v)) for v in verts]
    cols = ["Sales vertical", "MM IVR", "3P IVR", "IVR advantage", "MM CPV", "3P CPV",
            "MM CPA", "3P CPA", "CPA advantage", "MM ROAS", "3P ROAS", "ROAS advantage",
            "MM advertisers", "3P advertisers"]
    out = pd.DataFrame(rows)[cols]
    return wb.table(
        name, out, finding=finding, method=method, kind=kind, toc=toc,
        formats={"MM IVR": FMT.PCT2, "3P IVR": FMT.PCT2, "IVR advantage": FMT.MULT,
                 "MM CPV": FMT.USD, "3P CPV": FMT.USD, "MM CPA": FMT.USD, "3P CPA": FMT.USD,
                 "CPA advantage": FMT.MULT, "MM ROAS": FMT.ROAS, "3P ROAS": FMT.ROAS,
                 "ROAS advantage": FMT.MULT, "MM advertisers": FMT.INT, "3P advertisers": FMT.INT},
        heat={"IVR advantage": "high", "CPA advantage": "high"},
        first_col_width=27, query="audi_1141_cohort_scorecard.sql",
    )


compare(bv2, ov2, "bucket2", "MM (all)", "3P", "MM vs 3P by vertical",
        finding="MNTN Matched beats 3P on visit rate and cost per visit in all 8 verticals",
        method="Every MM campaign vs 3P: the realistic average. Median advertiser, 20k-impression "
               "floor. Higher visit rate and lower cost are better. See Read me for definitions.",
        toc="The headline: every MM campaign vs 3P, by vertical", kind="headline")

compare(bv, ov, "bucket_detail", "MM (gated)", "3P", "MM gated vs 3P by vertical",
        finding="Gated MM beats 3P 6.6x on visit rate, the best-configured subset not the average",
        method="MM with the intent gate on, the best case, vs 3P. Pair it with the blended tab; "
               "quoting only this one overstates the typical account. See Read me.",
        toc="Best case: MM with the intent gate on vs 3P", kind="data")

# ------------------------------------------------------------------ the pitch-deck CPA cohort cut
nrt = nr.rename(columns={"cohort": "Advertiser cohort", "group": "Targeting group",
                         "n_adv_qual": "Advertisers", "n_adv_cpa": "With conversions",
                         "share_with_conv": "Share with conversions", "CPA_med": "CPA (median)",
                         "CPA_pooled": "CPA (pooled)", "spend": "Spend"})
nrt = nrt[["Advertiser cohort", "Targeting group", "Advertisers", "With conversions",
           "Share with conversions", "CPA (median)", "CPA (pooled)", "Spend"]]
wb.table(
    "CPA on non-revenue accounts", nrt,
    finding="The CPA gap closes without revenue: 2.1x across all accounts, 1.0x for B2B software",
    method="Advertisers with no tracked revenue, where ROAS cannot be computed. A CPA median uses "
           "only advertisers with at least one conversion, so both counts are shown. See Read me.",
    formats={"Advertisers": FMT.INT, "With conversions": FMT.INT,
             "Share with conversions": FMT.PCT0, "CPA (median)": FMT.USD,
             "CPA (pooled)": FMT.USD, "Spend": FMT.USD0},
    kind="data", toc="Does the CPA advantage survive on non-revenue and B2B accounts",
    first_col_width=24, query="audi_1141_cohort_scorecard.sql",
)

# ------------------------------------------------------------------------------- detail + overall
fcols = {"sales_vertical": "Sales vertical", "bucket_detail": "Targeting group",
         "n_adv": "Advertisers", "n_camp": "Campaigns", "spend": "Spend", "imps": "Impressions",
         "IVR_med": "IVR (median)", "CVR_med": "CVR (median)", "CTR_med": "CTR (median)",
         "CPV_med": "CPV (median)", "CPA_med": "CPA (median)", "ROAS_med": "ROAS (median)",
         "IVR_pooled": "IVR (pooled)", "CPV_pooled": "CPV (pooled)", "CPM_pooled": "CPM (pooled)",
         "CPA_pooled": "CPA (pooled)", "ROAS_pooled": "ROAS (pooled)"}
FFMT = {"Advertisers": FMT.INT, "Campaigns": FMT.INT, "Spend": FMT.USD0, "Impressions": FMT.INT,
        "IVR (median)": FMT.PCT2, "CVR (median)": FMT.PCT3, "CTR (median)": FMT.PCT3,
        "CPV (median)": FMT.USD, "CPA (median)": FMT.USD, "ROAS (median)": FMT.ROAS,
        "IVR (pooled)": FMT.PCT2, "CPV (pooled)": FMT.USD, "CPM (pooled)": FMT.USD,
        "CPA (pooled)": FMT.USD, "ROAS (pooled)": FMT.ROAS}

wb.table("Full scorecard", bv[list(fcols)].rename(columns=fcols),
         finding="Every targeting group by vertical, median and pooled side by side",
         method="Median is advertiser-weighted and whale-robust; pooled is impression or spend "
                "weighted. Where the two disagree, one account is carrying the pooled number.",
         formats=FFMT, kind="detail", toc="All four targeting groups, every metric, by vertical",
         first_col_width=27, query="audi_1141_cohort_scorecard.sql")

ocols = {k: v for k, v in fcols.items() if k != "sales_vertical"}
wb.table("Overall", ov[list(ocols)].rename(columns=ocols),
         finding="The intent gate is the single biggest lever inside MNTN Matched",
         method="All verticals pooled into one row per targeting group. Median advertiser, "
                "20k-impression floor.",
         formats=FFMT, kind="detail", toc="One row per targeting group, all verticals",
         first_col_width=20, query="audi_1141_cohort_scorecard.sql")

dcols = {"company_name": "Advertiser", "advertiser_id": "Advertiser ID", "campaign_id": "Campaign ID",
         "sales_vertical": "Sales vertical", "vertical_name": "MNTN vertical",
         "bucket_detail": "Targeting group", "semantics": "3P join", "hhst_current": "Intent gate",
         "imps": "Impressions", "visits": "Visits", "clicks": "Clicks", "conv": "Conversions",
         "revenue": "Revenue", "spend": "Spend"}
dt = det[[c for c in dcols if c in det.columns]].rename(columns=dcols).sort_values(
    "Spend", ascending=False)
wb.table("Campaign detail", dt,
         finding="Every classified campaign, pivotable back to the advertiser that produced it",
         method="One row per Stage 1 prospecting campaign in the cohort, ranked by spend. Intent "
                "gate is the latest household score threshold; 0 means no gate.",
         formats={"Impressions": FMT.INT, "Visits": FMT.INT, "Clicks": FMT.INT,
                  "Conversions": FMT.INT, "Revenue": FMT.USD0, "Spend": FMT.USD0,
                  "Intent gate": FMT.INT, "Advertiser ID": FMT.INT, "Campaign ID": FMT.INT},
         band=False, kind="detail", toc="Campaign-grain backing data, ranked by spend",
         first_col_width=34, query="audi_1141_cohort_scorecard.sql")

# ---------------------------------------------------------------------------------------- read me
wb.glossary(
    "Read me",
    intro="What each column means and which number to quote. Rates are all over impressions.",
    rows=[
        ("The comparison", ""),
        ("MNTN Matched (MM)", "A campaign targeted by MNTN's own intent model. A 3P segment joined "
                              "with OR adds reach on top and the campaign still counts as MM."),
        ("3P", "A campaign targeted by a bought third-party segment with no MNTN Matched signal."),
        ("MM restricted", "MM narrowed by a 3P segment joined with AND, or by sub-DMA geo. The "
                          "narrowing, not the model, is what changes the result."),
        ("Intent gate", "The household score threshold. Above 0 the campaign only bids on "
                        "model-scored high-intent households; at 0 it bids broadly, like a 3P buy."),
        ("The metrics", ""),
        ("IVR", "Visit rate. Visits divided by impressions, where a visit is a view or a click."),
        ("CPV", "Cost per visit. Spend divided by visits. Lower is better."),
        ("CPA", "Cost per acquisition. Spend divided by conversions. Depends on the advertiser's "
                "pixel, so it is directional, like ROAS. Lower is better."),
        ("ROAS", "Revenue divided by spend, prospecting and last-touch only. Directional."),
        ("Advantage", "How many times better MM is than 3P on that metric. For a cost metric it is "
                      "3P divided by MM, so above 1.0x always means MM is cheaper."),
        ("Which number to quote", ""),
        ("Median", "The middle advertiser, each advertiser counting once, 20,000-impression floor. "
                   "This is the headline: it cannot be moved by one large account."),
        ("Pooled", "Every impression or dollar added up first. A cross-check only. On this data it "
                   "flips the 3P visit rate, because 39% of 3P impressions are one account."),
        ("With conversions", "How many advertisers had at least one conversion. Only those can have "
                             "a CPA, so a low share means that CPA rests on a small, self-selected set."),
    ],
)

wb.sql("Queries", SQLFILE.read_text(),
       note="BigQuery cohort SQL. Classification is a polarity-aware walk of the targeting "
            "expression tree, so an OR-joined 3P segment is read as additive, not as narrowing.")

# -------------------------------------------------------------------------------- method + caveats
wb.notes(
    "Method & caveats",
    intro="Read these before quoting a number outside the team.",
    blocks=[
        ("Cohort", "Stage 1 prospecting campaigns (objective 1, funnel level 1) that delivered "
                   "impressions in the trailing 180 days to 2026-07-20. 8,202 campaigns after "
                   "dropping CRM-only and geo-only setups, 7,138 in the scored MM and 3P groups."),
        ("Pick one lens and hold it", "Two MM lenses are published on purpose: every MM campaign "
                                      "(the realistic average) and gated MM (the best-configured "
                                      "subset). Choosing whichever reads better slide to slide is "
                                      "not a defensible comparison. Pick one and label it."),
        ("ROAS is directional", "Prospecting and last-touch only. Revenue concentrates in "
                                "retargeting, which is excluded, and some pixels are unreliable "
                                "(one account reads over 800x). Use the median, never the mean. "
                                "Visit rate and cost per visit are the solid metrics."),
        ("CPA does not carry the B2B claim", "Across all accounts MM's CPA is 2.1x better than 3P. "
                                             "On accounts with no tracked revenue it is 1.16x, and "
                                             "on B2B software 1.06x, on 22 3P advertisers. Do not "
                                             "build a B2B CPA claim on this data."),
        ("Conversion coverage differs by group", "Only 54% of qualifying 3P advertisers recorded any "
                                                 "conversion, against 78% of MM. Advertisers with no "
                                                 "pixel drop out of CPA entirely, which flatters 3P, "
                                                 "so the MM CPA win is a conservative one."),
        ("B2B is not a sales vertical here", "B2B Software & Services is an MNTN vertical folded "
                                             "into the 8 sales buckets by an interim crosswalk that "
                                             "still needs RevOps sign-off. Treat any B2B split as "
                                             "provisional."),
        ("Data window", "Built 2026-08-20 from a trailing-180-day window ending 2026-07-20. Re-run "
                        "artifacts/audi_1141_aggregate.py then this builder to refresh."),
    ],
)

wb.cover(takeaways=[
    "MNTN Matched beats 3P on visit rate and cost per visit in all 8 verticals, on the median advertiser",
    "The intent gate is the biggest lever: gated MM 0.46% visit rate vs 0.13% with the gate off",
    "The CPA advantage is 2.1x overall but only 1.0x on B2B software, so CPA cannot carry a B2B slide",
])

print("saved local:", wb.save_local(str(DEST)))
print("saved drive:", wb.save_drive(TICKET, "MM vs 3P Scorecard"))
