"""INCR-75 — build the shareable workbook on the shared MNTN format (2026-08-19 rerun).

Replaces the hand-rolled incr_75_build_xlsx.py (openpyxl fills by hand) with lib/mntn_xlsx.
Reads the refreshed outputs/ CSVs; writes the workbook to the Drive ticket folder.
"""
import csv
import math
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers/artifacts")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

from incr_75_lift_stats import pool_rr, simple  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers"
OUT = f"{T}/outputs"
GENERATED = "2026-08-19"
WINDOW = "2026-06-23 to 2026-07-07"

gi = lambda r, k: int(r[k])  # noqa: E731


def load(fn):
    with open(f"{OUT}/{fn}") as f:
        return list(csv.DictReader(f))


def num(r, k, default=None):
    v = r.get(k, "")
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


elig = load("incr_75_eligible_with_current_lift.csv")
allf = load("incr_75_all_flagged.csv")
funnel = load("incr_75_funnel_counts.csv")
ghost = load("incr_75_ghost_clean_window.csv")
bands = load("incr_75_band_lift.csv")
metrics = {r["advertiser_id"]: r for r in load("incr_75_advertiser_metrics.csv")}

# ---------------------------------------------------------------- aggregate lift
vis = pool_rr([(gi(r, "v_t"), gi(r, "n_t"), gi(r, "v_h"), gi(r, "n_h")) for r in ghost])
conv = pool_rr([(gi(r, "c_t"), gi(r, "n_t"), gi(r, "c_h"), gi(r, "n_h")) for r in ghost])
tot = simple(sum(gi(r, "v_t") for r in ghost), sum(gi(r, "n_t") for r in ghost),
             sum(gi(r, "v_h") for r in ghost), sum(gi(r, "n_h") for r in ghost))
N_T = sum(gi(r, "n_t") for r in ghost)
N_H = sum(gi(r, "n_h") for r in ghost)

BAND_ORDER = ["Unscored", "Peak Performance", "High Intent", "Mid Intent", "Max Reach"]
BAND_MEAN = {
    "High Intent": "In the advertiser's vertical and matching its keywords",
    "Peak Performance": "In the vertical, matching no keyword",
    "Mid Intent": "In the parent industry, not the vertical",
    "Max Reach": "Matches a keyword, outside the industry",
    "Unscored": "Not scored by the matching layer",
}
rows_band = []
for b in BAND_ORDER:
    sub = [r for r in bands if r["band"] == b]
    if not sub:
        continue
    p = pool_rr([(gi(r, "v_t"), gi(r, "n_t"), gi(r, "v_h"), gi(r, "n_h")) for r in sub])
    nt = sum(gi(r, "n_t") for r in sub)
    nh = sum(gi(r, "n_h") for r in sub)
    vh = sum(gi(r, "v_h") for r in sub)
    rows_band.append({
        "Audience band": b,
        "What it means": BAND_MEAN[b],
        "Visit lift": p["rel"],
        "Low": p["lo"], "High": p["hi"],
        "Real?": "Yes" if p["p"] < 0.05 else "No",
        "Base rate": vh / nh,
        "Advertisers": p["k"],
        "Held-out IPs": nh,
        "Bid-on IPs": nt,
    })
df_band = pd.DataFrame(rows_band).sort_values("Visit lift", ascending=False)

df_agg = pd.DataFrame([
    {"Outcome": "Site visits", "Lift": vis["rel"], "Range low": vis["lo"], "Range high": vis["hi"],
     "Real?": "Yes" if vis["p"] < 0.05 else "No", "Advertisers": vis["k"],
     "Held-out IPs": N_H, "Bid-on IPs": N_T},
    {"Outcome": "Conversions", "Lift": conv["rel"], "Range low": conv["lo"], "Range high": conv["hi"],
     "Real?": "Yes" if conv["p"] < 0.05 else "No", "Advertisers": conv["k"],
     "Held-out IPs": N_H, "Bid-on IPs": N_T},
])

# ---------------------------------------------------------------- candidate sheets
TIER_ORDER = {"Top": 0, "Mid": 1, "Low": 2}


def cand_rows(rows):
    out = []
    for r in rows:
        m = metrics.get(r["advertiser_id"], {})
        rel = num(r, "current_rel_lift")
        crel = num(r, "conv_rel_lift")
        out.append({
            "Advertiser": r["advertiser_name"],
            "Industry": (m.get("vertical_buckets") or "").split(" | ")[0] or None,
            "Test readiness": r["final_tier"],
            "Candidate score": num(r, "value_score"),
            "Monthly spend": num(r, "avg_monthly_spend"),
            "Visit rate": num(r, "ivr"),
            "Detectable lift": (num(r, "mde_ivr_at_normal_pct") or 0) / 100.0,
            "Test budget": num(r, "budget_for_mde_ivr_5pct"),
            "Powered at 5%": r.get("can_hit_ivr_5pct_8w"),
            "Measured lift": (rel / 100.0 if rel is not None else None),
            "Lift is real": "Yes" if r["current_lift_confirms"] == "confirmed +" else "No",
            "Lift status": r["current_lift_confirms"],
            "Conversion lift": (crel / 100.0 if crel is not None else None),
            "Held-out visits": (int(r["ghost_vis_clean"]) if r["ghost_vis_clean"] else None),
        })
    return out


srt = sorted(elig, key=lambda r: (TIER_ORDER[r["final_tier"]], -(num(r, "value_score") or 0)))
df_top = pd.DataFrame(cand_rows([r for r in srt if r["final_tier"] == "Top"]))
df_all_elig = pd.DataFrame(cand_rows(srt))

# ---------------------------------------------------------------- funnel
STEP_LABEL = {
    "Starting universe (delivered, trailing 30d)": "Advertisers that delivered in the last 30 days",
    "Clean & active": "Active, named, and serving",
    "Not B2B": "Not a B2B software or services advertiser",
    "Measurable IVR": "Visit rate is measurable (100+ visiting IPs)",
    "Measured lift not negative": "No measured negative lift",
    "FINAL ELIGIBLE (must-pass)": "Eligible for a lift test",
}
df_funnel = pd.DataFrame([
    {"Screening step": STEP_LABEL.get(r["filter"], r["filter"]),
     "Rule": None if r["threshold"] == "-" else r["threshold"],
     "Removed": int(r["removed"]), "Remaining": int(r["remaining"]),
     "Share of start": int(r["remaining"]) / int(funnel[0]["remaining"])}
    for r in funnel
])

# ---------------------------------------------------------------- audit
df_audit = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "Monthly spend": num(r, "avg_monthly_spend"),
    "Visit rate": num(r, "ivr"),
    "Visiting IPs": int(num(r, "visiting_ips_30d") or 0),
    "Screened out at": None if r["failed_at_filter"] == "PASSED" else r["failed_at_filter"],
    "Test readiness": r["final_tier"] or None,
} for r in sorted(allf, key=lambda r: -(num(r, "avg_monthly_spend") or 0))])

# ---------------------------------------------------------------- spend -> MDE curve
med = lambda xs: sorted(xs)[len(xs) // 2]  # noqa: E731
ep = [r for r in elig]
m_cpm = med([num(r, "cpm") for r in ep if num(r, "cpm")])
m_ipi = med([num(r, "imps_per_ip") for r in ep if num(r, "imps_per_ip")])
m_ivr = med([num(r, "ivr") for r in ep if num(r, "ivr")])
sys.path.insert(0, f"{T}/../ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts")
from ti_884_mde_calculator import mde_binomial  # noqa: E402

curve = []
for spend in (10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000):
    treated = (spend / m_cpm * 1000.0) / m_ipi
    holdout = treated * (0.10 / 0.90)
    _, rel = mde_binomial(treated, holdout, m_ivr, alpha=0.05, power=0.80, var_reduction=1.0)
    curve.append({"Budget": spend, "Detectable lift": rel,
                  "Reachable IPs": int(treated + holdout)})
df_curve = pd.DataFrame(curve)

# ---------------------------------------------------------------- build
PCT = FMT.PCT1
wb = MntnWorkbook(
    title="Incrementality Lift Test Candidates",
    ticket="INCR-75",
    subtitle="Which advertisers can run a credible ghost-bid lift test, and what the platform is lifting today",
    period=f"Advertiser metrics trailing 30d to {GENERATED}; measured lift {WINDOW}",
    generated=GENERATED,
)

wb.table(
    "Best candidates", df_top,
    finding=f"{len(df_top)} advertisers can both power a 5% test in 8 weeks and already show a real lift",
    method=f"Advertisers delivering in the trailing 30 days, screened and ranked. Measured lift is the ghost-bid holdout, {WINDOW}. See Read me.",
    formats={"Candidate score": FMT.NUM1, "Monthly spend": FMT.USD, "Visit rate": FMT.PCT2,
             "Detectable lift": PCT, "Test budget": FMT.USD,
             "Measured lift": PCT, "Conversion lift": PCT, "Held-out visits": FMT.INT},
    signal={"Measured lift": {"sig": "Lift is real"}},
    heat={"Candidate score": "high", "Detectable lift": "low"},
    kind="headline",
    toc="Start here: the shortlist for the beta",
    query="incr_75_advertiser_metrics.sql",
)

wb.table(
    "Lift in aggregate", df_agg,
    finding=f"Visits lift {100 * vis['rel']:.1f}% and conversions {100 * conv['rel']:.1f}% across {vis['k']:,} advertisers",
    method=f"Held-out IPs were selected for bidding, then not bid on. {WINDOW}. Range low and high bound the 95% confidence interval. See Read me.",
    formats={"Lift": PCT, "Range low": PCT, "Range high": PCT, "Advertisers": FMT.INT,
             "Held-out IPs": FMT.INT, "Bid-on IPs": FMT.INT},
    signal={"Lift": {"sig": "Real?"}},
    kind="headline",
    toc="Start here: the two headline numbers and their ranges",
    query="incr_75_entry_cohort_clean.sql",
)

wb.table(
    "Screening funnel", df_funnel,
    finding=f"{int(funnel[-1]['remaining']):,} of {int(funnel[0]['remaining']):,} delivering advertisers are eligible for a lift test",
    method="Each step is a hard rule; an advertiser removed at one step is not retested later. See Read me for each rule.",
    formats={"Removed": FMT.INT, "Remaining": FMT.INT, "Share of start": FMT.PCT0},
    kind="data",
    toc="How the base narrows to the eligible list",
)

wb.table(
    "All eligible", df_all_elig,
    finding=f"All {len(df_all_elig):,} eligible advertisers, ranked within tier by candidate score",
    method="Test readiness crosses power with confirmed lift: Top needs both, Mid one, Low neither. Candidate score ranks within a readiness group.",
    formats={"Candidate score": FMT.NUM1, "Monthly spend": FMT.USD, "Visit rate": FMT.PCT2,
             "Detectable lift": PCT, "Test budget": FMT.USD,
             "Measured lift": PCT, "Conversion lift": PCT, "Held-out visits": FMT.INT},
    signal={"Measured lift": {"sig": "Lift is real"}},
    kind="data",
    toc="The full eligible list",
)

wb.table(
    "Budget and sensitivity", df_curve,
    finding=f"At the eligible-cohort median a 5% test needs roughly $250K over 8 weeks",
    method=f"Median cohort inputs: {m_ivr:.2%} visit rate, ${m_cpm:.2f} CPM, {m_ipi:.1f} impressions per IP, 10% holdout, 80% power.",
    formats={"Budget": FMT.USD, "Detectable lift": PCT, "Reachable IPs": FMT.INT},
    kind="data",
    toc="What a test costs at each level of sensitivity",
)

wb.table(
    "Every advertiser", df_audit,
    finding=f"Audit trail: all {len(df_audit):,} delivering advertisers and where each one left the funnel",
    method="Every advertiser that served an impression in the trailing 30 days, ranked by monthly spend.",
    formats={"Monthly spend": FMT.USD, "Visit rate": FMT.PCT2, "Visiting IPs": FMT.INT,
             "Advertiser ID": "0"},
    kind="detail",
    toc="Audit trail for all delivering advertisers",
)

wb.glossary(
    "Read me",
    intro="Two different questions are answered here. What a test COULD detect is a forecast from spend. What the platform IS lifting is a measurement from a live holdout. They are different instruments and their numbers do not compare.",
    rows=[
        ("The measurement", ""),
        ("Ghost bid", "The platform picks the households it would bid on, then withholds the bid for a fixed 10% of them. Both groups were chosen the same way, so the difference between them is the ad's effect."),
        ("Held-out IPs", "The withheld 10%. They are the control group."),
        ("Visit lift", "How much more often a bid-on IP visits the site than a held-out one, as a percentage of the held-out rate. +5% on a 1% base means 1.05%, not 6%."),
        ("Real?", "Whether the range excludes zero at 95% confidence. 'No' means the measurement cannot tell the lift apart from nothing."),
        ("Range low / Range high", "The 95% confidence interval. The true lift is somewhere between them."),
        ("", ""),
        ("The forecast", ""),
        ("Visit rate", "Of the IPs an advertiser served in the last 30 days, the share that visited the site."),
        ("Detectable lift", "The smallest real lift a test could prove at this advertiser's normal 8-week spend. Lower is better. Relative, so 3% on a 1% visit rate means detecting 1.03%."),
        ("Test budget", "Total spend needed over 8 weeks to detect a 5% lift. Compare it to monthly spend times 1.8."),
        ("Powered at 5%", "Whether normal 8-week spend already covers that budget."),
        ("", ""),
        ("Test readiness", ""),
        ("Top", "Can power a 5% test at normal spend AND already shows a real positive lift."),
        ("Mid", "One of the two, not both."),
        ("Low", "Neither. Still eligible, but a test would need more spend or a longer window."),
        ("Candidate score", "A 0-100 quality score from power, spend, brand size, visit rate and past test results. It ranks advertisers WITHIN a readiness group, not across them. Measured lift is not part of it."),

    ],
)

wb.notes(
    "Method & caveats",
    intro="Read the first two blocks before quoting any lift number.",
    blocks=[
        ("The measured window is 15 days and cannot be extended",
         f"Lift is measured on entries from {WINDOW}, though the source table now holds 06-22 to 08-18. Each IP is anchored at its first bid. A held-out IP never wins, so it never leaves the pool and is anchored almost at once; bid-on IPs churn and new ones keep arriving. Later days sample almost no held-out IPs."),
        ("What that looks like, and why a longer window reads higher",
         "The observed held-out share falls from 10.5% on 06-23 to 8.4% on 08-11 against a fixed 10% platform holdout, and measured lift climbs from +3% to +25% over the same days. Pooling the full window gives +18.6%. That number is an artifact of the shrinking control group, not a better estimate."),
        ("Lift is pooled on the log risk ratio, not by counting",
         "Base visit rates run from under 0.1% to over 10%. Adding all visits and dividing lets the few largest advertisers decide the answer. Each advertiser is measured separately and combined by precision. The count pool reads +6.3% against the +4.7% reported here."),
        ("Which audiences produce the lift is NOT answered here",
         "A split by intent band was built and then pulled. The band rule reproduced the platform's own split on only 3.7% of campaigns, so any number built on it would be wrong. Separately, the recorded band ordering does not survive re-estimation. Both are open with Matt Brorby."),
        ("Only the Beeswax bidder is included",
         "The second bidder entered the source table the week of 2026-07-05 with a 6.6% to 8.3% held-out share from its first day, and reads +128% to +290%. It is excluded as unreliable rather than averaged in."),
        ("Measured lift and detectable lift are different instruments",
         "Measured lift is what the holdout showed over 15 days on bid-on IPs. Smallest detectable lift is what a future 8-week test could prove on served IPs at a given budget. Judge a measured lift against zero using its own range, never against the detectable-lift column."),
        ("Conversion lift is thinner than visit lift",
         f"Conversions are about 25 times rarer than visits, so the conversion range is wide: {100 * conv['rel']:.1f}% with a range of {100 * conv['lo']:.1f}% to {100 * conv['hi']:.1f}%. Treat it as directional support for the visit number, not as an independent result."),
        ("A confirmed lift does not make an advertiser testable",
         "Power is the binding constraint for the Top tier. An advertiser can show a real lift today and still be unable to prove a 5% effect in 8 weeks at its spend. That is why measured lift gates the tier and is displayed, but is not folded into the score."),
        ("The 10% holdout is fixed platform-wide",
         "It is not a per-test setting. Power comes only from campaign size or from pooling advertisers, never from a larger holdout."),
        ("Spend now includes data and platform fees",
         "Monthly spend and the test budgets are advertiser-facing totals. The June version of this workbook counted media spend only for the monthly figure, which understated it and made the extra spend a test needs look larger than it is."),
    ],
)

wb.sql_dir(
    "Queries", f"{T}/queries",
    order=["incr_75_advertiser_metrics.sql", "incr_75_entry_cohort_clean.sql",
           "incr_75_entry_cohort_byday_window.sql"],
    ignore=["incr_75_band_lift_clean.sql", "incr_75_entry_cohort_excl_leftedge.sql", "incr_75_entry_cohort_per_advertiser.sql",
            "incr_75_entry_cohort_pooled_byday.sql", "incr_75_matt_entry_cohort_perday_51660.sql",
            "incr_75_entry_cohort_window.sql", "incr_75_gold_clean_ivw.sql"],
    note="The four queries behind this workbook. Superseded 2026-06 variants are kept in the ticket, not here.",
)

wb.cover(takeaways=[
    f"{len(df_top)} of {len(df_all_elig):,} eligible advertisers are ready now: they can power a 5% test in 8 weeks and already show a real lift.",
    f"Across {vis['k']:,} advertisers the platform lifts site visits {100 * vis['rel']:.1f}% and conversions {100 * conv['rel']:.1f}%.",
    "At the eligible-cohort median a test needs about $250K over 8 weeks to prove a 5% lift.",
])

# The ticket's Drive folder is Tickets/INCR/INCR-75 (the INCR project groups its tickets),
# not the default Tickets/INCR-75.
path = wb.save_drive(
    "INCR-75", "Incrementality Lift Test Candidates",
    drive_root="/Users/malachi/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/Tickets/INCR",
)
print("wrote", path)
wb.save_local(f"{OUT}/incr_75_lift_test_candidates.xlsx")
