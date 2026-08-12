"""Build the AUDI lapsed-advertiser eligibility workbook.

  python3 audi_1204_build_xlsx.py [advertiser_id] [--ticket AUDI-1204] [--drive]

Without an advertiser_id it builds only what does not depend on one: the
IVR-to-budget curve, the VR/CR evidence, and the method sheet.
"""
import argparse
import csv
import sys
from datetime import date
from pathlib import Path

import pandas as pd

WS = Path("/Users/malachi/Developer/work/mntn/workspace")
TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"
sys.path.insert(0, str(WS))
sys.path.insert(0, str(WS / "tickets" / "ber_2250_incrementality_overhaul"
                      / "ti_884_power_sample_size_analysis" / "artifacts"))
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402
from ti_884_mde_calculator import mde_binomial, spend_required  # noqa: E402

ALPHA, POWER, HOLDOUT_FRAC, VAR_REDUCTION = 0.05, 0.80, 0.10, 1.0
TEST_MONTHS = 56 / 30.4
REF_CPM, REF_IMPS = 30.0, 15.0
CURVE_IVR = [0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.13]
SATURATED_IVR = 0.12          # INCR-75 IVR_SATURATED
COHORT_CPM, COHORT_IMPS = 27.54, 3.30   # median, $25-60k/30d band


def rows(path):
    return list(csv.DictReader(open(path))) if path.exists() else []


def curve_df():
    out = []
    for p in CURVE_IVR:
        r = {"Visit rate": p}
        for label, t in (("8-wk budget @ 5% MDE", 0.05), ("8-wk budget @ 10% MDE", 0.10)):
            r[label] = spend_required(p, t, cpm=REF_CPM, alpha=ALPHA, power=POWER,
                                      holdout_frac=HOLDOUT_FRAC, var_reduction=VAR_REDUCTION,
                                      impressions_per_ip=REF_IMPS)["spend_dollars"]
        r["Monthly @ 5% MDE"] = r["8-wk budget @ 5% MDE"] / TEST_MONTHS
        out.append(r)
    return pd.DataFrame(out)


def evidence_df():
    d = pd.DataFrame(rows(OUT / "audi_1204_vr_cr_spend_check.csv"))
    if d.empty:
        sys.exit("run audi_1204_vr_cr_spend_check.py first")
    return pd.DataFrame({
        "Visit-rate band": [f"{float(a)*100:.2f}-{float(b)*100:.2f}%"
                            for a, b in zip(d["ivr_low"], d["ivr_high"])],
        "Advertisers": d["n"].astype(int),
        "Spend p10": d["spend_p10"].astype(float),
        "Spend median": d["spend_p50"].astype(float),
        "Spend p90": d["spend_p90"].astype(float),
        "p90 / p10": d["p90_over_p10"].astype(float),
    })


def profile_df(m):
    ivr, cvr = float(m["p_visit"]), float(m["p_cvr"])
    served = float(m["distinct_ips_30d"])
    reach56 = float(m["distinct_ips_56d"])
    lapsed = (date.today() - date.fromisoformat(m["window_end"])).days
    _, mde_direct = mde_binomial(reach56 * (1 - HOLDOUT_FRAC), reach56 * HOLDOUT_FRAC, ivr,
                                 alpha=ALPHA, power=POWER, var_reduction=VAR_REDUCTION)
    return pd.DataFrame([
        {"Measure": "Vertical", "Value": m["vertical_buckets"], "Note": "not B2B"},
        {"Measure": "Last active", "Value": m["window_end"],
         "Note": f"lapsed {lapsed} days ({lapsed/30.4:.1f} months)"},
        {"Measure": "Measurement window", "Value": f"{m['window_start']} to {m['window_end']}",
         "Note": "their last 30 delivering days"},
        {"Measure": "Visit rate (IVR)", "Value": f"{ivr*100:.2f}%",
         "Note": f"{int(m['visiting_ips_30d']):,} visiting of {served:,.0f} served IPs"
                 + ("  |  above the 12% saturation band" if ivr > SATURATED_IVR else "")},
        {"Measure": "Conversion rate (CVR)", "Value": f"{cvr*100:.3f}%",
         "Note": f"{int(m['converting_ips_30d']):,} converting IPs"},
        {"Measure": "CPM", "Value": f"${float(m['cpm']):.2f}",
         "Note": f"cohort median ${COHORT_CPM:.2f} for this spend band"},
        {"Measure": "Impressions per IP", "Value": f"{float(m['imps_per_ip']):.2f}",
         "Note": f"cohort median {COHORT_IMPS:.2f}"},
        {"Measure": "56-day distinct-IP reach", "Value": f"{reach56:,.0f}",
         "Note": "the direct power cross-check denominator"},
        {"Measure": "Detectable at that reach", "Value": f"{mde_direct*100:.2f}%",
         "Note": "relative IVR MDE, no extrapolation - the defensible number"},
        {"Measure": "Final month spend", "Value": f"${float(m['spend_30d']):,.0f}",
         "Note": "their exit run-rate"},
        {"Measure": "Peak month spend", "Value": f"${float(m['max_month_spend']):,.0f}", "Note": ""},
        {"Measure": "Typical active month", "Value": f"${float(m['typical_active_month_spend']):,.0f}",
         "Note": f"median across {m['active_months_count']} active months; they ramped up before pausing"},
    ])


def budget_df(d, m):
    typical = float(m["typical_active_month_spend"])
    exit_rate = float(m["spend_30d"])
    out = []
    for r in d:
        monthly = float(r["required_monthly"])
        out.append({
            "Metric": r["metric"],
            "Target MDE": {"5pct": 0.05, "10pct": 0.10, "15pct": 0.15}[r["target"]],
            "Baseline rate": float(r["baseline_rate"]),
            "IPs needed": float(r["required_ips"]),
            "8-wk test budget": float(r["test_budget_8wk"]),
            "Required monthly": monthly,
            "vs typical month": monthly / typical if typical else None,
            "vs exit run-rate": monthly / exit_rate if exit_rate else None,
            "Verdict": ("informational only" if r["metric"] == "CVR"
                        else "clears" if monthly <= exit_rate else "needs a budget increase"),
        })
    return pd.DataFrame(out)


def funnel_df(aid):
    d = rows(OUT / f"audi_1204_funnel_split_{aid}.csv")
    if not d:
        return None
    return pd.DataFrame([{
        "Funnel stage": r["funnel"],
        "Served IPs": int(r["served_ips"]),
        "Impressions": int(r["impressions"]),
        "Spend": float(r["spend"]),
        "CPM": float(r["cpm"]),
        "Visiting IPs": int(r["visiting_ips"]),
        "Visit rate": float(r["ivr_pct"]) / 100,
    } for r in d])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("advertiser_id", nargs="?", type=int, default=None)
    ap.add_argument("--ticket", default="AUDI-1204")
    ap.add_argument("--drive", action="store_true")
    a = ap.parse_args()

    m = (rows(OUT / f"audi_1204_metrics_{a.advertiser_id}.csv") or [None])[0] if a.advertiser_id else None
    spend = rows(OUT / f"audi_1204_required_spend_{a.advertiser_id}.csv") if a.advertiser_id else []
    who = m["advertiser_name"] if m else "pending advertiser"

    wb = MntnWorkbook(
        title="Incrementality-Test Eligibility — Lapsed Advertiser",
        ticket=a.ticket,
        subtitle=f"What an 8-week ghost-bid visit-lift test would cost — {who}",
        period=(f"{m['window_start']} to {m['window_end']}" if m else "last-active window"),
        generated=date.today().isoformat(),
    )

    if m and spend:
        bdf = budget_df(spend, m)
        ivr5 = bdf[(bdf["Metric"] == "IVR") & (bdf["Target MDE"] == 0.05)].iloc[0]
        wb.table(
            "Required spend", bdf,
            finding=(f"{who} needs ${ivr5['Required monthly']:,.0f}/month to detect a 5% visit lift; "
                     f"they were running ${float(m['spend_30d']):,.0f}"),
            method=("TI-884 two-proportion binomial power. 8 weeks, 10% holdout, alpha .05, power .80, "
                    "no variance reduction. MDE is RELATIVE to the baseline rate, not percentage points: "
                    "a 5% MDE on a 12.93% visit rate means detecting a move to 13.58%."),
            formats={"Target MDE": FMT.PCT2, "Baseline rate": FMT.PCT2, "IPs needed": FMT.INT,
                     "8-wk test budget": FMT.USD, "Required monthly": FMT.USD,
                     "vs typical month": FMT.MULT, "vs exit run-rate": FMT.MULT},
            heat={"Required monthly": "low"},
            kind="headline",
            toc="What a test would cost",
        )
        wb.table(
            "Advertiser profile", profile_df(m),
            finding=("Their own last 8 weeks of delivery would have powered this test on its own"),
            method=("Measured over their last 30 delivering days. Rates are per-IP probabilities: "
                    "distinct visiting-and-served IPs over distinct served IPs, the grain the power "
                    "calculator requires. Cohort medians are the 176 advertisers at $25-60k/30d."),
            toc="Who they are and what they delivered",
        )
        fdf = funnel_df(a.advertiser_id)
        if fdf is not None:
            wb.table(
                "Funnel check", fdf,
                finding="Delivery was 99.9% prospecting, so the visit rate needs no adjustment",
                method=("A ghost-bid holdout is prospecting-only by construction, so an all-funnel "
                        "visit rate would overstate the testable baseline. This advertiser's campaigns "
                        "span objectives 1,4,5,6,7 including retargeting, but only prospecting "
                        "(1,5,6) delivered in the window. Prospecting = objective_id IN (1,5,6)."),
                formats={"Served IPs": FMT.INT, "Impressions": FMT.INT, "Spend": FMT.USD,
                         "CPM": FMT.USD, "Visiting IPs": FMT.INT, "Visit rate": FMT.PCT2},
                toc="Is this really a prospecting visit rate?",
            )

    wb.table(
        "Budget curve", curve_df(),
        finding="Required budget scales as 1 over visit rate: halve the visit rate, double the budget",
        method=(f"Reference delivery shape: ${REF_CPM:.0f} CPM, {REF_IMPS:.0f} impressions per IP. "
                "Scale by (their CPM/30) x (their imps-per-IP/15). These reference figures are NOT "
                "this advertiser's - see the profile tab for theirs."),
        formats={"Visit rate": FMT.PCT2, "8-wk budget @ 5% MDE": FMT.USD,
                 "8-wk budget @ 10% MDE": FMT.USD, "Monthly @ 5% MDE": FMT.USD},
        heat={"8-wk budget @ 5% MDE": "low"},
        toc="What any visit rate costs to test",
    )

    wb.table(
        "Why not estimate spend", evidence_df(),
        finding="Visit and conversion rate explain 10% of spend, so they cannot imply what an advertiser spends",
        method=("1,566 delivering advertisers from the INCR-75 screen with 30-day spend over $1,000. "
                "OLS on log(spend): log(IVR) R2 .04, log(CVR) R2 .10, both together R2 .10. Rates are "
                "scale-free, so two advertisers at the same visit rate can differ 15-66x in spend."),
        formats={"Spend p10": FMT.USD, "Spend median": FMT.USD, "Spend p90": FMT.USD,
                 "p90 / p10": FMT.MULT, "Advertisers": FMT.INT},
        heat={"p90 / p10": "low"},
        toc="Why spend cannot be predicted from rates",
    )

    blocks = [
        ("MDE is relative, not percentage points",
         "A 5% MDE on a 12.93% visit rate means detecting a move to 13.58%, a 5% proportional lift. "
         "It does not mean 17.93%. Every budget here is sized to that relative target."),
        ("A lapsed advertiser cannot reach Top tier",
         "INCR-75 tiers on power x confirmed ghost-bid lift. Confirmed lift needs a live holdout, and "
         "an advertiser who is not delivering has no bids. Best achievable is Mid, on power alone. "
         "They re-qualify for Top once they resume and accumulate holdout visits."),
        ("Conversions are out of reach; this is a visit test",
         "Their conversion baseline is 0.082%, about 160x below their visit rate, so a conversion-powered "
         "test would need roughly $325k/month. Conversion figures are reported for context and are "
         "never a pass/fail gate."),
        ("A high visit rate is not purely good news",
         "At 12.93% they sit just inside the saturation band INCR-75 penalizes above 12%. It makes them "
         "easy to measure, but the rule exists because a high baseline leaves less headroom to move. "
         "Expect a smaller proportional lift than a mid-range advertiser."),
        ("The budget figures are an optimistic floor; the reach figure is not",
         "Impressions-per-IP is measured over 30 days and grows with a longer window, so spend_required "
         "understates how far a real 8-week budget stretches. The direct 56-day MDE on the profile tab "
         "uses observed reach with no extrapolation and is the number to defend."),
        ("The baseline is as of April-May 2026",
         "Rates come from their last active window, 98 days before this was built. If the site, pricing, "
         "creative or offer changed since, the baseline moves and so does the budget."),
    ]
    wb.notes("Method & caveats", blocks=blocks, toc="How to read these numbers")

    qs = ["audi_1204_last_active.sql", "audi_1204_lapsed_advertiser_metrics.sql",
          "audi_1204_funnel_split_check.sql"]
    wb.sql("Queries", "\n\n".join((TICKET / "queries" / q).read_text() for q in qs),
           note="Window resolution, the metrics pull, and the prospecting-share check.")

    if m and spend:
        bdf = budget_df(spend, m)
        ivr5 = bdf[(bdf["Metric"] == "IVR") & (bdf["Target MDE"] == 0.05)].iloc[0]
        takeaways = [
            f"{who} needs ${ivr5['Required monthly']:,.0f}/month for a 5% visit-lift test, "
            f"against the ${float(m['spend_30d']):,.0f} they were running.",
            "Their own last 8 weeks of delivery would have powered it, with no budget increase at all.",
            "Ceiling is Mid tier while they stay paused, and it has to be a visit test, not conversions.",
        ]
    else:
        takeaways = [
            "Required test budget scales as 1 over visit rate; the calculator already existed in TI-884.",
            "Visit and conversion rate explain 10% of spend, so they cannot be used to estimate it.",
            "A lapsed advertiser tops out at Mid tier: confirmed lift needs a live holdout.",
        ]
    wb.cover(takeaways=takeaways)

    if a.drive:
        p = wb.save_drive(a.ticket, "Lapsed Advertiser Test Eligibility")
    else:
        p = wb.save_local(str(OUT / "audi_1204_lapsed_advertiser_eligibility.xlsx"))
    print(f"[ok] {p}")


if __name__ == "__main__":
    main()
