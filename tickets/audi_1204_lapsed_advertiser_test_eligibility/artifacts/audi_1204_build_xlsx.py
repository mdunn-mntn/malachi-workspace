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
from ti_884_mde_calculator import mde_binomial  # noqa: E402

ALPHA, POWER, HOLDOUT_FRAC, VAR_REDUCTION = 0.05, 0.80, 0.10, 1.0
TEST_MONTHS = 56 / 30.4
SATURATED_IVR = 0.12          # INCR-75 IVR_SATURATED
COHORT_CPM, COHORT_IMPS = 27.54, 3.30   # median, $25-60k/30d band


def rows(path):
    return list(csv.DictReader(open(path))) if path.exists() else []




def profile_df(m, spend_rows):
    ivr, cvr = float(m["p_visit"]), float(m["p_cvr"])
    served = float(m["distinct_ips_30d"])
    reach56 = float(m["distinct_ips_56d"])
    lapsed = (date.today() - date.fromisoformat(m["window_end"])).days
    ips_5pct = float(next((r["required_ips"] for r in spend_rows
                           if r["metric"] == "IVR" and r["target"] == "5pct"), 0) or 0)
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
        {"Measure": "IPs needed for a 5% test", "Value": f"{ips_5pct:,.0f}",
         "Note": "total across both arms at a 10% holdout"},
    ])


def budget_df(d, m):
    """Five columns: what we would detect, the two budget views, and one spend anchor.

    Anchor is their exit run-rate (the last 30 delivering days), not the multi-month
    median — it is what they were actually running when they paused. The typical-month
    figure lives on the profile tab so only one denominator appears here.
    """
    exit_rate = float(m["spend_30d"])
    label = {("IVR", "5pct"): "5% visit lift", ("IVR", "10pct"): "10% visit lift",
             ("CVR", "15pct"): "15% conversion lift"}
    out = []
    for r in d:
        monthly = float(r["required_monthly"])
        out.append({
            "What we'd detect": label.get((r["metric"], r["target"]),
                                          f"{r['target']} {r['metric']}"),
            "8-wk budget": float(r["test_budget_8wk"]),
            "Monthly needed": monthly,
            f"Share of their ${exit_rate:,.0f}": monthly / exit_rate if exit_rate else None,
            "Verdict": ("out of reach" if r["metric"] == "CVR"
                        else "clears" if monthly <= exit_rate else "needs an increase"),
        })
    return pd.DataFrame(out)



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
        ivr5 = bdf[bdf["What we'd detect"] == "5% visit lift"].iloc[0]
        share_col = [c for c in bdf.columns if c.startswith("Share of")][0]
        wb.table(
            "Required spend", bdf,
            finding=(f"{who} needs ${ivr5['Monthly needed']:,.0f}/month; "
                     f"they were running ${float(m['spend_30d']):,.0f}"),
            method=("Two-proportion binomial power (TI-884). 8-week test, 10% holdout, alpha .05, "
                    "power .80, no variance reduction. See Read me for definitions."),
            formats={"8-wk budget": FMT.USD, "Monthly needed": FMT.USD, share_col: FMT.PCT0},
            heat={"Monthly needed": "low"},
            kind="headline",
            toc="What a test would cost",
        )
        wb.table(
            "Advertiser profile", profile_df(m, spend),
            finding="Their last 8 weeks of delivery would have powered this test",
            method=("Their last 30 delivering days. Rates are per-IP probabilities: distinct visiting "
                    "over distinct served IPs. Cohort medians = 176 advertisers at $25-60k/30d."),
            toc="Who they are and what they delivered",
        )



    wb.glossary(
        "Read me",
        intro=f"{a.ticket}.  How the required-test-budget numbers were produced and how to read them.",
        rows=[
            ("How to read this", ""),
            ("Relative MDE", "The smallest lift a test could detect, as a % OF the baseline rate. A 5% MDE "
                             "on a 12.93% visit rate means detecting a move to 13.58%, not to 17.93%."),
            ("Visit rate (IVR)", "Distinct IPs that visited the site over distinct IPs served an ad, in the "
                                 "measurement window. A per-IP probability, the grain the power calculator needs."),
            ("Required monthly", "The 8-week test budget spread over 1.84 months. Compare it to what they "
                                 "actually ran, not to a target."),
            ("Method & sources", ""),
            ("Power calculation", "Two-proportion binomial (TI-884 mde_calculator). 10% holdout is fixed "
                                  "platform-wide, not a per-test knob, so power comes from campaign size only."),
            ("Direct 56-day MDE", "What their own observed 8-week reach could already detect, with no "
                                  "impressions-per-IP extrapolation. The defensible number; the budget figures "
                                  "are an optimistic floor."),
            ("Spend", "media + data + platform, from cost_impression_log. Monthly history from "
                      "sum_by_advertiser_by_day (advertiser x day, 2024-01-01 onward)."),
            ("Coverage & window", ""),
            ("Measurement window", "Their last 30 delivering days, resolved automatically from the last day "
                                   "with impressions. They paused after it, so nothing newer exists."),
            ("Why a lapsed advertiser", "The INCR-75 eligible list only covers advertisers that delivered in "
                                        "the trailing 30 days. Spend was scored there, never cut."),
        ],
        toc="Definitions and how the numbers were produced",
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
        ("The visit rate is prospecting-only, as a ghost-bid test requires",
         "Their campaigns span objectives 1,4,5,6,7 including retargeting, which would have inflated "
         "the baseline. In the measured window 285,905 of 285,910 served IPs were prospecting "
         "(objectives 1,5,6) and retargeting delivered nothing, so 12.93% needs no adjustment."),
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
        ivr5 = bdf[bdf["What we'd detect"] == "5% visit lift"].iloc[0]
        takeaways = [
            f"{who} needs ${ivr5['Monthly needed']:,.0f}/month for a 5% visit-lift test, "
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
