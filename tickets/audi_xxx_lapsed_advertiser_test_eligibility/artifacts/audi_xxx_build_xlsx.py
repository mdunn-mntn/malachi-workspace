"""Build the AUDI lapsed-advertiser eligibility workbook.

  python3 audi_xxx_build_xlsx.py [advertiser_id] [--ticket AUDI-XXXX] [--drive]

Without an advertiser_id it builds everything that does not depend on one: the
IVR-to-budget curve, the VR/CR evidence, and the method sheet. Pass the id once
Al names the advertiser and their row lands on the headline tab.
"""
import argparse
import csv
import sys
from datetime import date
from pathlib import Path

import pandas as pd

WS = Path("/Users/malachi/Developer/work/mntn/workspace")
TICKET = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WS))
sys.path.insert(0, str(WS / "tickets" / "ber_2250_incrementality_overhaul"
                      / "ti_884_power_sample_size_analysis" / "artifacts"))
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402
from ti_884_mde_calculator import spend_required  # noqa: E402

ALPHA, POWER, HOLDOUT_FRAC, VAR_REDUCTION = 0.05, 0.80, 0.10, 1.0
TEST_MONTHS = 56 / 30.4
REF_CPM, REF_IMPS = 30.0, 15.0
CURVE_IVR = [0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10]


def curve_df():
    rows = []
    for p in CURVE_IVR:
        r = {"Visit rate": p}
        for label, target in (("8-wk budget @ 5% MDE", 0.05), ("8-wk budget @ 10% MDE", 0.10)):
            r[label] = spend_required(p, target, cpm=REF_CPM, alpha=ALPHA, power=POWER,
                                      holdout_frac=HOLDOUT_FRAC, var_reduction=VAR_REDUCTION,
                                      impressions_per_ip=REF_IMPS)["spend_dollars"]
        r["Monthly @ 5% MDE"] = r["8-wk budget @ 5% MDE"] / TEST_MONTHS
        rows.append(r)
    return pd.DataFrame(rows)


def evidence_df():
    src = TICKET / "outputs" / "audi_xxx_vr_cr_spend_check.csv"
    if not src.exists():
        sys.exit(f"missing {src} — run audi_xxx_vr_cr_spend_check.py first")
    d = pd.DataFrame(list(csv.DictReader(open(src))))
    out = pd.DataFrame({
        "Visit-rate band": [f"{float(a)*100:.2f}-{float(b)*100:.2f}%"
                            for a, b in zip(d["ivr_low"], d["ivr_high"])],
        "Advertisers": d["n"].astype(int),
        "Spend p10": d["spend_p10"].astype(float),
        "Spend median": d["spend_p50"].astype(float),
        "Spend p90": d["spend_p90"].astype(float),
        "p90 / p10": d["p90_over_p10"].astype(float),
    })
    return out.sort_values("Visit-rate band")


def advertiser_df(aid):
    src = TICKET / "outputs" / f"audi_xxx_required_spend_{aid}.csv"
    met = TICKET / "outputs" / f"audi_xxx_metrics_{aid}.csv"
    if not (src.exists() and met.exists()):
        return None, None
    m = list(csv.DictReader(open(met)))[0]
    d = pd.DataFrame(list(csv.DictReader(open(src))))
    out = pd.DataFrame({
        "Metric": d["metric"],
        "Target MDE": d["target"].map({"5pct": 0.05, "10pct": 0.10, "15pct": 0.15}),
        "Baseline rate": d["baseline_rate"].astype(float),
        "IPs needed": d["required_ips"].astype(float),
        "8-wk test budget": d["test_budget_8wk"].astype(float),
        "Implied monthly": d["required_monthly"].astype(float),
        "Their typical month": d["typical_monthly"].astype(float),
        "Ask": d["ask_band"],
    })
    return out, m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("advertiser_id", nargs="?", type=int, default=None)
    ap.add_argument("--ticket", default="AUDI-XXXX")
    ap.add_argument("--drive", action="store_true")
    a = ap.parse_args()

    adv_df, meta = (advertiser_df(a.advertiser_id) if a.advertiser_id else (None, None))
    who = meta["advertiser_name"] if meta else "pending advertiser"

    wb = MntnWorkbook(
        title="Lapsed-Advertiser Incrementality-Test Eligibility",
        ticket=a.ticket,
        subtitle=f"Required 8-week test budget from visit and conversion rate — {who}",
        period="Last-active window",
        generated=date.today().isoformat(),
    )

    if adv_df is not None:
        wb.table(
            "Required spend", adv_df,
            finding=(f"{meta['advertiser_name']} needs "
                     f"${adv_df.iloc[0]['Implied monthly']:,.0f}/mo to detect a 5% visit lift; "
                     f"they typically ran ${adv_df.iloc[0]['Their typical month']:,.0f}"),
            method=("TI-884 two-proportion binomial power. 8-week test, 10% holdout, alpha .05, "
                    "power .80, no variance reduction. Rates measured over their last-active "
                    "30-day window. MDE is RELATIVE to the baseline rate, not percentage points."),
            formats={"Target MDE": FMT.PCT2, "Baseline rate": FMT.PCT2,
                     "IPs needed": FMT.INT, "8-wk test budget": FMT.USD,
                     "Implied monthly": FMT.USD, "Their typical month": FMT.USD},
            heat={"8-wk test budget": "low"},
            kind="headline",
            toc="What a test would cost this advertiser",
        )

    wb.table(
        "Budget curve", curve_df(),
        finding="Required budget scales as 1/visit-rate — halve the visit rate, double the budget",
        method=(f"Reference delivery shape: ${REF_CPM:.0f} CPM, {REF_IMPS:.0f} impressions per IP. "
                "Scale by (their CPM/30) x (their imps-per-IP/15) — an advertiser at $12 CPM and "
                "3.6 imps/IP needs roughly a tenth of these figures."),
        formats={"Visit rate": FMT.PCT2, "8-wk budget @ 5% MDE": FMT.USD,
                 "8-wk budget @ 10% MDE": FMT.USD, "Monthly @ 5% MDE": FMT.USD},
        heat={"8-wk budget @ 5% MDE": "low"},
        toc="What any visit rate costs to test",
    )

    wb.table(
        "Why not estimate spend", evidence_df(),
        finding="Visit and conversion rate explain 10% of spend — they cannot imply what an advertiser spends",
        method=("1,566 delivering advertisers from the INCR-75 screen with 30-day spend over $1,000. "
                "OLS on log(spend): log(IVR) R2 .04, log(CVR) R2 .10, both together R2 .10. "
                "Rates are scale-free, so two advertisers at the same visit rate can differ 15-66x in spend."),
        formats={"Spend p10": FMT.USD, "Spend median": FMT.USD, "Spend p90": FMT.USD,
                 "p90 / p10": FMT.MULT, "Advertisers": FMT.INT},
        heat={"p90 / p10": "low"},
        toc="Why spend cannot be predicted from rates",
    )

    wb.notes("Method & caveats", blocks=[
        ("MDE is relative, not percentage points",
         "A 5% MDE on a 2% visit rate means detecting a move to 2.1%, a 5% proportional lift. "
         "It does not mean 7%. Every budget here is sized to that relative target."),
        ("Conversion rate is informational, never pass/fail",
         "Conversion baselines run about 30x below visit rates, so a conversion-powered test costs "
         "7-10x more. Report it at a looser 15% target and gate eligibility on visits only."),
        ("These budgets are an optimistic floor",
         "Impressions-per-IP is measured over 30 days and grows with a longer window, so a real "
         "8-week test reaches more unique IPs per dollar. Cross-check against the direct 56-day "
         "distinct-IP MDE, which uses observed reach with no extrapolation."),
        ("A lapsed advertiser cannot reach Top tier",
         "INCR-75 tiers on power x confirmed ghost-bid lift. Confirmed lift needs a live holdout, "
         "and an advertiser who is not delivering has no bids. Best achievable is Mid, on the "
         "a-priori power gate alone."),
        ("The baseline is as-of their last active window",
         "Visit and conversion rates come from when they were still spending. If the site, pricing, "
         "creative or offer changed since, the baseline is stale and the budget moves with it."),
    ], toc="How to read these numbers")

    wb.sql("Queries", "\n\n".join(
        (TICKET / "queries" / q).read_text()
        for q in ("audi_xxx_last_active.sql", "audi_xxx_lapsed_advertiser_metrics.sql")),
        note="Window resolution then the metrics pull. Placeholders are filled by audi_xxx_run_metrics.py.")

    takeaways = [
        "Required test budget scales as 1 over visit rate; the calculator already existed in TI-884.",
        "Visit and conversion rate explain 10% of spend, so they cannot be used to estimate it.",
        "A lapsed advertiser tops out at Mid tier: confirmed lift needs a live holdout.",
    ]
    if adv_df is not None:
        takeaways.insert(0, (f"{meta['advertiser_name']} needs "
                             f"${adv_df.iloc[0]['Implied monthly']:,.0f}/mo for a 5% visit-lift test."))
    wb.cover(takeaways=takeaways)

    if a.drive:
        p = wb.save_drive(a.ticket, "Lapsed Advertiser Test Eligibility")
    else:
        p = wb.save_local(str(TICKET / "outputs" / "audi_xxx_lapsed_advertiser_eligibility.xlsx"))
    print(f"[ok] {p}")


if __name__ == "__main__":
    main()
