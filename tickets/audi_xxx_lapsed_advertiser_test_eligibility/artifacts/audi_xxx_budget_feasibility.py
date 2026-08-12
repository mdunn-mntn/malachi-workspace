"""Invert the power calc: does a known budget support a test, and at what visit rate?

Answers "they used to spend $X/month, is that enough?" before the metrics pull
lands. Solves spend_required() for p:

  budget = k(1-p)/p  where  k = z^2/(mde^2 h(1-h)) * (1-h) * imps * cpm/1000
  => p = k / (budget + k)

  python3 audi_xxx_budget_feasibility.py <monthly_budget> [--cpm N] [--imps N]

Defaults to the median delivery shape of advertisers in the $25-60k/30d band
from the INCR-75 cohort. Pass the advertiser's real CPM and imps/IP once known.
"""
import argparse
import csv
import sys
from pathlib import Path

import numpy as np

TICKET = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TICKET.parents[1] / "tickets" / "ber_2250_incrementality_overhaul"
                      / "ti_884_power_sample_size_analysis" / "artifacts"))
from ti_884_mde_calculator import spend_required, z_factor  # noqa: E402

ALPHA, POWER, HOLDOUT_FRAC = 0.05, 0.80, 0.10
TEST_MONTHS = 56 / 30.4
COHORT = (TICKET.parents[0] / "incr_75_eligible_advertisers" / "outputs"
          / "incr_75_advertiser_metrics.csv")


def comparator(lo, hi):
    """Delivery-shape and IVR percentiles for advertisers in a 30d spend band."""
    cpm, imps, ivr = [], [], []
    for r in csv.DictReader(open(COHORT)):
        try:
            s, c, i, v = (float(r["spend_30d"]), float(r["cpm"]),
                          float(r["imps_per_ip"]), float(r["p_visit"]))
        except (TypeError, ValueError):
            continue
        if lo < s < hi and c > 0 and i > 0 and v > 0:
            cpm.append(c), imps.append(i), ivr.append(v)
    return np.array(cpm), np.array(imps), np.array(ivr)


def ivr_needed(budget, target, cpm, imps):
    z = z_factor(ALPHA, POWER)
    h = HOLDOUT_FRAC
    k = (z ** 2) / (target ** 2 * h * (1 - h)) * (1 - h) * imps * cpm / 1000.0
    return k / (budget + k)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("monthly_budget", type=float)
    ap.add_argument("--cpm", type=float, default=None)
    ap.add_argument("--imps", type=float, default=None)
    a = ap.parse_args()

    lo, hi = a.monthly_budget * 0.625, a.monthly_budget * 1.5
    c_cpm, c_imps, c_ivr = comparator(lo, hi)
    cpm = a.cpm if a.cpm else float(np.median(c_cpm))
    imps = a.imps if a.imps else float(np.median(c_imps))
    src = "supplied" if a.cpm else f"median of {len(c_cpm)} advertisers at ${lo:,.0f}-${hi:,.0f}/30d"

    total = a.monthly_budget * TEST_MONTHS
    print(f"${a.monthly_budget:,.0f}/mo = ${total:,.0f} over an 8-week test")
    print(f"delivery shape: CPM ${cpm:.2f}, {imps:.2f} imps/IP ({src})\n")

    for t in (0.05, 0.10):
        p = ivr_needed(total, t, cpm, imps)
        print(f"  supports a {t*100:.0f}% relative MDE if visit rate >= {p*100:.2f}%")

    if len(c_ivr):
        qs = np.percentile(c_ivr, [25, 50, 75])
        print(f"\ncomparator visit rates: p25 {qs[0]*100:.2f}% / "
              f"median {qs[1]*100:.2f}% / p75 {qs[2]*100:.2f}%\n")
        for lbl, p in zip(("p25", "median", "p75"), qs):
            s5 = spend_required(p, 0.05, cpm=cpm, impressions_per_ip=imps)["spend_dollars"]
            s10 = spend_required(p, 0.10, cpm=cpm, impressions_per_ip=imps)["spend_dollars"]
            print(f"  at {lbl} IVR {p*100:5.2f}%  ->  5% MDE ${s5/TEST_MONTHS:>9,.0f}/mo   "
                  f"10% MDE ${s10/TEST_MONTHS:>8,.0f}/mo")

        need5 = ivr_needed(total, 0.05, cpm, imps)
        verdict = ("clears comfortably" if need5 < qs[0] else
                   "a coin flip on their actual visit rate" if need5 < qs[2] else
                   "unlikely without a budget increase")
        print(f"\n5% MDE at this budget: {verdict} "
              f"(needs {need5*100:.2f}%, comparator median {qs[1]*100:.2f}%)")


if __name__ == "__main__":
    main()
