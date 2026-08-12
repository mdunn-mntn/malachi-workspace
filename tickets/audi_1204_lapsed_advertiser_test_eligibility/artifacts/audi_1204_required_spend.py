"""Required 8-week test budget for one advertiser, from their IVR and CVR.

Wraps TI-884's calculator; the math lives there, not here.

  python3 audi_1204_required_spend.py <advertiser_id>

Reads outputs/audi_1204_metrics_<id>.csv from audi_1204_run_metrics.py and writes
outputs/audi_1204_required_spend_<id>.csv.
"""
import argparse
import csv
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TICKET.parents[1] / "tickets" / "ber_2250_incrementality_overhaul"
                      / "ti_884_power_sample_size_analysis" / "artifacts"))
from ti_884_mde_calculator import mde_binomial, spend_required  # noqa: E402

# INCR-75 constants, verbatim (incr_75_score_and_filter.py:33-45)
ALPHA, POWER, HOLDOUT_FRAC, VAR_REDUCTION = 0.05, 0.80, 0.10, 1.0
IVR_TARGETS = {"5pct": 0.05, "10pct": 0.10}
CVR_TARGET = ("15pct", 0.15)
TEST_DAYS, MONTH_DAYS = 56, 30.4
TEST_MONTHS = TEST_DAYS / MONTH_DAYS
MIN_VISITING_IPS, MIN_CONVERTING_IPS = 100, 50
ASK_EASY, ASK_STRETCH = 0.25, 0.50


def ask_band(pct):
    if pct is None:
        return "n/a"
    if pct <= ASK_EASY:
        return "easy"
    if pct <= ASK_STRETCH:
        return "stretch"
    return "unreasonable"


def budget_for(p, target, cpm, imps_per_ip):
    s = spend_required(p, target, cpm=cpm, alpha=ALPHA, power=POWER,
                       holdout_frac=HOLDOUT_FRAC, var_reduction=VAR_REDUCTION,
                       impressions_per_ip=imps_per_ip)
    return s["spend_dollars"], s["n_total_ips"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("advertiser_id", type=int)
    a = ap.parse_args()

    src = TICKET / "outputs" / f"audi_1204_metrics_{a.advertiser_id}.csv"
    if not src.exists():
        sys.exit(f"missing {src} — run audi_1204_run_metrics.py {a.advertiser_id} first")
    r = list(csv.DictReader(open(src)))[0]

    ivr, cvr = float(r["p_visit"]), float(r["p_cvr"])
    cpm, imps_per_ip = float(r["cpm"]), float(r["imps_per_ip"])
    ips_56d = float(r["distinct_ips_56d"])
    typical = float(r["typical_active_month_spend"])
    visiting, converting = int(r["visiting_ips_30d"]), int(r["converting_ips_30d"])

    print(f"{r['advertiser_name']} ({r['advertiser_id']})  {r['vertical_buckets']}")
    print(f"window {r['window_start']}..{r['window_end']}   "
          f"IVR {ivr*100:.2f}%   CVR {cvr*100:.3f}%   CPM ${cpm:.2f}   imps/IP {imps_per_ip:.1f}")
    print(f"typical active month ${typical:,.0f} over {r['active_months_count']} months\n")

    if visiting < MIN_VISITING_IPS:
        print(f"[warn] {visiting} visiting IPs < {MIN_VISITING_IPS} — IVR too unstable to quote")

    # Direct 56d MDE: what their ACTUAL observed reach could already detect.
    # No imps/IP extrapolation, so this is the defensible cross-check.
    n_t = ips_56d * (1 - HOLDOUT_FRAC)
    n_c = ips_56d * HOLDOUT_FRAC
    _, mde_direct = mde_binomial(n_t, n_c, ivr, alpha=ALPHA, power=POWER,
                                 var_reduction=VAR_REDUCTION)
    print(f"[CAN-DETECT] at their own 56d reach ({ips_56d:,.0f} IPs): "
          f"relative IVR MDE {mde_direct*100:.2f}%\n")

    out = []
    for name, target in IVR_TARGETS.items():
        budget, n_ips = budget_for(ivr, target, cpm, imps_per_ip)
        monthly = budget / TEST_MONTHS
        extra = monthly - typical
        pct = extra / typical if typical > 0 else None
        band = ask_band(pct) if pct and pct > 0 else "already there"
        out.append(dict(metric="IVR", target=name, baseline_rate=ivr,
                        required_ips=round(n_ips), test_budget_8wk=round(budget, 2),
                        required_monthly=round(monthly, 2),
                        typical_monthly=round(typical, 2),
                        extra_monthly=round(extra, 2),
                        extra_pct=round(pct, 4) if pct is not None else "",
                        ask_band=band))
        if pct is None:
            verdict = "no spend history to compare"
        elif extra <= 0:
            verdict = f"already covered, {-pct*100:.0f}% headroom"
        else:
            verdict = f"needs {pct*100:+.0f}% vs typical — ask {band}"
        print(f"IVR {target*100:.0f}% relative MDE: 8-wk budget ${budget:,.0f} "
              f"(${monthly:,.0f}/mo, {n_ips:,.0f} IPs) — {verdict}")

    name, target = CVR_TARGET
    if converting >= MIN_CONVERTING_IPS and cvr > 0:
        budget, n_ips = budget_for(cvr, target, cpm, imps_per_ip)
        monthly = budget / TEST_MONTHS
        out.append(dict(metric="CVR", target=name, baseline_rate=cvr,
                        required_ips=round(n_ips), test_budget_8wk=round(budget, 2),
                        required_monthly=round(monthly, 2),
                        typical_monthly=round(typical, 2), extra_monthly="",
                        extra_pct="", ask_band="informational"))
        print(f"\nCVR {target*100:.0f}% relative MDE: 8-wk budget ${budget:,.0f} "
              f"(${monthly:,.0f}/mo) — INFORMATIONAL, never pass/fail")
    else:
        print(f"\nCVR: no_data ({converting} converting IPs < {MIN_CONVERTING_IPS})")

    dest = TICKET / "outputs" / f"audi_1204_required_spend_{a.advertiser_id}.csv"
    with open(dest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    print(f"\n[ok] wrote {dest}")

    powered5 = any(o["metric"] == "IVR" and o["target"] == "5pct"
                   and o["required_monthly"] <= typical * (1 + ASK_STRETCH) for o in out)
    print(f"\na-priori tier ceiling: {'Mid' if powered5 else 'Low'} "
          "(Top needs a confirmed ghost-bid lift, impossible while not delivering)")

    # The "$14,100 / IVR" shortcut is only valid at $30 CPM / 15 imps-per-IP.
    # Print the scaled form so nobody quotes the bare version at an advertiser
    # whose delivery shape differs — here it would be off by ~10x.
    shortcut = 14_100 / ivr * (cpm / 30) * (imps_per_ip / 15)
    print(f"shortcut check: $14,100/IVR x (CPM/30) x (impsPerIP/15) = ${shortcut:,.0f} "
          f"vs computed ${out[0]['test_budget_8wk']:,.0f}")


if __name__ == "__main__":
    main()
