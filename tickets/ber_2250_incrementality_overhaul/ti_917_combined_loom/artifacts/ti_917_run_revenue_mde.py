"""TI-917 — Revenue / iROAS MDE per advertiser.

Joins TI-884's spend / N_treated / N_holdout cohort with TI-917's per-IP
revenue mean+sigma, then runs Lewis-Rao MDE for the continuous outcome
(revenue per IP) at α=0.05, power=0.8, with the variance-reduction stack.

Output: outputs/ti_917_revenue_mde_per_advertiser.csv

Columns:
  advertiser_id            advertiser ID
  monthly_spend            April 2026 Stage 1 spend ($)
  treated_ips              N treated (Stage 1 served)
  biddable_holdout_ips     N control (10% biddable holdout from TI-884)
  converting_ips           IPs with > $0 revenue
  mu_rev_per_ip            mean revenue per IP ($)
  sigma_rev_per_ip         stddev of revenue per IP ($)
  mde_rev_abs_raw          MDE in $/IP, raw (no var reduction)
  mde_rev_rel_raw          MDE as fraction of mu, raw
  mde_rev_abs_post_stack   MDE in $/IP, post-stack (CUPED+ghostad+strat)
  mde_rev_rel_post_stack   MDE as fraction of mu, post-stack
  mde_iroas_raw            min detectable iROAS @ raw   (incremental $/$ spent)
  mde_iroas_post_stack     min detectable iROAS @ post  (incremental $/$ spent)
  tier_iroas_raw           well_powered / borderline / underpowered (relative MDE)
  tier_iroas_post_stack    same, post-stack

Conventions match TI-884: post-stack SE multiplier = 0.595 (CUPED 0.934 ×
ghost-ad 0.75 × stratified 0.85 — all measured/canonical from TI-884
methodology). 10% holdout, 90% treated.
"""
import csv
import json
import os
import sys
from pathlib import Path

# Reuse TI-884's calculator without modification.
TI884_DIR = Path(__file__).resolve().parents[1].parent / "ti_884_power_sample_size_analysis" / "artifacts"
sys.path.insert(0, str(TI884_DIR))
from ti_884_mde_calculator import mde_continuous, tier_label  # noqa: E402

POST_STACK_VR = 0.595  # canonical TI-884 post-stack SE multiplier

THIS_DIR = Path(__file__).resolve().parent
TI917_ROOT = THIS_DIR.parent
TI884_OUT = TI884_DIR.parent / "outputs"

REVENUE_JSON = TI917_ROOT / "outputs" / "ti_917_revenue_sigma_per_advertiser.json"
COHORT_CSV = TI884_OUT / "ti_884_top50_mde_tiers.csv"
OUT_CSV = TI917_ROOT / "outputs" / "ti_917_revenue_mde_per_advertiser.csv"


def load_revenue() -> dict:
    with open(REVENUE_JSON) as f:
        rows = json.load(f)
    out = {}
    for r in rows:
        aid = int(r["advertiser_id"])
        out[aid] = {
            "treated_ips": int(r["treated_ips"]),
            "converting_ips": int(r["converting_ips"]),
            "total_revenue": float(r["total_revenue"]),
            "mu_rev_per_ip": float(r["mu_rev_per_ip"]),
            "sigma_rev_per_ip": float(r["sigma_rev_per_ip"]),
            "max_rev_per_ip": float(r["max_rev_per_ip"]),
        }
    return out


def load_cohort() -> dict:
    out = {}
    with open(COHORT_CSV) as f:
        for row in csv.DictReader(f):
            aid = int(row["advertiser_id"])
            out[aid] = {
                "monthly_spend": float(row["monthly_spend"]),
                "treated_ips_stage1": int(row["treated_ips_stage1"]),
                "biddable_holdout_ips_stage1": int(row["biddable_holdout_ips_stage1"]),
                "monthly_impressions": int(row["monthly_impressions"]),
                "cpm_dollars": float(row["cpm_dollars"]),
                "p_visit": float(row["p_visit"]),
                "p_cvr": float(row["p_cvr"]),
            }
    return out


def main():
    rev = load_revenue()
    cohort = load_cohort()

    rows = []
    aids = sorted(set(rev) & set(cohort), key=lambda a: -cohort[a]["monthly_spend"])
    for aid in aids:
        r = rev[aid]
        c = cohort[aid]
        n_t = c["treated_ips_stage1"]
        n_c = c["biddable_holdout_ips_stage1"]
        mu = r["mu_rev_per_ip"]
        sigma = r["sigma_rev_per_ip"]

        if mu <= 0 or sigma <= 0 or n_t <= 0 or n_c <= 0:
            mde_abs_raw = mde_rel_raw = float("inf")
            mde_abs_post = mde_rel_post = float("inf")
            mde_iroas_raw = mde_iroas_post = float("inf")
            tier_raw = tier_post = "no_data"
        else:
            mde_abs_raw, mde_rel_raw = mde_continuous(n_t, n_c, mu, sigma)
            mde_abs_post, mde_rel_post = mde_continuous(n_t, n_c, mu, sigma, var_reduction=POST_STACK_VR)
            # iROAS: min detectable incremental dollars per dollar of spend.
            # Δrev_total_treated = mde_abs * n_t  (extra revenue across treated arm)
            # Δspend_treated     = monthly_spend (treated takes 100% of spend; holdouts unserved)
            # min iROAS = Δrev_total / Δspend
            spend = c["monthly_spend"]
            mde_iroas_raw = (mde_abs_raw * n_t) / spend if spend > 0 else float("inf")
            mde_iroas_post = (mde_abs_post * n_t) / spend if spend > 0 else float("inf")
            tier_raw = tier_label(mde_rel_raw)
            tier_post = tier_label(mde_rel_post)

        rows.append({
            "advertiser_id": aid,
            "monthly_spend": round(c["monthly_spend"], 2),
            "treated_ips": n_t,
            "biddable_holdout_ips": n_c,
            "converting_ips": r["converting_ips"],
            "mu_rev_per_ip": round(mu, 4),
            "sigma_rev_per_ip": round(sigma, 4),
            "mde_rev_abs_raw": round(mde_abs_raw, 6) if mde_abs_raw != float("inf") else "",
            "mde_rev_rel_raw": round(mde_rel_raw, 6) if mde_rel_raw != float("inf") else "",
            "mde_rev_abs_post_stack": round(mde_abs_post, 6) if mde_abs_post != float("inf") else "",
            "mde_rev_rel_post_stack": round(mde_rel_post, 6) if mde_rel_post != float("inf") else "",
            "mde_iroas_raw": round(mde_iroas_raw, 6) if mde_iroas_raw != float("inf") else "",
            "mde_iroas_post_stack": round(mde_iroas_post, 6) if mde_iroas_post != float("inf") else "",
            "tier_iroas_raw": tier_raw,
            "tier_iroas_post_stack": tier_post,
        })

    fields = list(rows[0].keys()) if rows else []
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    n_well = sum(1 for r in rows if r["tier_iroas_post_stack"] == "well_powered")
    n_border = sum(1 for r in rows if r["tier_iroas_post_stack"] == "borderline")
    n_under = sum(1 for r in rows if r["tier_iroas_post_stack"] == "underpowered")
    n_nodata = sum(1 for r in rows if r["tier_iroas_post_stack"] == "no_data")
    print(f"[OK] wrote {OUT_CSV.name} ({len(rows)} advertisers)")
    print(f"[INFO] post-stack iROAS tiers: well={n_well} borderline={n_border} under={n_under} no_data={n_nodata}")


if __name__ == "__main__":
    main()
