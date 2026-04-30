"""TI-884 runner — apply MDE calculator to top-50 advertisers.

Inputs (all in ../outputs):
  ti_884_top50_spend_ranking.json        — total_spend, impressions, approx_distinct_ips
  ti_884_top50_per_advertiser_metrics.json — treated_ips, visiting_treated_ips, converting_treated_ips
                                              (Stage 1 only)

Outputs (written to ../outputs):
  ti_884_top50_mde_tiers.csv             — per-advertiser tiered MDE table
  ti_884_spend_threshold_curve.csv       — monthly budget -> MDE at median IVR
  ti_884_lauren_validation.csv           — cross-check vs Lauren's 7 completed tests

Methodology assumes 10% holdout (validated TI-837), Stage 1 (funnel_level=1) only.
Variance reduction set to None initially (raw MDE) plus stacked (CUPED-pending).
"""
import csv
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ti_884_mde_calculator import (
    mde_binomial, n_required_binomial, spend_required, tier_label
)

ROOT = Path(__file__).parent.parent
OUT = ROOT / "outputs"

ALPHA = 0.05
POWER = 0.80
HOLDOUT_FRAC = 0.10
# Post-stack SE multiplier: CUPED * ghost-ad * stratified.
# CUPED literature midpoint sqrt(1-0.5^2)=0.866 (will be replaced with measured).
# Ghost-ad ~25% SE reduction → 0.75. Stratified ~15% → 0.85.
# Stack: 0.866 * 0.75 * 0.85 = 0.552. Use 0.55 as the planned post-stack multiplier.
VAR_REDUCTION_STACK = 0.55  # PLACEHOLDER — replace with MNTN-measured CUPED ρ

# Lauren's 7 completed tests (from incremental_lift_tests_customer_tracker_summary.md).
# AIDs from tracker; lift_pct reported in tracker. April 2026 metrics pulled separately
# in ti_884_lauren7_metrics.json (some advertisers paused; not all 7 have current data).
LAUREN_TESTS = [
    {"name": "ReversePhone",  "lift_pct": 0.89, "advertiser_id": 42405},
    {"name": "Ownerly",       "lift_pct": 0.72, "advertiser_id": 44630},
    {"name": "Bumper",        "lift_pct": 0.60, "advertiser_id": 44631},
    {"name": "Grow Therapy",  "lift_pct": 0.57, "advertiser_id": 37963},
    {"name": "GLD",           "lift_pct": 0.67, "advertiser_id": 40586},
    {"name": "Nav.com",       "lift_pct": 0.74, "advertiser_id": 33804},
    {"name": "Boll & Branch", "lift_pct": 1.00, "advertiser_id": 31966},
]


def load_inputs():
    spend = {int(r["advertiser_id"]): r
             for r in json.loads((OUT / "ti_884_top50_spend_ranking.json").read_text())}
    metrics = {int(r["advertiser_id"]): r
               for r in json.loads((OUT / "ti_884_top50_per_advertiser_metrics.json").read_text())}

    # Lauren's 7 — separate pull (their AIDs not in top-50)
    lauren_path = OUT / "ti_884_lauren7_metrics.json"
    if lauren_path.exists():
        for r in json.loads(lauren_path.read_text()):
            aid = int(r["advertiser_id"])
            # spend table uses the same column names; merge in
            spend[aid] = {
                "advertiser_id": aid,
                "total_spend": r["total_spend"],
                "media_cost_only": r["media_cost_only"],
                "impressions": r["impressions"],
                "approx_distinct_ips": r["approx_distinct_ips"],
            }
            metrics[aid] = {
                "advertiser_id": aid,
                "treated_ips": r["treated_ips"],
                "visiting_treated_ips": r["visiting_treated_ips"],
                "converting_treated_ips": r["converting_treated_ips"],
            }
    return spend, metrics


def per_advertiser_table(spend, metrics):
    """Build per-advertiser rows with raw + post-stack MDE for visits and conversions."""
    rows = []
    for aid, m in metrics.items():
        treated = int(m["treated_ips"])
        visiting = int(m["visiting_treated_ips"])
        converting = int(m["converting_treated_ips"])
        if treated <= 0:
            continue

        # Holdout sample (biddable): 10/90 of treated, since hash buckets are uniform
        # over the same biddable population (validated TI-837 phase 0c).
        holdout = treated * (HOLDOUT_FRAC / (1 - HOLDOUT_FRAC))

        p_visit = visiting / treated
        p_cvr = converting / treated

        # Raw MDE (no variance reduction)
        if p_visit > 0:
            mde_v_abs, mde_v_rel = mde_binomial(treated, holdout, p_visit, alpha=ALPHA, power=POWER)
            mde_v_abs_stack, mde_v_rel_stack = mde_binomial(
                treated, holdout, p_visit, alpha=ALPHA, power=POWER,
                var_reduction=VAR_REDUCTION_STACK)
        else:
            mde_v_abs = mde_v_rel = mde_v_abs_stack = mde_v_rel_stack = float("inf")

        if p_cvr > 0:
            mde_c_abs, mde_c_rel = mde_binomial(treated, holdout, p_cvr, alpha=ALPHA, power=POWER)
            mde_c_abs_stack, mde_c_rel_stack = mde_binomial(
                treated, holdout, p_cvr, alpha=ALPHA, power=POWER,
                var_reduction=VAR_REDUCTION_STACK)
        else:
            mde_c_abs = mde_c_rel = mde_c_abs_stack = mde_c_rel_stack = float("inf")

        s = spend.get(aid, {})
        total_spend = float(s.get("total_spend", 0))
        total_imps = int(s.get("impressions", 0))
        cpm = (total_spend / total_imps * 1000.0) if total_imps else 0.0
        # Imps per IP across ALL campaigns (proxy for monthly frequency on Stage 1 IPs):
        approx_ips = int(s.get("approx_distinct_ips", 0))
        imps_per_ip = (total_imps / approx_ips) if approx_ips else 0

        rows.append({
            "advertiser_id": aid,
            "monthly_spend": total_spend,
            "monthly_impressions": total_imps,
            "approx_distinct_ips": approx_ips,
            "cpm_dollars": cpm,
            "imps_per_ip": imps_per_ip,
            "treated_ips_stage1": treated,
            "biddable_holdout_ips_stage1": int(holdout),
            "visiting_treated_ips": visiting,
            "converting_treated_ips": converting,
            "p_visit": p_visit,
            "p_cvr": p_cvr,
            "mde_visits_abs_pp": mde_v_abs * 100,
            "mde_visits_rel_pct": mde_v_rel * 100,
            "mde_visits_rel_pct_post_stack": mde_v_rel_stack * 100,
            "tier_visits_raw": tier_label(mde_v_rel),
            "tier_visits_post_stack": tier_label(mde_v_rel_stack),
            "mde_cvr_abs_pp": mde_c_abs * 100,
            "mde_cvr_rel_pct": mde_c_rel * 100,
            "mde_cvr_rel_pct_post_stack": mde_c_rel_stack * 100,
            "tier_cvr_raw": tier_label(mde_c_rel),
            "tier_cvr_post_stack": tier_label(mde_c_rel_stack),
        })
    rows.sort(key=lambda r: -r["monthly_spend"])
    return rows


def write_csv(rows, path):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow({
                k: (f"{v:.6f}" if isinstance(v, float) and math.isfinite(v)
                    else "inf" if isinstance(v, float) else v)
                for k, v in r.items()
            })
    print(f"  wrote {path} ({len(rows)} rows)")


def spend_threshold_curve(per_adv_rows):
    """Sweep monthly spend $50k -> $5M; for each, compute MDE at median IVR/CVR.

    Uses median CPM and imps/IP from the top-50 cohort. Outputs both raw and
    post-stack MDE columns. Identifies threshold where MDE crosses 15%, 10%, 5%.
    """
    p_visits = sorted(r["p_visit"] for r in per_adv_rows if r["p_visit"] > 0)
    p_cvrs = sorted(r["p_cvr"] for r in per_adv_rows if r["p_cvr"] > 0)
    cpms = sorted(r["cpm_dollars"] for r in per_adv_rows if r["cpm_dollars"] > 0)
    imps_per_ip = sorted(r["imps_per_ip"] for r in per_adv_rows if r["imps_per_ip"] > 0)

    median = lambda xs: xs[len(xs) // 2] if xs else 0.0
    p_visit_med = median(p_visits)
    p_cvr_med = median(p_cvrs)
    cpm_med = median(cpms)
    ipi_med = median(imps_per_ip)

    print(f"  cohort medians: p_visit={p_visit_med*100:.3f}% p_cvr={p_cvr_med*100:.4f}% "
          f"CPM=${cpm_med:.2f} imps/IP={ipi_med:.2f}")

    # spend grid: log-spaced 50k -> 5M
    spend_grid = [50_000, 75_000, 100_000, 150_000, 200_000, 300_000, 500_000,
                  750_000, 1_000_000, 1_500_000, 2_000_000, 3_000_000, 5_000_000]

    rows = []
    for monthly_spend in spend_grid:
        # Treated IPs implied by spend at median CPM and imps/IP
        impressions = monthly_spend / cpm_med * 1000
        treated = impressions / ipi_med
        holdout = treated * (HOLDOUT_FRAC / (1 - HOLDOUT_FRAC))

        # MDE at median rates, raw + post-stack, for visits and CVR
        _, mde_v_rel = mde_binomial(treated, holdout, p_visit_med, alpha=ALPHA, power=POWER)
        _, mde_v_rel_stack = mde_binomial(
            treated, holdout, p_visit_med, alpha=ALPHA, power=POWER,
            var_reduction=VAR_REDUCTION_STACK)
        _, mde_c_rel = mde_binomial(treated, holdout, p_cvr_med, alpha=ALPHA, power=POWER)
        _, mde_c_rel_stack = mde_binomial(
            treated, holdout, p_cvr_med, alpha=ALPHA, power=POWER,
            var_reduction=VAR_REDUCTION_STACK)

        rows.append({
            "monthly_spend": monthly_spend,
            "implied_impressions": int(impressions),
            "implied_treated_ips": int(treated),
            "implied_holdout_ips": int(holdout),
            "mde_visits_rel_pct": mde_v_rel * 100,
            "mde_visits_rel_pct_post_stack": mde_v_rel_stack * 100,
            "mde_cvr_rel_pct": mde_c_rel * 100,
            "mde_cvr_rel_pct_post_stack": mde_c_rel_stack * 100,
        })

    return rows, {"p_visit_med": p_visit_med, "p_cvr_med": p_cvr_med,
                  "cpm_med": cpm_med, "imps_per_ip_med": ipi_med}


def lauren_cross_validation(per_adv_rows, name_to_aid):
    """For each Lauren test, look up MDE at that advertiser's actual scale and compare
    against reported lift. If MDE > reported lift, the result is unreliable."""
    rows = []
    by_aid = {r["advertiser_id"]: r for r in per_adv_rows}
    for t in LAUREN_TESTS:
        aid = t["advertiser_id"] or name_to_aid.get(t["name"].lower())
        adv = by_aid.get(aid) if aid else None
        if not adv:
            rows.append({
                "test_name": t["name"], "advertiser_id": aid or "unknown",
                "reported_lift_pct": t["lift_pct"],
                "in_top50": False,
                "treated_ips_stage1": "",
                "p_visit": "",
                "mde_visits_rel_pct_raw": "",
                "mde_visits_rel_pct_post_stack": "",
                "lift_above_mde_raw": "",
                "lift_above_mde_post_stack": "",
            })
            continue

        rows.append({
            "test_name": t["name"],
            "advertiser_id": aid,
            "reported_lift_pct": t["lift_pct"],
            "in_top50": True,
            "treated_ips_stage1": adv["treated_ips_stage1"],
            "p_visit": adv["p_visit"],
            "mde_visits_rel_pct_raw": adv["mde_visits_rel_pct"],
            "mde_visits_rel_pct_post_stack": adv["mde_visits_rel_pct_post_stack"],
            "lift_above_mde_raw": t["lift_pct"] > adv["mde_visits_rel_pct"],
            "lift_above_mde_post_stack": t["lift_pct"] > adv["mde_visits_rel_pct_post_stack"],
        })
    return rows


def print_summary(per_adv_rows, curve_rows, curve_meta, lauren_rows):
    print("\n" + "=" * 110)
    print("PER-ADVERTISER MDE — TOP 25 (full table in CSV)")
    print(f"{'aid':>7} {'spend_$k':>9} {'p_visit':>8} {'mde_v%':>8} {'tier_v':>14} "
          f"{'p_cvr':>8} {'mde_c%':>8} {'tier_c':>14}")
    print("-" * 110)
    for r in per_adv_rows[:25]:
        print(f"{r['advertiser_id']:>7} {r['monthly_spend']/1000:>8,.0f}k "
              f"{r['p_visit']*100:>7.3f}% {r['mde_visits_rel_pct']:>7.2f}% "
              f"{r['tier_visits_raw']:>14} "
              f"{r['p_cvr']*100:>7.4f}% {r['mde_cvr_rel_pct']:>7.2f}% "
              f"{r['tier_cvr_raw']:>14}")

    print("\n" + "=" * 110)
    print("TIER COUNTS (visits, raw)")
    from collections import Counter
    c = Counter(r["tier_visits_raw"] for r in per_adv_rows)
    for tier in ("well_powered", "borderline", "underpowered", "no_data"):
        print(f"  {tier:>14}: {c.get(tier, 0):>3}")
    print("\nTIER COUNTS (visits, post-stack)")
    c = Counter(r["tier_visits_post_stack"] for r in per_adv_rows)
    for tier in ("well_powered", "borderline", "underpowered", "no_data"):
        print(f"  {tier:>14}: {c.get(tier, 0):>3}")
    print("\nTIER COUNTS (CVR, raw)")
    c = Counter(r["tier_cvr_raw"] for r in per_adv_rows)
    for tier in ("well_powered", "borderline", "underpowered", "no_data"):
        print(f"  {tier:>14}: {c.get(tier, 0):>3}")

    print("\n" + "=" * 110)
    print(f"SPEND-THRESHOLD CURVE (at cohort medians: p_v={curve_meta['p_visit_med']*100:.2f}%, "
          f"p_c={curve_meta['p_cvr_med']*100:.4f}%, CPM=${curve_meta['cpm_med']:.2f}, "
          f"imps/IP={curve_meta['imps_per_ip_med']:.2f})")
    print(f"{'spend':>12} {'treated_ips':>12} {'mde_v_raw%':>11} {'mde_v_stack%':>13} "
          f"{'mde_c_raw%':>11} {'mde_c_stack%':>13}")
    print("-" * 95)
    for r in curve_rows:
        print(f"${r['monthly_spend']:>10,} {r['implied_treated_ips']:>12,} "
              f"{r['mde_visits_rel_pct']:>10.1f}% {r['mde_visits_rel_pct_post_stack']:>12.1f}% "
              f"{r['mde_cvr_rel_pct']:>10.0f}% {r['mde_cvr_rel_pct_post_stack']:>12.0f}%")

    print("\n" + "=" * 110)
    print("LAUREN'S 7 COMPLETED TESTS — POWER vs REPORTED LIFT")
    print(f"{'test':<16} {'aid':>7} {'lift%':>7} {'mde_v_raw%':>11} {'mde_v_stack%':>13} "
          f"{'sig_raw':>9} {'sig_stack':>11}")
    print("-" * 90)
    for r in lauren_rows:
        if r["in_top50"]:
            print(f"{r['test_name']:<16} {r['advertiser_id']:>7} {r['reported_lift_pct']:>6.2f}% "
                  f"{r['mde_visits_rel_pct_raw']:>10.2f}% {r['mde_visits_rel_pct_post_stack']:>12.2f}% "
                  f"{'YES' if r['lift_above_mde_raw'] else 'no':>9} "
                  f"{'YES' if r['lift_above_mde_post_stack'] else 'no':>11}")
        else:
            print(f"{r['test_name']:<16} {'?':>7} {r['reported_lift_pct']:>6.2f}% "
                  f"{'(not in top-50; needs separate pull)':>50}")


def main():
    spend, metrics = load_inputs()

    per_adv = per_advertiser_table(spend, metrics)
    write_csv(per_adv, OUT / "ti_884_top50_mde_tiers.csv")

    # Spend curve: compute medians from top-50 only (not Lauren — different scale class)
    top50_aids = {int(r["advertiser_id"])
                  for r in json.loads((OUT / "ti_884_top50_spend_ranking.json").read_text())}
    curve, meta = spend_threshold_curve([r for r in per_adv if r["advertiser_id"] in top50_aids])
    write_csv(curve, OUT / "ti_884_spend_threshold_curve.csv")
    (OUT / "ti_884_curve_metadata.json").write_text(json.dumps(meta, indent=2))

    name_to_aid = {t["name"].lower(): t["advertiser_id"] for t in LAUREN_TESTS}
    lauren = lauren_cross_validation(per_adv, name_to_aid)
    write_csv(lauren, OUT / "ti_884_lauren_validation.csv")

    print_summary(per_adv, curve, meta, lauren)


if __name__ == "__main__":
    main()
