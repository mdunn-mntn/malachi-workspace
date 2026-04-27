"""TI-837: Compute ATT, IVW meta-analysis, and per-cell N-gating from BQ output.

Input JSON: rows of (advertiser_id, group_name, intent_tier, n_ips,
clickpass_visitors, guid_visitors, clickpass_visit_rate, guid_visit_rate).

Outputs per-(advertiser, tier, outcome) ATT cells, per-tier IVW pools across
advertisers, and an MNTN-overall IVW pool across all non-empty cells. Computes
leave-one-advertiser-out sensitivity for the overall numbers and flags any
single-advertiser drop that moves the overall ATT >0.2pp.

Cells are gated by the per-(advertiser, tier) 95% CI half-width on guid-ATT:
- pass: half-width <0.5pp (used in the per-advertiser deck slide)
- fail: cell goes in the appendix, but still contributes to per-tier and
  MNTN-overall IVW pools (IVW correctly down-weights noisy cells).

Usage:
    python ti_837_compute_att.py outputs/ti_837_lift_7adv_7day_2026_04_20_to_26.json \
        --out-json outputs/ti_837_meta_analysis_2026_04_20_to_26.json \
        --out-csv  outputs/ti_837_per_cell_table.csv

Single-arg call (smoke / single-advertiser): also accepts the original Zazzle
1-day output for backward-compat:
    python ti_837_compute_att.py outputs/ti_837_lift_zazzle_1day_2026_04_24.json
"""
import argparse
import json
import math
import sys
from pathlib import Path

# Advertiser ID -> human-readable name (TI-837 cohorts; superset of Phase 1 + Phase 2)
ADVERTISER_NAMES = {
    # Phase 1 cohort (7)
    31276: "Ferguson Home",
    31455: "Ancient Nutrition",
    34143: "First Watch",
    34611: "HexClad",
    34838: "Clayton Homes",
    37775: "Zazzle",
    40563: "Northern Tool",
    # Phase 2 cohort (30 — 28 new + 31276 + 31455 shared with Phase 1)
    30181: "Longines",
    30392: "Swatch",
    30496: "Lofta",
    31297: "Mountain Mike's Pizza",
    31464: "Fiji Airways",
    32244: "Sur La Table",
    32320: "Biz2Credit",
    32404: "National University",
    32527: "Haggar Clothing",
    32899: "Balance of Nature",
    33467: "Outback Presents",
    33572: "Jase Medical",
    33684: "SUMMIT One Vanderbilt",
    34141: "UD - Daniels College of Business",
    34365: "Barbara B. Mann Performing Arts Hall",
    34862: "Planned Parenthood Federation of America",
    35086: "TurboTenant",
    35374: "Experience Scottsdale",
    35573: "Casper",
    37222: "NET-A-PORTER",
    37796: "California Grown",
    38307: "Re-Bath Horney",
    38422: "Signature Hardware Account",
    42097: "Gruns",
    43996: "JS Health",
    46426: "BoggBag",
    50525: "Overjet",
    56187: "Ignite Attachments",
}

OUTCOMES = ("clickpass", "guid")
TIER_ORDER = ("high", "peak", "mid", "max_reach")
N_GATE_HALF_WIDTH_PP = 0.5  # per-advertiser per-tier guid CI half-width gate
SENSITIVITY_FLAG_PP = 0.2   # leave-one-out swing > this pp triggers a flag

Z = 1.96  # 95% CI


# ---------- statistics helpers ----------

def two_prop_se(p1, n1, p0, n0):
    """Wald SE of (p1 - p0) for two independent proportions."""
    if n1 <= 0 or n0 <= 0:
        return 0.0
    v1 = p1 * (1 - p1) / n1 if 0 < p1 < 1 else 0.0
    v0 = p0 * (1 - p0) / n0 if 0 < p0 < 1 else 0.0
    return math.sqrt(v1 + v0)


def two_prop_p(p1, n1, p0, n0):
    """Two-proportion z-test p-value (pooled SE)."""
    if n1 <= 0 or n0 <= 0:
        return 1.0
    diff = p1 - p0
    p_pool = (p1 * n1 + p0 * n0) / (n1 + n0)
    if not (0 < p_pool < 1):
        return 1.0
    se_pool = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n0))
    if se_pool == 0:
        return 1.0
    z = diff / se_pool
    return 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))


def ivw_combine(estimates_with_var):
    """Inverse-variance-weighted meta-analysis.

    Args: iterable of (estimate, variance). Drops cells with variance==0 or
    NaN. Returns (pooled_estimate, pooled_se, n_cells_used).
    """
    valid = [(e, v) for e, v in estimates_with_var if v and v > 0 and math.isfinite(e)]
    if not valid:
        return 0.0, 0.0, 0
    inv_var = sum(1.0 / v for _, v in valid)
    weighted = sum(e / v for e, v in valid)
    pooled = weighted / inv_var
    pooled_se = math.sqrt(1.0 / inv_var)
    return pooled, pooled_se, len(valid)


# ---------- per-cell ATT ----------

def per_cell_att(rows):
    """Pivot raw rows into per-(advertiser, tier, outcome) ATT cells.

    Returns list of dicts. Each cell carries point estimate, SE, 95% CI,
    p-value, sample sizes for both arms, and a passes_gate flag (computed
    on guid only, applied to both outcomes for the same cell).
    """
    # group rows by (advertiser_id, tier) → {group_name: row}
    grouped = {}
    for r in rows:
        aid = int(r["advertiser_id"])
        tier = r["intent_tier"]
        grouped.setdefault((aid, tier), {})[r["group_name"]] = {
            "n_ips": int(r["n_ips"]),
            "clickpass_visitors": int(r["clickpass_visitors"]),
            "guid_visitors": int(r["guid_visitors"]),
            "clickpass_visit_rate": float(r["clickpass_visit_rate"] or 0),
            "guid_visit_rate": float(r["guid_visit_rate"] or 0),
        }

    cells = []
    for (aid, tier), groups in grouped.items():
        tx = groups.get("treated_served")
        ctrl = groups.get("holdout_biddable")
        if not tx or not ctrl:
            continue  # one arm missing — drop cell

        # gate is on guid-ATT precision
        n1, n0 = tx["n_ips"], ctrl["n_ips"]
        p1_g, p0_g = tx["guid_visit_rate"], ctrl["guid_visit_rate"]
        se_g = two_prop_se(p1_g, n1, p0_g, n0)
        half_g = Z * se_g
        passes_gate = (n1 > 0 and n0 > 0 and half_g > 0
                       and half_g * 100 < N_GATE_HALF_WIDTH_PP)

        for outcome in OUTCOMES:
            p1 = tx[f"{outcome}_visit_rate"]
            p0 = ctrl[f"{outcome}_visit_rate"]
            v1 = tx[f"{outcome}_visitors"]
            v0 = ctrl[f"{outcome}_visitors"]
            att = p1 - p0
            se = two_prop_se(p1, n1, p0, n0)
            var = se * se if se > 0 else float("nan")
            ci_low = att - Z * se
            ci_high = att + Z * se
            pval = two_prop_p(p1, n1, p0, n0)
            cells.append({
                "advertiser_id": aid,
                "advertiser_name": ADVERTISER_NAMES.get(aid, str(aid)),
                "intent_tier": tier,
                "outcome": outcome,
                "n_treated": n1,
                "n_holdout": n0,
                "visitors_treated": v1,
                "visitors_holdout": v0,
                "rate_treated": p1,
                "rate_holdout": p0,
                "att": att,
                "se": se,
                "variance": var,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "ci_half_width_pp": (Z * se) * 100,
                "p_value": pval,
                "passes_n_gate": passes_gate,
            })
    # stable ordering: tier (canonical) then advertiser_id then outcome
    tier_rank = {t: i for i, t in enumerate(TIER_ORDER)}
    cells.sort(key=lambda c: (tier_rank.get(c["intent_tier"], 99),
                              c["advertiser_id"], c["outcome"]))
    return cells


# ---------- pooling ----------

def pool_per_tier(cells):
    """Per-tier IVW pool across advertisers, separately by outcome."""
    out = {}
    for tier in TIER_ORDER:
        for outcome in OUTCOMES:
            sub = [c for c in cells
                   if c["intent_tier"] == tier and c["outcome"] == outcome]
            if not sub:
                continue
            pooled, se, k = ivw_combine([(c["att"], c["variance"]) for c in sub])
            out.setdefault(tier, {})[outcome] = {
                "att": pooled,
                "se": se,
                "ci_low": pooled - Z * se,
                "ci_high": pooled + Z * se,
                "ci_half_width_pp": (Z * se) * 100,
                "n_cells_pooled": k,
                "n_cells_total": len(sub),
                "advertisers": sorted(c["advertiser_id"] for c in sub),
            }
    return out


def pool_overall(cells):
    """MNTN-overall IVW pool across all non-empty cells, separately by outcome."""
    out = {}
    for outcome in OUTCOMES:
        sub = [c for c in cells if c["outcome"] == outcome]
        pooled, se, k = ivw_combine([(c["att"], c["variance"]) for c in sub])
        out[outcome] = {
            "att": pooled,
            "se": se,
            "ci_low": pooled - Z * se,
            "ci_high": pooled + Z * se,
            "ci_half_width_pp": (Z * se) * 100,
            "n_cells_pooled": k,
            "n_cells_total": len(sub),
        }
    return out


def leave_one_out_sensitivity(cells):
    """For each advertiser dropped, recompute MNTN-overall IVW per outcome.

    Returns dict: {advertiser_id: {outcome: {att, swing_pp}}}. Flags any
    single-advertiser drop whose swing exceeds SENSITIVITY_FLAG_PP.
    """
    full = pool_overall(cells)
    advertisers = sorted({c["advertiser_id"] for c in cells})
    out = {}
    for aid in advertisers:
        sub = [c for c in cells if c["advertiser_id"] != aid]
        pooled = pool_overall(sub)
        out[aid] = {
            "advertiser_name": ADVERTISER_NAMES.get(aid, str(aid)),
        }
        for outcome in OUTCOMES:
            full_att = full[outcome]["att"]
            drop_att = pooled[outcome]["att"]
            swing_pp = (drop_att - full_att) * 100
            out[aid][outcome] = {
                "att": drop_att,
                "att_full": full_att,
                "swing_pp": swing_pp,
                "flag": abs(swing_pp) > SENSITIVITY_FLAG_PP,
            }
    return {"by_advertiser": out, "full": full}


# ---------- writers ----------

def write_csv(cells, path):
    cols = [
        "advertiser_id", "advertiser_name", "intent_tier", "outcome",
        "n_treated", "n_holdout", "visitors_treated", "visitors_holdout",
        "rate_treated", "rate_holdout",
        "att", "se", "ci_low", "ci_high", "ci_half_width_pp",
        "p_value", "passes_n_gate",
    ]
    lines = [",".join(cols)]
    for c in cells:
        row = []
        for k in cols:
            v = c[k]
            if isinstance(v, float):
                row.append(f"{v:.8f}")
            elif isinstance(v, bool):
                row.append("true" if v else "false")
            else:
                row.append(str(v))
        lines.append(",".join(row))
    Path(path).write_text("\n".join(lines) + "\n")


def print_per_cell(cells):
    print("=" * 120)
    print(f"{'aid':>5} {'advertiser':<20} {'tier':<10} {'outcome':<10} "
          f"{'n_tx':>9} {'n_ho':>9} "
          f"{'rate_tx':>9} {'rate_ho':>9} {'att_pp':>9} {'ci_low':>9} {'ci_high':>9} "
          f"{'half_pp':>8} {'p':>8} {'pass':>6}")
    print("-" * 120)
    for c in cells:
        print(f"{c['advertiser_id']:>5} {c['advertiser_name']:<20} {c['intent_tier']:<10} {c['outcome']:<10} "
              f"{c['n_treated']:>9,} {c['n_holdout']:>9,} "
              f"{c['rate_treated']*100:>8.4f}% {c['rate_holdout']*100:>8.4f}% "
              f"{c['att']*100:>8.3f}pp {c['ci_low']*100:>8.3f}pp {c['ci_high']*100:>8.3f}pp "
              f"{c['ci_half_width_pp']:>7.3f}pp {c['p_value']:>8.4f} "
              f"{'PASS' if c['passes_n_gate'] else 'fail':>6}")


def print_pools(per_tier, overall):
    print("\n" + "=" * 100)
    print("PER-TIER IVW POOLS (across advertisers)")
    print(f"{'tier':<10} {'outcome':<10} {'att_pp':>9} {'ci_low':>9} {'ci_high':>9} {'half_pp':>8} {'k':>4}/{'n':<4}")
    print("-" * 100)
    for tier in TIER_ORDER:
        if tier not in per_tier:
            continue
        for outcome in OUTCOMES:
            r = per_tier[tier].get(outcome)
            if not r:
                continue
            print(f"{tier:<10} {outcome:<10} "
                  f"{r['att']*100:>8.3f}pp {r['ci_low']*100:>8.3f}pp {r['ci_high']*100:>8.3f}pp "
                  f"{r['ci_half_width_pp']:>7.3f}pp {r['n_cells_pooled']:>4}/{r['n_cells_total']:<4}")

    print("\n" + "=" * 100)
    print("MNTN-OVERALL IVW POOL")
    print(f"{'outcome':<10} {'att_pp':>9} {'ci_low':>9} {'ci_high':>9} {'half_pp':>8} {'k':>4}/{'n':<4}")
    print("-" * 100)
    for outcome in OUTCOMES:
        r = overall[outcome]
        print(f"{outcome:<10} "
              f"{r['att']*100:>8.3f}pp {r['ci_low']*100:>8.3f}pp {r['ci_high']*100:>8.3f}pp "
              f"{r['ci_half_width_pp']:>7.3f}pp {r['n_cells_pooled']:>4}/{r['n_cells_total']:<4}")


def print_sensitivity(sens):
    print("\n" + "=" * 100)
    print("LEAVE-ONE-ADVERTISER-OUT SENSITIVITY (MNTN-overall)")
    print(f"{'aid':>5} {'advertiser':<20} {'outcome':<10} {'full_att':>9} {'drop_att':>9} {'swing':>9} {'flag':>5}")
    print("-" * 100)
    for aid, info in sorted(sens["by_advertiser"].items()):
        for outcome in OUTCOMES:
            r = info[outcome]
            print(f"{aid:>5} {info['advertiser_name']:<20} {outcome:<10} "
                  f"{r['att_full']*100:>8.3f}pp {r['att']*100:>8.3f}pp {r['swing_pp']:>8.3f}pp "
                  f"{'⚠' if r['flag'] else '':>5}")


# ---------- legacy single-advertiser path ----------

def run_legacy_single_advertiser(rows):
    """Backward-compat for the original Zazzle 1-day output (no advertiser_id)."""
    by_group = {}
    for r in rows:
        g = r["group_name"]
        t = r["intent_tier"]
        by_group.setdefault(t, {})[g] = {
            "n_ips": int(r["n_ips"]),
            "clickpass_visitors": int(r["clickpass_visitors"]),
            "guid_visitors": int(r["guid_visitors"]),
            "clickpass_visit_rate": float(r["clickpass_visit_rate"] or 0),
            "guid_visit_rate": float(r["guid_visit_rate"] or 0),
        }
    print("=" * 88)
    print(f"{'tier':<10} {'outcome':<10} {'tx_rate':>9} {'ctrl_rate':>9} "
          f"{'lift_pp':>9} {'p':>8} {'ci_low':>9} {'ci_high':>9}")
    print("-" * 88)
    for tier, groups in by_group.items():
        if "treated_served" not in groups or "holdout_biddable" not in groups:
            continue
        tx, ctrl = groups["treated_served"], groups["holdout_biddable"]
        for outcome in OUTCOMES:
            p1, n1 = tx[f"{outcome}_visit_rate"], tx["n_ips"]
            p0, n0 = ctrl[f"{outcome}_visit_rate"], ctrl["n_ips"]
            diff = p1 - p0
            se = two_prop_se(p1, n1, p0, n0)
            pval = two_prop_p(p1, n1, p0, n0)
            lo = diff - Z * se
            hi = diff + Z * se
            print(f"{tier:<10} {outcome:<10} {p1:>9.4%} {p0:>9.4%} "
                  f"{diff*100:>8.3f}pp {pval:>8.4f} {lo*100:>8.3f}pp {hi*100:>8.3f}pp")


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="BQ output JSON")
    parser.add_argument("--out-json", help="Meta-analysis JSON output", default=None)
    parser.add_argument("--out-csv", help="Per-cell CSV output", default=None)
    args = parser.parse_args()

    rows = json.loads(Path(args.input).read_text())
    if not rows:
        print("No rows in input — aborting", file=sys.stderr)
        sys.exit(1)

    is_multi_advertiser = "advertiser_id" in rows[0]
    if not is_multi_advertiser:
        run_legacy_single_advertiser(rows)
        return

    cells = per_cell_att(rows)
    per_tier = pool_per_tier(cells)
    overall = pool_overall(cells)
    sens = leave_one_out_sensitivity(cells)

    print_per_cell(cells)
    print_pools(per_tier, overall)
    print_sensitivity(sens)

    summary = {
        "n_advertisers": len(set(c["advertiser_id"] for c in cells)),
        "n_cells_total": len(cells),
        "per_cell": cells,
        "per_tier_ivw": per_tier,
        "mntn_overall_ivw": overall,
        "leave_one_out_sensitivity": sens,
        "config": {
            "n_gate_half_width_pp": N_GATE_HALF_WIDTH_PP,
            "sensitivity_flag_pp": SENSITIVITY_FLAG_PP,
            "z_score": Z,
        },
    }

    if args.out_json:
        Path(args.out_json).write_text(json.dumps(summary, indent=2, default=str))
        print(f"\nWrote {args.out_json}")
    if args.out_csv:
        write_csv(cells, args.out_csv)
        print(f"Wrote {args.out_csv}")


if __name__ == "__main__":
    main()
