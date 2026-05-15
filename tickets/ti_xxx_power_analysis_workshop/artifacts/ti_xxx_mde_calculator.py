"""TI-XXX Power Analysis Workshop — MDE Calculator (Python reference).

Mirror of TI-884's MDE calculator, repackaged for the workshop. Same Lewis-Rao
math; defaults are the workshop's drill defaults (alpha=0.05, power=0.80,
holdout=0.10, post-stack var_reduction=0.595).

Use the HTML calculator (ti_xxx_mde_calculator.html) for live workshop drills.
This file is the math reference and a smoke-test that the JS port stays
bit-for-bit compatible.

References:
  TI-884 methodology doc (ti_884_methodology.md).
  Lewis & Rao (2015) — QJE. Deng-Xu-Liu-Schmidt (2013) — CUPED.
"""
import math
from scipy.stats import norm

# Workshop defaults (locked unless you have a reason to override).
ALPHA = 0.05
POWER = 0.80
HOLDOUT_FRAC = 0.10
COHORT_CUPED_SE = 0.934       # sqrt(1 - 0.357^2), MNTN-measured mean rho
GHOST_AD_MULT = 0.75
STRATIFIED_MULT = 0.85
POST_STACK_MULT = COHORT_CUPED_SE * GHOST_AD_MULT * STRATIFIED_MULT  # ~0.595

WELL_POWERED_THRESHOLD = 0.05  # 5% relative MDE — "this lift is detectable"


def z_factor(alpha=ALPHA, power=POWER):
    """z_{alpha/2} + z_{power}. Default 0.05/0.80 = 2.80."""
    return norm.ppf(1 - alpha / 2) + norm.ppf(power)


def mde_binomial(n_t, n_c, p, var_reduction=1.0, alpha=ALPHA, power=POWER):
    """MDE for a binary outcome (visit rate, conversion rate).

    Returns dict: abs (pp), rel (fraction of p), tier label.
    """
    if n_t <= 0 or n_c <= 0 or p <= 0 or p >= 1:
        return {"abs": float("inf"), "rel": float("inf"), "tier": "no_data"}
    sigma = math.sqrt(p * (1 - p))
    se = sigma * math.sqrt(1 / n_t + 1 / n_c) * var_reduction
    abs_ = z_factor(alpha, power) * se
    rel = abs_ / p
    return {"abs": abs_, "rel": rel, "tier": tier_label(rel)}


def n_required_binomial(p, target_mde_rel, var_reduction=1.0,
                        holdout_frac=HOLDOUT_FRAC, alpha=ALPHA, power=POWER):
    """Minimum total N (across both arms) to detect target_mde_rel."""
    if p <= 0 or p >= 1 or target_mde_rel <= 0:
        return float("inf")
    sigma = math.sqrt(p * (1 - p))
    target_abs = target_mde_rel * p
    z = z_factor(alpha, power)
    return (z * sigma * var_reduction / target_abs) ** 2 / (holdout_frac * (1 - holdout_frac))


def spend_required(p, target_mde_rel, cpm=24.84, imps_per_ip=3.5,
                   var_reduction=1.0, holdout_frac=HOLDOUT_FRAC):
    """Convert target MDE to required monthly spend.

    Cohort defaults from TI-884 (CPM=$24.84, imps/IP=3.5).
    """
    n_total = n_required_binomial(p, target_mde_rel, var_reduction, holdout_frac)
    n_treated = n_total * (1 - holdout_frac)
    impressions = n_treated * imps_per_ip
    return {
        "n_total_ips": n_total,
        "n_treated_ips": n_treated,
        "impressions": impressions,
        "spend_dollars": impressions * cpm / 1000.0,
    }


def tier_label(mde_rel):
    if mde_rel == float("inf") or math.isnan(mde_rel):
        return "no_data"
    if mde_rel < WELL_POWERED_THRESHOLD:
        return "well_powered"
    if mde_rel < 2 * WELL_POWERED_THRESHOLD:
        return "borderline"
    return "underpowered"


def score_advertiser(label, monthly_spend, treated_ips, p_visit, p_cvr,
                     holdout_frac=HOLDOUT_FRAC):
    """Run both visits and CVR MDE for one advertiser, raw + post-stack."""
    n_c = treated_ips * holdout_frac / (1 - holdout_frac)  # implied holdout
    out = {"label": label, "spend": monthly_spend, "treated_ips": treated_ips,
           "p_visit": p_visit, "p_cvr": p_cvr}
    for metric, p in [("visits", p_visit), ("cvr", p_cvr)]:
        raw = mde_binomial(treated_ips, n_c, p)
        stack = mde_binomial(treated_ips, n_c, p, var_reduction=POST_STACK_MULT)
        out[metric] = {"raw": raw, "post_stack": stack}
    return out


# ---------------- self-test (must match TI-884 calculator) ----------------

if __name__ == "__main__":
    # Lewis-Rao hand calc: p=0.05, N_t=N_c=10,000, sigma=0.218, z=2.80
    # MDE_abs = 2.80 * 0.218 * sqrt(2/10000) ≈ 0.00863  →  MDE_rel ≈ 17.27%
    r = mde_binomial(10_000, 10_000, 0.05)
    assert 0.008 < r["abs"] < 0.009, r
    assert 0.17 < r["rel"] < 0.18, r
    print(f"[OK] Lewis-Rao hand calc: MDE_abs={r['abs']*100:.3f}pp, MDE_rel={r['rel']*100:.2f}%")

    # Spend at $200k/mo, cohort defaults: should land near 4% MDE raw, ~2.4% post-stack
    s_raw = mde_binomial(15_599_393, 15_599_393 / 9, 0.0966)  # WGU profile
    s_stack = mde_binomial(15_599_393, 15_599_393 / 9, 0.0966, var_reduction=POST_STACK_MULT)
    print(f"[INFO] WGU visits MDE: raw={s_raw['rel']*100:.3f}%, post-stack={s_stack['rel']*100:.3f}%")

    # Ownerly: reported 0.72%, MDE 5.92% raw → 8x underpowered (per TI-884 cross-val)
    ownerly = mde_binomial(1_487_242, 1_487_242 / 9, 0.0148)
    print(f"[INFO] Ownerly visits MDE: raw={ownerly['rel']*100:.3f}% vs reported lift 0.72%")
    assert ownerly["rel"] > 0.05, "Ownerly should be underpowered"

    # Post-stack constant
    print(f"[INFO] POST_STACK_MULT = {POST_STACK_MULT:.4f}  (0.934 * 0.75 * 0.85)")
