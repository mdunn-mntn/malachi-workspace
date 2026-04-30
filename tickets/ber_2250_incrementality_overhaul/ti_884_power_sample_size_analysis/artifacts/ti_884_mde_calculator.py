"""TI-884 MDE Calculator — outcome-agnostic Lewis-Rao power analysis.

Answers two questions:
  (1) Given current sample sizes (N_treated, N_holdout) and baseline rate,
      what's the minimum detectable effect (MDE)?
  (2) Given a target MDE and baseline rate, how many IPs / how much budget
      do we need?

Outcome-agnostic: core function takes (sigma) directly. Wrappers handle
binomial (visits, conversions) and continuous (iROAS) outcomes.

Variance-reduction stack:
  CUPED multiplier (sqrt(1 - rho^2)) — measured separately, plug in the
  SE multiplier as `var_reduction`. Defaults: 1.0 = no reduction.

References:
  Lewis & Rao (2015) — power analysis ground truth (QJE).
  Deng-Xu-Liu-Schmidt (2013) — CUPED.
  Johnson-Lewis-Reiley (2017) — ghost-ad conditioning.
"""
import math
from scipy.stats import norm


def z_factor(alpha=0.05, power=0.8):
    """z_{alpha/2} + z_beta. Default 0.05/0.8 = 1.96 + 0.84 = 2.80."""
    return norm.ppf(1 - alpha / 2) + norm.ppf(power)


def mde_absolute(n_t, n_c, sigma, alpha=0.05, power=0.8, var_reduction=1.0):
    """MDE in absolute outcome units, given fixed N per arm.

    Args:
      n_t, n_c: sample sizes (treated, control). Independent.
      sigma: per-unit outcome SD. For binomial: sqrt(p(1-p)).
      alpha, power: significance level and power. Default 5% / 80%.
      var_reduction: SE multiplier in [0,1]. 0.75 = 25% SE reduction.

    Returns:
      Absolute MDE (delta) — same units as sigma.
    """
    if n_t <= 0 or n_c <= 0:
        return float("inf")
    z = z_factor(alpha, power)
    se = sigma * math.sqrt(1 / n_t + 1 / n_c) * var_reduction
    return z * se


def mde_binomial(n_t, n_c, p, **kwargs):
    """MDE for a binary outcome (visits, conversions).

    Returns (mde_abs, mde_rel) where mde_rel = mde_abs / p.
    Note: SE uses the baseline p, NOT pooled. Conservative for small lifts.
    """
    if p <= 0 or p >= 1:
        return float("inf"), float("inf")
    sigma = math.sqrt(p * (1 - p))
    mde_abs = mde_absolute(n_t, n_c, sigma, **kwargs)
    return mde_abs, mde_abs / p


def mde_continuous(n_t, n_c, mu, sigma, **kwargs):
    """MDE for a continuous outcome (iROAS, revenue per IP).

    Returns (mde_abs, mde_rel) where mde_rel = mde_abs / mu.
    """
    mde_abs = mde_absolute(n_t, n_c, sigma, **kwargs)
    return mde_abs, mde_abs / mu if mu > 0 else float("inf")


def n_required_binomial(p, target_mde_abs, alpha=0.05, power=0.8,
                        holdout_frac=0.1, var_reduction=1.0):
    """Minimum total N required to detect target_mde_abs at given holdout split.

    holdout_frac: e.g. 0.1 means 10% holdout / 90% treated (MNTN's split).
    Returns total N across both arms.

    Math: from MDE = z * sigma * sqrt(1/n_t + 1/n_c) * vr, with
    n_t = (1-h)N and n_c = hN, the variance term simplifies to
    1/(h(1-h)N), so N = (z*sigma*vr/MDE)^2 / (h(1-h)).
    """
    if p <= 0 or p >= 1 or target_mde_abs <= 0:
        return float("inf")
    z = z_factor(alpha, power)
    sigma = math.sqrt(p * (1 - p))
    return (z * sigma * var_reduction / target_mde_abs) ** 2 / (holdout_frac * (1 - holdout_frac))


def spend_required(p, target_mde_rel, cpm, alpha=0.05, power=0.8,
                   holdout_frac=0.1, var_reduction=1.0, impressions_per_ip=10):
    """Convert target MDE to required monthly spend.

    Args:
      p: baseline rate.
      target_mde_rel: target MDE as fraction of p (e.g. 0.05 = 5% relative MDE).
      cpm: cost per thousand impressions, in dollars.
      impressions_per_ip: typical impressions delivered per unique IP per month.
        MNTN typical CTV: ~10-25. Override per-advertiser if known.

    Returns:
      dict with n_total_ips, n_treated_ips, n_holdout_ips, impressions, spend_dollars.
    """
    target_mde_abs = target_mde_rel * p
    n_total = n_required_binomial(p, target_mde_abs, alpha, power, holdout_frac, var_reduction)
    n_treated = n_total * (1 - holdout_frac)
    n_holdout = n_total * holdout_frac
    # Only treated IPs receive impressions (holdout, by definition, is unserved).
    impressions = n_treated * impressions_per_ip
    spend = impressions * cpm / 1000.0
    return {
        "n_total_ips": n_total,
        "n_treated_ips": n_treated,
        "n_holdout_ips": n_holdout,
        "impressions": impressions,
        "spend_dollars": spend,
    }


def tier_label(mde_rel):
    """Bucket relative MDE into a measurement-capacity tier."""
    if mde_rel == float("inf") or math.isnan(mde_rel):
        return "no_data"
    if mde_rel < 0.05:
        return "well_powered"
    if mde_rel < 0.10:
        return "borderline"
    return "underpowered"


# ---------------- self-test ----------------

if __name__ == "__main__":
    # Lewis-Rao hand calc reference: at p=0.05, N_t=N_c=10,000, sigma=sqrt(0.05*0.95)=0.218,
    # z=2.80, MDE_abs = 2.80 * 0.218 * sqrt(2/10000) = 2.80 * 0.218 * 0.01414 = 0.00863
    # MDE_rel = 0.00863 / 0.05 = 17.27%
    abs_, rel_ = mde_binomial(10_000, 10_000, 0.05)
    assert 0.008 < abs_ < 0.009, f"MDE_abs sanity failed: {abs_}"
    assert 0.17 < rel_ < 0.18, f"MDE_rel sanity failed: {rel_}"
    print(f"[OK] Lewis-Rao hand calc: MDE_abs={abs_*100:.3f}pp, MDE_rel={rel_*100:.2f}%")

    # n_required: target 5% relative MDE on p=0.02 (typical MNTN IVR), 10% holdout, no var reduction
    n_total = n_required_binomial(0.02, 0.05 * 0.02)
    print(f"[INFO] N required for 5% MDE on p=0.02 (10% holdout): {n_total:,.0f} total IPs")

    # spend: same target, $30 CPM, 15 imps/IP
    s = spend_required(0.02, 0.05, cpm=30, impressions_per_ip=15)
    print(f"[INFO] Spend for 5% MDE on p=0.02 @ $30CPM, 15imps/IP: ${s['spend_dollars']:,.0f}")

    # var reduction sanity
    abs_vr, rel_vr = mde_binomial(10_000, 10_000, 0.05, var_reduction=0.75)
    assert abs_vr < abs_, "var_reduction should reduce MDE"
    print(f"[OK] var_reduction=0.75: MDE_rel={rel_vr*100:.2f}% (vs raw {rel_*100:.2f}%)")
