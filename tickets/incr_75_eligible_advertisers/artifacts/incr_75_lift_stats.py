"""Pooling helpers for INCR-75 ghost-bid lift.

Relative lift is pooled as a random-effects-free inverse-variance meta-analysis on the
LOG RISK RATIO, not as a count pool and not as (IVW absolute)/(IVW baseline).

Why: baseline visit rates run from ~0.01% to ~10% across the advertiser base. A naive
count pool lets the largest advertisers decide the answer and is Simpson-confounded (the
unscored band once read +29% naive against ~0 weighted). Weighting the ABSOLUTE effect
fixes that, but converting it back to a relative number needs a baseline, and an
inverse-variance-weighted baseline is dominated by the lowest-variance strata — it
collapsed to 0.00002 here and turned a +3% lift into +242%. The log risk ratio carries
the relative effect and its own variance together, so it needs no external baseline.
"""
import math


def _phi(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def log_rr(v_t, n_t, v_h, n_h):
    """(log risk ratio, its variance) for one stratum. None if either arm has no events."""
    if not (n_t and n_h and v_t and v_h):
        return None
    p1, p2 = v_t / n_t, v_h / n_h
    var = (1 - p1) / (p1 * n_t) + (1 - p2) / (p2 * n_h)
    if var <= 0:
        return None
    return math.log(p1 / p2), var


def pool_rr(strata):
    """Inverse-variance pool of (v_t, n_t, v_h, n_h) strata on the log-RR scale.

    Returns dict(rel, lo, hi, z, p, k) with rel/lo/hi as relative lift (0.05 = +5%).
    """
    num = den = 0.0
    k = 0
    for v_t, n_t, v_h, n_h in strata:
        r = log_rr(v_t, n_t, v_h, n_h)
        if r is None:
            continue
        lrr, var = r
        w = 1 / var
        num += lrr * w
        den += w
        k += 1
    if den == 0:
        return None
    lrr = num / den
    se = math.sqrt(1 / den)
    z = lrr / se
    return dict(rel=math.exp(lrr) - 1, lo=math.exp(lrr - 1.96 * se) - 1,
                hi=math.exp(lrr + 1.96 * se) - 1, z=z, p=2 * (1 - _phi(abs(z))), k=k)


def simple(v_t, n_t, v_h, n_h):
    """Single-stratum absolute/relative lift with a normal-approximation z and p."""
    if not (n_t and n_h and v_h):
        return None
    p1, p2 = v_t / n_t, v_h / n_h
    se = math.sqrt(p1 * (1 - p1) / n_t + p2 * (1 - p2) / n_h)
    if se <= 0:
        return None
    z = (p1 - p2) / se
    return dict(abs=p1 - p2, rel=(p1 - p2) / p2, base=p2, z=z, p=2 * (1 - _phi(abs(z))),
                lo=(p1 - p2) - 1.96 * se, hi=(p1 - p2) + 1.96 * se)
