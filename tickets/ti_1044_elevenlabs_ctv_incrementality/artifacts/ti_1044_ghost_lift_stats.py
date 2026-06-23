"""TI-1044 — ghost-ad lift significance (two-proportion test + delta-method CI on relative lift).
Reads outputs/ti_1044_ghost_lift.json (treated vs control: ips, visitors, converters).
Reports IVR and CVR relative lift, 95% CI, two-sided p-value, and the power-tier verdict.
"""
import json, math, csv
from scipy.stats import norm

BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality"
rows = json.load(open(f"{BASE}/outputs/ti_1044_ghost_lift.json"))
g = {r["grp"]: r for r in rows}
t, c = g["treated"], g["control"]
n_t, n_c = int(t["ips"]), int(c["ips"])

def lift_stats(num_t, num_c, label):
    p1, p2 = num_t / n_t, num_c / n_c
    rel = p1 / p2 - 1 if p2 > 0 else float("nan")
    # delta-method SE of log(p1/p2)
    se_log = math.sqrt((1 - p1) / (p1 * n_t) + (1 - p2) / (p2 * n_c)) if p1 > 0 and p2 > 0 else float("nan")
    lo, hi = math.exp(math.log(p1 / p2) - 1.96 * se_log) - 1, math.exp(math.log(p1 / p2) + 1.96 * se_log) - 1
    # two-proportion z-test (pooled)
    p_pool = (num_t + num_c) / (n_t + n_c)
    se_diff = math.sqrt(p_pool * (1 - p_pool) * (1 / n_t + 1 / n_c))
    z = (p1 - p2) / se_diff
    pval = 2 * (1 - norm.cdf(abs(z)))
    print(f"\n{label}")
    print(f"  treated rate : {p1*100:.4f}%  ({num_t:,}/{n_t:,})")
    print(f"  control rate : {p2*100:.4f}%  ({num_c:,}/{n_c:,})")
    print(f"  RELATIVE LIFT: {rel*100:+.1f}%   95% CI [{lo*100:+.1f}%, {hi*100:+.1f}%]")
    print(f"  p-value      : {pval:.4f}   {'(significant)' if pval < 0.05 else '(NOT significant)'}")
    return {"metric": label, "treated_rate_pct": round(p1*100, 5), "control_rate_pct": round(p2*100, 5),
            "lift_rel_pct": round(rel*100, 2), "ci_lo_pct": round(lo*100, 2), "ci_hi_pct": round(hi*100, 2),
            "p_value": round(pval, 4), "n_treated": n_t, "n_control": n_c}

print("="*70)
print("ElevenLabs (AID 51660) — ghost-ad incrementality (new bidder ghost logs)")
print(f"Treated (served) IPs: {n_t:,}   |   Control (ghost-holdout) IPs: {n_c:,}")
print("="*70)
res = [lift_stats(int(t["visitors"]), int(c["visitors"]), "VISIT RATE (IVR)"),
       lift_stats(int(t["converters"]), int(c["converters"]), "CONVERSION RATE (CVR)")]
print("\n(caveat: ghost holdout not frequency-capped → control rate biased high → lift is a LOWER bound)")

with open(f"{BASE}/outputs/ti_1044_ghost_lift_stats.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(res[0].keys())); w.writeheader(); w.writerows(res)
print(f"\nsaved -> outputs/ti_1044_ghost_lift_stats.csv")
