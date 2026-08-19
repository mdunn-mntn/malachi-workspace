"""Definitive current-lift fold + STAGED measured-lift gate (per user, 2026-07-02).

Measured lift = entry-anchored, 7d-from-first-bid window, entry dates 2026-06-23..
2026-07-07, Beeswax leg (partner_id 8) only. Source: silver enriched.lift__ghost_bid_visits.

Window note (2026-08-19 rerun): the table now holds 06-22..08-18, but the window does NOT
extend. The entry-cohort design exhausts the holdout arm — a holdout IP never wins, so it
never leaves the pool and is anchored almost immediately, while treatment IPs churn and new
ones keep arriving. Observed holdout share falls 0.105 -> 0.084 against a fixed 10% platform
holdout and measured lift inflates with it (+3% -> +25%). Valid only while the observed
holdout share sits in the clean 0.09-0.11 band, which ends 2026-07-07.

Staged gate applied on top of the a-priori (power/spend/prior) tiers:
  • EXCLUDE advertisers with a significant NEGATIVE measured lift (we've shown they
    don't work) — new hard filter F4_measured_neg.
  • TOP tier now REQUIRES a confirmed-positive measured lift (in addition to the
    a-priori Top criteria: MDE<=5% & value_score>=60). A-priori Tops that aren't
    confirmed-positive DEMOTE to Mid (they stay eligible; re-gate as the 10d window
    matures toward 30d ~late-July).
  • 'inconclusive' advertisers (window too short) stay eligible at Mid/Low.

Verdict values (current_lift_confirms):
  confirmed +  = >=20 holdout visits, two-sided p<.05, positive
  flat so far  = in table, >=100 holdout visits, not significant (enough data, ~0)
  too early    = in table, <100 holdout visits, not significant (thin/short window)
  no data yet  = not in the ghost-bid table
  (significant-NEGATIVE -> excluded, never shown on the eligible sheet)
"""
import csv, os, math
BASE = "tickets/incr_75_eligible_advertisers/outputs"


def load(fn):
    with open(os.path.join(BASE, fn)) as f:
        return list(csv.DictReader(f))


def Phi(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def stats(vt, nt, vh, nh):
    """returns (abs_pp, rel, z, p, lo_pp, hi_pp) or Nones."""
    if nt == 0 or nh == 0 or vh == 0:
        return (None,) * 6
    p1, p2 = vt / nt, vh / nh
    se = math.sqrt(p1 * (1 - p1) / nt + p2 * (1 - p2) / nh)
    rel = (p1 - p2) / p2
    z = (p1 - p2) / se if se > 0 else 0.0
    p = 2 * (1 - Phi(abs(z)))
    return (p1 - p2), rel, z, p, (p1 - p2) - 1.96 * se, (p1 - p2) + 1.96 * se


gi = lambda r, k: int(r[k])


def _conv(R):
    """Conversion-arm read for the same clean cohort (new in the 2026-08 rerun)."""
    if not R or gi(R, "n_h") == 0:
        return dict(conv_rel_lift="", conv_z="", conv_holdout_count="")
    _, rel, z, _, _, _ = stats(gi(R, "c_t"), gi(R, "n_t"), gi(R, "c_h"), gi(R, "n_h"))
    return dict(
        conv_rel_lift=(round(rel * 100, 1) if rel is not None else ""),
        conv_z=(round(z, 2) if z is not None else ""),
        conv_holdout_count=gi(R, "c_h"),
    )
ex = {r["advertiser_id"]: r for r in load("incr_75_ghost_clean_window.csv")}
elig = load("incr_75_final_tiered.csv")           # a-priori eligible (1,287)
allf = load("incr_75_all_flagged.csv")            # a-priori all (2,009)
funnel = load("incr_75_funnel_counts.csv")

MEAS_COLS = ["in_ghost_table", "current_lift_confirms", "current_rel_lift",
             "current_abs_lift_pp", "ghost_vis_clean", "current_z", "current_p",
             "current_ci_low_pp", "current_ci_high_pp", "current_lift_source",
             "conv_rel_lift", "conv_z", "conv_holdout_count"]


def verdict(R):
    """returns (verdict_str, is_negative_exclude, metrics_dict)."""
    if not R or gi(R, "n_h") == 0:
        return "no data yet", False, {}
    nt, nh, vt, vh = gi(R, "n_t"), gi(R, "n_h"), gi(R, "v_t"), gi(R, "v_h")
    ab, rel, z, p, lo, hi = stats(vt, nt, vh, nh)
    m = dict(abs=ab, rel=rel, z=z, p=p, lo=lo, hi=hi, vh=vh)
    sig = (p is not None and p < 0.05 and vh >= 20)
    if sig and rel > 0:
        return "confirmed +", False, m
    if sig and rel < 0:
        return "measured neg", True, m          # -> excluded
    if vh >= 100:
        return "flat so far", False, m
    return "too early", False, m


# ---- classify + gate ----
gated = []       # eligible after F4 (measured non-negative)
excluded_neg = set()
# TIER = power × confirmed-lift 2x2 (user 2026-07-05). POWER is a HARD Top gate:
# Top must be RUNNABLE for a rigorous 5% test (can_hit_ivr_5pct_8w='Yes') AND already
# show a 'confirmed +' lift. This reinstates power (dropped by the earlier score-band
# tiers, which let unpowered high-lift advertisers into Top). Trade-off: the lift-adjusted
# value_score is now a WITHIN-tier quality rank, not a global high→low sort (a few unpowered
# high-lift advertisers, e.g. Axos, score high but sit in Mid because they can't power a 5% test).
for r in elig:
    a = r["advertiser_id"]
    v, is_neg, m = verdict(ex.get(a))
    if is_neg:
        excluded_neg.add(a)
        continue
    d = dict(r)
    d["apriori_tier"] = r["final_tier"]          # pre-lift a-priori tier (from the scorer), kept for reference
    inrow = ex.get(a)
    d.update(
        in_ghost_table="Y" if inrow and gi(inrow, "n_h") > 0 else "N",
        current_lift_confirms=v,
        current_rel_lift=(round(m["rel"] * 100, 1) if m.get("rel") is not None else ""),
        current_abs_lift_pp=(round(m["abs"] * 100, 4) if m.get("abs") is not None else ""),
        ghost_vis_clean=(m.get("vh", "") if m else ""),
        current_z=(round(m["z"], 2) if m.get("z") is not None else ""),
        current_p=(round(m["p"], 4) if m.get("p") is not None else ""),
        current_ci_low_pp=(round(m["lo"] * 100, 4) if m.get("lo") is not None else ""),
        current_ci_high_pp=(round(m["hi"] * 100, 4) if m.get("hi") is not None else ""),
        current_lift_source="entry_cohort_2026_06_23_to_07_07_partner8",
        **_conv(inrow),
    )
    # value_score stays the ORIGINAL a-priori 0–100 quality score (user 2026-07-06: reverted the
    # lift-adjusted "new score" — measured lift lives in the [MEASURED NOW] columns + the tier gate, not the score).
    # ---- TIER = power × confirmed-lift 2x2 (powered-for-5% is a HARD Top gate) ----
    powered5 = (str(r.get("can_hit_ivr_5pct_8w", "")).strip().lower() == "yes")
    is_conf = (v == "confirmed +")
    d["final_tier"] = "Top" if (powered5 and is_conf) else ("Mid" if (powered5 or is_conf) else "Low")
    gated.append(d)

# safety: every Top must be BOTH powered-for-5% AND confirmed
_bad = [d for d in gated if d["final_tier"] == "Top" and
        (d["current_lift_confirms"] != "confirmed +" or str(d.get("can_hit_ivr_5pct_8w", "")).strip().lower() != "yes")]
if _bad:
    print(f"WARNING: {len(_bad)} Top advertisers are not (powered-5% AND confirmed) — tier logic bug.")

cols = list(elig[0].keys()) + ["apriori_tier"] + [c for c in MEAS_COLS if c not in elig[0].keys()]
with open(os.path.join(BASE, "incr_75_eligible_with_current_lift.csv"), "w", newline="") as fo:
    w = csv.DictWriter(fo, fieldnames=cols)
    w.writeheader()
    tier_ord = {"Top": 0, "Mid": 1, "Low": 2}
    for x in sorted(gated, key=lambda r: (tier_ord[r["final_tier"]], -(float(r["value_score"]) if r["value_score"] else 0))):
        w.writerow({k: x.get(k, "") for k in cols})

# ---- update all_flagged so Sheet 2 (audit) is consistent ----
gated_tier = {r["advertiser_id"]: r["final_tier"] for r in gated}
for r in allf:
    a = r["advertiser_id"]
    if a in excluded_neg:
        r["failed_at_filter"] = "F4_measured_neg"
        r["final_tier"] = "EXCLUDED"
    elif a in gated_tier:
        r["final_tier"] = gated_tier[a]
with open(os.path.join(BASE, "incr_75_all_flagged.csv"), "w", newline="") as fo:
    w = csv.DictWriter(fo, fieldnames=list(allf[0].keys()))
    w.writeheader(); w.writerows(allf)

# ---- add F4 step to the funnel (idempotent: drop any prior F4 first) ----
funnel = [s for s in funnel if not s["filter"].startswith("Measured lift not negative")]
n_start = next(int(s["remaining"]) for s in funnel if s["step"] in ("0", 0))
n_f3 = next(int(s["remaining"]) for s in funnel if s["filter"].startswith("Measurable IVR"))
n_f4 = n_f3 - len(excluded_neg)
new_funnel = []
for s in funnel:
    if s["filter"].startswith("FINAL"):
        new_funnel.append({"step": 4, "filter": "Measured lift not negative", "type": "HARD",
                           "threshold": "exclude significant-NEGATIVE ghost-bid lift",
                           "removed": len(excluded_neg), "remaining": n_f4,
                           "pct_of_start": f"{100.0 * n_f4 / n_start:.1f}%"})
        s = dict(s); s["remaining"] = n_f4
    new_funnel.append(s)
with open(os.path.join(BASE, "incr_75_funnel_counts.csv"), "w", newline="") as fo:
    w = csv.DictWriter(fo, fieldnames=list(new_funnel[0].keys()))
    w.writeheader(); w.writerows(new_funnel)

from collections import Counter
print(f"Excluded (measured negative, F4): {len(excluded_neg)}")
print("TIER = power x confirmed 2x2: Top = powered-5% AND confirmed / Mid = powered OR confirmed / Low = neither")
for t in ("Top", "Mid", "Low"):
    sub = [r for r in gated if r["final_tier"] == t]
    c = Counter(r["current_lift_confirms"] for r in sub)
    print(f"  {t} ({len(sub)}): " + "  ".join(f"{k}={v}" for k, v in sorted(c.items())))
print(f"ELIGIBLE total: {len(gated)}  (Top {sum(1 for r in gated if r['final_tier']=='Top')} / "
      f"Mid {sum(1 for r in gated if r['final_tier']=='Mid')} / Low {sum(1 for r in gated if r['final_tier']=='Low')})")
