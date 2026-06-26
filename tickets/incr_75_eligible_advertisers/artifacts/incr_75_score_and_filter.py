"""INCR-75 — score & filter advertisers for incrementality lift-test eligibility.

Reads the per-advertiser metrics CSV (queries/incr_75_advertiser_metrics.sql output),
applies the hard funnel filters, computes the power/budget columns with the TI-884
MDE engine (var_reduction=1.0, both 5% & 10% IVR targets), scores + tiers, and merges
prior-demonstrated-lift from TI-933 / TI-837.

Outputs (../outputs):
  incr_75_all_flagged.csv     — every advertiser, all metrics + per-filter flags + tier
  incr_75_final_tiered.csv    — eligible only, sorted tier -> value_score
  incr_75_funnel_counts.csv   — the waterfall (removed/remaining per hard filter)

User decisions (INCR-75, 2026-06-25): compute BOTH 5% & 10% IVR targets (tier on 10%,
flag 5%); var_reduction=1.0; spend = scored not hard-cut; extra-ask = label only;
CVR = informational (IVR gates eligibility).
"""
import csv
import json
import math
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # incr_75_eligible_advertisers/
OUT = ROOT / "outputs"
BER = ROOT.parent / "ber_2250_incrementality_overhaul"  # reused predecessor assets (TI-884/933/837) live here

# Reuse the TI-884 MDE engine unchanged.
sys.path.insert(0, str(BER / "ti_884_power_sample_size_analysis" / "artifacts"))
from ti_884_mde_calculator import mde_binomial, spend_required, tier_label  # noqa: E402

# ---------------- constants (one-line re-tunable) ----------------
ALPHA, POWER, HOLDOUT_FRAC, VAR_REDUCTION = 0.05, 0.80, 0.10, 1.0
IVR_TARGETS = {"5pct": 0.05, "10pct": 0.10}            # both computed; tier on 10%, flag 5%
CVR_TARGETS = {"5pct": 0.05, "10pct": 0.10, "15pct": 0.15}  # informational
TEST_DAYS = 56                                          # 8 weeks
MONTH_DAYS = 30.4
TEST_MONTHS = TEST_DAYS / MONTH_DAYS                    # ~1.84 months of spend in an 8-week test

MIN_VISITING_IPS = 100        # hard filter: stable IVR estimate
MIN_CONVERTING_IPS = 50       # CVR MDE reported only above this (else no_data)
SPEND_SWEET = (25_000, 200_000)   # mid-spend bonus band (monthly); NOT a hard cut
IVR_PEAK = (0.03, 0.06)       # measurable-AND-movable sweet spot
IVR_SATURATED = 0.12          # above this = saturated / hard-to-move penalty
ASK_EASY, ASK_STRETCH = 0.25, 0.50   # extra-spend % bands

METRICS_CSV = OUT / "incr_75_advertiser_metrics.csv"
TI933_CSV = BER / "ti_933_select_lift_analysis" / "outputs" / "ti_933_per_advertiser_lift.csv"
TI837_JSON = BER / "ti_837_implementation_plan" / "outputs" / "ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json"


# ---------------- prior-demonstrated-lift (bonus signal) ----------------
def load_prior_lift():
    """{advertiser_id: {'pp':float, 'source':str, 'name':str}} — prior demonstrated positive
    lift, in percentage points (stable, interpretable; we avoid relative % which blows up for
    low-organic-traffic brands). TI-933 = Select clickpass visit-rate pp; TI-837 = guid
    total-traffic pp (treated_served − holdout_biddable, segment='all'). Prefer TI-933 when both."""
    prior = {}
    # TI-933 — significant positive only (clean Select visit-rate test)
    if TI933_CSV.exists():
        for r in csv.DictReader(open(TI933_CSV)):
            try:
                pp = float(r["lift_pp"])
            except (ValueError, KeyError):
                continue
            if r.get("significant", "").strip().upper() == "Y" and pp > 0:
                prior[int(r["advertiser_id"])] = {
                    "pp": pp, "source": "TI-933 Select", "name": r.get("advertiser_name", "")}
    # TI-837 — treated vs holdout guid rate across intent tiers (segment='all'), reported in pp
    if TI837_JSON.exists():
        agg = {}  # aid -> {'t_vis':, 't_n':, 'h_vis':, 'h_n':}
        for rec in json.load(open(TI837_JSON)):
            if rec.get("segment") != "all":
                continue
            aid = int(rec["advertiser_id"])
            n = float(rec["n_ips"]); vr = float(rec["guid_visit_rate"])
            a = agg.setdefault(aid, {"t_vis": 0.0, "t_n": 0.0, "h_vis": 0.0, "h_n": 0.0})
            if rec["group_name"] == "treated_served":
                a["t_vis"] += vr * n; a["t_n"] += n
            elif rec["group_name"] == "holdout_biddable":
                a["h_vis"] += vr * n; a["h_n"] += n
        for aid, a in agg.items():
            # stability guard: both arms need enough estimated visitors.
            if a["t_n"] > 0 and a["h_n"] >= 2000 and a["h_vis"] >= 50 and a["t_vis"] >= 50:
                pp = (a["t_vis"] / a["t_n"] - a["h_vis"] / a["h_n"]) * 100.0
                if pp > 0 and aid not in prior:   # prefer the cleaner TI-933 number if present
                    prior[aid] = {"pp": pp, "source": "TI-837 guid", "name": ""}
    return prior


# ---------------- per-advertiser MDE / budget helpers ----------------
def mde_at_spend(total_spend, cpm, imps_per_ip, p):
    """Relative IVR/CVR MDE achievable if `total_spend` is spent over the test, given the
    advertiser's CPM and imps/IP. Returns inf if inputs unusable."""
    if not (total_spend > 0 and cpm and cpm > 0 and imps_per_ip and imps_per_ip > 0 and p and p > 0):
        return float("inf")
    impressions = total_spend / cpm * 1000.0
    treated = impressions / imps_per_ip
    holdout = treated * (HOLDOUT_FRAC / (1 - HOLDOUT_FRAC))
    _, rel = mde_binomial(treated, holdout, p, alpha=ALPHA, power=POWER, var_reduction=VAR_REDUCTION)
    return rel


def budget_for(p, target_rel, cpm, imps_per_ip):
    """Total test budget ($) to detect target_rel at baseline p. inf if unusable."""
    if not (p and p > 0 and cpm and cpm > 0 and imps_per_ip and imps_per_ip > 0):
        return float("inf")
    return spend_required(p, target_rel, cpm, alpha=ALPHA, power=POWER,
                          holdout_frac=HOLDOUT_FRAC, var_reduction=VAR_REDUCTION,
                          impressions_per_ip=imps_per_ip)["spend_dollars"]


def ask_band(pct):
    if pct is None or math.isinf(pct):
        return "n/a"
    if pct <= 0:
        return "none"
    if pct <= ASK_EASY:
        return "easy"
    if pct <= ASK_STRETCH:
        return "stretch"
    return "unreasonable"


def f(x):
    """csv float parse, '' -> 0.0."""
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0


# ---------------- main ----------------
def main():
    rows = list(csv.DictReader(open(METRICS_CSV)))
    prior = load_prior_lift()
    n_start = len(rows)

    recs = []
    for r in rows:
        aid = int(r["advertiser_id"])
        ivr = f(r["p_visit"]); cvr = f(r["p_cvr"])
        cpm = f(r["cpm"]); ipi = f(r["imps_per_ip"])
        avg_monthly = f(r["typical_active_month_spend"])
        spend_30d = f(r["spend_30d"])
        dips30 = f(r["distinct_ips_30d"]); dips56 = f(r["distinct_ips_56d"])
        vis = f(r["visiting_ips_30d"]); conv = f(r["converting_ips_30d"])
        active = r["active"].strip().lower() == "true"
        is_b2b = r["is_b2b"].strip().lower() == "true"
        test_spend = avg_monthly * TEST_MONTHS               # 8-week spend at normal rate

        # ---- hard filters ----
        pass_clean = active and bool(r["advertiser_name"]) and dips30 > 0
        pass_not_b2b = not is_b2b
        pass_measurable = (vis >= MIN_VISITING_IPS) and ivr > 0
        if not pass_clean:
            failed = "F1_clean_active"
        elif not pass_not_b2b:
            failed = "F2_not_b2b"
        elif not pass_measurable:
            failed = "F3_measurable_ivr"
        else:
            failed = "PASSED"
        eligible = failed == "PASSED"

        # ---- power / budget (IVR) at both targets, var_reduction=1.0 ----
        mde_ivr_normal = mde_at_spend(test_spend, cpm, ipi, ivr)   # achievable IVR MDE at normal 8wk spend
        budget_ivr = {k: budget_for(ivr, t, cpm, ipi) for k, t in IVR_TARGETS.items()}
        req_monthly_ivr = {k: (b / TEST_MONTHS if math.isfinite(b) else float("inf")) for k, b in budget_ivr.items()}
        can_hit_ivr = {k: (math.isfinite(budget_ivr[k]) and test_spend >= budget_ivr[k]) for k in IVR_TARGETS}

        # extra spend to reach each target (over the 8-week test budget)
        extra_abs = {k: max(0.0, budget_ivr[k] - test_spend) if math.isfinite(budget_ivr[k]) else float("inf")
                     for k in IVR_TARGETS}
        extra_pct = {k: (extra_abs[k] / test_spend if test_spend > 0 and math.isfinite(extra_abs[k]) else float("inf"))
                     for k in IVR_TARGETS}
        # primary tiering target = 10%
        ask = ask_band(extra_pct["10pct"])
        # Spend-feasible? Yes if already AT/OVER the required minimum, OR within a reasonable
        # (<= ASK_STRETCH = 50%) bump. No only if it would take an unreasonable (>50%) increase.
        # (One-sided: spending well over the minimum is feasible — never flag it "No".)
        def spend_feasible(b):
            if not math.isfinite(b) or b <= 0:
                return False
            return test_spend >= b / (1 + ASK_STRETCH)
        close_ivr = spend_feasible(budget_ivr["10pct"])

        # ---- direct 56-day power cross-check (no extrapolation) ----
        if dips56 > 0 and ivr > 0:
            _, mde_ivr_direct = mde_binomial(dips56, dips56 * HOLDOUT_FRAC / (1 - HOLDOUT_FRAC), ivr,
                                             alpha=ALPHA, power=POWER, var_reduction=VAR_REDUCTION)
        else:
            mde_ivr_direct = float("inf")

        # ---- CVR (informational) ----
        cvr_ok = conv >= MIN_CONVERTING_IPS and cvr > 0
        mde_cvr_normal = mde_at_spend(test_spend, cpm, ipi, cvr) if cvr_ok else float("inf")
        budget_cvr = {k: (budget_for(cvr, t, cpm, ipi) if cvr_ok else float("inf")) for k, t in CVR_TARGETS.items()}
        can_hit_cvr15 = cvr_ok and math.isfinite(budget_cvr["15pct"]) and test_spend >= budget_cvr["15pct"]
        close_cvr = spend_feasible(budget_cvr["15pct"]) if cvr_ok else None

        # ---- proxies ----
        reach_to_spend = (dips30 / (avg_monthly / 1000.0)) if avg_monthly > 0 else float("inf")  # IPs per $1k
        pl = prior.get(aid, {})
        prior_pp = pl.get("pp")
        has_prior_lift = bool(pl)

        recs.append({
            "advertiser_id": aid,
            "advertiser_name": r["advertiser_name"],
            "vertical_buckets": r["vertical_buckets"],
            "active": active, "is_b2b": is_b2b,
            "avg_monthly_spend": avg_monthly,
            "spend_30d": spend_30d,
            "max_month_spend": f(r["max_month_spend"]),
            "active_months_count": int(f(r["active_months_count"])),
            "distinct_ips_30d": int(dips30), "distinct_ips_56d": int(dips56),
            "visiting_ips_30d": int(vis), "converting_ips_30d": int(conv),
            "cpm": cpm, "imps_per_ip": ipi,
            "ivr": ivr, "cvr": cvr,
            # IVR power
            "mde_ivr_at_normal_pct": mde_ivr_normal * 100 if math.isfinite(mde_ivr_normal) else None,
            "mde_ivr_direct_56d_pct": mde_ivr_direct * 100 if math.isfinite(mde_ivr_direct) else None,
            "budget_for_mde_ivr_5pct": budget_ivr["5pct"] if math.isfinite(budget_ivr["5pct"]) else None,
            "budget_for_mde_ivr_10pct": budget_ivr["10pct"] if math.isfinite(budget_ivr["10pct"]) else None,
            "req_monthly_spend_ivr_5pct": req_monthly_ivr["5pct"] if math.isfinite(req_monthly_ivr["5pct"]) else None,
            "req_monthly_spend_ivr_10pct": req_monthly_ivr["10pct"] if math.isfinite(req_monthly_ivr["10pct"]) else None,
            "can_hit_ivr_5pct_8w": "Yes" if can_hit_ivr["5pct"] else "No",
            "can_hit_ivr_10pct_8w": "Yes" if can_hit_ivr["10pct"] else "No",
            "extra_spend_ivr_10pct_abs": extra_abs["10pct"] if math.isfinite(extra_abs["10pct"]) else None,
            "extra_spend_ivr_10pct_pct": extra_pct["10pct"] * 100 if math.isfinite(extra_pct["10pct"]) else None,
            "ivr_ask_band": ask,
            "close_to_ivr_min": "Yes" if close_ivr else "No",
            # CVR informational
            "mde_cvr_at_normal_pct": mde_cvr_normal * 100 if math.isfinite(mde_cvr_normal) else None,
            "budget_for_mde_cvr_15pct": budget_cvr["15pct"] if math.isfinite(budget_cvr["15pct"]) else None,
            "req_monthly_spend_cvr_15pct": (budget_cvr["15pct"] / TEST_MONTHS) if (cvr_ok and math.isfinite(budget_cvr["15pct"])) else None,
            "can_hit_cvr_15pct_8w": "Yes" if can_hit_cvr15 else ("No" if cvr_ok else "no_data"),
            "close_to_cvr_min": ("Yes" if close_cvr else "No") if close_cvr is not None else "no_data",
            # proxies / prior lift
            "reach_to_spend_ip_per_1k": reach_to_spend if math.isfinite(reach_to_spend) else None,
            "prior_lift_pp": prior_pp,
            "prior_lift_source": pl.get("source", ""),
            "has_prior_lift": has_prior_lift,
            # filters
            "pass_f1_clean_active": pass_clean,
            "pass_f2_not_b2b": pass_not_b2b,
            "pass_f3_measurable_ivr": pass_measurable,
            "failed_at_filter": failed,
            "eligible": eligible,
            # filled below
            "value_score": None, "final_tier": "EXCLUDED",
            "_mde_ivr_normal": mde_ivr_normal, "_test_spend": test_spend,
        })

    # ---- value score + tiers over the eligible set ----
    elig = [x for x in recs if x["eligible"]]
    spends = sorted(x["avg_monthly_spend"] for x in elig if x["avg_monthly_spend"] > 0)
    rts = sorted(x["reach_to_spend_ip_per_1k"] for x in elig if x["reach_to_spend_ip_per_1k"])

    def pctile_rank(sorted_xs, v):
        if not sorted_xs or v is None:
            return 0.5
        below = sum(1 for y in sorted_xs if y < v)
        return below / len(sorted_xs)

    def clamp(v, lo=0.0, hi=1.0):
        return max(lo, min(hi, v))

    for x in elig:
        mde = x["_mde_ivr_normal"]
        # power (30): mde 5% -> 30, 20% -> 0 (linear); inf -> 0
        power_pts = 30 * clamp((0.20 - mde) / (0.20 - 0.05)) if math.isfinite(mde) else 0.0
        # mid-spend sweet (20): triangular peak $50k-$100k, 0 at $10k and $300k
        s = x["avg_monthly_spend"]
        if 50_000 <= s <= 100_000:
            spend_pts = 20.0
        elif s < 50_000:
            spend_pts = 20 * clamp((s - 10_000) / 40_000)
        else:
            spend_pts = 20 * clamp((300_000 - s) / 200_000)
        # smaller-brand / movability (20): low spend percentile + high reach-to-spend
        small_pts = 20 * (0.6 * (1 - pctile_rank(spends, s)) + 0.4 * pctile_rank(rts, x["reach_to_spend_ip_per_1k"]))
        # IVR band position (15): peak 3-6%, penalize <1% and >12%
        ivr = x["ivr"]
        if IVR_PEAK[0] <= ivr <= IVR_PEAK[1]:
            ivr_pts = 15.0
        elif ivr < IVR_PEAK[0]:
            ivr_pts = 15 * clamp((ivr - 0.005) / (IVR_PEAK[0] - 0.005))
        else:
            ivr_pts = 15 * clamp((IVR_SATURATED - ivr) / (IVR_SATURATED - IVR_PEAK[1]))
        # low saturation / incrementality (15): high reach-to-spend = broad/net-new
        sat_pts = 15 * pctile_rank(rts, x["reach_to_spend_ip_per_1k"])
        # prior-lift bonus (+10): scaled by pp magnitude, 8pp -> full bonus
        bonus = clamp((x["prior_lift_pp"] or 0) / 8.0) * 10 if x["has_prior_lift"] else 0.0

        score = power_pts + spend_pts + small_pts + ivr_pts + sat_pts + bonus
        x["value_score"] = round(score, 1)

    # tier assignment (power-gated)
    for x in elig:
        mde = x["_mde_ivr_normal"]
        vs = x["value_score"]
        hits5 = math.isfinite(mde) and mde <= 0.05
        hits10 = math.isfinite(mde) and mde <= 0.10
        if hits5 and vs >= 60:
            x["final_tier"] = "Top"
        elif hits10 or (hits5) or (x["ivr_ask_band"] in ("easy", "stretch") and vs >= 45):
            x["final_tier"] = "Mid"
        else:
            x["final_tier"] = "Low"

    # ---- waterfall ----
    n_after_f1 = sum(1 for x in recs if x["pass_f1_clean_active"])
    n_after_f2 = sum(1 for x in recs if x["pass_f1_clean_active"] and x["pass_f2_not_b2b"])
    n_after_f3 = len(elig)
    funnel = [
        {"step": 0, "filter": "Starting universe (delivered, trailing 30d)", "type": "-", "threshold": "-",
         "removed": 0, "remaining": n_start},
        {"step": 1, "filter": "Clean & active", "type": "HARD", "threshold": "active=TRUE, has name, served",
         "removed": n_start - n_after_f1, "remaining": n_after_f1},
        {"step": 2, "filter": "Not B2B", "type": "HARD", "threshold": "exclude 'B2B Software & Services'",
         "removed": n_after_f1 - n_after_f2, "remaining": n_after_f2},
        {"step": 3, "filter": "Measurable IVR", "type": "HARD", "threshold": f">= {MIN_VISITING_IPS} visiting IPs & IVR>0",
         "removed": n_after_f2 - n_after_f3, "remaining": n_after_f3},
        {"step": 99, "filter": "FINAL ELIGIBLE (must-pass)", "type": "-", "threshold": "-",
         "removed": 0, "remaining": n_after_f3},
    ]
    tiers = {t: sum(1 for x in elig if x["final_tier"] == t) for t in ("Top", "Mid", "Low")}

    # ---- write CSVs ----
    drop = {"_mde_ivr_normal", "_test_spend"}
    cols = [k for k in recs[0].keys() if k not in drop]

    def fmt(v):
        if isinstance(v, float):
            return "" if (math.isinf(v) or math.isnan(v)) else f"{v:.6f}"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return "" if v is None else v

    with open(OUT / "incr_75_all_flagged.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
        for x in sorted(recs, key=lambda r: (r["failed_at_filter"] != "PASSED",
                                             -(r["value_score"] or -1), r["failed_at_filter"])):
            w.writerow({k: fmt(x[k]) for k in cols})

    with open(OUT / "incr_75_final_tiered.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
        tier_ord = {"Top": 0, "Mid": 1, "Low": 2}
        for x in sorted(elig, key=lambda r: (tier_ord[r["final_tier"]], -(r["value_score"] or 0))):
            w.writerow({k: fmt(x[k]) for k in cols})

    with open(OUT / "incr_75_funnel_counts.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["step", "filter", "type", "threshold", "removed", "remaining", "pct_of_start"])
        w.writeheader()
        for s in funnel:
            s["pct_of_start"] = f"{s['remaining'] / n_start * 100:.1f}%"
            w.writerow(s)

    # ---- console summary ----
    print(f"start={n_start}  after F1 clean/active={n_after_f1}  after F2 not-B2B={n_after_f2}  "
          f"after F3 measurable-IVR (ELIGIBLE)={n_after_f3}")
    print(f"tiers: Top={tiers['Top']}  Mid={tiers['Mid']}  Low={tiers['Low']}")
    print(f"eligible with prior demonstrated lift: {sum(1 for x in elig if x['has_prior_lift'])}")
    print("\nTop-15 by value_score:")
    print(f"{'aid':>7} {'name':<28} {'spend/mo':>10} {'IVR%':>6} {'mde@norm%':>9} {'tier':>5} {'score':>5} {'prior'}")
    for x in sorted(elig, key=lambda r: -(r["value_score"] or 0))[:15]:
        mden = x["mde_ivr_at_normal_pct"]
        pl = f"{x['prior_lift_pp']:.1f}pp" if x["prior_lift_pp"] else ""
        print(f"{x['advertiser_id']:>7} {x['advertiser_name'][:28]:<28} ${x['avg_monthly_spend']/1000:>8,.0f}k "
              f"{x['ivr']*100:>5.2f}% {mden if mden is not None else float('nan'):>8.2f}% "
              f"{x['final_tier']:>5} {x['value_score']:>5.0f} {pl}")


if __name__ == "__main__":
    main()
