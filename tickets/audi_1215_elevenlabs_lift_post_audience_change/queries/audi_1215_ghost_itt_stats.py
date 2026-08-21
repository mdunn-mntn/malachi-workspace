import csv, json, math

BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1215_elevenlabs_lift_post_audience_change/outputs"
rows = list(csv.DictReader(open(f"{BASE}/audi_1215_ghost_itt_prepost_raw.csv")))
rows = [r for r in rows if r.get("grain") in ("period", "week")]

def norm_p(z):
    return math.erfc(abs(z) / math.sqrt(2))

def contrast(sub, gho, num_field):
    n1, x1 = sub["n_ip"], sub[num_field]
    n0, x0 = gho["n_ip"], gho[num_field]
    p1, p0 = x1 / n1, x0 / n0
    diff = p1 - p0
    se = math.sqrt(p1 * (1 - p1) / n1 + p0 * (1 - p0) / n0)
    z = diff / se if se > 0 else float("nan")
    return {
        "rate_submitted": p1, "rate_ghost": p0,
        "abs_lift_pp": diff * 100, "rel_lift": diff / p0 if p0 > 0 else None,
        "se_pp": se * 100, "z": z, "p_two_sided": norm_p(z),
    }

def bucketize(grain):
    out = {}
    for r in rows:
        if r["grain"] != grain:
            continue
        d = {k: int(r[k]) for k in ("n_ip", "visited_ct", "converted_ct", "won_ct")}
        d.update(anchor_min_dt=r["anchor_min_dt"], anchor_max_dt=r["anchor_max_dt"])
        out.setdefault(r["bucket"], {})[r["arm"]] = d
    return out

periods = bucketize("period")
weeks = bucketize("week")

result = {
    "meta": {
        "ticket": "AUDI-1215", "run_date": "2026-08-21",
        "source": "dw-main-silver.enriched.lift__ghost_bid_visits (physical sqlmesh__enriched.enriched__lift__ghost_bid_visits__2999749496, modified 2026-08-21, MAX(dt)=2026-08-20)",
        "filters": "campaign_group_id=122748, partner_id=8 (Beeswax only), dt 2026-06-22..2026-08-20",
        "anchor": "first dt per (advertiser_id, campaign_id, ip), ROW_NUMBER ORDER BY dt, arm; global first day 2026-06-22 excluded (left-censored); anchor edge 2026-08-13 = MAX(dt)-7d (7-day outcome window)",
        "periods": {"pre": "2026-06-23..2026-06-30", "blackout_excluded": "2026-07-01..2026-07-10", "post": "2026-07-11..2026-08-13"},
        "stats": "unpooled two-proportion SE; z = diff/SE; two-sided normal p; delta SE = sqrt(SE_pre^2+SE_post^2)",
    },
    "periods": {},
}

for name, arms in sorted(periods.items()):
    sub, gho = arms["submitted"], arms["ghost"]
    result["periods"][name] = {
        "anchor_dates": f'{sub["anchor_min_dt"]}..{sub["anchor_max_dt"]}',
        "submitted": sub, "ghost": gho,
        "ghost_frac": gho["n_ip"] / (gho["n_ip"] + sub["n_ip"]),
        "visit": contrast(sub, gho, "visited_ct"),
        "conversion": contrast(sub, gho, "converted_ct"),
        "submitted_won_rate": sub["won_ct"] / sub["n_ip"],
    }

def log_rr(arms, num_field):
    sub, gho = arms["submitted"], arms["ghost"]
    p1, p0 = sub[num_field] / sub["n_ip"], gho[num_field] / gho["n_ip"]
    return math.log(p1 / p0), math.sqrt((1 - p1) / sub[num_field] + (1 - p0) / gho[num_field])

for metric, field in (("visit", "visited_ct"), ("conversion", "converted_ct")):
    pre, post = result["periods"]["pre"][metric], result["periods"]["post"][metric]
    d = post["abs_lift_pp"] - pre["abs_lift_pp"]
    se = math.sqrt(pre["se_pp"] ** 2 + post["se_pp"] ** 2)
    z = d / se
    lr_pre, lr_pre_se = log_rr(periods["pre"], field)
    lr_post, lr_post_se = log_rr(periods["post"], field)
    dlr = lr_post - lr_pre
    dlr_se = math.sqrt(lr_pre_se ** 2 + lr_post_se ** 2)
    result.setdefault("delta_post_minus_pre", {})[metric] = {
        "abs_lift_pp_delta": d, "se_pp": se, "z": z, "p_two_sided": norm_p(z),
        "rel_lift_pre": pre["rel_lift"], "rel_lift_post": post["rel_lift"],
        "log_rr_delta": dlr, "log_rr_delta_se": dlr_se,
        "log_rr_z": dlr / dlr_se, "log_rr_p_two_sided": norm_p(dlr / dlr_se),
    }

week_rows = []
for wk, arms in sorted(weeks.items()):
    sub, gho = arms["submitted"], arms["ghost"]
    v = contrast(sub, gho, "visited_ct")
    week_rows.append({
        "iso_week": wk, "anchor_dates": f'{sub["anchor_min_dt"]}..{sub["anchor_max_dt"]}',
        "n_submitted": sub["n_ip"], "n_ghost": gho["n_ip"],
        "ghost_frac": round(gho["n_ip"] / (gho["n_ip"] + sub["n_ip"]), 5),
        "visit_rate_submitted": round(v["rate_submitted"], 6), "visit_rate_ghost": round(v["rate_ghost"], 6),
        "visit_abs_lift_pp": round(v["abs_lift_pp"], 5), "visit_se_pp": round(v["se_pp"], 5),
        "visit_rel_lift": round(v["rel_lift"], 4), "visit_z": round(v["z"], 3),
        "conv_ct_submitted": sub["converted_ct"], "conv_ct_ghost": gho["converted_ct"],
    })

with open(f"{BASE}/audi_1215_ghost_itt_weekly.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(week_rows[0].keys()))
    w.writeheader()
    w.writerows(week_rows)

result["weekly_series"] = week_rows
with open(f"{BASE}/audi_1215_ghost_itt_prepost.json", "w") as f:
    json.dump(result, f, indent=2)

for name in ("pre", "blackout", "post"):
    p = result["periods"][name]
    print(name, "gf=%.5f" % p["ghost_frac"])
    for m in ("visit", "conversion"):
        c = p[m]
        print("  %s: sub=%.6f gho=%.6f abs=%.5fpp rel=%+.2f%% se=%.5f z=%.2f p=%.3g"
              % (m, c["rate_submitted"], c["rate_ghost"], c["abs_lift_pp"], 100 * c["rel_lift"], c["se_pp"], c["z"], c["p_two_sided"]))
print(json.dumps(result["delta_post_minus_pre"], indent=1))
