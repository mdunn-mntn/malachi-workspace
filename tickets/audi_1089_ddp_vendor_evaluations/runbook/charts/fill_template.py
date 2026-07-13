#!/usr/bin/env python3
"""Fill audi_1089_quality_template.xlsx from the canonical run CSVs.

Reads every q*.csv in the run directory plus the template workbook, writes a filled
copy to outputs/audi_1089_quality_template_filled.xlsx. The original template is
never modified. Idempotent: cells backed by q7b/q7c print "pending scan" until those
CSVs land, then a rerun replaces them.

Sources per template section are documented in runbook/README.md (Template map).
Formulas for the ECONOMICS rows follow runbook/dependency_valuation.md with the
margin ladder updated to 15/20/30% (user's blended-margin estimate, 2026-07-12).

Usage: python3 fill_template.py            (from workspace root or charts dir)
"""
import csv
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))
TICKET = os.path.dirname(os.path.dirname(HERE))
RDIR = os.path.join(TICKET, "outputs", "run_2026_07_10")
TEMPLATE = os.path.join(TICKET, "outputs", "audi_1089_quality_template.xlsx")
OUT = os.path.join(TICKET, "outputs", "audi_1089_quality_template_filled.xlsx")

# Template columns B..O in order.
DS_COLS = [28, 40, 33, 24, 36, 25, 26, 39, 27, 30, 23, 35, 17, 29]
ACTIVE = [28, 40, 33, 24, 36, 25, 26, 39, 30, 23]      # deliver into svs
EXT = [24, 25, 26, 28, 33, 36, 39, 40]                  # paid external vendors
FREE = [23, 30]
METERED = [24, 28, 33, 36, 40]
FLAT = [25, 26, 39]
OOS = {27: "n/a — disabled (enabled=false), no feed",
       35: "n/a — not an MM site-visit feed (3P interests)",
       17: "n/a — not an MM site-visit feed (3P interests)",
       29: "n/a — not an MM site-visit feed (CRM ingest)"}
SHORT = {23: "guid_log", 24: "Justuno", 25: "5x5", 26: "Predactiv", 27: "LaunchLabs",
         28: "33Across", 30: "augmentor", 33: "Sovrn", 36: "Cybba", 39: "Klickly",
         40: "33A API", 35: "LiveRamp IP", 17: "ShareThis", 29: "deepsync"}
BITSQ = {23: 0, 24: 1, 25: 2, 26: 3, 28: 4, 30: 5, 33: 6, 36: 7, 39: 8, 40: 9}
FREE_MASK = (1 << 0) | (1 << 5)
ANN30 = 365.0 / 30.0            # 30d window -> year
MARGINS = (0.15, 0.20, 0.30)    # blended-margin ladder
BASELINE_VR = 0.0223            # no-svs-vendor sole-serve IVR %, q7


# ---------------- formatting ----------------
def fmtn(v):
    v = float(v)
    if abs(v) >= 1e9:
        return f"{v / 1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"{v / 1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:,.0f}"


def money(v):
    v = float(v)
    if abs(v) >= 1e6:
        return f"${v / 1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:,.0f}"


def pc(v, d=1):
    return f"{float(v):.{d}f}%"


def _poisson_ci(k):
    z = 1.959964

    def chi2_q(pz, df):
        if df == 0:
            return 0.0
        return df * (1 - 2.0 / (9 * df) + pz * (2.0 / (9 * df)) ** 0.5) ** 3

    lo = 0.0 if k == 0 else 0.5 * chi2_q(-z, 2 * k)
    hi = 0.5 * chi2_q(z, 2 * k + 2)
    return lo, hi


# ---------------- loaders ----------------
def rows_of(fname):
    p = os.path.join(RDIR, fname)
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        return []
    with open(p) as f:
        return list(csv.DictReader(f))


def by_ds(fname, key=None):
    out = {}
    for r in rows_of(fname):
        k = key or ("ds" if "ds" in r else "data_source_id")
        out[int(r[k])] = r
    return out


q0 = {}          # ds -> {registry fields, june bill}
for r in rows_of("q0_roster_cost.csv"):
    d = int(r["data_source_id"])
    ent = q0.setdefault(d, {"reg": r, "june_usd": None, "june_imps": None})
    if r.get("reporting_month", "").startswith("2026-06"):
        ent["june_usd"] = float(r["usage_dollars"])
        ent["june_imps"] = float(r["impressions"])
        ent["reg"] = r

q1 = {}          # ds -> aggregates over the 30d window
for r in rows_of("q1_scale_by_day.csv"):
    d = int(r["data_source_id"])
    a = q1.setdefault(d, {"days": [], "rows": 0, "ipv6": 0, "path": 0})
    n = int(r["n_rows"])
    a["days"].append(n)
    a["rows"] += n
    a["ipv6"] += int(r["ipv6_rows"])
    a["path"] += int(r["rows_with_path"])

q1b = {}         # ds -> {field: pct}
for r in rows_of("q1b_column_richness.csv"):
    q1b.setdefault(int(r["data_source_id"]), {})[r["field"]] = float(r["pct_populated"])

q1c = by_ds("q1c_content_quality.csv")
q1d = by_ds("q1d_billed_usage.csv")
q2 = by_ds("q2_window_reach.csv")
q2b = by_ds("q2b_daily_drops.csv")
q2c = by_ds("q2c_funnel.csv")
q3 = by_ds("q3_usable_uniqueness.csv")
q3r = by_ds("q3_pair_recency.csv")
q4 = by_ds("q4_domain_value.csv")
q6 = by_ds("q6_value_tiers.csv")
q7 = by_ds("q7_sole_vr.csv")

q5 = {}          # ds -> cohort -> row
for r in rows_of("q5_score_tiers.csv"):
    q5.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r

q6b = {}         # ds -> {prosp_imps, total_imps}
for r in rows_of("q6b_sole_by_funnel.csv"):
    d = int(r["ds"])
    a = q6b.setdefault(d, {"prosp": 0, "tot": 0})
    a["tot"] += int(r["imps"])
    if r["obj_bucket"] == "prospecting_family":
        a["prosp"] += int(r["imps"])

q7b = {}         # ds -> cohort -> row  (may be empty until scan lands)
for r in rows_of("q7b_perf_by_cohort.csv"):
    q7b.setdefault(int(r["ds"]), {})[r["cohort"]] = r

q7c = {}         # ds -> cohort -> row  (may be empty until scan lands)
for r in rows_of("q7c_conversions.csv"):
    q7c.setdefault(int(r["ds"]), {})[r["cohort"]] = r

q7d = rows_of("q7d_platform_week.csv")
PLAT_IPS = float(q7d[0]["ips_served_week"]) if q7d else None

masks, reassign = {}, {}
for r in rows_of("q3b_credit_reassignment.csv"):
    if r["rec"] == "mask":
        masks[int(r["k1"])] = int(r["n_pairs"])
    elif r["rec"] == "reassign":
        reassign.setdefault(int(r["k1"]), {})[r["k2"]] = int(r["n_pairs"])


# ---------------- portfolio math (masks) ----------------
def cov(keep):
    km = FREE_MASK
    for d in keep:
        km |= (1 << BITSQ[d])
    return sum(p for m, p in masks.items() if m & km)


FULL_COV = cov(EXT)

# Greedy add-order (optima are nested — verified vs exhaustive in q9e).
add_order, add_gain = [], {}
remaining = list(EXT)
cur = cov([])
while remaining:
    best = max(remaining, key=lambda d: cov(add_order + [d]))
    g = cov(add_order + [best]) - cur
    cur = cov(add_order + [best])
    add_order.append(best)
    add_gain[best] = 100.0 * g / FULL_COV
    remaining.remove(best)

coverage_lost = {d: 100.0 * (FULL_COV - cov([x for x in EXT if x != d])) / FULL_COV
                 for d in EXT}


# ---------------- economics ----------------
def june_bill(d):
    return q0.get(d, {}).get("june_usd")


def t2_ann(d):          # dependent (sole-won) media revenue $/yr
    return float(q6[d]["media_sole"]) * 52 if d in q6 else None


def t1_ann(d):          # provable score-gated floor $/yr
    return float(q6[d]["media_sole_scored"]) * 52 if d in q6 else None


# ---------------- composite score (q9b formula, unchanged) ----------------
def curved_scores():
    max_sc = max(float(q4[d]["sole_classified"]) for d in EXT)
    max_t1 = max(float(q6[d]["imps_sole_scored_nonrtc"]) for d in EXT)
    comp = {}
    for d in EXT:
        V = math.log10(float(q4[d]["sole_classified"]) + 1) / math.log10(max_sc + 1)
        R = (float(q3r[d]["pct_sole"]) + float(q3r[d]["pct_freshest"])) / 100
        Q = 0.5 * float(q4[d]["pct_classified"]) / 100 \
            + 0.5 * (1 - float(q5[d]["sole"]["pct_unscored_delivered"]) / 100)
        D = math.log10(float(q6[d]["imps_sole_scored_nonrtc"]) + 1) / math.log10(max_t1 + 1)
        P = min(float(q7[d]["vr_overall_pct"]) / BASELINE_VR, 2) / 2 \
            if float(q7[d]["sole_imps"]) >= 5000 else 0.5
        comp[d] = 100 * (0.40 * V + 0.15 * R + 0.15 * Q + 0.10 * D + 0.20 * P)
    best = max(comp.values())
    return {d: (comp[d], 100 * comp[d] / best) for d in comp}


CURVE = curved_scores()

VERDICTS = {
    28: "NEGOTIATE — one deal with DS40 (~$598K/yr combined); target rate cut, cite free-log overlap",
    40: "NEGOTIATE — same vendor as DS28; fold into one combined renegotiation",
    33: "DROP — 77% malformed, near-zero sole value; exact recovery $109K/yr (sequencing-safe)",
    24: "KEEP-trim — cleanest feed, 91.6% sole pairs; trim rate if possible",
    36: "DROP — subscale (700x below siblings); $21K/yr recovery",
    25: "KEEP — anchor vendor, 69.3% sole; lock flat price at renewal",
    26: "KEEP — #1 unique classified domains + HEM prod dependency (drop blocker); lock flat price",
    39: "DROP unless ~free — T2 ceiling $2.2K/yr revenue vs flat fee; or convert to $0.50 meter",
}

ASKS = {
    28: "Stop sending webmail junk (yahoo.com alone 25% of rows; 29% of rows DS13-blocklisted); scrub the 6.4% Googlebot IPs",
    40: "Billed domains are cookie-sync infra junk (nextmillmedia 9%, programmaticx 8%) — send real page URLs, not sync pixels",
    33: "Fix the URL-doubling bug: 77% of URLs malformed (host concatenated twice, e.g. 'msn.comhttps') — even billed domains are corrupted",
    24: "Cleanest feed of the roster — asks: populate user_agent and advertiser_id (both 0%)",
    36: "Scale is the problem (1.5M rows/day, ~700x below siblings); also one IP = 3.5% of all rows — dedupe server-side",
    25: "outbrain.com widget URLs = 52.7% of rows — send the publisher page URL, not the widget iframe URL",
    26: "Filter adult content at source (explicit domains in sample); otherwise strongest domain breadth on the roster",
    39: "94% of rows are *.myshopify.com checkout — diversify beyond Shopify or price accordingly; tiny scale (169K rows/day)",
    23: "internal log — n/a",
    30: "internal log — n/a",
}

PENDING_7B = "pending scan (q7b running)"
PENDING_7C = "pending scan (q7c running)"


def g7b(d, cohort, field):
    try:
        return float(q7b[d][cohort][field])
    except (KeyError, TypeError):
        return None


def g7c(d, cohort, field):
    try:
        return float(q7c[d][cohort][field])
    except (KeyError, TypeError):
        return None


# ---------------- per-row cell builders ----------------
def contract_only(d):
    """Columns that only get CONTRACT & IDENTITY + bill rows."""
    return d in OOS


def f_dsid(d):
    sib = {28: " (sibling of DS40 — same vendor)", 40: " (sibling of DS28 — same vendor)"}
    return f"DS{d}" + sib.get(d, "")


def f_billing(d):
    if d in FREE:
        return "free — internal MNTN log"
    reg = q0.get(d, {}).get("reg", {})
    bt = reg.get("billing_type", "?")
    if bt == "fixed_cpm":
        return "metered CPM"
    if bt == "flat_fee":
        return "flat fee"
    if bt == "variable_cpm":
        return "variable CPM"
    return bt


def f_rate(d):
    if d in FREE:
        return "$0"
    reg = q0.get(d, {}).get("reg", {})
    if reg.get("billing_type") == "flat_fee":
        return "flat — amount pending (Maya / renewal schedule)"
    if d == 35:
        return "variable, ~$1.19-1.32 CPM realized"
    if d == 17:
        return "$0.95 CPM (was $1.20 until May)"
    cpm = reg.get("fixed_cpm")
    return f"${float(cpm):.2f} CPM" if cpm else "?"


def f_renewal(d):
    if d in FREE:
        return "n/a — internal"
    if d == 39:
        return "renewal LIVE NOW — pass/play answer due Mon 2026-07-13 (Paulo)"
    if d == 27:
        return "disabled since delivery never scaled (enabled=false)"
    reg = q0.get(d, {}).get("reg", {})
    vf = (reg.get("valid_from") or "?")[:10]
    return f"contract valid_from {vf}; renewal date pending (Maya / renewal schedule)"


def f_ingest(d):
    if d in (23, 25, 26, 28, 30, 36):
        return "batch drop -> daily ingest DAG (ENABLED_DSIDS); off-switch: Data Eng DAG config"
    if d in (24, 33, 39, 40):
        return f"real-time pixel -> Kafka (fpa_dsid{d}_kafka_log); off-switch: Kafka topic (Data Eng)"
    if d == 27:
        return "was metered batch; disabled"
    if d in (35, 17):
        return "3P interests pipeline (not svs)"
    if d == 29:
        return "CRM ingestion pipeline (not svs)"
    return "?"


def f_blast(d):
    if d == 26:
        return "HEM (hashed-email) feed powers CRM/identity matching in PROD — hard drop blocker beyond MM"
    if d in FREE:
        return "internal platform log — never drop"
    if d in (35, 17):
        return "3P interests targeting layer (outside MM scope here)"
    if d == 29:
        return "CRM audience ingestion (outside MM scope here)"
    if d == 27:
        return "none (disabled)"
    return "none known — MM site-visit only"


def f_rows30(d):
    if d not in q1:
        return None
    return f"{fmtn(q1[d]['rows'])} rows"


def f_median_day(d):
    if d not in q1:
        return None
    days = sorted(q1[d]["days"])
    med = days[len(days) // 2] if len(days) % 2 else (days[len(days) // 2 - 1] + days[len(days) // 2]) / 2
    return f"{fmtn(med)}/day"


def f_weakest(d):
    if d not in q1:
        return None
    days = sorted(q1[d]["days"])
    med = days[len(days) // 2] if len(days) % 2 else (days[len(days) // 2 - 1] + days[len(days) // 2]) / 2
    weakest = min(q1[d]["days"])
    partial = sum(1 for x in q1[d]["days"] if x < 0.5 * med)
    return f"{100 * weakest / med:.0f}% of median; {partial} day(s) <50% median"


def f_liveness(d):
    if d not in q1:
        return None
    n = len(q1[d]["days"])
    ok = "PASS" if n >= 0.95 * 30 else "FAIL"
    return f"{n}/30 days ({ok} >=95% gate)"


def f_ipv6(d):
    if d not in q1:
        return None
    return pc(100 * q1[d]["ipv6"] / q1[d]["rows"], 1)


def f_ips30(d):
    return fmtn(q2[d]["ips_30d"]) if d in q2 else None


def f_dom30(d):
    return fmtn(q2[d]["domains_30d"]) if d in q2 else None


def f_pairs30(d):
    return fmtn(q2[d]["ip_domain_pairs_30d"]) if d in q2 else None


def f_urlbad(d):
    if d not in q1c:
        return None
    return f"{pc(q1c[d]['url_parse_fail_pct'], 2)} unparseable / {pc(q1c[d]['url_malformed_pct'], 2)} malformed"


def f_bots(d):
    if d not in q1c:
        return None
    ua = q1c[d].get("ua_bot_pct") or ""
    ua_s = f", {pc(ua, 1)} bot UA" if ua not in ("", None) else " (no UA sent)"
    return f"{pc(q1c[d]['pct_googlebot_ip'], 2)} Googlebot IPs{ua_s}"


def f_conc(d):
    if d not in q1c:
        return None
    td = q1c[d]["top_domain"] or "(empty)"
    return f"{td} {pc(q1c[d]['top_domain_share'], 1)} (top-5 {pc(q1c[d]['top5_domain_share'], 1)})"


def f_integrity(d):
    if d not in q1c:
        return None
    dup = max(0.0, float(q1c[d]["uid_dup_pct"]))
    return (f"private IPs {pc(q1c[d]['pct_private_ip'], 3)}; uid dupes {pc(dup, 2)}; "
            f"top-1 timestamp {pc(q1c[d]['time_top1_share'], 2)} (stamping check)")


def f_richness(d):
    if d not in q1b:
        return None
    f = q1b[d]
    path = 100 * q1[d]["path"] / q1[d]["rows"] if d in q1 and q1[d]["rows"] else 0
    return (f"UA {f.get('user_agent', 0):.0f}%; URL {f.get('url', 0):.0f}% "
            f"(path {path:.0f}%); qparams {f.get('query_parameters', 0):.0f}%; "
            f"advertiser_id {f.get('advertiser_id', 0):.0f}%")


def f_rows_used(d):
    if d not in q2c:
        return None
    r = q2c[d]
    return f"{fmtn(r['rows_used'])} ({pc(100 * int(r['rows_used']) / int(r['rows_raw']), 1)} of delivered)"


def f_drop_decomp(d):
    if d not in q2b:
        return None
    r = q2b[d]
    bot = 100 * int(r["rows_bot_ua"]) / int(r["rows_day"])
    return (f"hard-drop {pc(r['pct_hard_dropped'], 1)}; DS13-blocklist {pc(r['pct_blocked_ds13'], 1)}; "
            f"bot-UA {pc(bot, 1)}")


def f_ips_used(d):
    if d not in q2c:
        return None
    r = q2c[d]
    return f"{fmtn(r['ips_used'])} ({pc(100 * int(r['ips_used']) / int(r['ips_raw']), 1)})"


def f_dom_used(d):
    if d not in q2c:
        return None
    r = q2c[d]
    return f"{fmtn(r['domains_classified'])} classified ({pc(100 * int(r['domains_classified']) / int(r['domains_raw']), 1)} of delivered)"


def f_pairs_usable(d):
    if d not in q3 or d not in q2:
        return None
    p = 100 * int(q3[d]["usable_pairs"]) / int(q2[d]["ip_domain_pairs_30d"])
    ps = "~100%" if p > 100 else pc(p, 1)   # >100 = sub-1% q3-vs-q2 window mismatch
    return f"{fmtn(q3[d]['usable_pairs'])} ({ps})"


def _share(d, table, field):
    tot = sum(float(r[field]) for r in table.values())
    return f"{pc(100 * float(table[d][field]) / tot, 1)} of column total (sources overlap)"


def f_rows_used_share(d):
    return _share(d, q2c, "rows_used") if d in q2c else None


def f_ips_used_share(d):
    return _share(d, q2c, "ips_used") if d in q2c else None


def f_dom_used_share(d):
    return _share(d, q2c, "domains_classified") if d in q2c else None


def f_sole_ips(d):
    if d not in q3:
        return None
    return f"{fmtn(q3[d]['sole_ips'])} ({pc(100 * int(q3[d]['sole_ips']) / int(q3[d]['usable_ips']), 1)} of usable)"


def f_sole_dom(d):
    if d not in q4:
        return None
    return f"{fmtn(q4[d]['sole_domains'])} sole / {fmtn(q4[d]['sole_classified'])} sole classified"


def f_ppi(d):
    return f"{float(q3[d]['pairs_per_ip']):.1f} pairs/IP" if d in q3 else None


def f_fresh_mix(d):
    if d not in q3r:
        return None
    r = q3r[d]
    return (f"sole {pc(r['pct_sole'], 1)} / freshest {pc(r['pct_freshest'], 1)} / "
            f"tied {pc(r['pct_tied'], 1)} / stale {pc(r['pct_stale'], 1)}")


def f_netnew(d):
    if d in FREE:
        return "— (is a free log)"
    if d not in q3r:
        return None
    return pc(q3r[d]["pct_netnew_vs_free"], 1)


def f_marginal(d):
    if d not in add_gain:
        return "— (free logs always kept)" if d in FREE else None
    rank = add_order.index(d) + 1
    return f"+{add_gain[d]:.2f}pp (#{rank} in add-order)"


def f_touched_served(d):
    if d not in q5 or "touched" not in q5[d]:
        return None
    r = q5[d]["touched"]
    return f"{fmtn(r['vendor_ips'])} touched -> {fmtn(r['delivered_ips'])} served-won ({pc(r['pct_delivered'], 1)})"


def f_sole_served(d):
    if d not in q6 or d not in q3:
        return None
    return f"{fmtn(q6[d]['ips_sole'])} ({pc(100 * float(q6[d]['ips_sole']) / float(q3[d]['sole_ips']), 2)} of sole stock)"


def f_shared_served(d):
    if d not in q6:
        return None
    sh = float(q6[d]["ips_touched"]) - float(q6[d]["ips_sole"])
    return f"{fmtn(sh)} ({pc(100 * sh / float(q6[d]['ips_touched']), 1)} of served)"


def f_sole_bids(d):
    if d not in q6:
        return None
    imps, ips = float(q6[d]["imps_sole"]), float(q6[d]["ips_sole"])
    per = imps / ips if ips else 0
    return f"{fmtn(imps)}/wk ({per:.1f}/served IP) -> {fmtn(imps * 52)}/yr"


def f_billed_dom(d):
    if d not in q1d or not q1d[d].get("billed_domains"):
        return "— (no meter)" if d in FLAT else ("— (free)" if d in FREE else None)
    return f"{fmtn(q1d[d]['billed_domains'])} billed domains (June)"


def f_served_share(d):
    if d not in q6 or not PLAT_IPS:
        return None
    return f"{pc(100 * float(q6[d]['ips_touched']) / PLAT_IPS, 1)} of platform served IPs (week)"


def _tier(d, num_field, pct_field, extra_overall=False):
    if d not in q5 or "touched" not in q5[d]:
        return None
    r = q5[d]["touched"]
    s = f"{fmtn(r[num_field])} ({pc(r[pct_field], 1)})"
    if extra_overall:
        tot = sum(float(q5[x]["touched"][num_field]) for x in ACTIVE if x in q5 and "touched" in q5[x])
        s += f"; {pc(100 * float(r[num_field]) / tot, 1)} of column total"
    return s


def f_hi(d):
    return _tier(d, "hi_10000", "pct_hi", extra_overall=True)


def f_pp(d):
    return _tier(d, "pp_8000", "pct_pp", extra_overall=True)


def f_hgrad(d):
    return _tier(d, "high_grad", "pct_high_grad")


def f_mid_max_unscored(d):
    if d not in q5 or "touched" not in q5[d]:
        return None
    r = q5[d]["touched"]
    return (f"mid {pc(r['pct_mid'], 1)} / max-reach {pc(r['pct_maxreach'], 1)} / "
            f"unscored {pc(r['pct_unscored_delivered'], 1)}")


def f_avg_hs_touched(d):
    v = g7b(d, "touched", "avg_hs_scored")
    if v is None:
        return PENDING_7B if d in ACTIVE else None
    ps = g7b(d, "touched", "pct_scored")
    return f"{v:,.0f} (scored imps only; {ps:.0f}% scored)"


def f_avg_hs_sole(d):
    v = g7b(d, "sole", "avg_hs_scored")
    if v is None:
        return PENDING_7B if d in ACTIVE else None
    ps = g7b(d, "sole", "pct_scored")
    return f"{v:,.0f} (scored imps only; {ps:.0f}% scored)"


def f_spend_t(d):
    return money(q6[d]["media_touched"]) + "/wk" if d in q6 else None


def f_imps_t(d):
    return fmtn(q6[d]["imps_touched"]) + "/wk" if d in q6 else None


def f_visits_t(d):
    v = g7b(d, "touched", "visits")
    return f"{fmtn(v)}/wk" if v is not None else (PENDING_7B if d in ACTIVE else None)


def f_conv_t(d):
    v = g7c(d, "touched", "conversions")
    return f"{fmtn(v)}/wk" if v is not None else (PENDING_7C if d in ACTIVE else None)


def f_rev_t(d):
    v = g7c(d, "touched", "revenue")
    return money(v) + "/wk" if v is not None else (PENDING_7C if d in ACTIVE else None)


def f_cpm_t(d):
    if d not in q6:
        return None
    return f"${1000 * float(q6[d]['media_touched']) / float(q6[d]['imps_touched']):.2f}"


def f_ivr_t(d):
    v = g7b(d, "touched", "vr_pct")
    return pc(v, 4) if v is not None else (PENDING_7B if d in ACTIVE else None)


def f_cvr_t(d):
    c, v = g7c(d, "touched", "conversions"), g7b(d, "touched", "visits")
    if c is None or v is None:
        return PENDING_7C if d in ACTIVE else None
    return pc(100 * c / v, 2) if v else "n/a (0 visits)"


def f_aov_t(d):
    c, rv = g7c(d, "touched", "conversions"), g7c(d, "touched", "revenue")
    if c is None:
        return PENDING_7C if d in ACTIVE else None
    return money(rv / c) if c else "n/a (0 conv)"


def f_roas_t(d):
    rv = g7c(d, "touched", "revenue")
    if rv is None:
        return PENDING_7C if d in ACTIVE else None
    sp = float(q6[d]["media_touched"])
    return f"{rv / sp:.2f}x" if sp else "n/a"


def f_spend_s(d):
    return money(q6[d]["media_sole"]) + "/wk" if d in q6 else None


def f_imps_s(d):
    return fmtn(q6[d]["imps_sole"]) + "/wk" if d in q6 else None


def f_visits_s(d):
    if d not in q7:
        return None
    k = int(q7[d]["sole_visits"])
    lo, hi = _poisson_ci(k)
    return f"{k:,}/wk (95% CI {lo:.0f}-{hi:.0f})"


def f_conv_s(d):
    v = g7c(d, "sole", "conversions")
    return f"{v:,.0f}/wk" if v is not None else (PENDING_7C if d in ACTIVE else None)


def f_rev_s(d):
    v = g7c(d, "sole", "revenue")
    return money(v) + "/wk" if v is not None else (PENDING_7C if d in ACTIVE else None)


def f_cpm_s(d):
    if d not in q6 or not float(q6[d]["imps_sole"]):
        return None
    return f"${1000 * float(q6[d]['media_sole']) / float(q6[d]['imps_sole']):.2f}"


def f_ivr_s(d):
    if d not in q7:
        return None
    v = float(q7[d]["vr_overall_pct"])
    return f"{pc(v, 4)} ({v / BASELINE_VR:.1f}x baseline {BASELINE_VR}%)"


def f_cvr_s(d):
    c = g7c(d, "sole", "conversions")
    if c is None:
        return PENDING_7C if d in ACTIVE else None
    v = int(q7[d]["sole_visits"]) if d in q7 else 0
    return pc(100 * c / v, 2) if v else ("0 conv / 0 visits" if not c else f"{c:,.0f} conv / 0 visits")


def f_aov_s(d):
    c, rv = g7c(d, "sole", "conversions"), g7c(d, "sole", "revenue")
    if c is None:
        return PENDING_7C if d in ACTIVE else None
    return money(rv / c) if c else "n/a (0 conv)"


def f_roas_s(d):
    rv = g7c(d, "sole", "revenue")
    if rv is None:
        return PENDING_7C if d in ACTIVE else None
    sp = float(q6[d]["media_sole"])
    return f"{rv / sp:.2f}x" if sp else "n/a"


def f_bill(d):
    if d in FREE:
        return "$0 — internal"
    jb = june_bill(d)
    if jb is not None:
        return f"{money(jb)} June -> {money(jb * 12)}/yr run rate"
    if d in FLAT:
        return "flat fee — amount pending (Maya / renewal schedule)"
    if d == 27:
        return "$0 (disabled)"
    return None


def f_pct_billed(d):
    if d in FREE:
        return "— (free)"
    if d in FLAT:
        return "— (no meter; flat fee regardless of use)"
    if d not in q1d or d not in q1:
        return None
    monthly_rows = q1[d]["rows"] * (365.0 / 12) / 30.0
    p = 100 * float(q1d[d]["billed_imps"]) / monthly_rows
    junk = ""
    if d == 40:
        junk = "; billed list = cookie-sync junk"
    if d == 33:
        junk = "; billed list shows concat-corrupted domains"
    return f"{pc(p, 2)} of delivered rows billed{junk}"


def f_attrib(d):
    if d not in q6b:
        return None
    a = q6b[d]
    return f"{pc(100 * a['prosp'] / a['tot'], 1)} of sole-IP serves via prospecting family (vendor-dependent)"


def f_cpm_all_rows(d):
    if d in FREE:
        return "— (free)"
    if d not in q6 or d not in q1:
        return None
    val = t2_ann(d) * MARGINS[-1]
    ann_rows = q1[d]["rows"] * ANN30
    return f"${1000 * val / ann_rows:.4f} CPM max (at 30% margin)"


def f_cpm_used(d):
    if d in FREE:
        return "— (free)"
    if d not in q6:
        return None
    val = t2_ann(d) * MARGINS[-1]
    if d in q1d and q1d[d].get("billed_imps"):
        ann_used = float(q1d[d]["billed_imps"]) * 12
        return f"${1000 * val / ann_used:.4f} CPM max vs $0.50 paid"
    if d in q2c:
        ann_used = float(q2c[d]["rows_used"]) * ANN30
        return f"${1000 * val / ann_used:.4f} CPM max (hypothetical meter)"
    return None


def f_flat_equiv(d):
    if d in FREE:
        return "— (free)"
    if d not in q6:
        return None
    t1, t2 = t1_ann(d), t2_ann(d)
    return (f"floor {money(t1 * 0.15)} (T1 x 15%) -> fair {money(t2 * 0.20)} (T2 x 20%) "
            f"-> ceiling {money(t2 * 0.30)} (T2 x 30%)")


def f_t2(d):
    if d not in q6:
        return None
    t2 = t2_ann(d)
    return f"{money(t2)}/yr revenue (envelope {money(t2 * 0.4)}-{money(t2 * 1.8)})"


def f_t1(d):
    if d not in q6:
        return None
    return f"{money(t1_ann(d))}/yr (score-gated, non-RTC)"


def f_feeband(d):
    if d in FREE:
        return "— (free)"
    if d not in q4:
        return None
    sc = float(q4[d]["sole_classified"])
    return f"{money(sc * 3)}-{money(sc * 13)}/yr ({fmtn(sc)} sole classified x $3-13)"


def f_loo_savings(d):
    if d in FREE:
        return "— (never drop)"
    if d in FLAT:
        return "savings = the flat fee itself (amount pending); no meter to recover"
    if d not in reassign or d not in q1d:
        return None
    rr = reassign[d]
    tot = sum(rr.values())
    sh = 1 - rr.get("metered", 0) / tot
    bill = float(q1d[d]["billed_usd"]) * 12
    return f"{money(bill * sh)}/yr ({pc(100 * sh, 0)} of {money(bill)} bill)"


def f_loo_mix(d):
    if d in FREE:
        return "— (never drop)"
    if d not in reassign:
        return "— (no meter; coverage effect in next row)" if d in FLAT else None
    rr = reassign[d]
    tot = sum(rr.values())

    def s(k):
        return 100 * rr.get(k, 0) / tot

    return (f"vanish {s('none'):.0f}% / ->flat-fee {s('flat_fee'):.0f}% / "
            f"->free {s('free_first') + s('free_later'):.0f}% / ->metered {s('metered'):.1f}%")


def f_loo_cov(d):
    if d in FREE:
        return "— (never drop)"
    if d not in coverage_lost:
        return None
    return f"-{coverage_lost[d]:.2f}pp of usable-pair coverage"


def f_score(d):
    if d in FREE:
        return "n/a — internal log (not scored)"
    if d not in CURVE:
        return None
    raw, curved = CURVE[d]
    return f"{curved:.0f} (raw {raw:.1f})"


def f_verdict(d):
    if d in FREE:
        return "KEEP — free internal log"
    return VERDICTS.get(d)


def f_asks(d):
    return ASKS.get(d)


ROWFN = {
    3: f_dsid, 4: f_billing, 5: f_rate, 6: f_renewal, 7: f_ingest, 8: f_blast,
    11: f_rows30, 12: f_median_day, 13: f_weakest, 14: f_liveness, 15: f_ipv6,
    16: f_ips30, 17: f_dom30, 18: f_pairs30,
    21: f_urlbad, 22: f_bots, 23: f_conc, 24: f_integrity, 25: f_richness,
    28: f_rows_used, 29: f_drop_decomp, 30: f_ips_used, 31: f_dom_used,
    32: f_pairs_usable, 33: f_rows_used_share, 34: f_ips_used_share, 35: f_dom_used_share,
    38: f_sole_ips, 39: f_sole_dom, 40: f_ppi, 41: f_fresh_mix, 42: f_netnew, 43: f_marginal,
    46: f_touched_served, 47: f_sole_served, 48: f_shared_served, 49: f_sole_bids,
    50: f_billed_dom, 51: f_served_share,
    54: f_hi, 55: f_pp, 56: f_hgrad, 57: f_mid_max_unscored,
    58: f_avg_hs_touched, 59: f_avg_hs_sole,
    62: f_spend_t, 63: f_imps_t, 64: f_visits_t, 65: f_conv_t, 66: f_rev_t,
    67: f_cpm_t, 68: f_ivr_t, 69: f_cvr_t, 70: f_aov_t, 71: f_roas_t,
    74: f_spend_s, 75: f_imps_s, 76: f_visits_s, 77: f_conv_s, 78: f_rev_s,
    79: f_cpm_s, 80: f_ivr_s, 81: f_cvr_s, 82: f_aov_s, 83: f_roas_s,
    86: f_bill, 87: f_pct_billed, 88: f_attrib,
    91: f_cpm_all_rows, 92: f_cpm_used, 93: f_flat_equiv, 94: f_t2, 95: f_t1, 96: f_feeband,
    99: f_loo_savings, 100: f_loo_mix, 101: f_loo_cov,
    104: f_score, 105: f_verdict, 106: f_asks,
}

# Rows that out-of-scope columns still get real answers for.
OOS_ROWS = {3, 4, 5, 6, 7, 8, 86}


def main():
    import openpyxl
    wb = openpyxl.load_workbook(TEMPLATE)
    ws = wb["Sheet2"]
    pending = filled = 0
    for row_idx, fn in ROWFN.items():
        for col_off, d in enumerate(DS_COLS):
            cell = ws.cell(row=row_idx, column=2 + col_off)
            if contract_only(d) and row_idx not in OOS_ROWS:
                cell.value = OOS[d]
                continue
            try:
                v = fn(d)
            except Exception as e:
                v = f"ERR: {e}"
            if v is None:
                v = OOS.get(d, "—")
            if isinstance(v, str) and v.startswith("pending scan"):
                pending += 1
            else:
                filled += 1
            cell.value = v
    wb.save(OUT)
    total = len(ROWFN) * len(DS_COLS)
    print(f"wrote {OUT}")
    print(f"cells: {filled} filled, {pending} pending scans, {total} total addressable")
    empty = [(r, DS_COLS[c]) for r, fn in ROWFN.items()
             for c in range(len(DS_COLS))
             if ws.cell(row=r, column=2 + c).value in (None, "", "—")]
    if empty:
        print(f"UNFILLED ({len(empty)}):")
        for r, d in empty:
            print(f"  row {r} x DS{d}")
    else:
        print("zero empty cells — every row x column answered")


if __name__ == "__main__":
    main()
