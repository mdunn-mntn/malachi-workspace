#!/usr/bin/env python3
"""Build the filled AUDI-1089 vendor-quality workbook from the canonical run CSVs.

Output workbook (outputs/audi_1089_quality_template_filled.xlsx) has two sheets:
  - numbers: ONE question per row, ONE value per cell (real numbers with Excel
    number formats — counts, %, $, decimals — so columns sort/compute). Vendors
    are columns in the template's order. "—" = not applicable for that column.
  - notes: one row per vendor with every text answer (scope, billing detail,
    renewal status, ingestion + off-switch, blast radius, full verdict, asks)
    plus a CONVENTIONS block (windows, annualization, formulas, caveats).

The question set is the user's template (outputs/audi_1089_quality_template.xlsx)
with compound rows split so each row carries exactly one metric. Sources per
section: runbook/README.md (Template map). Economics formulas:
runbook/dependency_valuation.md; margin ladder 15/20/30% (blended estimate).

Usage: python3 fill_template.py
"""
import csv
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))
TICKET = os.path.dirname(os.path.dirname(HERE))
RDIR = os.path.join(TICKET, "outputs", "run_2026_07_10")
OUT = os.path.join(TICKET, "outputs", "audi_1089_quality_template_filled.xlsx")

DS_COLS = [28, 40, 33, 24, 36, 25, 26, 39, 27, 30, 23, 35, 17, 29]
ACTIVE = [28, 40, 33, 24, 36, 25, 26, 39, 30, 23]
EXT = [24, 25, 26, 28, 33, 36, 39, 40]
FREE = [23, 30]
METERED = [24, 28, 33, 36, 40]
FLAT = [25, 26, 39]
OOS = [27, 35, 17, 29]
SHORT = {23: "guid_log", 24: "Justuno", 25: "5x5", 26: "Predactiv", 27: "LaunchLabs",
         28: "33Across", 30: "augmentor", 33: "Sovrn", 36: "Cybba", 39: "Klickly",
         40: "33A API", 35: "LiveRamp IP", 17: "ShareThis", 29: "deepsync"}
HDR_NAMES = {28: "33Across", 40: "33Across API", 33: "Sovrn", 24: "Justuno", 36: "Cybba",
             25: "5x5", 26: "sharethis_predactiv", 39: "Klickly", 27: "LaunchLabs",
             30: "augmentor_log", 23: "guid_log", 35: "LiveRamp IP", 17: "ShareThis",
             29: "deepsync"}
BITSQ = {23: 0, 24: 1, 25: 2, 26: 3, 28: 4, 30: 5, 33: 6, 36: 7, 39: 8, 40: 9}
FREE_MASK = (1 << 0) | (1 << 5)
ANN30 = 365.0 / 30.0
BASELINE_VR = 0.0223
NA = "—"


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


q0 = {}
for r in rows_of("q0_roster_cost.csv"):
    d = int(r["data_source_id"])
    ent = q0.setdefault(d, {"reg": r, "june_usd": None})
    if r.get("reporting_month", "").startswith("2026-06"):
        ent["june_usd"] = float(r["usage_dollars"])
        ent["reg"] = r

q1 = {}
for r in rows_of("q1_scale_by_day.csv"):
    d = int(r["data_source_id"])
    a = q1.setdefault(d, {"days": [], "rows": 0, "ipv6": 0, "path": 0})
    n = int(r["n_rows"])
    a["days"].append(n)
    a["rows"] += n
    a["ipv6"] += int(r["ipv6_rows"])
    a["path"] += int(r["rows_with_path"])

q1b = {}
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

q5 = {}
for r in rows_of("q5_score_tiers.csv"):
    q5.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r

q6b = {}
for r in rows_of("q6b_sole_by_funnel.csv"):
    d = int(r["ds"])
    a = q6b.setdefault(d, {"prosp": 0, "tot": 0})
    a["tot"] += int(r["imps"])
    if r["obj_bucket"] == "prospecting_family":
        a["prosp"] += int(r["imps"])

q7bd = {}
for r in rows_of("q7b_perf_by_cohort.csv"):
    q7bd.setdefault(int(r["ds"]), {})[r["cohort"]] = r

q7cd = {}
for r in rows_of("q7c_conversions.csv"):
    q7cd.setdefault(int(r["ds"]), {})[r["cohort"]] = r

q7d = rows_of("q7d_platform_week.csv")
PLAT_IPS = float(q7d[0]["ips_served_week"]) if q7d else None

masks, reassign = {}, {}
for r in rows_of("q3b_credit_reassignment.csv"):
    if r["rec"] == "mask":
        masks[int(r["k1"])] = int(r["n_pairs"])
    elif r["rec"] == "reassign":
        reassign.setdefault(int(r["k1"]), {})[r["k2"]] = int(r["n_pairs"])


# ---------------- derived ----------------
def cov(keep):
    km = FREE_MASK
    for d in keep:
        km |= (1 << BITSQ[d])
    return sum(p for m, p in masks.items() if m & km)


FULL_COV = cov(EXT)
add_order, add_gain = [], {}
_rem, _cur = list(EXT), cov([])
while _rem:
    _best = max(_rem, key=lambda d: cov(add_order + [d]))
    _g = cov(add_order + [_best]) - _cur
    _cur = cov(add_order + [_best])
    add_order.append(_best)
    add_gain[_best] = 100.0 * _g / FULL_COV
    _rem.remove(_best)

coverage_lost = {d: 100.0 * (FULL_COV - cov([x for x in EXT if x != d])) / FULL_COV for d in EXT}


def t2_ann(d):
    return float(q6[d]["media_sole"]) * 52


def t1_ann(d):
    return float(q6[d]["media_sole_scored"]) * 52


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


def _med(d):
    days = sorted(q1[d]["days"])
    n = len(days)
    return days[n // 2] if n % 2 else (days[n // 2 - 1] + days[n // 2]) / 2


def g7b(d, c, f):
    return float(q7bd[d][c][f])


def g7c(d, c, f):
    return float(q7cd[d][c][f])


def share(d, table, field):
    tot = sum(float(r[field]) for r in table.values())
    return 100 * float(table[d][field]) / tot


def tier_tot(f):
    return sum(float(q5[x]["touched"][f]) for x in ACTIVE if x in q5 and "touched" in q5[x])


# ---------------- text banks (notes sheet) ----------------
VERDICT_SHORT = {28: "NEGOTIATE (w/ DS40)", 40: "NEGOTIATE (w/ DS28)", 33: "DROP",
                 24: "KEEP-trim", 36: "DROP", 25: "KEEP", 26: "KEEP (HEM)",
                 39: "DROP unless ~free", 23: "KEEP (free)", 30: "KEEP (free)"}
VERDICT_FULL = {
    28: "NEGOTIATE — one deal with DS40 (~$598K/yr combined); target rate cut, cite free-log overlap",
    40: "NEGOTIATE — same vendor as DS28; fold into one combined renegotiation",
    33: "DROP — 77% malformed, near-zero sole value; exact recovery $109K/yr (sequencing-safe)",
    24: "KEEP-trim — cleanest feed, 91.6% sole pairs; trim rate if possible",
    36: "DROP — subscale (~700x below siblings); $21K/yr recovery",
    25: "KEEP — anchor vendor, 69.3% sole; lock flat price at renewal",
    26: "KEEP — #1 unique classified domains + HEM prod dependency (drop blocker); lock flat price",
    39: "DROP unless ~free — T2 ceiling $2.2K/yr revenue vs flat fee; or convert to $0.50 meter",
    23: "KEEP — free internal log", 30: "KEEP — free internal log",
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
    23: "internal log — n/a", 30: "internal log — n/a",
}
SCOPE = {
    28: "Active MM site-visit DDP (batch). Same vendor as DS40 (batch vs real-time feeds, per Ryan) — one combined negotiation (~$598K/yr).",
    40: "Active MM site-visit DDP (real-time). Sibling of DS28 — same vendor.",
    33: "Active MM site-visit DDP (real-time Kafka).",
    24: "Active MM site-visit DDP (real-time Kafka).",
    36: "Active MM site-visit DDP (batch).",
    25: "Active MM site-visit DDP (batch, flat fee).",
    26: "Active MM site-visit DDP (batch, flat fee).",
    39: "Active MM site-visit DDP (real-time Kafka, flat fee).",
    23: "Internal free log (MNTN guid pixel) — always kept; $0.",
    30: "Internal free log (bid-time augmentor) — always kept; $0. In svs since 2026-05-12.",
    27: "Registered metered vendor, DISABLED (enabled=false) — no feed, no bill. Feed rows are '—'.",
    35: "NOT an MM site-visit feed: 3P interests source (variable CPM). Contract + bill rows only; feed rows are '—'.",
    17: "NOT an MM site-visit feed: 3P interests source. Contract + bill rows only; feed rows are '—'.",
    29: "NOT an MM site-visit feed: CRM ingestion source. Contract + bill rows only; feed rows are '—'.",
}
RATE_NOTE = {24: "$0.50 CPM metered (pay per used signal imp)", 28: "$0.50 CPM metered",
             33: "$0.50 CPM metered", 36: "$0.50 CPM metered", 40: "$0.50 CPM metered",
             25: "Flat fee — amount pending (Maya / renewal schedule)",
             26: "Flat fee — amount pending (Maya / renewal schedule)",
             39: "Flat fee — amount pending (Maya / renewal schedule)",
             23: "Free — internal", 30: "Free — internal",
             27: "$0.50 CPM registered, disabled",
             35: "Variable CPM, ~$1.19-1.32 realized", 17: "$0.95 CPM (was $1.20 until May)",
             29: "$0.50 CPM metered"}
RENEWAL_NOTE = {39: "Renewal LIVE NOW — pass/play answer due Mon 2026-07-13 (Paulo)",
                25: "Contract valid_from 2025-10-17; renewal date pending (Maya / renewal schedule)",
                26: "Contract valid_from 2025-10-17; renewal date pending (Maya / renewal schedule)",
                35: "Contract valid_from 2025-05-01; renewal date pending",
                27: "Disabled since delivery never scaled",
                23: "n/a — internal", 30: "n/a — internal"}
INGEST_NOTE = {}
for _d in (23, 25, 26, 28, 30, 36):
    INGEST_NOTE[_d] = "Batch drop -> daily ingest DAG (ENABLED_DSIDS); off-switch: Data Eng DAG config"
for _d in (24, 33, 39, 40):
    INGEST_NOTE[_d] = f"Real-time pixel -> Kafka (fpa_dsid{_d}_kafka_log); off-switch: Kafka topic (Data Eng)"
INGEST_NOTE[27] = "Was metered batch; disabled"
INGEST_NOTE[35] = INGEST_NOTE[17] = "3P interests pipeline (not svs)"
INGEST_NOTE[29] = "CRM ingestion pipeline (not svs)"
BLAST_NOTE = {26: "HEM (hashed-email) feed powers CRM/identity matching in PROD — hard drop blocker beyond MM",
              23: "Internal platform log — never drop", 30: "Internal platform log — never drop",
              35: "3P interests targeting layer (outside MM scope here)",
              17: "3P interests targeting layer (outside MM scope here)",
              29: "CRM audience ingestion (outside MM scope here)",
              27: "None (disabled)"}
for _d in (28, 40, 33, 24, 36, 25, 39):
    BLAST_NOTE[_d] = "None known — MM site-visit only"

CONVENTIONS = [
    "One question per row; one value per cell. '—' = not applicable for that column (free log, flat fee with no meter, disabled, or out-of-scope source — see Scope).",
    "Numbers are real Excel values (sortable); % cells store the percent number (21.8 = 21.8%).",
    "'share of column total' rows: sources overlap, so these sum >100% down a row... across vendors.",
    "'% of pairs usable' can read slightly >100%: q3 usable pairs marginally exceed q2 delivered pairs (<1% window mismatch between scans).",
    "'% of platform served IPs': denominator = 28.03M distinct served IPs in the valuation week (q7d).",
    "Windows: delivery metrics = 30d (2026-06-02..07-01); serving/performance = 37d svs union x valuation week 2026-07-02..08.",
    "Annualization: weekly flows x52; 30d stocks x365/30; June bills x12. Never annualize unique-IP stocks.",
    "Economics: T2 = sole-won media x52 (dependency ceiling); T1 = score-gated sole media x52 (provable floor); margins 15/20/30% on observed eCPM.",
    "Max justified CPM = (T2 x 30% margin) spread over 100% of delivered rows, or over billed/used imps (compare to the $0.50 we pay). Flat vendors on the used-imps row = hypothetical meter.",
    "Flat-contract equivalent rows: floor = T1 x 15%, fair = T2 x 20%, ceiling = T2 x 30%.",
    "T2 envelope = T2 x0.4..x1.8 (volume x0.5-1.5, CPM x0.8-1.2) — scenario range, not a confidence interval.",
    "Exact drop savings = annual bill x share of credits NOT reassigning to another metered vendor (q3b first-reporter classes). Flat vendors: savings = the flat fee itself.",
    "Sole-cohort conversion/visit counts are Poisson-tiny — read 0 as '<~1/wk', not exactly zero. CVR-sole is '—' when sole visits = 0.",
    "Touched-cohort performance mirrors the platform (pools cover 12-97% of served IPs) — vendor differentiation lives in the SOLE rows.",
    "Conversions (q7c): ui_conversions deduped to one row per conversion event preferring last-touch; assists + disputed excluded; revenue = order_amt.",
    "Row sources: runbook/README.md 'Template map' (q0..q7d, one SQL + one CSV each).",
]


# ---------------- SPEC: (label, fmt, fn, oos_ok) ----------------
def S(label):
    return (label, None, None, False)


def R(label, fmt, fn, oos_ok=False):
    return (label, fmt, fn, oos_ok)


SPEC = [
    S("CONTRACT & IDENTITY"),
    R("Data source ID", "int", lambda d: d, True),
    R("Billing type", "txt", lambda d: "free" if d in FREE else
      {"fixed_cpm": "metered CPM", "flat_fee": "flat fee", "variable_cpm": "variable CPM"}
      .get(q0[d]["reg"]["billing_type"]), True),
    R("Contract rate ($ CPM; flat amounts pending)", "usd2", lambda d:
      0.0 if d in FREE else ("pending" if d in FLAT else
      (1.28 if d == 35 else float(q0[d]["reg"]["fixed_cpm"]))), True),
    R("Renewal status", "txt", lambda d: NA if d in FREE else
      ("LIVE NOW" if d == 39 else ("disabled" if d == 27 else "pending")), True),
    R("Ingestion path", "txt", lambda d: "batch" if d in (23, 25, 26, 28, 30, 36) else
      ("Kafka RT" if d in (24, 33, 39, 40) else ("disabled" if d == 27 else NA)), True),
    R("Non-MM blast radius", "txt", lambda d: {26: "HEM -> CRM prod", 23: "internal",
      30: "internal", 35: "3P interests", 17: "3P interests", 29: "CRM", 27: "none"}
      .get(d, "none known"), True),

    S("FEED SCALE (30d)"),
    R("Total rows delivered", "int", lambda d: q1[d]["rows"]),
    R("Median rows/day", "int", lambda d: _med(d)),
    R("Weakest day (% of median)", "pct0", lambda d: 100 * min(q1[d]["days"]) / _med(d)),
    R("Days <50% of median (count)", "int", lambda d: sum(1 for x in q1[d]["days"] if x < 0.5 * _med(d))),
    R("Days delivered (of 30)", "int", lambda d: len(q1[d]["days"])),
    R("% IPv6 rows", "pct", lambda d: 100 * q1[d]["ipv6"] / q1[d]["rows"]),
    R("Unique IPs delivered", "int", lambda d: int(q2[d]["ips_30d"])),
    R("Unique domains delivered", "int", lambda d: int(q2[d]["domains_30d"])),
    R("Unique IP x domain pairs delivered", "int", lambda d: int(q2[d]["ip_domain_pairs_30d"])),

    S("DATA QUALITY (junk flags)"),
    R("% URLs unparseable", "pct2", lambda d: float(q1c[d]["url_parse_fail_pct"])),
    R("% URLs malformed", "pct2", lambda d: float(q1c[d]["url_malformed_pct"])),
    R("% Googlebot IPs", "pct2", lambda d: float(q1c[d]["pct_googlebot_ip"])),
    R("% bot user-agents", "pct", lambda d: float(q1c[d]["ua_bot_pct"]) if q1c[d].get("ua_bot_pct") else None),
    R("Top-1 domain", "txt", lambda d: q1c[d]["top_domain"] or NA),
    R("Top-1 domain share", "pct", lambda d: float(q1c[d]["top_domain_share"])),
    R("Top-5 domain share", "pct", lambda d: float(q1c[d]["top5_domain_share"])),
    R("% private IPs", "pct3", lambda d: float(q1c[d]["pct_private_ip"])),
    R("% uid duplicates (clamped >=0)", "pct2", lambda d: max(0.0, float(q1c[d]["uid_dup_pct"]))),
    R("Top-1 timestamp share (stamping check)", "pct2", lambda d: float(q1c[d]["time_top1_share"])),
    R("% user_agent populated", "pct0", lambda d: q1b[d].get("user_agent", 0.0)),
    R("% url populated", "pct0", lambda d: q1b[d].get("url", 0.0)),
    R("% URLs with path", "pct0", lambda d: 100 * q1[d]["path"] / q1[d]["rows"]),
    R("% query_parameters populated", "pct0", lambda d: q1b[d].get("query_parameters", 0.0)),
    R("% advertiser_id populated", "pct0", lambda d: q1b[d].get("advertiser_id", 0.0)),

    S("USABLE FUNNEL (survives to DS13/DS19)"),
    R("Rows used", "int", lambda d: int(q2c[d]["rows_used"])),
    R("% of rows used (within vendor)", "pct", lambda d: 100 * int(q2c[d]["rows_used"]) / int(q2c[d]["rows_raw"])),
    R("% rows hard-dropped", "pct", lambda d: float(q2b[d]["pct_hard_dropped"])),
    R("% rows DS13-blocklisted", "pct", lambda d: float(q2b[d]["pct_blocked_ds13"])),
    R("% rows bot-UA", "pct", lambda d: 100 * int(q2b[d]["rows_bot_ua"]) / int(q2b[d]["rows_day"])),
    R("Unique IPs used", "int", lambda d: int(q2c[d]["ips_used"])),
    R("% of unique IPs used (within vendor)", "pct", lambda d: 100 * int(q2c[d]["ips_used"]) / int(q2c[d]["ips_raw"])),
    R("Unique domains used (classified)", "int", lambda d: int(q2c[d]["domains_classified"])),
    R("% of domains classified (within vendor)", "pct", lambda d: 100 * int(q2c[d]["domains_classified"]) / int(q2c[d]["domains_raw"])),
    R("Usable IP x domain pairs", "int", lambda d: int(q3[d]["usable_pairs"])),
    R("% of pairs usable", "pct", lambda d: 100 * int(q3[d]["usable_pairs"]) / int(q2[d]["ip_domain_pairs_30d"])),
    R("% of rows used — share of column total", "pct", lambda d: share(d, q2c, "rows_used")),
    R("% of IPs used — share of column total", "pct", lambda d: share(d, q2c, "ips_used")),
    R("% of domains used — share of column total", "pct", lambda d: share(d, q2c, "domains_classified")),

    S("UNIQUENESS & FRESHNESS (vs all other sources incl. free logs)"),
    R("Sole usable IPs", "int", lambda d: int(q3[d]["sole_ips"])),
    R("% of usable IPs sole", "pct", lambda d: 100 * int(q3[d]["sole_ips"]) / int(q3[d]["usable_ips"])),
    R("Sole usable domains", "int", lambda d: int(q4[d]["sole_domains"])),
    R("Sole CLASSIFIED domains (fee-band axis)", "int", lambda d: int(q4[d]["sole_classified"])),
    R("Pairs per IP (visit density)", "dec1", lambda d: float(q3[d]["pairs_per_ip"])),
    R("% pairs sole", "pct", lambda d: float(q3r[d]["pct_sole"])),
    R("% pairs freshest", "pct", lambda d: float(q3r[d]["pct_freshest"])),
    R("% pairs tied", "pct", lambda d: float(q3r[d]["pct_tied"])),
    R("% pairs stale", "pct", lambda d: float(q3r[d]["pct_stale"])),
    R("% net-new vs free logs", "pct", lambda d: None if d in FREE else float(q3r[d]["pct_netnew_vs_free"])),
    R("Marginal coverage when added (pp)", "dec2", lambda d: add_gain.get(d)),
    R("Frontier add-order rank", "int", lambda d: add_order.index(d) + 1 if d in add_order else None),

    S("SERVING & WON BIDS (valuation week; served = won impression)"),
    R("Touched IPs (37d union)", "int", lambda d: int(q5[d]["touched"]["vendor_ips"])),
    R("Served-won IPs", "int", lambda d: int(q5[d]["touched"]["delivered_ips"])),
    R("% of touched IPs served-won", "pct", lambda d: float(q5[d]["touched"]["pct_delivered"])),
    R("Sole IPs served-won", "int", lambda d: int(q6[d]["ips_sole"])),
    R("% of sole stock served-won", "pct2", lambda d: 100 * float(q6[d]["ips_sole"]) / float(q3[d]["sole_ips"])),
    R("Shared IPs served-won", "int", lambda d: float(q6[d]["ips_touched"]) - float(q6[d]["ips_sole"])),
    R("% of served IPs shared", "pct", lambda d: 100 * (float(q6[d]["ips_touched"]) - float(q6[d]["ips_sole"])) / float(q6[d]["ips_touched"])),
    R("Sole won bids / wk", "int", lambda d: float(q6[d]["imps_sole"])),
    R("Sole won bids per served sole IP", "dec1", lambda d: float(q6[d]["imps_sole"]) / float(q6[d]["ips_sole"])),
    R("Sole won bids annualized (x52)", "int", lambda d: float(q6[d]["imps_sole"]) * 52),
    R("Billed domains (June)", "int", lambda d: int(q1d[d]["billed_domains"]) if d in q1d and q1d[d].get("billed_domains") else None),
    R("% of platform served IPs touched (week)", "pct", lambda d: 100 * float(q6[d]["ips_touched"]) / PLAT_IPS),

    S("SCORE QUALITY (of served/delivered IPs, touched cohort)"),
    R("HI 10000 count", "int", lambda d: int(q5[d]["touched"]["hi_10000"])),
    R("% HI (within vendor)", "pct", lambda d: float(q5[d]["touched"]["pct_hi"])),
    R("% HI — share of column total", "pct", lambda d: 100 * float(q5[d]["touched"]["hi_10000"]) / tier_tot("hi_10000")),
    R("PP 8000 count", "int", lambda d: int(q5[d]["touched"]["pp_8000"])),
    R("% PP (within vendor)", "pct", lambda d: float(q5[d]["touched"]["pct_pp"])),
    R("% PP — share of column total", "pct", lambda d: 100 * float(q5[d]["touched"]["pp_8000"]) / tier_tot("pp_8000")),
    R("High-graduated count (Fangorn band)", "int", lambda d: int(q5[d]["touched"]["high_grad"])),
    R("% high-graduated", "pct", lambda d: float(q5[d]["touched"]["pct_high_grad"])),
    R("% mid", "pct", lambda d: float(q5[d]["touched"]["pct_mid"])),
    R("% max-reach", "pct", lambda d: float(q5[d]["touched"]["pct_maxreach"])),
    R("% unscored", "pct", lambda d: float(q5[d]["touched"]["pct_unscored_delivered"])),
    R("Avg household score — touched (scored imps)", "int", lambda d: g7b(d, "touched", "avg_hs_scored")),
    R("% imps scored — touched", "pct", lambda d: g7b(d, "touched", "pct_scored")),
    R("Avg household score — sole (scored imps)", "int", lambda d: g7b(d, "sole", "avg_hs_scored")),
    R("% imps scored — sole", "pct", lambda d: g7b(d, "sole", "pct_scored")),

    S("PERFORMANCE — TOUCHED-WON COHORT (per week; mirrors platform — see notes)"),
    R("Spend (media $) — touched", "usd", lambda d: float(q6[d]["media_touched"])),
    R("Impressions (won bids) — touched", "int", lambda d: float(q6[d]["imps_touched"])),
    R("Visits — touched", "int", lambda d: g7b(d, "touched", "visits")),
    R("Conversions — touched", "int", lambda d: g7c(d, "touched", "conversions")),
    R("Revenue — touched", "usd", lambda d: g7c(d, "touched", "revenue")),
    R("CPM — touched", "usd2", lambda d: 1000 * float(q6[d]["media_touched"]) / float(q6[d]["imps_touched"])),
    R("IVR — touched", "pct3", lambda d: g7b(d, "touched", "vr_pct")),
    R("CVR (conv/visits) — touched", "pct2", lambda d: 100 * g7c(d, "touched", "conversions") / g7b(d, "touched", "visits")),
    R("AOV — touched", "usd", lambda d: g7c(d, "touched", "revenue") / g7c(d, "touched", "conversions")),
    R("ROAS — touched", "x2", lambda d: g7c(d, "touched", "revenue") / float(q6[d]["media_touched"])),

    S("PERFORMANCE — UNIQUE (SOLE) COHORT (per week; the vendor discriminator)"),
    R("Spend (media $) — sole", "usd", lambda d: float(q6[d]["media_sole"])),
    R("Impressions (won bids) — sole", "int", lambda d: float(q6[d]["imps_sole"])),
    R("Visits — sole", "int", lambda d: int(q7[d]["sole_visits"])),
    R("Visits — sole, 95% CI", "txt", lambda d: "{:.0f}-{:.0f}".format(*_poisson_ci(int(q7[d]["sole_visits"])))),
    R("Conversions — sole", "int", lambda d: g7c(d, "sole", "conversions")),
    R("Revenue — sole", "usd", lambda d: g7c(d, "sole", "revenue")),
    R("CPM — sole", "usd2", lambda d: 1000 * float(q6[d]["media_sole"]) / float(q6[d]["imps_sole"])),
    R("IVR — sole", "pct4", lambda d: float(q7[d]["vr_overall_pct"])),
    R("IVR — sole, x of 0.0223% baseline", "dec1", lambda d: float(q7[d]["vr_overall_pct"]) / BASELINE_VR),
    R("CVR (conv/visits) — sole", "pct2", lambda d: 100 * g7c(d, "sole", "conversions") / int(q7[d]["sole_visits"]) if int(q7[d]["sole_visits"]) else None),
    R("AOV — sole", "usd", lambda d: g7c(d, "sole", "revenue") / g7c(d, "sole", "conversions") if g7c(d, "sole", "conversions") else None),
    R("ROAS — sole", "x2", lambda d: g7c(d, "sole", "revenue") / float(q6[d]["media_sole"])),

    S("ECONOMICS — COST"),
    R("June bill", "usd", lambda d: 0.0 if d in FREE or d == 27 else
      (q0[d]["june_usd"] if q0.get(d, {}).get("june_usd") is not None else ("pending" if d in FLAT else None)), True),
    R("Run-rate $/yr (June x12)", "usd", lambda d: 0.0 if d in FREE or d == 27 else
      (q0[d]["june_usd"] * 12 if q0.get(d, {}).get("june_usd") is not None else ("pending" if d in FLAT else None)), True),
    R("% of delivered rows billed", "pct2", lambda d: None if d in FREE or d in FLAT else
      100 * float(q1d[d]["billed_imps"]) / (q1[d]["rows"] * (365.0 / 12) / 30.0)),
    R("% of sole serves via prospecting (vendor-dependent)", "pct", lambda d: 100 * q6b[d]["prosp"] / q6b[d]["tot"]),

    S("ECONOMICS — WHAT THE DATA IS WORTH (formulas in notes)"),
    R("Max justified CPM — on 100% of delivered rows", "usd4", lambda d: None if d in FREE else
      1000 * t2_ann(d) * 0.30 / (q1[d]["rows"] * ANN30)),
    R("Max justified CPM — on used/billed imps (vs $0.50)", "usd4", lambda d: None if d in FREE else
      (1000 * t2_ann(d) * 0.30 / (float(q1d[d]["billed_imps"]) * 12) if d in q1d and q1d[d].get("billed_imps")
       else 1000 * t2_ann(d) * 0.30 / (float(q2c[d]["rows_used"]) * ANN30))),
    R("Flat equivalent — floor (T1 x 15%)", "usd", lambda d: None if d in FREE else t1_ann(d) * 0.15),
    R("Flat equivalent — fair (T2 x 20%)", "usd", lambda d: None if d in FREE else t2_ann(d) * 0.20),
    R("Flat equivalent — ceiling (T2 x 30%)", "usd", lambda d: None if d in FREE else t2_ann(d) * 0.30),
    R("T2 dependent revenue $/yr (sole-won media x52)", "usd", lambda d: t2_ann(d)),
    R("T2 envelope low (x0.4)", "usd", lambda d: t2_ann(d) * 0.4),
    R("T2 envelope high (x1.8)", "usd", lambda d: t2_ann(d) * 1.8),
    R("T1 provable floor $/yr (score-gated)", "usd", lambda d: t1_ann(d)),
    R("Fee band, domain axis — low (sole classified x $3)", "usd", lambda d: None if d in FREE else float(q4[d]["sole_classified"]) * 3),
    R("Fee band, domain axis — high (sole classified x $13)", "usd", lambda d: None if d in FREE else float(q4[d]["sole_classified"]) * 13),

    S("PORTFOLIO (leave-one-out, exact from q3b)"),
    R("Exact drop savings $/yr", "usd", lambda d: None if d in FREE else
      ("= flat fee" if d in FLAT else float(q1d[d]["billed_usd"]) * 12 * (1 - reassign[d].get("metered", 0) / sum(reassign[d].values())))),
    R("Drop savings as % of bill", "pct0", lambda d: None if d in FREE or d in FLAT else
      100 * (1 - reassign[d].get("metered", 0) / sum(reassign[d].values()))),
    R("% credits vanish (were sole)", "pct0", lambda d: 100 * reassign[d]["none"] / sum(reassign[d].values())),
    R("% credits -> flat-fee vendors", "pct0", lambda d: 100 * reassign[d]["flat_fee"] / sum(reassign[d].values())),
    R("% credits -> free logs", "pct0", lambda d: 100 * (reassign[d].get("free_first", 0) + reassign[d].get("free_later", 0)) / sum(reassign[d].values())),
    R("% credits -> other metered (still paid)", "pct", lambda d: 100 * reassign[d].get("metered", 0) / sum(reassign[d].values())),
    R("Coverage lost if dropped (pp of pair coverage)", "dec2", lambda d: -coverage_lost[d] if d in coverage_lost else None),

    S("VERDICT"),
    R("Composite quality score (curved, best=100)", "int", lambda d: CURVE[d][1] if d in CURVE else None),
    R("Composite quality score (raw)", "dec1", lambda d: CURVE[d][0] if d in CURVE else None),
    R("Verdict", "txt", lambda d: VERDICT_SHORT.get(d)),
    R("Asks / weird things (full text in notes)", "txt", lambda d: "see notes" if d in ASKS else None),
]

# pct* store the FRACTION (0.218) with a true percent format so Excel/Sheets
# recognize the cell as a percentage; the writer divides percent numbers by 100.
FMT = {"int": "#,##0", "pct0": "0%", "pct": "0.0%", "pct2": "0.00%",
       "pct3": "0.000%", "pct4": "0.0000%", "usd": "$#,##0", "usd2": "$#,##0.00",
       "usd4": "$#,##0.0000", "dec1": "0.0", "dec2": "0.00", "x2": '0.00"x"', "txt": None}


def main():
    import openpyxl
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "numbers"

    bold = Font(bold=True)
    section_fill = PatternFill("solid", fgColor="1F3864")
    section_font = Font(bold=True, color="FFFFFF")

    ws.cell(row=1, column=1, value="Question").font = bold
    for i, d in enumerate(DS_COLS):
        c = ws.cell(row=1, column=2 + i, value=HDR_NAMES[d])
        c.font = bold
        c.alignment = Alignment(horizontal="right")

    filled = err = 0
    r = 1
    for label, fmt, fn, oos_ok in SPEC:
        r += 1
        a = ws.cell(row=r, column=1, value=label)
        if fn is None:                       # section header
            a.font = section_font
            a.fill = section_fill
            for i in range(len(DS_COLS)):
                ws.cell(row=r, column=2 + i).fill = section_fill
            continue
        for i, d in enumerate(DS_COLS):
            cell = ws.cell(row=r, column=2 + i)
            cell.alignment = Alignment(horizontal="right")
            if d in OOS and not oos_ok:
                cell.value = NA
                continue
            try:
                v = fn(d)
            except Exception:
                v = None
            if v is None:
                cell.value = NA
            elif isinstance(v, str):
                cell.value = v
                filled += 1
            else:
                v = float(v)
                if fmt and fmt.startswith("pct"):
                    v /= 100.0          # store fraction; percent format displays it
                cell.value = round(v, 8)
                if FMT.get(fmt):
                    cell.number_format = FMT[fmt]
                filled += 1

    ws.column_dimensions["A"].width = 48
    for c in range(2, 2 + len(DS_COLS)):
        ws.column_dimensions[get_column_letter(c)].width = 16
    ws.freeze_panes = "B2"

    # ---- notes sheet ----
    from openpyxl.styles import Border, Side

    ns = wb.create_sheet("notes")
    hdr = ["Vendor", "DS", "Scope", "Billing / rate", "Renewal / contract status",
           "Ingestion + off-switch", "Blast radius (non-MM prod deps)",
           "Verdict (full)", "Asks / weird things to raise with the vendor"]
    ncols = len(hdr)
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    band_fill = PatternFill("solid", fgColor="F2F2F2")
    verdict_color = {"KEEP": "1E7A1E", "NEGO": "B26B00", "DROP": "B00020"}

    ns.append(hdr)
    for c in range(1, ncols + 1):
        cell = ns.cell(row=1, column=c)
        cell.font = section_font
        cell.fill = section_fill
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        cell.border = border
    ns.row_dimensions[1].height = 22

    for i, d in enumerate(DS_COLS):
        reg = q0.get(d, {}).get("reg", {})
        vf = (reg.get("valid_from") or "")[:10]
        renewal = RENEWAL_NOTE.get(
            d, f"Contract valid_from {vf}; renewal date pending (Maya / renewal schedule)" if vf else "pending")
        vals = [HDR_NAMES[d], d, SCOPE.get(d, ""), RATE_NOTE.get(d, ""), renewal,
                INGEST_NOTE.get(d, ""), BLAST_NOTE.get(d, ""),
                VERDICT_FULL.get(d, NA), ASKS.get(d, NA)]
        r_i = 2 + i
        for c, v in enumerate(vals, start=1):
            cell = ns.cell(row=r_i, column=c, value=v)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = border
            if i % 2 == 1:
                cell.fill = band_fill
        ns.cell(row=r_i, column=1).font = bold
        vcell = ns.cell(row=r_i, column=8)
        vword = str(vcell.value or "")[:4].upper()
        if vword in verdict_color:
            vcell.font = Font(bold=True, color=verdict_color[vword])
        ns.row_dimensions[r_i].height = 44

    # CONVENTIONS block: section bar + one merged full-width row per item
    conv_hdr_row = len(DS_COLS) + 3
    ns.cell(row=conv_hdr_row, column=1, value="CONVENTIONS — how to read this workbook")
    ns.merge_cells(start_row=conv_hdr_row, start_column=1, end_row=conv_hdr_row, end_column=ncols)
    hc = ns.cell(row=conv_hdr_row, column=1)
    hc.font = section_font
    hc.fill = section_fill
    hc.alignment = Alignment(vertical="center")
    ns.row_dimensions[conv_hdr_row].height = 20
    for j, ctext in enumerate(CONVENTIONS):
        r_j = conv_hdr_row + 1 + j
        ns.cell(row=r_j, column=1, value=f"{j + 1}. {ctext}")
        ns.merge_cells(start_row=r_j, start_column=1, end_row=r_j, end_column=ncols)
        cc = ns.cell(row=r_j, column=1)
        cc.alignment = Alignment(vertical="top", wrap_text=True)
        cc.border = border
        if j % 2 == 1:
            cc.fill = band_fill
        ns.row_dimensions[r_j].height = 28

    widths = [16, 5, 46, 34, 42, 46, 44, 62, 70]
    for i, w in enumerate(widths, start=1):
        ns.column_dimensions[get_column_letter(i)].width = w
    ns.freeze_panes = "A2"

    wb.save(OUT)
    nrows = sum(1 for s in SPEC if s[2] is not None)
    print(f"wrote {OUT}")
    print(f"numbers sheet: {nrows} question rows x {len(DS_COLS)} vendors, {filled} values")
    empty = []
    r = 1
    for label, fmt, fn, oos_ok in SPEC:
        r += 1
        if fn is None:
            continue
        for i, d in enumerate(DS_COLS):
            if ws.cell(row=r, column=2 + i).value in (None, ""):
                empty.append((label, d))
    print("empty:", empty if empty else "none")


if __name__ == "__main__":
    main()
