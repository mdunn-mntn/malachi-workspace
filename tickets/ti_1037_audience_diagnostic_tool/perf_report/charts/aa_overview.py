"""Module OVERVIEW — top-level flag scorecard + auto-TLDR (the headline layer).

Runs LAST (reads the other modules' CSVs) but DISPLAYS FIRST. Synthesizes the deep-dive outputs
into the quick "here's likely your issue" headlines: geo/3P restriction, VV-window change, HHST
change + avg pre/post, short flights before/after, campaign count, spend/ROAS pre/post, HI-share.
Emits a flag scorecard PNG + an auto-written TLDR markdown (plain-language, like a Slack update).

Reads  <outdir>/04_prospecting_yoy_metrics.csv · 11_vv_window_change_log.csv · 03_hhst_gate_history.csv
       08_prospecting_flights.csv · 02_prospecting_audience_expressions.csv · 01_campaign_group_gantt.csv
       06_prospecting_score_buckets_monthly.csv
Writes <outdir>/overview_flags.png · overview_tldr.md
"""
import argparse
import csv
import json
import os
from datetime import date
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, RED, AMBER, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#888888"
US_LOC = 237


def d(s):
    return date(*(int(x) for x in s[:10].split("-")))


def rd(path):
    return list(csv.DictReader(open(path))) if os.path.exists(path) else []


def pct(a, b):
    return (a / b - 1) * 100 if b else float("nan")


# ---- expression helpers (stage-1 audience) ----
def geo_inc(e):
    inc = []

    def rec(n, neg):
        if isinstance(n, list):
            [rec(x, neg) for x in n]
        elif isinstance(n, dict):
            v = n.get("value")
            if isinstance(v, dict) and "location_ids" in v:
                (None if neg else inc.extend(v["location_ids"]))
                return
            if v is not None:
                rec(v, neg ^ (n.get("op") == "not"))
    rec((e.get("geos") or {}).get("where"), False)
    return set(inc)


def contains(node, ds):
    if isinstance(node, list):
        return any(contains(x, ds) for x in node)
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, dict) and v.get("data_source_id") == ds:
            return True
        return contains(v, ds) if v is not None else False
    return False


def join_op(node, a=19, b=35):
    best = [None, -1]

    def w(n, dep):
        if isinstance(n, dict):
            op, v = n.get("op"), n.get("value")
            if op in ("and", "or") and v is not None and contains(v, a) and contains(v, b) and dep > best[1]:
                best[0], best[1] = op, dep
            if v is not None:
                w(v, dep + 1)
        elif isinstance(n, list):
            [w(c, dep + 1) for c in n]
    w(node, 0)
    return best[0]


def val_at(changes, when, key):
    """forward-fill: value of `key` effective at date `when` from a time-ordered change list."""
    v = None
    for c in changes:
        if c["date"] <= when:
            v = c[key]
    return v


def gate_avg(hist, ps, pe):
    """time-weighted average threshold across prospecting campaigns over [ps, pe)."""
    by_c = {}
    for r in hist:
        by_c.setdefault(r["campaign_id"], []).append((d(r["update_time"]), float(r["threshold"])))
    num = den = 0.0
    for cid, seq in by_c.items():
        seq.sort()
        for i, (t, v) in enumerate(seq):
            seg_s = max(t, ps)
            seg_e = min(seq[i + 1][0] if i + 1 < len(seq) else pe, pe)
            days = (seg_e - seg_s).days
            if days > 0:
                num += v * days
                den += days
    return (num / den) if den else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="outputs/kindred_35094")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    ap.add_argument("--p1-label", default="P1")
    ap.add_argument("--p2-label", default="P2")
    a = ap.parse_args()
    o = a.outdir.rstrip("/")
    csv.field_size_limit(10 ** 7)
    p1s, p1e, p2s, p2e = d(a.p1[0]), d(a.p1[1]), d(a.p2[0]), d(a.p2[1])

    flags = []  # {name, pre, post, sev, note}

    def add(name, pre, post, sev, note):
        flags.append({"name": name, "pre": pre, "post": post, "sev": sev, "note": note})

    # ---------- headline metrics (04) ----------
    met = {r["period"]: r for r in rd(f"{o}/04_prospecting_yoy_metrics.csv")}
    if "P1" in met and "P2" in met:
        def f(p, k):
            return float(met[p][k])
        r1, r2 = f("P1", "revenue") / f("P1", "spend"), f("P2", "revenue") / f("P2", "spend")
        add("Prospecting ROAS", f"{r1:.2f}x", f"{r2:.2f}x",
            RED if pct(r2, r1) <= -20 else (AMBER if pct(r2, r1) < 0 else GREEN),
            f"{pct(r2, r1):+.0f}% — the headline decline" if pct(r2, r1) < 0 else "holding/up")
        s1, s2 = f("P1", "spend"), f("P2", "spend")
        add("Prospecting spend", f"${s1/1e3:.0f}K", f"${s2/1e3:.0f}K",
            AMBER if pct(s2, s1) >= 25 else GRAY,
            f"{pct(s2, s1):+.0f}%" + ("  — scaling up while ROAS falls (over-scaling risk)" if pct(s2, s1) >= 25 and r2 < r1 else ""))
        vr1, vr2 = f("P1", "visits") / f("P1", "impressions") * 100, f("P2", "visits") / f("P2", "impressions") * 100
        add("Visit rate", f"{vr1:.2f}%", f"{vr2:.2f}%",
            RED if pct(vr2, vr1) <= -20 else (AMBER if pct(vr2, vr1) < 0 else GREEN), f"{pct(vr2, vr1):+.0f}%")

    # ---------- VV window (11) ----------
    vv = [{"date": d(r["change_date"]), "pro": int(r["pro_window"])} for r in rd(f"{o}/11_vv_window_change_log.csv")]
    if vv:
        w1, w2 = val_at(vv, p1e, "pro"), val_at(vv, p2e, "pro")
        add("VV window (prospecting)", f"{w1}d" if w1 else "?", f"{w2}d" if w2 else "?",
            RED if (w1 and w2 and w2 < w1) else GREEN,
            "shortened -> fewer connectable visits & conversions + lower visit rate (CVR effect ambiguous)"
            if (w1 and w2 and w2 < w1)
            else ("lengthened" if (w1 and w2 and w2 > w1) else "unchanged"))

    # ---------- HHST gate (03) ----------
    hist = rd(f"{o}/03_hhst_gate_history.csv")
    if hist:
        g1, g2 = gate_avg(hist, p1s, p1e), gate_avg(hist, p2s, p2e)
        chg1 = sum(1 for r in hist if p1s <= d(r["update_time"]) < p1e)
        chg2 = sum(1 for r in hist if p2s <= d(r["update_time"]) < p2e)
        add("Avg HHST gate", f"{g1:,.0f}" if g1 is not None else "—", f"{g2:,.0f}" if g2 is not None else "—",
            RED if (g1 and g2 and g2 < g1 * 0.9) else (AMBER if (g1 and g2 and g2 < g1) else GREEN),
            "gate lowered -> serving lower-intent households" if (g1 and g2 and g2 < g1 * 0.9) else "gate ~held")
        add("HHST changes (thrash)", str(chg1), str(chg2),
            RED if chg2 > max(chg1 * 1.5, chg1 + 10) else (AMBER if chg2 > chg1 else GRAY),
            f"gate re-set {chg2}x in {a.p2_label} vs {chg1}x in {a.p1_label}")

    # ---------- short flights (08) ----------
    fl = rd(f"{o}/08_prospecting_flights.csv")
    if fl:
        def short_in(ps, pe):
            return sum(1 for r in fl if r.get("flight_days") and int(r["flight_days"]) <= 3 and ps <= d(r["flight_start"]) < pe)
        sf1, sf2 = short_in(p1s, p1e), short_in(p2s, p2e)
        add("Short flights (<=3d)", str(sf1), str(sf2),
            RED if sf2 > sf1 + 5 else (AMBER if sf2 > sf1 else GREEN),
            "short flights auto-drop the gate to 0 (serve anyone)" if sf2 > sf1 else "")

    # ---------- campaign count + broadest-off (01 gantt, prospecting groups from expr) ----------
    exprs = [r for r in rd(f"{o}/02_prospecting_audience_expressions.csv") if r.get("expression")]
    prosp_grps = {r["campaign_group_id"] for r in exprs}
    gantt = {r["campaign_group_id"]: r for r in rd(f"{o}/01_campaign_group_gantt.csv")}
    if gantt and prosp_grps:
        def active(g, ps, pe):
            r = gantt.get(g)
            return r and d(r["first_active_day"]) < pe and d(r["last_active_day"]) >= ps
        n1 = sum(1 for g in prosp_grps if active(g, p1s, p1e))
        n2 = sum(1 for g in prosp_grps if active(g, p2s, p2e))
        add("Prospecting campaigns", str(n1), str(n2),
            AMBER if n2 > n1 + 1 else GRAY,
            f"went {n1} -> {n2} campaigns (more campaigns compete for the same bids)" if n2 > n1 else "")
        # broadest = highest-spend prospecting group that stopped before P2 end
        off = [g for g in prosp_grps if gantt.get(g) and d(gantt[g]["last_active_day"]) < p2e - (p2e - p2s) // 4]
        if off:
            b = max(off, key=lambda g: float(gantt[g]["total_spend"]))
            add("Broadest campaign wound down", "on", "off",
                AMBER, f"{b} '{gantt[b]['group_name']}' (${float(gantt[b]['total_spend'])/1e3:.0f}K) stopped before period end")

    # ---------- geo / 3P restriction (stage-1 expr) ----------
    if exprs:
        natl = restr = has3p = and3p = 0
        for r in exprs:
            e = json.loads(r["expression"])
            ids = geo_inc(e)
            ndma = len([i for i in ids if 461 <= i <= 672])
            if US_LOC in ids and ndma == 0:
                natl += 1
            elif ndma:
                restr += 1
            cw = (e.get("categories") or {}).get("where")
            if contains(cw, 35):
                has3p += 1
                if join_op(cw) == "and":
                    and3p += 1
        add("Geo restriction", "", f"{restr}/{len(exprs)} DMA-limited",
            RED if restr > len(exprs) / 2 else (AMBER if restr else GREEN),
            "prospecting is geo-restricted (removing geo limits widens the pool)" if restr else "national (no geo limit)")
        add("3P restriction", "", f"{has3p}/{len(exprs)} use 3P" + (f", {and3p} AND-required" if and3p else ""),
            RED if and3p else (AMBER if has3p else GREEN),
            "a required 3P segment narrows the audience" if and3p else ("3P added as OR (additive, not restrictive)" if has3p else "no 3P"))

    # ---------- HI-share (06; often P2-only, score logging began 2025-06) ----------
    sc = rd(f"{o}/06_prospecting_score_buckets_monthly.csv")
    if sc:
        def hi_share(ps, pe):
            tot = hi = 0
            for r in sc:
                mo = r["mo"]
                md = date(int(mo[:4]), int(mo[5:7]), 1)
                if ps <= md < pe:
                    scored = float(r["total"]) - float(r["notlogged"])
                    tot += scored
                    hi += float(r["hi"])
            return (hi / tot * 100) if tot else None
        h1, h2 = hi_share(p1s, p1e), hi_share(p2s, p2e)
        add("HI-share of reached", f"{h1:.0f}%" if h1 else "no data", f"{h2:.0f}%" if h2 else "no data",
            RED if (h1 and h2 and h2 < h1 - 5) else (AMBER if (h2 and h2 < 70) else GREEN),
            "reaching lower-intent households" if (h2 and h2 < 70) else ("P1 predates score logging" if not h1 else "high-intent"))

    reds = [f for f in flags if f["sev"] == RED]
    ambers = [f for f in flags if f["sev"] == AMBER]

    # =====================================================================
    # RENDER — TLDR block (top) + flag scorecard (bottom)
    # =====================================================================
    n = len(flags)
    H = 2.8 + 0.30 * len(reds + ambers) + 0.46 * n
    fig = plt.figure(figsize=(14, H))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    def Y(inch):
        return 1 - inch / H
    cy = 0.55
    fig.text(0.03, Y(cy), f"{a.adv} — prospecting diagnosis at a glance", fontsize=21, fontweight="bold", color="#222")
    cy += 0.44
    verdict = "REAL DECLINE" if any("ROAS" in f["name"] and f["sev"] == RED for f in flags) else "watch"
    fig.text(0.03, Y(cy), f"Verdict: {verdict}.  {len(reds)} red flag(s), {len(ambers)} amber.  "
             f"Likely drivers headlined below; deep-dive charts follow.", fontsize=12.5, color="#444")
    cy += 0.5

    # TLDR bullets (auto)
    ax.text(0.03, Y(cy), "TL;DR — likely drivers", fontsize=13.5, fontweight="bold", color=NAVY)
    cy += 0.34
    for f in (reds + ambers) or [{"note": "no red/amber flags — prospecting looks stable", "name": ""}]:
        bullet = f["note"] or f["name"]
        col = RED if f in reds else AMBER
        ax.text(0.05, Y(cy), "•", fontsize=12, color=col, fontweight="bold")
        pp = f" ({f['pre']} -> {f['post']})" if f.get("pre") or f.get("post") else ""
        ax.text(0.075, Y(cy), f"{f['name']}{pp}: {bullet}" if f["name"] else bullet, fontsize=11, color="#222")
        cy += 0.30
    cy += 0.28

    # scorecard table
    ax.text(0.03, Y(cy), "FLAG SCORECARD", fontsize=13.5, fontweight="bold", color=NAVY)
    cy += 0.30
    cols = ["Flag", a.p1_label + " (pre)", a.p2_label + " (post)", "Signal"]
    xs = [0.03, 0.34, 0.50, 0.66]
    for x, c in zip(xs, cols):
        ax.text(x, Y(cy), c, fontsize=11.5, fontweight="bold", color="#555", va="center")
    cy += 0.10
    ax.plot([0.03, 0.985], [Y(cy), Y(cy)], color=NAVY, lw=1.3)
    cy += 0.22
    for i, f in enumerate(flags):
        if i % 2 == 0:
            ax.axhspan(Y(cy + 0.23), Y(cy - 0.23), color="#000", alpha=0.03)
        dot = {RED: "●", AMBER: "●", GREEN: "●", GRAY: "○"}[f["sev"]]
        ax.text(0.03, Y(cy), f["name"], fontsize=11, va="center", color="#222", fontweight="bold")
        ax.text(xs[1], Y(cy), str(f["pre"]) if f["pre"] != "" else "—", fontsize=11, va="center", color="#333")
        ax.text(xs[2], Y(cy), str(f["post"]) if f["post"] != "" else "—", fontsize=11, va="center", color="#333")
        ax.text(xs[3], Y(cy), dot + " ", fontsize=11, va="center", color=f["sev"], fontweight="bold")
        ax.text(xs[3] + 0.018, Y(cy), f["note"][:60], fontsize=9.7, va="center", color=f["sev"] if f["sev"] in (RED, AMBER) else "#555")
        cy += 0.44
    plt.savefig(f"{o}/overview_flags.png", dpi=200, bbox_inches="tight")
    print(f"wrote {o}/overview_flags.png")
    plt.close(fig)

    # ---- auto TLDR markdown ----
    md = [f"# {a.adv} — prospecting diagnosis (TL;DR)", "",
          f"**Verdict: {verdict}.** {len(reds)} red flag(s), {len(ambers)} amber. "
          f"Comparison: {a.p1_label} (pre) vs {a.p2_label} (post).", "", "## Likely drivers", ""]
    for f in reds + ambers:
        pp = f" — {f['pre']} → {f['post']}" if (f.get("pre") or f.get("post")) else ""
        md.append(f"- **{f['name']}**{pp}: {f['note']}")
    if not reds and not ambers:
        md.append("- No red/amber flags — prospecting looks stable.")
    md += ["", "## Full flag scorecard", "", "| Flag | Pre | Post | Signal |", "|---|---|---|---|"]
    sevname = {RED: "🔴", AMBER: "🟠", GREEN: "🟢", GRAY: "⚪"}
    for f in flags:
        md.append(f"| {f['name']} | {f['pre'] or '—'} | {f['post'] or '—'} | {sevname[f['sev']]} {f['note']} |")
    open(f"{o}/overview_tldr.md", "w").write("\n".join(md) + "\n")
    print(f"wrote {o}/overview_tldr.md")
    print(f"FINDING: verdict {verdict}; {len(reds)} red / {len(ambers)} amber flags. "
          f"Top: {'; '.join((f['name'] + ' ' + str(f['pre']) + '->' + str(f['post'])) for f in reds[:3])}")


if __name__ == "__main__":
    main()
