#!/usr/bin/env python3
"""
TI-837 Phase 0c — compare cross-window run to v5 baseline.

Reads:
  - outputs/ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json
    (v5 canonical, 30-adv, window 2026-04-20 → 04-26)
  - outputs/ti_837_lift_30adv_7day_v5_xwin_2026_04_22_to_28.json
    (xwin replication, same cohort/SQL, window 2026-04-22 → 04-28)

For each (segment, intent_tier) pair:
  - Computes pooled sample-weighted ATT for both windows
  - Reports the delta in pp
  - Validates that segment ordering reproduces (rtg > prosp > stage1)

For each (segment, advertiser, intent_tier):
  - Per-cell ATT_v5 vs ATT_xwin
  - Flags cells where the delta exceeds 5pp (suggests window-specific
    advertiser behavior, not a methodology issue)

Output:
  - Stdout: human-readable summary
  - outputs/ti_837_xwin_vs_v5_comparison.json — structured data for
    deck/doc consumption.

Usage:
  python3 compare_xwin_vs_v5.py
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
TICKET_ROOT = REPO_ROOT / "tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan"

V5_PATH = TICKET_ROOT / "outputs/ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json"
XWIN_PATH = TICKET_ROOT / "outputs/ti_837_lift_30adv_7day_v5_xwin_2026_04_22_to_28.json"
OUT_PATH = TICKET_ROOT / "outputs/ti_837_xwin_vs_v5_comparison.json"


def load_cells(path: Path) -> dict:
    """Read a v5-style output (list of dicts or NDJSON) and key by
    (segment, advertiser_id, group_name, intent_tier)."""
    text = path.read_text()
    rows = []
    text_stripped = text.lstrip()
    if text_stripped.startswith("["):
        rows = json.loads(text)
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    out = {}
    for r in rows:
        key = (
            r["segment"],
            int(r["advertiser_id"]),
            r["group_name"],
            r["intent_tier"],
        )
        out[key] = {
            "n_ips": int(r["n_ips"]),
            "clickpass_visitors": int(r["clickpass_visitors"]),
            "guid_visitors": int(r["guid_visitors"]),
            "clickpass_visit_rate": float(r["clickpass_visit_rate"] or 0),
            "guid_visit_rate": float(r["guid_visit_rate"] or 0),
        }
    return out


def pool_sample_weighted(cells: dict, segment: str, intent_tier: str) -> dict:
    """Pool ATT across advertisers via sample-weighted averaging.

    Returns dict with treated/holdout n + visit rates + ATT (pp).
    """
    h_n = h_cv = h_gv = 0
    t_n = t_cv = t_gv = 0
    advertisers = set()
    for (seg, adv, group, tier), c in cells.items():
        if seg != segment or tier != intent_tier:
            continue
        advertisers.add(adv)
        if group == "holdout_biddable":
            h_n += c["n_ips"]
            h_cv += c["clickpass_visitors"]
            h_gv += c["guid_visitors"]
        elif group == "treated_served":
            t_n += c["n_ips"]
            t_cv += c["clickpass_visitors"]
            t_gv += c["guid_visitors"]
    h_cr = h_cv / h_n if h_n else 0
    h_gr = h_gv / h_n if h_n else 0
    t_cr = t_cv / t_n if t_n else 0
    t_gr = t_gv / t_n if t_n else 0
    return {
        "n_advertisers": len(advertisers),
        "holdout_n": h_n, "treated_n": t_n,
        "holdout_clickpass_rate": h_cr, "treated_clickpass_rate": t_cr,
        "holdout_guid_rate": h_gr, "treated_guid_rate": t_gr,
        "clickpass_ATT_pp": (t_cr - h_cr) * 100,
        "guid_ATT_pp": (t_gr - h_gr) * 100,
    }


def main():
    if not XWIN_PATH.exists() or XWIN_PATH.stat().st_size < 1000:
        print(f"[compare] xwin output not yet available at {XWIN_PATH}")
        print(f"[compare] (size: {XWIN_PATH.stat().st_size if XWIN_PATH.exists() else 'missing'})")
        return

    print(f"[compare] v5   : {V5_PATH}")
    print(f"[compare] xwin : {XWIN_PATH}")
    v5 = load_cells(V5_PATH)
    xw = load_cells(XWIN_PATH)
    print(f"[compare] v5 cells   : {len(v5)}")
    print(f"[compare] xwin cells : {len(xw)}")

    SEGMENTS = ["all", "prosp", "stage1", "rtg"]
    TIERS = ["high", "peak", "mid"]

    summary = {"sample_weighted_pooled": {}}
    print()
    print(f"{'segment':<10} {'tier':<6}  {'guid_v5':>10}  {'guid_xw':>10}  {'Δguid':>8}    {'cp_v5':>10}  {'cp_xw':>10}  {'Δcp':>8}")
    print("-" * 100)
    for seg in SEGMENTS:
        for tier in TIERS:
            v5_pool = pool_sample_weighted(v5, seg, tier)
            xw_pool = pool_sample_weighted(xw, seg, tier)
            d_guid = xw_pool["guid_ATT_pp"] - v5_pool["guid_ATT_pp"]
            d_cp = xw_pool["clickpass_ATT_pp"] - v5_pool["clickpass_ATT_pp"]
            print(
                f"  {seg:<8} {tier:<6}  {v5_pool['guid_ATT_pp']:+9.3f}pp  {xw_pool['guid_ATT_pp']:+9.3f}pp  {d_guid:+7.3f}pp    "
                f"{v5_pool['clickpass_ATT_pp']:+9.3f}pp  {xw_pool['clickpass_ATT_pp']:+9.3f}pp  {d_cp:+7.3f}pp"
            )
            summary["sample_weighted_pooled"][f"{seg}.{tier}"] = {
                "v5": v5_pool,
                "xwin": xw_pool,
                "delta_guid_ATT_pp": d_guid,
                "delta_clickpass_ATT_pp": d_cp,
            }

    # Segment ordering check (high-intent guid)
    print()
    print("=== segment ordering (high-intent guid sample-weighted) ===")
    for window_label, src in [("v5", v5), ("xwin", xw)]:
        ordering = sorted(
            [(seg, pool_sample_weighted(src, seg, "high")["guid_ATT_pp"]) for seg in SEGMENTS],
            key=lambda x: -x[1],
        )
        print(f"  {window_label:<6}: " + " > ".join(f"{s}({a:+.2f}pp)" for s, a in ordering))

    # Per-cell large deltas
    print()
    print("=== Per-cell guid-ATT deltas > 5pp (high-intent only) ===")
    for adv in sorted({a for (_, a, _, _) in v5}):
        for seg in SEGMENTS:
            kt_v5 = (seg, adv, "treated_served", "high")
            kh_v5 = (seg, adv, "holdout_biddable", "high")
            kt_xw = (seg, adv, "treated_served", "high")
            kh_xw = (seg, adv, "holdout_biddable", "high")
            if not all(k in v5 for k in [kt_v5, kh_v5]): continue
            if not all(k in xw for k in [kt_xw, kh_xw]): continue
            v5_att = (v5[kt_v5]["guid_visit_rate"] - v5[kh_v5]["guid_visit_rate"]) * 100
            xw_att = (xw[kt_xw]["guid_visit_rate"] - xw[kh_xw]["guid_visit_rate"]) * 100
            d = xw_att - v5_att
            if abs(d) >= 5:
                print(f"  adv={adv} seg={seg:<8}  v5={v5_att:+.2f}pp  xwin={xw_att:+.2f}pp  Δ={d:+.2f}pp")

    OUT_PATH.write_text(json.dumps(summary, indent=2, default=str))
    print(f"\n[compare] wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
