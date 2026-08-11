"""AUDI-431: promote AI-confident manual-band rows into the shipped lists.

Usage: python3 artifacts/audi_431_promote.py <promotion_verify_output.json>

Rule: verdict != 'unsure' AND confidence >= 0.7, minus anything the adversarial
verify pass refuted. Whitelist requires unanimity across 3 refuter lenses (a wrong
whitelist entry permanently mislabels a domain as a shop); blocklist demotes
wholesale if the 150-row sample disputes > 5%.

Everything not promoted keeps a blank designation and stays Malachi's call.
Writes outputs/audi_431_ai_promotions.csv, consumed by audi_431_build_lists.py.
"""

import json
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"
FLOOR = 0.7


def main() -> None:
    raw = json.loads(Path(sys.argv[1]).read_text())
    res = raw.get("result", raw)
    ai = pd.read_csv(OUT / "audi_431_ai_review.csv")

    wl = set(ai.loc[(ai["verdict"] == "ecommerce") & (ai["confidence"] >= FLOOR), "domain"])
    bl = set(ai.loc[(ai["verdict"] == "not_ecommerce") & (ai["confidence"] >= FLOOR), "domain"])

    wl_refuted, notes = {}, {}
    for lens in res["wl"]:
        for v in lens["verdicts"]:
            if v["refuted"]:
                wl_refuted.setdefault(v["domain"], []).append(lens["lens"])
                notes[v["domain"]] = v["reason"]
    wl_keep = {d for d in wl if d not in wl_refuted}

    bl_seen, bl_disputed = set(), {}
    for batch in res["bl"]:
        for lensname in ("mechanism", "identity"):
            for v in batch.get(lensname) or []:
                bl_seen.add(v["domain"])
                if v["refuted"]:
                    bl_disputed.setdefault(v["domain"], []).append(lensname)
                    notes[v["domain"]] = v["reason"]
    # a blocklist row dies only if BOTH lenses refute it (single-lens noise is common)
    bl_dead = {d for d, ls in bl_disputed.items() if len(ls) >= 2}
    bl_rate = len(bl_dead) / max(len(bl_seen), 1)
    bl_keep = set() if bl_rate > 0.05 else bl - bl_dead

    rows = ([{"domain": d, "designation": "Whitelist"} for d in sorted(wl_keep)]
            + [{"domain": d, "designation": "Blocklist"} for d in sorted(bl_keep)])
    pd.DataFrame(rows).to_csv(OUT / "audi_431_ai_promotions.csv", index=False)

    report = {
        "floor": FLOOR,
        "wl_candidates": len(wl), "wl_refuted": len(wl_refuted), "wl_promoted": len(wl_keep),
        "bl_candidates": len(bl), "bl_sampled": len(bl_seen),
        "bl_both_lenses_refuted": len(bl_dead), "bl_dispute_rate": round(bl_rate, 4),
        "bl_band_demoted_wholesale": bl_rate > 0.05, "bl_promoted": len(bl_keep),
    }
    (OUT / "audi_431_promotion_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    if wl_refuted:
        print("\nwhitelist candidates killed:")
        for d, ls in wl_refuted.items():
            print(f"  {d:26s} refuted by {','.join(ls):26s} {notes.get(d,'')[:90]}")
    if bl_dead:
        print(f"\nblocklist rows killed by both lenses: {sorted(bl_dead)[:10]}")


if __name__ == "__main__":
    main()
