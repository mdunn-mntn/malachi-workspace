"""AUDI-431 Phase 5: emit list files + hygiene checks + impact summary.

Outputs:
  outputs/audi_431_blocklist_additions.csv   additions only (headerless bare domains)
  outputs/audi_431_ecommerce_blocklist.csv   full merged replacement (existing 1,464 + adds)
  outputs/audi_431_whitelist_additions.csv   additions only (headerless bare domains)
  outputs/audi_431_impact.json               impact + hygiene summary

Hygiene enforced: dedupe, WL/BL disjoint, no overlap with existing lists,
byte-format identical to shipped files (bare domain per line, \n, no header).
"""

import gzip
import json
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"
RAW = TICKET / "outputs" / "raw"
TOTAL_VOL = 16_046_965_205


def main() -> None:
    sheet = pd.read_csv(OUT / "audi_431_decision_sheet.csv")
    sheet["designation"] = sheet["designation"].fillna("")
    # QC demotions (written by audi_431_apply_qc.py after the workflow) override designations
    qc_path = OUT / "audi_431_qc_demotions.csv"
    if qc_path.exists():
        demoted = set(pd.read_csv(qc_path)["domain"])
        n = sheet["domain"].isin(demoted).sum()
        sheet.loc[sheet["domain"].isin(demoted), ["designation", "designation_source", "band"]] = ["", "", "manual"]
        print(f"QC demotions applied: {n} rows -> manual")

    promo_path = OUT / "audi_431_ai_promotions.csv"
    if promo_path.exists():
        promo = pd.read_csv(promo_path)
        idx = sheet["domain"].isin(set(promo["domain"]))
        sheet.loc[idx, "designation"] = sheet.loc[idx, "domain"].map(dict(zip(promo["domain"], promo["designation"])))
        sheet.loc[idx, "designation_source"] = "ai-verified"
        print(f"AI-verified promotions applied: {int(idx.sum())} rows")

    fetch_path = OUT / "audi_431_fetch_calls.csv"
    if fetch_path.exists():
        fc = pd.read_csv(fetch_path)
        idx = sheet["domain"].isin(set(fc["domain"]))
        sheet.loc[idx, "designation"] = sheet.loc[idx, "domain"].map(dict(zip(fc["domain"], fc["designation"])))
        sheet.loc[idx, "designation_source"] = "site-fetch"
        print(f"live-site fetch calls applied: {int(idx.sum())} rows")

    hc_path = OUT / "audi_431_human_calls.csv"
    if hc_path.exists():
        hc = pd.read_csv(hc_path)
        idx = sheet["domain"].isin(set(hc["domain"]))
        sheet.loc[idx, "designation"] = sheet.loc[idx, "domain"].map(dict(zip(hc["domain"], hc["designation"])))
        sheet.loc[idx, "designation_source"] = "human"
        print(f"human calls applied: {int(idx.sum())} rows")

    wl_add = sheet.loc[sheet["designation"] == "Whitelist", "domain"].drop_duplicates()
    bl_add = sheet.loc[sheet["designation"] == "Blocklist", "domain"].drop_duplicates()

    existing_bl = [l for l in (RAW / "ecommerce_blocklist.csv").read_text().splitlines() if l]
    with gzip.open(RAW / "ecommerce_whitelist.csv.gz", "rt") as fh:
        existing_wl = {l.strip() for l in fh} - {""}

    assert not (set(wl_add) & set(bl_add)), "WL/BL overlap in proposals"
    assert not (set(bl_add) & set(existing_bl)), "BL adds already in existing blocklist"
    assert not (set(wl_add) & existing_wl), "WL adds already in existing whitelist"
    assert not (set(wl_add) & set(existing_bl)), "WL adds present in existing blocklist"
    assert not (set(bl_add) & existing_wl), "BL adds present in existing whitelist"

    (OUT / "audi_431_blocklist_additions.csv").write_text("\n".join(bl_add) + "\n")
    (OUT / "audi_431_whitelist_additions.csv").write_text("\n".join(wl_add) + "\n")
    merged = existing_bl + [d for d in bl_add if d not in set(existing_bl)]
    (OUT / "audi_431_ecommerce_blocklist.csv").write_text("\n".join(merged) + "\n")

    vol = sheet.set_index("domain")["total_count"]
    impact = {
        "bl_additions": int(len(bl_add)),
        "wl_additions": int(len(wl_add)),
        "merged_blocklist_size": len(merged),
        "bl_volume_resolved": int(vol.loc[list(bl_add)].sum()),
        "wl_volume_resolved": int(vol.loc[list(wl_add)].sum()),
        "pct_missing_volume_resolved": round(float((vol.loc[list(bl_add)].sum() + vol.loc[list(wl_add)].sum()) / TOTAL_VOL), 4),
        "manual_rows_remaining": int((sheet["designation"] == "").sum()),
        "manual_volume_share": round(float(sheet.loc[sheet["designation"] == "", "total_count"].sum() / TOTAL_VOL), 4),
    }
    (OUT / "audi_431_impact.json").write_text(json.dumps(impact, indent=2))
    print(json.dumps(impact, indent=2))

    old = set(existing_bl)
    new = set(merged)
    assert old <= new and not (old - new), "merged blocklist must be additions-only"
    print("hygiene: all checks passed (dedupe, disjoint, additions-only)")


if __name__ == "__main__":
    main()
