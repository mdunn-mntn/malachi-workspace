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
import sys
from pathlib import Path

import pandas as pd

from audi_431_common import load_designated_sheet

sys.path.insert(0, str(Path(__file__).resolve().parent))

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"
RAW = TICKET / "outputs" / "raw"
TOTAL_VOL = 16_046_965_205


def main() -> None:
    sheet = load_designated_sheet()

    wl_add = sheet.loc[sheet["designation"] == "Whitelist", "domain"].drop_duplicates()
    bl_add = sheet.loc[sheet["designation"] == "Blocklist", "domain"].drop_duplicates()

    existing_bl = [l for l in (RAW / "ecommerce_blocklist.csv").read_text().splitlines() if l]
    with gzip.open(RAW / "ecommerce_whitelist.csv.gz", "rt") as fh:
        existing_wl_ordered = [l.strip() for l in fh if l.strip()]
    existing_wl = set(existing_wl_ordered)

    assert not (set(wl_add) & set(bl_add)), "WL/BL overlap in proposals"
    assert not (set(bl_add) & set(existing_bl)), "BL adds already in existing blocklist"
    assert not (set(wl_add) & existing_wl), "WL adds already in existing whitelist"
    assert not (set(wl_add) & set(existing_bl)), "WL adds present in existing blocklist"
    assert not (set(bl_add) & existing_wl), "BL adds present in existing whitelist"

    (OUT / "audi_431_blocklist_additions.csv").write_text("\n".join(bl_add) + "\n")
    (OUT / "audi_431_whitelist_additions.csv").write_text("\n".join(wl_add) + "\n")

    # Deploy-ready replacements for BOTH prod files: original content untouched, adds appended.
    merged = existing_bl + [d for d in bl_add if d not in set(existing_bl)]
    (OUT / "audi_431_ecommerce_blocklist.csv").write_text("\n".join(merged) + "\n")
    merged_wl = existing_wl_ordered + [d for d in wl_add if d not in existing_wl]
    with open(OUT / "audi_431_ecommerce_whitelist.csv.gz", "wb") as raw:
        with gzip.GzipFile(filename="ecommerce_whitelist.csv", mode="wb", fileobj=raw, mtime=0) as fh:
            fh.write(("\n".join(merged_wl) + "\n").encode())
    # 362 domains sit in BOTH prod lists already; carry that forward untouched, but never add to it
    pre_conflict = set(existing_bl) & existing_wl
    assert set(merged) & set(merged_wl) == pre_conflict, "introduced a NEW cross-list conflict"
    assert merged[:len(existing_bl)] == existing_bl, "blocklist: existing rows moved"
    assert merged_wl[:len(existing_wl_ordered)] == existing_wl_ordered, "whitelist: existing rows moved"

    vol = sheet.set_index("domain")["total_count"]
    impact = {
        "bl_additions": int(len(bl_add)),
        "wl_additions": int(len(wl_add)),
        "merged_blocklist_size": len(merged),
        "merged_whitelist_size": len(merged_wl),
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
