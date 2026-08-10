"""AUDI-431: replace free-text vertical suggestions with taxonomy-constrained ones.

Usage: python3 artifacts/audi_431_merge_resuggest.py <resuggest_workflow_output.json>

The first corrections pass let the model name a vertical in plain words, so 16 of 55 named
suggestions were not real MNTN verticals. This merges the enum-constrained re-run (every
suggestion copied verbatim from the 152-name wcv roster, or "NONE - not verticalizable")
into audi_431_vertical_corrections.csv and hard-validates the result.
"""

import json
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"


def main() -> None:
    raw = json.loads(Path(sys.argv[1]).read_text())
    result = raw.get("result", raw)
    rows = [r for b in result["batches"] for r in (b.get("rows") or [])]
    new = pd.DataFrame(rows).drop_duplicates("domain")

    corr = pd.read_csv(OUT / "audi_431_vertical_corrections.csv")
    tax = pd.read_csv(OUT / "audi_431_vertical_taxonomy.csv")
    valid = set(tax["vertical_name"]) | {"NONE - not verticalizable"}

    bad = set(new["suggested_vertical"]) - valid
    if bad:
        raise SystemExit(f"enum violated, not in taxonomy: {sorted(bad)}")

    corr = corr.rename(columns={"suggested_vertical": "suggested_vertical_freetext"})
    corr = corr.merge(
        new[["domain", "suggested_vertical", "confidence", "reason"]].rename(
            columns={"confidence": "suggest_confidence", "reason": "suggest_reason"}),
        on="domain", how="left")

    wrong = corr["final_verdict"] == "wrong"
    n_named = (wrong & corr["suggested_vertical"].notna()
               & (corr["suggested_vertical"] != "NONE - not verticalizable")).sum()
    missing = int((wrong & corr["suggested_vertical"].isna()).sum())
    corr = corr.sort_values("n_urls", ascending=False)
    corr.to_csv(OUT / "audi_431_vertical_corrections.csv", index=False)

    print(f"re-suggested: {len(new)} rows, all within the {len(valid) - 1}-name taxonomy")
    print(f"  named a real vertical: {n_named}")
    print(f"  NONE - not verticalizable: {int((corr['suggested_vertical'] == 'NONE - not verticalizable').sum())}")
    if missing:
        print(f"  WARNING: {missing} agreed-wrong rows have no constrained suggestion")
    changed = corr[wrong & (corr["suggested_vertical_freetext"] != corr["suggested_vertical"])]
    print(f"\nchanged vs free-text pass: {len(changed)}")
    print(changed.head(15)[["domain", "vertical_name", "suggested_vertical_freetext", "suggested_vertical"]].to_string(index=False))


if __name__ == "__main__":
    main()
