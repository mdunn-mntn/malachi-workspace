"""AUDI-431: turn the AI-review workflow output into outputs/audi_431_ai_review.csv.

Usage: python3 artifacts/audi_431_extract_ai_review.py <workflow_output.json>

The workflow task output file is {summary, result: {batches: [{file, rows}]}}.
Rows: {domain, verdict in (ecommerce|not_ecommerce|unsure), confidence, reason}.
Reports any manual-band domain the review missed so a gap is never silent.
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
    df = pd.DataFrame(rows).drop_duplicates("domain")

    sheet = pd.read_csv(OUT / "audi_431_decision_sheet.csv")
    sheet["designation"] = sheet["designation"].fillna("")
    dem = OUT / "audi_431_qc_demotions.csv"
    if dem.exists():
        sheet.loc[sheet["domain"].isin(set(pd.read_csv(dem)["domain"])), "designation"] = ""
    manual = set(sheet.loc[sheet["designation"] == "", "domain"])

    missing = manual - set(df["domain"])
    df = df[df["domain"].isin(manual)]
    df[["domain", "verdict", "confidence", "reason"]].to_csv(OUT / "audi_431_ai_review.csv", index=False)

    print(f"AI verdicts: {len(df)} of {len(manual)} manual rows")
    print(df["verdict"].value_counts().to_string())
    if missing:
        print(f"NOT REVIEWED: {len(missing)} domains (ship blank): {sorted(missing)[:10]}")


if __name__ == "__main__":
    main()
