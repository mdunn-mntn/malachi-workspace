"""AUDI-431 shared state: the ONE place the decision sheet's designations are resolved.

Both the list builder and the workbook builder must see identical designations. They did not:
the workbook applied only the QC demotions and so silently shipped a pre-promotion view while
the CSVs were three passes ahead. Any new adjudication pass gets added to OVERLAYS here, once.

Overlays apply in order, each overriding the previous for the domains it names.
"""

from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"

# (filename, designation_source) — later entries win
OVERLAYS = [
    ("audi_431_ai_promotions.csv", "ai-verified"),
    ("audi_431_fetch_calls.csv", "site-fetch"),
    ("audi_431_sweep_calls.csv", "sweep-fetch"),
    ("audi_431_human_calls.csv", "human"),
]


def load_designated_sheet(verbose: bool = True) -> pd.DataFrame:
    """Return the decision sheet with every adjudication pass applied, in order."""
    sheet = pd.read_csv(OUT / "audi_431_decision_sheet.csv")
    sheet["designation"] = sheet["designation"].fillna("")
    sheet["designation_source"] = sheet["designation_source"].fillna("")

    qc = OUT / "audi_431_qc_demotions.csv"
    if qc.exists():
        demoted = set(pd.read_csv(qc)["domain"])
        idx = sheet["domain"].isin(demoted)
        sheet.loc[idx, ["designation", "designation_source", "band"]] = ["", "qc-demoted", "manual"]
        if verbose:
            print(f"QC demotions applied: {int(idx.sum())} rows -> manual")

    for fname, source in OVERLAYS:
        p = OUT / fname
        if not p.exists():
            continue
        df = pd.read_csv(p)
        mapping = dict(zip(df["domain"], df["designation"]))
        idx = sheet["domain"].isin(mapping)
        sheet.loc[idx, "designation"] = sheet.loc[idx, "domain"].map(mapping)
        sheet.loc[idx, "designation_source"] = source
        if verbose:
            n_wl = int((df["designation"] == "Whitelist").sum())
            print(f"{source:12s} applied: {int(idx.sum()):5d} rows ({n_wl} whitelist)")

    return sheet
