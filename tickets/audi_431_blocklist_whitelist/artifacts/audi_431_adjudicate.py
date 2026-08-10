"""AUDI-431 Phase 4: merge candidates + prod scores, assign designation bands.

Bands (every row carries the rule that fired):
  auto-Whitelist : n_urls >= 30 AND med_score >= 0.9181 (TGT-4016 P90) AND pct_ge_04 >= 0.9
  auto-Blocklist : n_urls >= 30 AND med_score <= 0.05 AND pct_ge_04 <= 0.05
                   (median deep in non-ecomm territory AND <5% of URLs would clear the prod
                    0.4 gate - blocklisting codifies what the model already decides)
  junk-rule      : stable trailing-dot parse artifacts (>=1M vol, >=14 days) - Blocklist,
                   'localhost.' precedent
  manual         : everything else - designation left BLANK for Malachi

Strict indicator columns: strict_med002 (med<=0.02), strict_p10 (med<=0.0002, TGT-4016 P10).
"""

from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"

P90, BL_MED, BL_PCT, MIN_URLS = 0.9181, 0.05, 0.05, 30
TOTAL_VOL = 16_046_965_205


def main() -> None:
    t = pd.read_csv(OUT / "audi_431_candidates_topn.csv")
    s = pd.read_csv(OUT / "audi_431_score_aggregates.csv")
    m = t.merge(s, on="domain", how="left")

    wl = (m["n_urls"] >= MIN_URLS) & (m["med_score"] >= P90) & (m["pct_ge_04"] >= 0.9)
    bl = (m["n_urls"] >= MIN_URLS) & (m["med_score"] <= BL_MED) & (m["pct_ge_04"] <= BL_PCT)

    m["band"] = "manual"
    m.loc[bl, "band"] = "auto_blocklist"
    m.loc[wl, "band"] = "auto_whitelist"
    m["band_rule"] = ""
    m.loc[wl, "band_rule"] = f"n_urls>={MIN_URLS} & med>={P90} & pct_ge_04>=0.9"
    m.loc[bl, "band_rule"] = f"n_urls>={MIN_URLS} & med<={BL_MED} & pct_ge_04<={BL_PCT}"
    m["designation"] = ""
    m.loc[wl, "designation"] = "Whitelist"
    m.loc[bl, "designation"] = "Blocklist"
    m["designation_source"] = ""
    m.loc[wl | bl, "designation_source"] = "auto-score"
    m["strict_med002"] = m["med_score"] <= 0.02
    m["strict_p10"] = m["med_score"] <= 0.0002

    j = pd.read_csv(OUT / "audi_431_junk_domains.csv")
    stable_junk = j[
        (j["junk_tier"] == "trailing_dot")
        & (j["total_count"] >= 1_000_000)
        & (j["days_seen"] >= 14)
    ].copy()
    stable_junk["band"] = "junk_rule"
    stable_junk["band_rule"] = "stable trailing-dot parse artifact ('localhost.' precedent)"
    stable_junk["designation"] = "Blocklist"
    stable_junk["designation_source"] = "junk-rule"
    stable_junk["in_core_80pct"] = stable_junk["cum_share"] <= 0.80

    sheet = pd.concat([m, stable_junk], ignore_index=True)
    sheet = sheet.sort_values("total_count", ascending=False).reset_index(drop=True)
    cols = [
        "rank", "domain", "total_count", "days_seen", "in_core_80pct", "junk_tier",
        "n_urls", "n_ips", "med_score", "p90_score", "pct_ge_04", "days_scored",
        "band", "band_rule", "designation", "designation_source",
        "strict_med002", "strict_p10",
    ]
    sheet = sheet[[c for c in cols if c in sheet.columns]]
    sheet.to_csv(OUT / "audi_431_decision_sheet.csv", index=False)

    for b in ("auto_whitelist", "auto_blocklist", "junk_rule", "manual"):
        sub = sheet[sheet["band"] == b]
        print(f"{b:15s}: {len(sub):5d} rows, {sub['total_count'].sum() / TOTAL_VOL:6.1%} of 28d missing volume")
    print(f"decision sheet: {len(sheet)} rows -> outputs/audi_431_decision_sheet.csv")


if __name__ == "__main__":
    main()
