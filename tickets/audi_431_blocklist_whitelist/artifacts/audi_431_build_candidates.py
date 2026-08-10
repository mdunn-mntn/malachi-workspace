"""AUDI-431 Phase 2: build the candidate frame from 28d of missing_domains + run the overlap gate.

Inputs (downloaded from GCS, gitignored):
  outputs/raw/missing_domains/dt=YYYY-MM-DD/*.parquet   cols: domain, count (dt from dir if not physical)
  outputs/raw/ecommerce_blocklist.csv                   headerless bare domains
  outputs/raw/ecommerce_whitelist.csv.gz                headerless bare domains, gzip
  outputs/raw/wcv/*.parquet                             cols: domain_name, vertical_id, ...

Outputs:
  outputs/audi_431_candidates_all.csv     per-domain aggregate, ranked desc
  outputs/audi_431_candidates_topn.csv    top-N adjudication set (non-junk, stability floor)
  outputs/audi_431_junk_domains.csv       junk-tier rows with tier labels
  outputs/audi_431_overlap_gate.json      kill-criterion gate counts (expect 0/0/0)
"""

import gzip
import json
import re
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
RAW = TICKET / "outputs" / "raw"
OUT = TICKET / "outputs"

WINDOW_START, WINDOW_END = "2026-07-13", "2026-08-09"
LAST7_START = "2026-08-03"
TOP_N_CAP = 3000
CUM_SHARE_TARGET = 0.80
MIN_DAYS_SEEN = 7

INFRA = {"steelhouse.com", "googlesyndication.com", "gtm-msr.appspot.com"}
DS13_HARDBLOCK = {"yahoo.com", "aol.com", "easybrain.com"}
IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


def junk_tier(d: str) -> str:
    if d.startswith("Unable to parse domain"):
        return "unparseable"
    if IP_RE.match(d):
        return "ip_literal"
    if d.endswith("."):
        return "trailing_dot"
    if "." not in d:
        return "single_label"
    if d in INFRA:
        return "infra"
    if d in DS13_HARDBLOCK:
        return "ds13_hardblock"
    return ""


def load_missing_domains() -> pd.DataFrame:
    frames = []
    for dt_dir in sorted((RAW / "missing_domains").glob("dt=*")):
        dt = dt_dir.name.split("=", 1)[1]
        if not (WINDOW_START <= dt <= WINDOW_END):
            continue
        for f in dt_dir.glob("*.parquet"):
            df = pd.read_parquet(f, columns=None)
            if "dt" not in df.columns:
                df["dt"] = dt
            frames.append(df[["domain", "dt", "count"]])
    md = pd.concat(frames, ignore_index=True)
    md = md.groupby(["domain", "dt"], as_index=False)["count"].sum()
    return md


def main() -> None:
    md = load_missing_domains()
    n_days = md["dt"].nunique()
    print(f"missing_domains: {len(md):,} (domain,dt) rows, {n_days} days, "
          f"{md['domain'].nunique():,} distinct domains, total volume {md['count'].sum():,}")

    agg = md.groupby("domain").agg(
        total_count=("count", "sum"),
        days_seen=("dt", "nunique"),
        last_dt=("dt", "max"),
    ).reset_index()
    agg["active_last_7d"] = agg["last_dt"] >= LAST7_START
    agg = agg.sort_values("total_count", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1
    agg["cum_share"] = agg["total_count"].cumsum() / agg["total_count"].sum()
    agg["junk_tier"] = agg["domain"].map(junk_tier)
    agg["is_punycode"] = agg["domain"].str.contains(r"(?:^|\.)xn--", regex=True)

    blocklist = set(
        (RAW / "ecommerce_blocklist.csv").read_text().splitlines()
    ) - {""}
    with gzip.open(RAW / "ecommerce_whitelist.csv.gz", "rt") as fh:
        whitelist = {line.strip() for line in fh} - {""}
    wcv = pd.concat(
        [pd.read_parquet(f, columns=["domain_name"]) for f in (RAW / "wcv").glob("*.parquet")],
        ignore_index=True,
    )
    wcv_set = set(wcv["domain_name"])
    print(f"lists: blocklist={len(blocklist):,} whitelist={len(whitelist):,} wcv={len(wcv_set):,}")

    domains = set(agg["domain"])
    gate = {
        "n_candidate_domains": len(domains),
        "overlap_blocklist": len(domains & blocklist),
        "overlap_whitelist": len(domains & whitelist),
        "overlap_wcv": len(domains & wcv_set),
        "window": [WINDOW_START, WINDOW_END],
        "n_days": int(n_days),
    }
    (OUT / "audi_431_overlap_gate.json").write_text(json.dumps(gate, indent=2))
    print("OVERLAP GATE:", json.dumps(gate))
    for k in ("overlap_blocklist", "overlap_whitelist", "overlap_wcv"):
        if gate[k] > 0:
            ex = sorted(domains & (blocklist if k.endswith("blocklist") else whitelist if k.endswith("whitelist") else wcv_set))[:10]
            print(f"  NONZERO {k}: examples {ex}")

    junk = agg[agg["junk_tier"] != ""]
    junk.to_csv(OUT / "audi_431_junk_domains.csv", index=False)

    clean = agg[agg["junk_tier"] == ""]
    eligible = clean[(clean["days_seen"] >= MIN_DAYS_SEEN) & clean["active_last_7d"]]
    topn = eligible.head(TOP_N_CAP).copy()
    topn["in_core_80pct"] = topn["cum_share"] <= CUM_SHARE_TARGET

    agg.to_csv(OUT / "audi_431_candidates_all.csv", index=False)
    topn.to_csv(OUT / "audi_431_candidates_topn.csv", index=False)
    print(f"junk rows: {len(junk):,} ({junk['total_count'].sum():,} volume)")
    print(f"eligible (days_seen>={MIN_DAYS_SEEN} & active): {len(eligible):,}")
    print(f"top-N selected: {len(topn):,} (cap {TOP_N_CAP}); core-80% rows: {int(topn['in_core_80pct'].sum())}")
    print(f"top-N covers {topn['total_count'].sum() / agg['total_count'].sum():.1%} of 28d missing volume")
    print(topn.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
