"""AUDI-431: apply the exhaustive blocklist sweep — rescue confirmed stores, keep the rest blocked.

Usage: python3 artifacts/audi_431_apply_sweep.py <sweep_output.json> [<audit_output.json> ...]

A domain moves OFF the blocklist and ONTO the whitelist only when the sweep called it a store
AND an independent confirm fetch verified the purchase mechanism. Everything else stays
blocklisted, which is the standing rule (when not clearly a shop, side with blocklist).

Also records the unreachable set: dead domains we are paying to score every day.
Writes outputs/audi_431_sweep_calls.csv, consumed by audi_431_build_lists.py.
"""

import json
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"


def main() -> None:
    rows, confirms = [], {}
    for path in sys.argv[1:]:
        raw = json.loads(Path(path).read_text())
        res = raw.get("result", raw)
        for b in res["batches"]:
            for c in b.get("confirms") or []:
                confirms[c["domain"]] = c
            rows.extend(b.get("rows") or [])

    df = pd.DataFrame(rows).drop_duplicates("domain")
    df["confirmed"] = df["domain"].map(lambda d: bool(confirms.get(d, {}).get("confirmed")))
    df["rescued"] = (df["verdict"] == "ecommerce") & df["confirmed"]
    df["confirm_evidence"] = df["domain"].map(lambda d: confirms.get(d, {}).get("evidence", ""))

    calls = df.assign(designation=lambda x: x["rescued"].map({True: "Whitelist", False: "Blocklist"}))
    calls[["domain", "designation", "verdict", "evidence", "confirm_evidence"]].to_csv(
        OUT / "audi_431_sweep_calls.csv", index=False)

    dead = df[df["verdict"] == "unreachable"]["domain"]
    dead.to_frame("domain").to_csv(OUT / "audi_431_dead_domains.csv", index=False)

    claimed = int((df["verdict"] == "ecommerce").sum())
    report = {
        "checked": len(df),
        "claimed_stores": claimed,
        "confirmed_stores_rescued": int(df["rescued"].sum()),
        "confirm_rejected": claimed - int(df["rescued"].sum()),
        "stay_blocklisted": int((~df["rescued"]).sum()),
        "unreachable": int(len(dead)),
    }
    (OUT / "audi_431_sweep_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    resc = df[df["rescued"]]
    print(f"\nRESCUED STORES ({len(resc)}) — moved from blocklist to whitelist:")
    for _, r in resc.sort_values("domain").iterrows():
        print(f"  {r['domain']:34s} {r['confirm_evidence'][:95]}")


if __name__ == "__main__":
    main()
