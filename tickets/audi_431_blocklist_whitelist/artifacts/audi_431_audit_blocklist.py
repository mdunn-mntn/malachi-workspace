"""AUDI-431: score the blocklist audit and decide whether either tier needs an exhaustive sweep.

Usage: python3 artifacts/audi_431_audit_blocklist.py <audit_workflow_output.json>

Two tiers were sampled (300 each, top-50 by volume + random):
  auditai — AI-promoted blocklist rows whose only basis was a no-web-access verdict
  auditsc — score-band rows (med<=0.05 & pct_ge_04<=0.05) whose only basis is the model itself

A "miss" = a domain we would blocklist that the audit fetched and confirmed is a real store.
Reports the observed rate plus a 95% upper bound (Wilson), and projects the implied miss count
over the full tier. Decision rule: upper bound above 1% means sweep that tier exhaustively.
"""

import json
import math
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"
TIERS = {"auditai": ("AI-promoted (no web access)", 866), "auditsc": ("score-band med<=0.05", 1617)}
SWEEP_THRESHOLD = 0.01


def wilson_upper(k: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 1.0
    p = k / n
    d = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre + margin) / d


def main() -> None:
    raw = json.loads(Path(sys.argv[1]).read_text())
    res = raw.get("result", raw)

    rows, confirms = [], {}
    for b in res["batches"]:
        tier = "auditai" if b["file"].startswith("auditai") else "auditsc"
        for c in b.get("confirms") or []:
            confirms[c["domain"]] = c
        for r in b.get("rows") or []:
            rows.append({**r, "tier": tier})
    df = pd.DataFrame(rows).drop_duplicates("domain")

    df["is_miss"] = df.apply(
        lambda r: r["verdict"] == "ecommerce" and bool(confirms.get(r["domain"], {}).get("confirmed")), axis=1)
    df["confirm_evidence"] = df["domain"].map(lambda d: confirms.get(d, {}).get("evidence", ""))
    df.to_csv(OUT / "audi_431_blocklist_audit.csv", index=False)

    report, verdicts = {}, []
    for tier, (label, total) in TIERS.items():
        sub = df[df["tier"] == tier]
        n, k = len(sub), int(sub["is_miss"].sum())
        ub = wilson_upper(k, n)
        sweep = ub > SWEEP_THRESHOLD
        report[tier] = {
            "label": label, "tier_size": total, "sampled": n, "misses": k,
            "observed_rate": round(k / n, 4) if n else None,
            "upper95": round(ub, 4), "implied_misses_upper": round(ub * total, 1),
            "sweep_recommended": sweep,
        }
        verdicts.append((tier, sweep))
    (OUT / "audi_431_blocklist_audit_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    miss = df[df["is_miss"]]
    if len(miss):
        print(f"\nCONFIRMED MISSES ({len(miss)}) — real stores we would have blocklisted:")
        for _, r in miss.iterrows():
            print(f"  [{r['tier']}] {r['domain']:30s} {r['evidence'][:100]}")
    else:
        print("\nNo confirmed misses in either sample.")
    unreach = int((df["verdict"] == "unreachable").sum())
    print(f"\nunreachable in audit: {unreach} of {len(df)} ({unreach/max(len(df),1):.1%}) — dead domains we are scoring")
    for tier, sweep in verdicts:
        print(f"{tier}: {'SWEEP the remaining rows' if sweep else 'no sweep needed'}")


if __name__ == "__main__":
    main()
