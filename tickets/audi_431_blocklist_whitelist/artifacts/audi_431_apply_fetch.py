"""AUDI-431: apply the live-site fetch verdicts to the remaining on-the-fence domains.

Usage: python3 artifacts/audi_431_apply_fetch.py <high_share_output.json> <rest_output.json>

Decision rule (set by the ticket owner 2026-08-11): a domain only reaches the WHITELIST on
positive, corroborated evidence that it sells. Everything else sides with the BLOCKLIST.

  high-share pass (2 lenses, both fetched): ecommerce iff BOTH lenses say ecommerce
  rest pass (fetch -> confirm stage):       ecommerce iff the fetch said so AND the
                                            independent confirm lens verified it
  not_ecommerce / unreachable / disagreement -> Blocklist

Rows where no lens managed to fetch anything at all stay blank (we know nothing, so we
assert nothing). Writes outputs/audi_431_fetch_calls.csv, consumed by audi_431_build_lists.py.
"""

import json
import sys
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"


def load(p: str) -> dict:
    raw = json.loads(Path(p).read_text())
    return raw.get("result", raw)


def main() -> None:
    hi, rest = load(sys.argv[1]), load(sys.argv[2])
    calls, ev, unresolved = {}, {}, []

    for b in hi["batches"]:
        lenses = [b.get("commerce") or [], b.get("substance") or []]
        seen: dict[str, list] = {}
        for rows in lenses:
            for r in rows:
                seen.setdefault(r["domain"], []).append(r)
        for domain, votes in seen.items():
            if not any(v.get("fetched") for v in votes):
                unresolved.append(domain)
                continue
            shop = all(v["verdict"] == "ecommerce" for v in votes) and len(votes) >= 2
            calls[domain] = "Whitelist" if shop else "Blocklist"
            ev[domain] = " | ".join(f"{v['verdict']}: {v['evidence']}" for v in votes)[:400]

    for b in rest["batches"]:
        confirmed = {c["domain"]: c for c in (b.get("confirms") or [])}
        for r in b.get("rows") or []:
            d = r["domain"]
            if not r.get("fetched") and r["verdict"] != "unreachable":
                unresolved.append(d)
                continue
            if r["verdict"] == "ecommerce":
                c = confirmed.get(d)
                shop = bool(c and c.get("confirmed"))
                ev[d] = f"{r['evidence']} || confirm: {(c or {}).get('evidence', 'not confirmed')}"[:400]
            else:
                shop = False
                ev[d] = f"{r['verdict']}: {r['evidence']}"[:400]
            calls[d] = "Whitelist" if shop else "Blocklist"

    df = pd.DataFrame(
        [{"domain": d, "designation": v, "fetch_evidence": ev.get(d, "")} for d, v in sorted(calls.items())])
    df.to_csv(OUT / "audi_431_fetch_calls.csv", index=False)

    report = {
        "resolved": len(df),
        "whitelist": int((df["designation"] == "Whitelist").sum()),
        "blocklist": int((df["designation"] == "Blocklist").sum()),
        "unresolved_no_fetch": len(set(unresolved) - set(calls)),
    }
    (OUT / "audi_431_fetch_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    wl = df[df["designation"] == "Whitelist"]
    print(f"\nconfirmed shops ({len(wl)}):")
    for _, r in wl.iterrows():
        print(f"  {r['domain']:30s} {r['fetch_evidence'][:110]}")
    if report["unresolved_no_fetch"]:
        print(f"\nleft blank (nothing fetched): {sorted(set(unresolved) - set(calls))}")


if __name__ == "__main__":
    main()
