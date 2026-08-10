"""AUDI-431 Phase 4b: apply the QC + corrections workflow output.

Reads outputs/audi_431_workflow_result.json (saved from the Workflow run):
  qc:          [{file, verdicts: [{domain, dispute, reason}]}]
  corrections: [{file, judge: [rows], defend: [rows]}]  rows: {domain, verdict, suggested_vertical, confidence, reason}

QC rules:
  - any disputed row -> demoted to manual (listed in audi_431_qc_demotions.csv)
  - auto_WL was 100% sampled -> per-row demotion is the band evaluation
  - auto_BL was sampled (100 of 1,617) -> if dispute rate > 5%, the WHOLE band demotes

Corrections: final_verdict = 'wrong' only when judge AND defend both say wrong
(perspective-diverse double judgment); otherwise the softer of the two verdicts.
"""

import json
from pathlib import Path

import pandas as pd

TICKET = Path(__file__).resolve().parents[1]
OUT = TICKET / "outputs"

SOFTNESS = {"plausible": 0, "unsure": 1, "wrong": 2}


def main() -> None:
    res = json.loads((OUT / "audi_431_workflow_result.json").read_text())
    sheet = pd.read_csv(OUT / "audi_431_decision_sheet.csv")
    band_by_domain = dict(zip(sheet["domain"], sheet["band"]))

    demotions, per_band = [], {"auto_whitelist": [0, 0], "auto_blocklist": [0, 0]}
    for batch in res["qc"]:
        for v in batch["verdicts"]:
            band = band_by_domain.get(v["domain"])
            if band not in per_band:
                continue
            per_band[band][1] += 1
            if v["dispute"]:
                per_band[band][0] += 1
                demotions.append({"domain": v["domain"], "band": band, "reason": v["reason"]})

    wl_rate = per_band["auto_whitelist"][0] / max(per_band["auto_whitelist"][1], 1)
    bl_rate = per_band["auto_blocklist"][0] / max(per_band["auto_blocklist"][1], 1)
    bl_wholesale = bl_rate > 0.05
    if bl_wholesale:
        for d, b in band_by_domain.items():
            if b == "auto_blocklist":
                demotions.append({"domain": d, "band": b, "reason": f"band-wholesale demotion: sampled dispute rate {bl_rate:.1%} > 5%"})

    dem = pd.DataFrame(demotions).drop_duplicates("domain") if demotions else pd.DataFrame(columns=["domain", "band", "reason"])
    dem.to_csv(OUT / "audi_431_qc_demotions.csv", index=False)
    report = {
        "auto_wl_sampled": per_band["auto_whitelist"][1], "auto_wl_disputed": per_band["auto_whitelist"][0],
        "auto_bl_sampled": per_band["auto_blocklist"][1], "auto_bl_disputed": per_band["auto_blocklist"][0],
        "bl_dispute_rate": round(bl_rate, 4), "wl_dispute_rate": round(wl_rate, 4),
        "bl_band_wholesale_demoted": bl_wholesale, "n_demotions": int(len(dem)),
    }
    (OUT / "audi_431_qc_report.json").write_text(json.dumps(report, indent=2))
    print("QC:", json.dumps(report))
    if len(dem):
        print(dem.to_string(index=False))

    rows = []
    for batch in res["corrections"]:
        judge = {r["domain"]: r for r in (batch.get("judge") or [])}
        defend = {r["domain"]: r for r in (batch.get("defend") or [])}
        for domain in judge.keys() | defend.keys():
            j, d = judge.get(domain), defend.get(domain)
            jv = j["verdict"] if j else "unsure"
            dv = d["verdict"] if d else "unsure"
            final = "wrong" if (jv == "wrong" and dv == "wrong") else min(jv, dv, key=lambda x: SOFTNESS[x])
            rows.append({
                "domain": domain, "final_verdict": final,
                "judge_verdict": jv, "defend_verdict": dv,
                "judge_confidence": j.get("confidence") if j else None,
                "defend_confidence": d.get("confidence") if d else None,
                "suggested_vertical": (j or {}).get("suggested_vertical") or (d or {}).get("suggested_vertical") or "",
                "judge_reason": (j or {}).get("reason", ""), "defend_reason": (d or {}).get("reason", ""),
            })
    corr = pd.DataFrame(rows)
    traffic = pd.read_csv(OUT / "audi_431_corrections_top500.csv")
    corr = traffic.merge(corr, on="domain", how="left")
    corr = corr.sort_values("n_urls", ascending=False)
    corr.to_csv(OUT / "audi_431_vertical_corrections.csv", index=False)
    n_wrong = (corr["final_verdict"] == "wrong").sum()
    print(f"corrections: {len(corr)} judged, {n_wrong} agreed-wrong, "
          f"{(corr['final_verdict'] == 'unsure').sum()} unsure")
    print(corr[corr["final_verdict"] == "wrong"].head(15)[["domain", "vertical_name", "suggested_vertical", "n_urls"]].to_string(index=False))


if __name__ == "__main__":
    main()
