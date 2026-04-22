"""TI-896 Track C — bootstrap median CIs for ROAS / conv-rate / AOV deltas.

Fixes:
  M1 — report n_total per cohort AND n_with_valid_metric (drops the
       silent zero-ROAS rows that produce NULL deltas).
  M3 — bootstrap 95% CIs around each median; report whether intervals
       overlap (i.e. whether the +46 vs +124 framing survives).

Reads outputs/ti_896_pp_vs_conv_scatter.csv (per-advertiser deltas) and
prints + saves a JSON summary with cohort sizes and bootstrap CIs.

Bootstrap: 1000 resamples with replacement, 95% percentile interval.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

TICKET_DIR = Path(__file__).resolve().parent.parent
OUTPUTS = TICKET_DIR / "outputs"
ARTIFACTS = TICKET_DIR / "artifacts"

RNG_SEED = 20260422
N_BOOT = 1000


def boot_median_ci(values: np.ndarray, n_boot: int = N_BOOT, seed: int = RNG_SEED) -> tuple[float, float, float]:
    rng = np.random.default_rng(seed)
    n = len(values)
    if n == 0:
        return (float("nan"), float("nan"), float("nan"))
    medians = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        sample = rng.choice(values, size=n, replace=True)
        medians[i] = np.median(sample)
    lo, hi = np.percentile(medians, [2.5, 97.5])
    return (float(np.median(values)), float(lo), float(hi))


def summarize(df: pd.DataFrame) -> dict:
    out: dict[str, dict] = {}

    cohort_flags = [
        ("new_adopter", "is_pp_new_adopter"),
        ("non_adopter", "is_non_adopter"),
        ("continuing",  "is_pp_continuing"),
    ]

    metrics = [
        ("delta_roas_rel", "delta_roas_rel"),
        ("delta_conv_rate_rel", "delta_conv_rate_rel"),
        ("delta_aov_rel", "delta_aov_rel"),
    ]

    for cohort_name, flag_col in cohort_flags:
        mask = df[flag_col].astype(str).str.lower() == "true"
        n_total = int(mask.sum())
        cohort_block: dict[str, object] = {"n_total": n_total}
        for metric_name, col in metrics:
            vals = df.loc[mask, col].to_numpy(dtype=float)
            valid = vals[np.isfinite(vals)]
            n_valid = int(len(valid))
            median, lo, hi = boot_median_ci(valid)
            cohort_block[metric_name] = {
                "n_valid": n_valid,
                "median": median,
                "ci_low": lo,
                "ci_high": hi,
            }
        out[cohort_name] = cohort_block

    # CI overlap on delta_roas_rel between new_adopter and non_adopter
    ad = out["new_adopter"]["delta_roas_rel"]
    nad = out["non_adopter"]["delta_roas_rel"]
    out["delta_roas_overlap"] = {
        "new_adopter_ci": [ad["ci_low"], ad["ci_high"]],
        "non_adopter_ci": [nad["ci_low"], nad["ci_high"]],
        "overlap": not (ad["ci_high"] < nad["ci_low"] or nad["ci_high"] < ad["ci_low"]),
    }
    return out


def main() -> None:
    df = pd.read_csv(OUTPUTS / "ti_896_pp_vs_conv_scatter.csv")
    summary = summarize(df)

    out_path = ARTIFACTS / "ti_896_track_c_bootstrap.json"
    out_path.write_text(json.dumps(summary, indent=2))
    print(f"wrote {out_path}\n")

    print("=== Track C bootstrap summary (1,000 resamples, 95% CI) ===\n")
    for cohort in ("new_adopter", "non_adopter", "continuing"):
        block = summary[cohort]
        print(f"--- {cohort} (n_total={block['n_total']}) ---")
        for metric in ("delta_roas_rel", "delta_conv_rate_rel", "delta_aov_rel"):
            m = block[metric]
            pct_valid = (m["n_valid"] / block["n_total"] * 100) if block["n_total"] > 0 else 0.0
            print(f"  {metric:24s}  median={m['median']*100:+7.1f}%  CI95=[{m['ci_low']*100:+7.1f}%, {m['ci_high']*100:+7.1f}%]  n_valid={m['n_valid']:4d} ({pct_valid:5.1f}% of cohort)")
        print()

    overlap = summary["delta_roas_overlap"]
    ad_lo, ad_hi = overlap["new_adopter_ci"]
    nad_lo, nad_hi = overlap["non_adopter_ci"]
    print(f"delta_roas CI overlap test:")
    print(f"  new_adopter CI95 = [{ad_lo*100:+.1f}%, {ad_hi*100:+.1f}%]")
    print(f"  non_adopter CI95 = [{nad_lo*100:+.1f}%, {nad_hi*100:+.1f}%]")
    print(f"  CIs overlap?     = {overlap['overlap']}")
    if overlap["overlap"]:
        print("  -> SOFTEN headline language; medians differ but CIs overlap, gap may be sampling noise.")
    else:
        print("  -> Headline gap is robust to resampling; CIs do NOT overlap.")


if __name__ == "__main__":
    main()
