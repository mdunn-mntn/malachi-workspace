"""
TI-961 local CI smoke test

Validates the CausalImpact math + pandas plumbing against a 60-day lean panel
pulled outside Databricks. Substitutes "never-flipped prospecting advertisers"
as the control set (vs Alex's notebook which uses future-flip tiers from
tpa.fangorn_advertiser_inclusion). Same CI mechanics; different control source.

Cohorts (by BQ first_flip_date):
- Wave 1: 3 AIDs flipped 2026-05-01 (27d post)
- Wave 2: ~31 AIDs by update_time 2026-05-05 + ~108 NULL-update_time (Wave-2 quirk)
  → use is_treated AND first_flip_date <= '2026-05-06' for cleanest Wave-2 read
- Wave 3: ~222 AIDs flipped 2026-05-18 (10d post)

Usage:
    python ti_961_local_ci_smoke_test.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from causalimpact import CausalImpact
except ImportError:
    print("Install: pip install causalimpact", file=sys.stderr)
    raise

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------
TICKET_DIR = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ti_961_fangorn_causal_impact")
PANEL_CSV = TICKET_DIR / "outputs" / "ti_961_ci_panel.csv"
OUT_DIR = TICKET_DIR / "outputs"
PLOT_DIR = TICKET_DIR / "artifacts" / "plots"
PLOT_DIR.mkdir(parents=True, exist_ok=True)

# Tier definitions (mapping BQ first_flip_date → wave label)
WAVE_DEFS = [
    ("wave1", pd.Timestamp("2026-05-01"), "first_flip_date == 2026-05-01"),
    ("wave2", pd.Timestamp("2026-05-06"), "first_flip_date <= 2026-05-06 (incl. NULL)"),
    ("wave3", pd.Timestamp("2026-05-18"), "first_flip_date == 2026-05-18"),
]


# ----------------------------------------------------------------------
# Load + classify
# ----------------------------------------------------------------------
def load_panel() -> pd.DataFrame:
    df = pd.read_csv(PANEL_CSV)
    df["day"] = pd.to_datetime(df["day"])
    df["is_treated"] = df["is_treated"].astype(str).str.lower().eq("true")
    df["first_flip_date"] = pd.to_datetime(df["first_flip_date"], errors="coerce")
    print(f"[load] {len(df):,} rows | "
          f"treated AIDs: {df[df['is_treated']]['advertiser_id'].nunique()} | "
          f"untreated AIDs: {df[~df['is_treated']]['advertiser_id'].nunique()} | "
          f"window {df['day'].min().date()} → {df['day'].max().date()}")
    return df


def classify_cohort(panel: pd.DataFrame, wave: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Return panel rows for the wave's treated AIDs, excluding flip day."""
    if wave == "wave1":
        mask = panel["is_treated"] & (panel["first_flip_date"] == cutoff)
    elif wave == "wave2":
        # Wave 2 cohort: flipped on May 5 OR NULL update_time (CDC quirk for Wave 2 specifically)
        mask = panel["is_treated"] & (
            (panel["first_flip_date"] == pd.Timestamp("2026-05-05"))
            | (panel["first_flip_date"].isna())
        )
    elif wave == "wave3":
        mask = panel["is_treated"] & (panel["first_flip_date"] == pd.Timestamp("2026-05-18"))
    else:
        raise ValueError(wave)
    sub = panel[mask].copy()
    # Exclude flip day (Alex's notebook excludes it)
    sub = sub[sub["day"] != cutoff]
    return sub


def build_control_series(panel: pd.DataFrame, exclude_aids: set[int]) -> pd.DataFrame:
    """Never-flipped prospecting AIDs as the synthetic control. Excludes any
    AID in `exclude_aids` (treated-set of the current wave)."""
    base = panel[
        (~panel["is_treated"])
        & (~panel["advertiser_id"].isin(exclude_aids))
    ].copy()
    agg = (base.groupby("day", as_index=False)
                .agg(impressions=("impressions", "sum"), vv=("vv", "sum")))
    agg["control_vr"] = agg["vv"] / agg["impressions"]
    return agg[["day", "control_vr"]]


def build_treated_series(cohort: pd.DataFrame) -> pd.DataFrame:
    agg = (cohort.groupby("day", as_index=False)
                  .agg(impressions=("impressions", "sum"), vv=("vv", "sum")))
    agg["y"] = agg["vv"] / agg["impressions"]
    return agg[["day", "y"]]


# ----------------------------------------------------------------------
# Run CI per wave
# ----------------------------------------------------------------------
def run_one(panel: pd.DataFrame, wave: str, cutoff: pd.Timestamp, label: str) -> dict | None:
    cohort = classify_cohort(panel, wave, cutoff)
    treated_aids = set(cohort["advertiser_id"].unique())
    print(f"\n[{wave}] cutoff={cutoff.date()} | {len(treated_aids)} AIDs | label='{label}'")

    if not treated_aids:
        print(f"  [skip] no treated AIDs found for {wave}")
        return None

    treated = build_treated_series(cohort)
    control = build_control_series(panel, treated_aids)
    df = (treated.merge(control, on="day", how="inner")
                .sort_values("day")
                .set_index("day"))
    df = df.dropna()

    pre_period = [df.index.min().strftime("%Y-%m-%d"),
                  (cutoff - pd.Timedelta(days=1)).strftime("%Y-%m-%d")]
    post_period = [(cutoff + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                   df.index.max().strftime("%Y-%m-%d")]
    n_pre = len(df.loc[pre_period[0]:pre_period[1]])
    n_post = len(df.loc[post_period[0]:post_period[1]])
    print(f"  pre={pre_period} ({n_pre}d) | post={post_period} ({n_post}d)")

    if n_pre < 30 or n_post < 5:
        print(f"  [skip] insufficient days (n_pre={n_pre}, n_post={n_post})")
        return None

    ci = CausalImpact(df[["y", "control_vr"]], pre_period, post_period)
    s = ci.summary_data
    avg_actual = float(s.loc["actual", "average"])
    avg_pred = float(s.loc["predicted", "average"])
    avg_lower = float(s.loc["predicted_lower", "average"])
    avg_upper = float(s.loc["predicted_upper", "average"])

    rel = avg_actual / avg_pred - 1.0 if avg_pred else float("nan")
    rel_lo = avg_actual / avg_upper - 1.0 if avg_upper else float("nan")
    rel_hi = avg_actual / avg_lower - 1.0 if avg_lower else float("nan")
    p = float(ci.p_value)

    print(f"  → rel_effect = {rel:+.2%} [{rel_lo:+.2%}, {rel_hi:+.2%}]  p={p:.3f}")

    try:
        ci.plot()
        fig = plt.gcf()
        fig.savefig(PLOT_DIR / f"ti_961_smoke_{wave}.png", dpi=140, bbox_inches="tight")
        plt.close(fig)
    except Exception as e:
        print(f"  [warn] plot save failed: {e}")

    return {
        "wave": wave,
        "label": label,
        "cutoff": cutoff.strftime("%Y-%m-%d"),
        "treated_aids": len(treated_aids),
        "pre_start": pre_period[0], "pre_end": pre_period[1], "n_pre": n_pre,
        "post_start": post_period[0], "post_end": post_period[1], "n_post": n_post,
        "avg_actual": avg_actual,
        "avg_predicted": avg_pred,
        "rel_effect": rel,
        "rel_ci_95_lower": rel_lo,
        "rel_ci_95_upper": rel_hi,
        "p_value": p,
    }


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------
def main():
    panel = load_panel()
    rows = []
    for wave, cutoff, label in WAVE_DEFS:
        r = run_one(panel, wave, cutoff, label)
        if r:
            rows.append(r)
    if rows:
        df = pd.DataFrame(rows)
        out = OUT_DIR / "ti_961_smoke_ci_results.csv"
        df.to_csv(out, index=False)
        print(f"\n[done] {len(rows)} fits → {out}")
        print(df[["wave", "treated_aids", "n_pre", "n_post",
                  "rel_effect", "rel_ci_95_lower", "rel_ci_95_upper", "p_value"]]
                .to_string(index=False, float_format=lambda x: f"{x:+.4f}"))


if __name__ == "__main__":
    main()
