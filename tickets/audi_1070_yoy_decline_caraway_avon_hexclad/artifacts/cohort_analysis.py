"""AUDI-1070 Step 5: cohort-level falsification of 'systemic MM degradation'.
Tests whether YoY VR/ROAS decline tracks spend-growth across the whole advertiser
cohort (saturation = general law) vs hits everyone regardless of spend (systemic MM).
Reads outputs/q5_cohort.csv (bq_run.sh output incl. footer -> parsed robustly)."""
import numpy as np, pandas as pd
from io import StringIO

BASE = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/outputs/q5_cohort.csv"
AIDS = {40341: "Caraway", 31921: "Avon", 34611: "HexClad"}

raw = open(BASE).read().splitlines()
start = next(i for i, l in enumerate(raw) if l.startswith("advertiser_id"))
rows = []
for l in raw[start:]:
    if l.strip() == "" or l.startswith("---") or l.startswith("Waiting") or l.startswith("Bytes"):
        break
    rows.append(l)
df = pd.read_csv(StringIO("\n".join(rows)))
for c in df.columns:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["s25", "s26", "i25", "i26", "v25", "v26", "r25", "r26"])

df["spend_growth"] = df.s26 / df.s25
df["imp_growth"]  = df.i26 / df.i25
df["vr25"] = df.v25 / df.i25
df["vr26"] = df.v26 / df.i26
df["vr_ratio"] = df.vr26 / df.vr25
df["roas25"] = df.r25 / df.s25
df["roas26"] = df.r26 / df.s26
df["roas_ratio"] = df.roas26 / df.roas25
# keep advertisers with positive, finite ratios
df = df[(df.vr25 > 0) & (df.vr26 > 0) & (df.roas25 > 0) & (df.roas26 > 0)]
df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["vr_ratio", "roas_ratio", "spend_growth"])
N = len(df)
print(f"Cohort N = {N} advertisers (spend>$20k & imps>100k in BOTH Feb-May 2025 & 2026)\n")

# Spearman correlations: does decline track spend growth?
def spearman(a, b):
    return pd.Series(a).rank().corr(pd.Series(b).rank())
print("Spearman rank correlations (across cohort):")
print(f"  spend_growth vs VR_ratio   : {spearman(df.spend_growth, df.vr_ratio):+.3f}")
print(f"  spend_growth vs ROAS_ratio : {spearman(df.spend_growth, df.roas_ratio):+.3f}")
print(f"  imp_growth   vs VR_ratio   : {spearman(df.imp_growth, df.vr_ratio):+.3f}\n")

# Flat-spend vs high-growth contrast (the systemic-vs-saturation test)
flat = df[(df.spend_growth >= 0.8) & (df.spend_growth <= 1.25)]
grow = df[df.spend_growth >= 1.5]
print("The systemic-vs-saturation test (median YoY ratios; 1.0 = no change):")
print(f"  FLAT-spend advertisers (0.8-1.25x, n={len(flat)}): VR x{flat.vr_ratio.median():.2f}, ROAS x{flat.roas_ratio.median():.2f}")
print(f"  GREW-spend advertisers (>=1.5x,   n={len(grow)}): VR x{grow.vr_ratio.median():.2f}, ROAS x{grow.roas_ratio.median():.2f}\n")

# Decile table by spend growth
df["sg_decile"] = pd.qcut(df.spend_growth, 10, labels=False, duplicates="drop")
dec = df.groupby("sg_decile").agg(n=("advertiser_id", "size"),
                                  spend_growth_med=("spend_growth", "median"),
                                  vr_ratio_med=("vr_ratio", "median"),
                                  roas_ratio_med=("roas_ratio", "median")).round(3)
print("Saturation gradient — median YoY ratio by spend-growth decile:")
print(dec.to_string())
print()

# Where do the 3 AIDs sit?
def pct(series, val):  # percentile rank of val within series
    return round((series < val).mean() * 100, 1)
print("The three AIDs vs cohort (percentile = how many advertisers are BELOW them):")
print(f"{'AID':>20} {'spend_x':>8} {'VR_x':>6} {'ROAS_x':>7} | {'sg_pctile':>9} {'vr_pctile':>9} {'roas_pctile':>11}")
for aid, name in AIDS.items():
    r = df[df.advertiser_id == aid]
    if len(r) == 0:
        print(f"{name:>20}  (not in cohort filter)"); continue
    r = r.iloc[0]
    print(f"{name:>20} {r.spend_growth:8.2f} {r.vr_ratio:6.2f} {r.roas_ratio:7.2f} | "
          f"{pct(df.spend_growth, r.spend_growth):9.1f} {pct(df.vr_ratio, r.vr_ratio):9.1f} {pct(df.roas_ratio, r.roas_ratio):11.1f}")

# Are the 3 AIDs abnormal GIVEN their spend growth? Compare their VR_ratio to the
# median VR_ratio of cohort peers in the same spend-growth decile.
print("\nResidual test — are they worse than peers at the SAME spend-growth level?")
for aid, name in AIDS.items():
    r = df[df.advertiser_id == aid]
    if len(r) == 0: continue
    r = r.iloc[0]
    peers = df[df.sg_decile == r.sg_decile]
    print(f"  {name:>9}: VR x{r.vr_ratio:.2f} vs peer-median x{peers.vr_ratio.median():.2f} "
          f"(same spend-growth decile, n={len(peers)}) -> "
          f"{'WORSE' if r.vr_ratio < peers.vr_ratio.median() else 'IN-LINE/BETTER'}")
