"""AUDI-1070 Avon (31921) ROAS deep-dive: is there a REAL YoY ROAS decline, or is it
spend + noise? Controls for monthly spend (Avon's ROAS is strongly spend-inverse) and
tests a year effect. Reads outputs/avon_monthly.csv (bq_run.sh output, footer-tolerant)."""
import numpy as np, pandas as pd
from io import StringIO

P = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/outputs/avon_monthly.csv"
raw = open(P).read().splitlines()
s = next(i for i, l in enumerate(raw) if l.startswith("month") or l.startswith("yr") or "spend" in l.split(",")[0:3].__str__())
# robust header find
hdr = next(i for i, l in enumerate(raw) if l.lower().startswith("month"))
rows = []
for l in raw[hdr:]:
    if l.strip() == "" or l.startswith(("---", "Waiting", "Bytes")): break
    rows.append(l)
df = pd.read_csv(StringIO("\n".join(rows)))
for c in ["spend", "roas", "vr", "imps", "visits", "rev", "yr"]:
    if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["spend", "roas"])
df = df[(df.spend > 0) & (df.roas > 0)]
df["ln_roas"], df["ln_spend"] = np.log(df.roas), np.log(df.spend)
print(f"N months = {len(df)} (2024-01 .. 2026-05)\n")

# naive YoY Feb-May
def febmay(yr):
    m = df[(df.yr == yr) & (df.month.astype(str).str[5:7].astype(int).between(2, 5))]
    return m.rev.sum() / m.spend.sum(), m.spend.sum()
r25, s25 = febmay(2025); r26, s26 = febmay(2026)
print(f"Naive Feb-May YoY: ROAS {r25:.1f}x ($ {s25:,.0f}) -> {r26:.1f}x ($ {s26:,.0f})  = {(r26/r25-1)*100:+.0f}%  (2026 spent {(s26/s25-1)*100:+.0f}%)\n")

# ROAS by spend bucket x year (does the spend-ROAS CURVE shift down YoY?)
df["bucket"] = pd.cut(df.spend, [0, 12000, 20000, 1e9], labels=["<12k", "12-20k", ">20k"])
piv = df.pivot_table("roas", "bucket", "yr", aggfunc="mean").round(1)
print("Avg ROAS by spend bucket x year (curve-shift test):")
print(piv.to_string(), "\n")

# Spend-controlled regression: ln_roas ~ ln_spend + year dummies (baseline 2024)
df["d25"] = (df.yr == 2025).astype(float); df["d26"] = (df.yr == 2026).astype(float)
X = np.column_stack([np.ones(len(df)), df.ln_spend, df.d25, df.d26])
y = df.ln_roas.values
beta, *_ = np.linalg.lstsq(X, y, rcond=None)
resid = y - X @ beta
dof = len(df) - X.shape[1]
sigma2 = (resid @ resid) / dof
cov = sigma2 * np.linalg.inv(X.T @ X)
se = np.sqrt(np.diag(cov))
from scipy import stats as st
names = ["intercept", "ln_spend", "year2025", "year2026"]
print("Spend-controlled OLS  ln(ROAS) ~ ln(spend) + year (baseline 2024):")
for n, b, s_ in zip(names, beta, se):
    t = b / s_; p = 2 * (1 - st.t.cdf(abs(t), dof))
    star = "  <-- " + ("SIGNIFICANT" if p < 0.05 else "not significant") if n.startswith("year") else ""
    print(f"  {n:>10}: coef {b:+.3f}  se {s_:.3f}  t {t:+.2f}  p {p:.3f}{star}")
print(f"  R^2 = {1 - (resid@resid)/(((y-y.mean())**2).sum()):.3f}")
print("\nInterpretation: ln_spend coef = elasticity of ROAS to spend (negative = saturation).")
print("year2026 coef ~0 / not significant  => NO real YoY ROAS decline beyond spend + noise.")
