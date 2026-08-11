"""Does an advertiser's visit/conversion rate tell you what they spend?

Al's ask reads as "estimate spend from VR and CR". This tests that directly on
the 2,009-advertiser INCR-75 cohort. No BigQuery — the metrics CSV already has
spend, IVR and CVR side by side.

  python3 audi_xxx_vr_cr_spend_check.py

Writes outputs/audi_xxx_vr_cr_spend_check.csv and the decile chart.
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

TICKET = Path(__file__).resolve().parents[1]
SRC = (TICKET.parents[0] / "incr_75_eligible_advertisers" / "outputs"
       / "incr_75_advertiser_metrics.csv")
MIN_SPEND = 1000.0


def load():
    rows = []
    for r in csv.DictReader(open(SRC)):
        try:
            s, v, c = float(r["spend_30d"]), float(r["p_visit"]), float(r["p_cvr"])
        except (TypeError, ValueError):
            continue
        if s > MIN_SPEND and v > 0:
            rows.append((s, v, c))
    return (np.array([x[0] for x in rows]), np.array([x[1] for x in rows]),
            np.array([x[2] for x in rows]))


def r2(cols, y):
    X = np.column_stack([np.ones(len(y))] + cols)
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    return 1 - (resid ** 2).sum() / ((y - y.mean()) ** 2).sum()


def main():
    spend, ivr, cvr = load()
    y = np.log(spend)
    log_ivr = np.log(ivr)
    log_cvr = np.log(np.where(cvr > 0, cvr, 1e-6))

    fits = [
        ("log(IVR)", r2([log_ivr], y)),
        ("log(CVR)", r2([log_cvr], y)),
        ("log(IVR) + log(CVR)", r2([log_ivr, log_cvr], y)),
        ("IVR + CVR (levels)", r2([ivr, cvr], y)),
    ]
    print(f"n = {len(spend):,} advertisers (spend_30d > ${MIN_SPEND:,.0f}, IVR > 0)\n")
    print("OLS on log(spend_30d):")
    for name, val in fits:
        print(f"  ~ {name:24} R2 = {val:.4f}")
    print(f"\nPearson r  log(IVR) vs log(spend): {np.corrcoef(log_ivr, y)[0,1]:+.3f}")
    print(f"Pearson r  log(CVR) vs log(spend): {np.corrcoef(log_cvr, y)[0,1]:+.3f}\n")

    edges = np.quantile(ivr, np.linspace(0, 1, 11))
    deciles = []
    print("spend spread inside each IVR decile:")
    for i in range(10):
        m = (ivr >= edges[i]) & (ivr <= edges[i + 1])
        if m.sum() < 5:
            continue
        s = np.sort(spend[m])
        p10, p50, p90 = (s[int(.1 * len(s))], float(np.median(s)), s[int(.9 * len(s))])
        deciles.append(dict(decile=i + 1, ivr_low=edges[i], ivr_high=edges[i + 1],
                            n=int(m.sum()), spend_p10=round(p10), spend_p50=round(p50),
                            spend_p90=round(p90), p90_over_p10=round(p90 / max(p10, 1), 1)))
        print(f"  {edges[i]*100:5.2f}-{edges[i+1]*100:5.2f}%  n={m.sum():4d}  "
              f"p10=${p10:>9,.0f}  p50=${p50:>9,.0f}  p90=${p90:>11,.0f}  "
              f"p90/p10={p90/max(p10,1):.0f}x")

    out = TICKET / "outputs" / "audi_xxx_vr_cr_spend_check.csv"
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(deciles[0].keys()))
        w.writeheader()
        w.writerows(deciles)

    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    ax.set_facecolor("#FAFAFA")
    x = np.arange(len(deciles))
    lo = [d["spend_p10"] for d in deciles]
    hi = [d["spend_p90"] for d in deciles]
    md = [d["spend_p50"] for d in deciles]
    ax.vlines(x, lo, hi, color="#B8B8B8", linewidth=6, zorder=1)
    ax.plot(x, md, "o", color="#1A1A1A", markersize=6, zorder=2)
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{d['ivr_low']*100:.1f}" for d in deciles])
    ax.set_xlabel("Advertiser visit rate, decile floor (%)")
    ax.set_ylabel("30-day spend (log scale)")
    ax.set_title("Visit rate barely narrows the spend range\n"
                 f"p10-p90 spend within each IVR decile, n={len(spend):,} advertisers",
                 loc="left", fontsize=11)
    combined = dict(fits)["log(IVR) + log(CVR)"]
    ax.annotate(f"IVR + CVR explain {combined*100:.0f}% of spend variance",
                xy=(0.02, 0.04), xycoords="axes fraction", fontsize=9, color="#555555")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color="#EEEEEE", linewidth=0.8)
    ax.set_axisbelow(True)
    fig.tight_layout()
    png = TICKET / "artifacts" / "audi_xxx_chart_vr_cr_spend.png"
    fig.savefig(png, facecolor="#FAFAFA")
    print(f"\n[ok] wrote {out}\n[ok] wrote {png}")


if __name__ == "__main__":
    main()
