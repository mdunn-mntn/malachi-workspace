#!/usr/bin/env python3
"""AUDI-1089 evidence report — one self-contained HTML: all charts (embedded) + evidence tables.
Reads ../outputs/*.csv + the chart PNGs. Re-run any time; verdicts pulled from VERDICTS below."""
import base64
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "outputs")

DS_NAME = {23: "guid_log (internal)", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
           30: "augmentor (internal)", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33Across API"}
BILLING = {24: "$0.50 CPM", 25: "flat fee", 26: "flat fee", 28: "$0.50 CPM", 33: "$0.50 CPM",
           36: "$0.50 CPM", 39: "flat fee", 40: "$0.50 CPM", 23: "internal", 30: "internal"}
# verdict, one-liner  (updated as evals complete)
VERDICTS = {
    25: ("KEEP", "TI-1027: #2 unique contributor, 3.4x leverage, B2B-concentrated; fair band $15–50K/mo"),
    39: ("DROP unless ~free", "126 sole classified domains; 26 gated sole imps/wk; max defensible fee ~$0.1–1.5K/yr"),
    24: ("KEEP — trim meter", "4,605 sole classified (least-redundant CPM vendor; 19.6% IPv6 undercounts it); bill ~$77K/yr vs $14–60K/yr band — just over the top"),
    26: ("KEEP (renew, lock price)", "226,826 sole classified = 2.2× all other externals combined ($0.7–3M/yr value); HARD non-MM dependency (HEM→CRM/identity); dropped metadata = negotiation lever"),
    28: ("NEGOTIATE — cap ≤$100K/yr", "bill ~$422K/yr vs $30–100K/yr band = 4–7× over; 54% tied; 38.6% augmentor-match corroboration (AUDI-647)"),
    33: ("DROP", "bill ~$116K/yr vs $0.5–2.4K/yr band = 50–200× over; 80% same-day tied; 0 visits/wk on sole IPs; PMP inventory relationship unaffected"),
    36: ("DROP", "bill ~$21.5K/yr vs $1.1–4.7K/yr band; 362 sole classified; off-switch = Sean removes 36 from ENABLED_DSIDS"),
    40: ("DROP / renegotiate", "bill ~$176K/yr vs $10–40K/yr band; 2% domain-unique (pair-depth ≠ classification value); ~81% of pixel-topic infra load"),
}

# Jun-2026 metered bill (from audi_1089_metered_usage_by_month.csv); flat-fee = None
def bills():
    b = {}
    with open(os.path.join(OUT, "audi_1089_metered_usage_by_month.csv")) as f:
        for r in csv.DictReader(f):
            if r["mo"] == "2026-06":
                b[int(r["data_source_id"])] = float(r["usage_dollars"])
    return b


BILLS = None


def rows(name):
    with open(os.path.join(OUT, name)) as f:
        return {int(r["data_source_id"]): r for r in csv.DictReader(f)}


def tiers(name):
    t = {}
    with open(os.path.join(OUT, name)) as f:
        for r in csv.DictReader(f):
            t.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r
    return t


def img(png):
    with open(os.path.join(HERE, png), "rb") as f:
        return f'<img src="data:image/png;base64,{base64.b64encode(f.read()).decode()}" alt="{png}">'


def fnum(v, dec=0):
    return f"{float(v):,.{dec}f}"


reach = rows("audi_1089_window_reach_30d.csv")
dom = rows("audi_1089_uniqueness_domains_30d.csv")
rec = rows("audi_1089_recency_pairs_30d.csv")
val = rows("audi_1089_value_tiers.csv")
vr = rows("audi_1089_vr_sole_by_ds.csv")
tier = tiers("audi_1089_score_tiers_sole_vs_touched.csv")

EXTERNAL = [26, 25, 28, 40, 24, 33, 36, 39]
INTERNAL = [30, 23]


def master_row(d):
    v, note = VERDICTS.get(d, ("—", ""))
    cls = "keep" if v.startswith("KEEP") else ("drop" if v.startswith(("PASS", "DROP")) else "pend")
    if v.startswith("NEGOTIATE"):
        cls = "pend"
    sole_del = tier[d]["sole"]["delivered_ips"]
    bill = BILLS.get(d)
    bill_txt = f"${bill:,.0f} (~${bill*12/1e3:,.0f}K/yr)" if bill else ("unknown (flat fee)" if d not in (23, 30) else "—")
    return f"""<tr>
      <td class="l"><b>{DS_NAME[d]}</b> <span class="ds">DS{d}</span></td>
      <td>{BILLING[d]}</td>
      <td>{bill_txt}</td>
      <td>{fnum(float(reach[d]['ips_30d'])/1e6,1)}M</td>
      <td>{fnum(reach[d]['domains_30d'])}</td>
      <td><b>{fnum(dom[d]['sole_classified'])}</b></td>
      <td>{rec[d]['pct_sole']}%</td>
      <td>{rec[d]['pct_tied']}%</td>
      <td>{fnum(sole_del)}</td>
      <td>{fnum(val[d]['imps_sole'])}</td>
      <td><b>{fnum(val[d]['imps_sole_scored_nonrtc'])}</b></td>
      <td>{float(vr[d]['vr_overall_pct']):.3f}%</td>
      <td class="l {cls}"><b>{v}</b><div class="note">{note}</div></td>
    </tr>"""


BILLS = bills()

table = f"""<table>
<thead><tr>
  <th class="l">Source</th><th>Billing</th><th>Jun '26 bill (metered)</th><th>IPs (30d)</th><th>Domains (30d)</th>
  <th>Sole classified domains</th><th>% pairs sole</th><th>% tied (insurance)</th>
  <th>Sole IPs delivered /wk</th><th>Sole imps /wk (T2)</th><th>Gated sole imps /wk (T1)</th>
  <th>Sole-IP VR</th><th class="l">Verdict</th>
</tr></thead><tbody>
{''.join(master_row(d) for d in EXTERNAL)}
<tr class="int-sep"><td colspan="13" class="l">Internal (free) baseline</td></tr>
{''.join(master_row(d) for d in INTERNAL)}
</tbody></table>
<div class="sub"><b>Metered spend reality (coredw.usage_reporting_data — $0.50 CPM confirmed: usage = imps × $0.0005):</b>
CPM-vendor total ≈ $67.7K/mo (Jun) ≈ $812K/yr run-rate. Dropping Sovrn + Cybba + 33Across API ≈ <b>$313K/yr</b>
against ≤$47K/yr defensible value; renegotiating 33Across to its band saves another ~$320K+/yr.</div>"""

html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>AUDI-1089 — DDP Vendor Renewal Evidence</title><style>
  body {{ font-family: "Helvetica Neue", Inter, Arial, sans-serif; background:#FAFAFA; color:#222;
         max-width: 1180px; margin: 0 auto; padding: 30px 40px; }}
  h1 {{ font-size: 26px; margin-bottom: 2px; }}
  h2 {{ font-size: 17px; color:#27496D; border-bottom: 1px solid #e2e2e2; padding-bottom: 5px; margin-top: 38px; }}
  .sub {{ color:#777; font-size: 13px; margin-bottom: 6px; }}
  img {{ max-width: 100%; margin: 14px 0; border: 1px solid #eee; border-radius: 4px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 12px; margin: 14px 0; }}
  th {{ text-align: right; color:#27496D; border-bottom: 2px solid #27496D; padding: 6px 8px; font-size: 11px; }}
  td {{ text-align: right; padding: 5px 8px; white-space: nowrap; }}
  th.l, td.l {{ text-align: left; }}
  tr:nth-child(even) td {{ background: #00000005; }}
  .ds {{ color:#aaa; font-size: 10px; }}
  .keep {{ color:#2E8B57; }} .drop {{ color:#D63B2F; }} .pend {{ color:#C77B30; }}
  .note {{ font-weight: 400; font-size: 10.5px; color:#888; white-space: normal; max-width: 260px; }}
  .int-sep td {{ background:#eef1f4 !important; color:#666; font-size: 11px; font-weight: 700; }}
  .cav {{ font-size: 12px; color:#666; line-height: 1.55; }}
</style></head><body>

<h1>DDP Vendor Renewal Evidence — AUDI-1089</h1>
<div class="sub">Malachi Dunn · site-visit data vendors feeding MNTN Matched · 30-day signal window (Jun 2 – Jul 1, 2026),
delivery valuation week (Jul 2–8) · method validated against the TI-1027 5x5 eval (recency cross-check 69.3% vs 69.8% ✓)</div>

<h2>Verdict &amp; evidence table — all sources</h2>
<div class="sub">"Sole" = no other source (internal or vendor) saw the IP / pair in the window. T1 = impressions that
were score-gated AND to vendor-sole IPs — the impressions that could not have served without that vendor.</div>
{table}

<h2>How replaceable is each vendor? (recency of its pairs)</h2>
{img("audi_1089_chart_recency_mix.png")}

<h2>What each vendor uniquely gives Matched (the value axis: classified domains)</h2>
{img("audi_1089_chart_sole_classified_domains.png")}

<h2>Real weekly dependency: impressions that needed the vendor</h2>
{img("audi_1089_chart_dependency_by_vendor.png")}

<h2>Uniqueness ≠ usefulness: quality of vendor-sole IPs</h2>
{img("audi_1089_chart_sole_quality.png")}

<h2>Delivery consistency (liveness)</h2>
{img("audi_1089_table_daily_delivery.png")}

<h2>Klickly (DS39) deep dive — verdict: PASS (drop) unless effectively free</h2>
{img("audi_1089_chart_klickly_dependency_ladder.png")}
{img("audi_1089_chart_klickly_adverse_selection.png")}

<h2>Method &amp; caveats</h2>
<div class="cav">
• Value lenses are base cost only (media / data spend) — client billing and take-rate math intentionally excluded.<br>
• Performance framing per leadership guidance: sole-IP visit rate is the dependency bound; for domain-value vendors
(Predactiv, 5x5) the MM value flows through domain→vertical coverage that scores SHARED IPs — sole-IP metrics do not
capture that, the classified-domains chart does.<br>
• Fixed-CPM vendors bill $0.50 / 1,000 on a metering basis not yet reconciled against an invoice; flat-fee amounts are
not in our data — verdicts are stated as fee bands / break-evens to compare against Paulo's renewal schedule.<br>
• svs signal is necessary for scoring (99.95% of scored delivered IPs have signal) — validates the T1 gating logic.<br>
• IPv6 excluded throughout (0–0.1% for most vendors; <b>Justuno 19.6%</b> — its footprint is undercounted; flagged in its eval).<br>
• Sole-set membership judged on the 37-day union window; every valued impression is preceded by the signal claiming credit.
</div>
</body></html>"""

path = os.path.join(HERE, "audi_1089_evidence_report.html")
with open(path, "w") as f:
    f.write(html)
print("wrote", path, f"({os.path.getsize(path)/1024:.0f} KB)")
