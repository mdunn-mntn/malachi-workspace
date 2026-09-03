"""Rebuild the MDE calculator from the TI-1019 shipped file: fresh prefill data + arm-split fixes."""
import json
import re
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
WORKSPACE = TICKET.parents[1]
SRC = (
    WORKSPACE
    / "tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill"
    / "artifacts/ti_xxx_mde_calculator_prefill.html"
)
PAYLOAD = TICKET / "outputs" / "audi_1213_prefill_compact.json"
OUT = TICKET / "artifacts" / "audi_1213_mde_calculator.html"
RUN_DATE = "2026-09-03"

EDITS = []


def sub(html, old, new, label):
    if html.count(old) != 1:
        raise SystemExit(f"anchor not unique ({html.count(old)}): {label}")
    EDITS.append(label)
    return html.replace(old, new)


def main():
    html = SRC.read_text()
    payload = json.loads(PAYLOAD.read_text())
    cohort = payload["cohort"]

    replacement = (
        "window.ADVERTISERS = "
        + json.dumps(payload["advertisers"], separators=(",", ":"))
        + ";\nwindow.COHORT = "
        + json.dumps(cohort, separators=(",", ":"))
        + ";"
    )
    html, n = re.subn(
        r"^window\.ADVERTISERS = \[.*\];$",
        lambda _: replacement,
        html,
        count=1,
        flags=re.M,
    )
    if n != 1:
        raise SystemExit("ADVERTISERS anchor not found")
    EDITS.append("advertisers + cohort payload")

    html = sub(
        html,
        "Generated 2026-06-04 by ti_xxx_advertiser_prefill_metrics.sql. */",
        f"Generated {RUN_DATE} by incr_75_advertiser_metrics.sql (AUDI-1213 refresh).\n"
        "   Spend is advertiser-facing (media + data + platform), not media cost. */",
        "provenance comment",
    )

    html = sub(
        html,
        """  const totalSpend = monthlyBudget * (durationWk / WEEKS_PER_MONTH);
  // Budget covers the full reach pool (treatment + holdout).
  // Holdout is a hash-bucket split of total reach: h% control, (1-h)% treated.
  const totalIps   = (totalSpend / cpm * 1000) / impsPerIp;
  const nTreated   = totalIps * (1 - holdoutFrac);
  const nControl   = totalIps * holdoutFrac;""",
        """  const totalSpend = monthlyBudget * (durationWk / WEEKS_PER_MONTH);
  // The holdout is never served, so the budget buys the treated arm only.
  const nTreated   = (totalSpend / cpm * 1000) / impsPerIp;
  const nControl   = nTreated * (holdoutFrac / (1 - holdoutFrac));
  const totalIps   = nTreated + nControl;""",
        "computeMDE arm split",
    )

    html = sub(
        html,
        "  const totalSpend = nTotal * impsPerIp * cpm / 1000;",
        "  const totalSpend = nTotal * (1 - holdoutFrac) * impsPerIp * cpm / 1000;",
        "spendRequired arm split",
    )

    html = sub(
        html,
        """function setOutcome(o) {
  const rates = { ivr: 2.15, cvr: 0.054 };
  document.getElementById('inp-base').value = rates[o] ?? 2.15;
  S.baselineRate = (rates[o] ?? 2.15) / 100;""",
        """function setOutcome(o) {
  S.currentOutcome = o;
  const adv = S.advertiser;
  const pct = adv
    ? (o === 'cvr' ? adv.pCvr : adv.pVisit) * 100
    : (o === 'cvr' ? window.COHORT.cvr : window.COHORT.ivr);
  document.getElementById('inp-base').value = pct.toFixed(3);
  S.baselineRate = pct / 100;""",
        "setOutcome respects the loaded advertiser",
    )

    html = sub(
        html,
        """  document.getElementById('inp-cpm').value  = '24.84';
  document.getElementById('inp-imps').value = '3.5';
  S.cpm = 24.84; S.impsPerIp = 3.5;""",
        """  document.getElementById('inp-cpm').value  = window.COHORT.cpm.toFixed(2);
  document.getElementById('inp-imps').value = window.COHORT.impsIp.toFixed(2);
  S.cpm = window.COHORT.cpm; S.impsPerIp = window.COHORT.impsIp;""",
        "clearAdvertiser cohort defaults",
    )

    html = sub(
        html,
        """  baselineRate  : 0.0215,
  cpm           : 24.84,
  impsPerIp     : 3.5,""",
        f"""  baselineRate  : {cohort['ivr'] / 100},
  cpm           : {cohort['cpm']},
  impsPerIp     : {cohort['impsIp']},""",
        "initial state defaults",
    )

    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-base" value="2.15" min="0.001" max="50" step="0.01">',
        f'<input type="number" class="num-inp" id="inp-base" value="{cohort["ivr"]}" min="0.001" max="50" step="0.01">',
        "baseline input default",
    )
    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-cpm" value="24.84" min="1" step="0.01">',
        f'<input type="number" class="num-inp" id="inp-cpm" value="{cohort["cpm"]}" min="1" step="0.01">',
        "cpm input default",
    )
    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-imps" value="3.5" min="0.1" step="0.1">',
        f'<input type="number" class="num-inp" id="inp-imps" value="{cohort["impsIp"]}" min="0.1" step="0.1">',
        "imps/IP input default",
    )

    OUT.write_text(html)
    print(f"wrote {OUT.relative_to(WORKSPACE)}  {OUT.stat().st_size / 1024:.0f} KB")
    for e in EDITS:
        print(f"  applied: {e}")


if __name__ == "__main__":
    sys.exit(main())
