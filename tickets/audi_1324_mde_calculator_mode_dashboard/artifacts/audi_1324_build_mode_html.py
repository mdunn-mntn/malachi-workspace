"""Build the Mode report's index.html from the standalone calculator: swap baked JSON for window.datasets."""
import re
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
WORKSPACE = TICKET.parents[1]
SRC = WORKSPACE / "tickets/audi_1213_mde_calculator_refresh/artifacts/audi_1213_mde_calculator.html"
OUT = TICKET / "artifacts" / "audi_1324_index.html"
QUERY_NAME = "Advertiser Prefill"

BRIDGE = """/* Mode data bridge. window.datasets is [{name, content:[rows]}]; rows key off the SQL
   column aliases. Everything below this block is the standalone calculator unchanged. */
(function () {
  var WANT = %r;
  var norm = function (s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); };
  var sets = window.datasets || [];
  var hit = null;
  for (var i = 0; i < sets.length; i++) {
    if (norm(sets[i].name) === norm(WANT)) { hit = sets[i]; break; }
  }
  if (!hit && sets.length === 1) hit = sets[0];
  var rows = (hit && hit.content) || [];

  var num = function (v) { var n = Number(v); return isFinite(n) ? n : 0; };
  window.ADVERTISERS = rows.map(function (r) {
    return {
      id: num(r.advertiser_id),
      name: r.advertiser_name,
      spend30: num(r.spend_30d),
      imps30: num(r.impressions_30d),
      ips30: num(r.distinct_ips_30d),
      cpm: num(r.cpm),
      impsIp: num(r.imps_per_ip),
      pVisit: num(r.p_visit),
      pCvr: num(r.p_cvr),
      typical: num(r.typical_active_month_spend),
      maxMo: num(r.max_month_spend),
      months: num(r.active_months_count)
    };
  }).filter(function (a) { return a.id && a.name; })
    .sort(function (a, b) { return b.spend30 - a.spend30; });

  var median = function (key) {
    var v = window.ADVERTISERS.map(function (a) { return a[key]; })
      .filter(function (x) { return x > 0; }).sort(function (a, b) { return a - b; });
    if (!v.length) return 0;
    var m = Math.floor(v.length / 2);
    return v.length %% 2 ? v[m] : (v[m - 1] + v[m]) / 2;
  };
  window.COHORT = {
    cpm: +median('cpm').toFixed(2),
    impsIp: +median('impsIp').toFixed(2),
    ivr: +(median('pVisit') * 100).toFixed(3),
    cvr: +(median('pCvr') * 100).toFixed(3)
  };

  var stamp = rows.length && rows[0].data_pull_date;
  window.DATA_PULL_DATE = stamp ? String(stamp).slice(0, 10) : 'refreshed weekly';
})();""" % QUERY_NAME


def main():
    html = SRC.read_text()
    html, n = re.subn(
        r"^window\.ADVERTISERS = \[.*\];\nwindow\.COHORT = \{.*\};$",
        lambda _: BRIDGE,
        html,
        count=1,
        flags=re.M,
    )
    if n != 1:
        raise SystemExit("payload anchor not found")

    html, n = re.subn(r'^window\.DATA_PULL_DATE = "[0-9-]+";$', "", html, count=1, flags=re.M)
    if n != 1:
        raise SystemExit("DATA_PULL_DATE anchor not found")

    OUT.write_text(html)
    print(f"wrote {OUT.relative_to(WORKSPACE)}  {OUT.stat().st_size / 1024:.0f} KB")
    print(f"payload swapped for window.datasets['{QUERY_NAME}']")


if __name__ == "__main__":
    sys.exit(main())
