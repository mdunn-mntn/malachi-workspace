"""Build the Mode report's index.html from the standalone calculator: swap baked JSON for window.datasets."""
import re
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
WORKSPACE = TICKET.parents[1]
SRC = WORKSPACE / "tickets/audi_1213_mde_calculator_refresh/artifacts/audi_1213_mde_calculator.html"
OUT = TICKET / "artifacts" / "audi_1324_index.html"
QUERY_NAME = "Advertiser Prefill"
SCOPE = "#mde"

BRIDGE = """/* Mode data bridge. window.datasets is [{name, content:[rows]}]; rows key off the SQL
   column aliases. Everything below this block is the standalone calculator unchanged. */
window.__mdeHydrate = function () {
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
  return window.ADVERTISERS.length;
};
window.__mdeHydrate();""" % QUERY_NAME


LAUNCHER = """
/* Mode injects this layout after the document has loaded, and injects it twice, so
   DOMContentLoaded has already fired and the boot has to be launched directly. Datasets
   can also land after the HTML, hence the poll. */
(function () {
  if (window.__mdeStarted) return;
  window.__mdeStarted = true;
  var tries = 0;
  function go() {
    window.__mdeHydrate();
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', __mdeBoot);
    } else {
      __mdeBoot();
    }
  }
  (function wait() {
    if (window.datasets && window.datasets.length && window.Chart) return go();
    if (++tries > 60) return go();
    setTimeout(wait, 100);
  })();
})();"""


def _split_top(css):
    """Yield (prelude, block) for each top-level rule; block is None past the last brace."""
    i = 0
    while i < len(css):
        brace = css.find("{", i)
        if brace == -1:
            tail = css[i:]
            if tail.strip():
                yield tail, None
            return
        prelude = css[i:brace]
        depth, j = 1, brace + 1
        while j < len(css) and depth:
            if css[j] == "{":
                depth += 1
            elif css[j] == "}":
                depth -= 1
            j += 1
        yield prelude, css[brace + 1 : j - 1]
        i = j


def _scope_selector(sel, scope):
    sel = sel.strip()
    if not sel:
        return ""
    if sel == "*":
        return scope + " *"
    if sel in (":root", "html", "body"):
        return scope
    for tag in ("html", "body"):
        if sel.startswith(tag) and (len(sel) == len(tag) or not sel[len(tag)].isalnum()):
            return scope + sel[len(tag) :]
    return scope + " " + sel


def scope_css(css, scope):
    """Prefix every selector with `scope` so the fragment cannot style Mode's own chrome."""
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    out = []
    for prelude, block in _split_top(css):
        if block is None:
            out.append(prelude)
            continue
        head = prelude.strip()
        if head.startswith("@media") or head.startswith("@supports"):
            out.append(f"{head} {{{scope_css(block, scope)}}}")
        elif head.startswith("@"):
            out.append(f"{head} {{{block}}}")
        else:
            sels, seen = [], set()
            for raw in head.split(","):
                one = _scope_selector(raw, scope)
                if one and one not in seen:
                    seen.add(one)
                    sels.append(one)
            out.append("{} {{{}}}".format(", ".join(sels), block))
    return "\n".join(out)


def to_fragment(html, scope):
    """Mode injects the layout into its own page, so emit a fragment, not a document."""
    css = re.search(r"<style[^>]*>(.*?)</style>", html, re.S).group(1)
    html = html.replace(css, scope_css(css, scope), 1)

    body = re.search(r"<body[^>]*>(.*?)</body>", html, re.S).group(1)
    head_keep = "".join(re.findall(r'<link[^>]+>|<script[^>]+src=[^>]+></script>|<style[^>]*>.*?</style>', html[: html.index("</head>")], re.S))
    tail_scripts = "".join(re.findall(r"<script(?![^>]*\ssrc=)[^>]*>.*?</script>", body, re.S))
    markup = re.sub(r"<script(?![^>]*\ssrc=)[^>]*>.*?</script>", "", body, flags=re.S).strip()
    return f'{head_keep}\n<div id="{scope[1:]}">\n{markup}\n</div>\n{tail_scripts}\n'


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

    html, n = re.subn(
        r"^document\.addEventListener\('DOMContentLoaded', \(\) => \{$",
        "function __mdeBoot() {",
        html,
        count=1,
        flags=re.M,
    )
    if n != 1:
        raise SystemExit("DOMContentLoaded anchor not found")

    html, n = re.subn(
        r"^  initChart\(\);$",
        "  try { initChart(); } catch (e) { console.error('chart init failed', e); }",
        html,
        count=1,
        flags=re.M,
    )
    if n != 1:
        raise SystemExit("initChart call not found")

    tail = "});\n</script>"
    if html.count(tail) != 1:
        raise SystemExit(f"boot tail not unique ({html.count(tail)})")
    html = html.replace(tail, "}\n" + LAUNCHER + "\n</script>")

    html = to_fragment(html, SCOPE)

    OUT.write_text(html)
    print(f"wrote {OUT.relative_to(WORKSPACE)}  {OUT.stat().st_size / 1024:.0f} KB")
    print(f"payload swapped for window.datasets['{QUERY_NAME}']")


if __name__ == "__main__":
    sys.exit(main())
