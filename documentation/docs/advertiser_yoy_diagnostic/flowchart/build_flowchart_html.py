#!/usr/bin/env python3
"""
Build a self-contained, shareable HTML page for the AUDI-1070 YoY-decline
diagnostic decision tree: the zoomable SVG flowchart + a reference table of
every node's precise question and the exact table/column to query.

Inputs  (same dir): diagnostic_tree.json, diagnostic_flowchart.svg
Output  (same dir): diagnostic_flowchart.html
Reproduce: python3 build_flowchart_html.py
"""
import json
import html
import pathlib

HERE = pathlib.Path(__file__).parent
tree = json.loads((HERE / "diagnostic_tree.json").read_text())
svg = (HERE / "diagnostic_flowchart.svg").read_text()
# strip the XML/doctype prologue so the <svg> embeds inline cleanly
svg = svg[svg.find("<svg"):]

CAT = {
    "artifact":     ("#5B6B7A", "#E7EBEF", "Artifact — re-pull / re-baseline, no delivery fix"),
    "healthy":      ("#3E7C59", "#E3EFE7", "Healthy — no action; the 'decline' is a misperception"),
    "real-decline": ("#C4342B", "#F2DEDC", "Real decline — a concrete fix applies"),
    "needs-more":   ("#B8863B", "#F5EAD2", "Needs more — gather evidence / re-open"),
}

nodes = {n["id"]: n for n in tree["nodes"]}
decisions = [n for n in tree["nodes"] if n["type"] == "decision"]
leaves = [n for n in tree["nodes"] if n["type"] == "diagnosis"]
# order leaves by category severity for the table
cat_order = {"real-decline": 0, "artifact": 1, "healthy": 2, "needs-more": 3}
leaves.sort(key=lambda n: (cat_order.get(n.get("category", "needs-more"), 9), n["id"]))


def esc(s):
    return html.escape(str(s or ""))


def chip(cat):
    border, fill, _ = CAT.get(cat, ("#888", "#eee", cat))
    return (f'<span class="chip" style="background:{fill};border-color:{border};'
            f'color:{border}">{esc(cat)}</span>')


# ---- decision rows ----
dec_rows = []
for n in decisions:
    branches = "".join(
        f'<div class="br"><span class="cond">{esc(b["condition"])}</span>'
        f'<span class="arrow">&rarr;</span>'
        f'<span class="tgt">{esc(nodes.get(b["target"],{}).get("label", b["target"]))}</span></div>'
        for b in n.get("branches", [])
    )
    dec_rows.append(f"""
    <tr>
      <td class="nid">{esc(n['id'])}</td>
      <td>
        <div class="nlabel">{esc(n['label'])}</div>
        <div class="q">{esc(n.get('question',''))}</div>
        <div class="branches">{branches}</div>
      </td>
      <td class="how"><code>{esc(n.get('check_how',''))}</code></td>
    </tr>""")

# ---- leaf rows ----
leaf_rows = []
for n in leaves:
    ex = n.get("exemplar", "")
    ex_html = f'<span class="ex">exemplar: {esc(ex)}</span>' if ex and ex.lower() not in ("none", "") else ""
    fm = n.get("failure_mode", "")
    fm_html = f'<span class="fm">{esc(fm)}</span>' if fm and fm.lower() not in ("none", "") else ""
    leaf_rows.append(f"""
    <tr>
      <td class="nid">{esc(n['id'])}</td>
      <td>
        <div class="nlabel">{esc(n['label'])} {chip(n.get('category',''))} {fm_html}</div>
        {ex_html}
        <div class="fix"><b>Fix / action:</b> {esc(n.get('fix',''))}</div>
      </td>
      <td class="how"><code>{esc(n.get('confirm_table',''))}</code></td>
    </tr>""")

legend = "".join(
    f'<div class="lg"><span class="chip" style="background:{fill};border-color:{border};color:{border}">'
    f'{esc(cat)}</span> {esc(desc)}</div>'
    for cat, (border, fill, desc) in CAT.items()
)

notes = "".join(f"<li>{esc(x)}</li>" for x in tree.get("notes", []))
notes_block = f'<h2>Method notes</h2><ul class="notes">{notes}</ul>' if notes else ""

doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AUDI-1070 — YoY Decline Diagnostic Decision Tree</title>
<style>
  :root {{ --navy:#1F3556; --ink:#2B2F36; --line:#E2E6EA; }}
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Helvetica Neue',Helvetica,Arial,sans-serif; color:var(--ink);
         background:#FAFAFA; margin:0; padding:0 0 80px; line-height:1.5; }}
  header {{ background:var(--navy); color:#fff; padding:26px 40px; }}
  header h1 {{ margin:0 0 6px; font-size:23px; letter-spacing:.2px; }}
  header .pl {{ margin:0; font-size:14px; opacity:.85; font-weight:400; }}
  header .by {{ margin:10px 0 0; font-size:12px; opacity:.6; }}
  .wrap {{ max-width:1180px; margin:0 auto; padding:0 40px; }}
  .figure {{ background:#fff; border:1px solid var(--line); border-radius:10px;
            margin:26px 0; padding:14px; overflow:auto; text-align:center; }}
  .figure svg {{ max-width:100%; height:auto; }}
  .legendbar {{ display:flex; flex-wrap:wrap; gap:16px 26px; margin:16px 0 4px; font-size:13px; }}
  .lg {{ display:flex; align-items:center; gap:8px; }}
  .chip {{ display:inline-block; font-size:11px; font-weight:600; padding:1px 8px;
          border-radius:10px; border:1.5px solid; white-space:nowrap; }}
  h2 {{ color:var(--navy); font-size:18px; margin:34px 0 6px; }}
  .sub {{ color:#6B7280; font-size:13px; margin:0 0 12px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; color:#6B7280; font-weight:600; font-size:11px; text-transform:uppercase;
       letter-spacing:.4px; border-bottom:2px solid var(--line); padding:8px 10px; }}
  td {{ vertical-align:top; padding:12px 10px; border-bottom:1px solid var(--line); }}
  .nid {{ font-weight:700; color:var(--navy); white-space:nowrap; width:46px; font-size:13px; }}
  .nlabel {{ font-weight:600; margin-bottom:4px; }}
  .q {{ color:#3d434c; font-size:12.5px; margin-bottom:6px; }}
  .branches {{ margin-top:4px; }}
  .br {{ font-size:12px; margin:2px 0; }}
  .cond {{ color:var(--ink); }}
  .arrow {{ color:#9aa1a9; margin:0 6px; }}
  .tgt {{ color:var(--navy); font-weight:600; }}
  .how {{ width:34%; }}
  .how code {{ font-family:'SF Mono',Menlo,Consolas,monospace; font-size:11px; color:#334;
             background:#F4F6F8; border:1px solid var(--line); border-radius:5px;
             padding:6px 8px; display:block; white-space:pre-wrap; word-break:break-word; }}
  .ex {{ display:inline-block; font-size:11.5px; color:#6B7280; margin:0 0 4px; }}
  .fm {{ font-size:11px; color:#C4342B; font-weight:600; margin-left:4px; }}
  .fix {{ font-size:12.5px; margin-top:3px; }}
  .notes li {{ font-size:13px; margin:4px 0; }}
  footer {{ max-width:1180px; margin:40px auto 0; padding:0 40px; color:#9aa1a9; font-size:12px; }}
</style></head>
<body>
<header>
  <h1>Prospecting ROAS/Visits Declined YoY — Diagnostic Decision Tree</h1>
  <p class="pl">Walk 0&rarr;5: fix the lens &amp; scope, rule out tracking outages, then the delivery spine.
     &ldquo;MM degraded&rdquo; is the verdict only if every kill-condition fails.</p>
  <p class="by">Malachi Dunn &middot; AUDI-1070 &middot; reusable for any advertiser &times; any two periods</p>
</header>

<div class="wrap">
  <div class="legendbar">{legend}</div>
  <div class="figure">{svg}</div>

  <h2>Decision nodes — what to ask, and the exact table to check</h2>
  <p class="sub">The spine runs cheapest-artifact-first. Q2 is the fork between the two failure modes
     (delivery <b>left</b> High-Intent via a gate flip, vs delivery <b>stayed</b> in HI but saturated).</p>
  <table>
    <thead><tr><th>Node</th><th>Question &amp; branches</th><th>How to check (table / column)</th></tr></thead>
    <tbody>{''.join(dec_rows)}</tbody>
  </table>

  <h2>Diagnosis leaves — verdict, exemplar, fix</h2>
  <p class="sub">Sorted by severity. The four <b style="color:#C4342B">real-decline</b> leaves are the ones that carry a concrete fix.</p>
  <table>
    <thead><tr><th>Node</th><th>Diagnosis &amp; fix</th><th>Confirm with</th></tr></thead>
    <tbody>{''.join(leaf_rows)}</tbody>
  </table>

  {notes_block}
</div>
<footer>
  Companion to the parameterized diagnostic query pack
  (<code>documentation/docs/advertiser_yoy_diagnostic/</code>: 7 queries + <code>run_diagnostic.sh</code> + playbook).
  Machine-readable tree: <code>flowchart/diagnostic_tree.json</code>. Vector source: <code>diagnostic_flowchart.svg</code> / <code>.dot</code>.
</footer>
</body></html>"""

out = HERE / "diagnostic_flowchart.html"
out.write_text(doc)
print(f"wrote {out} ({len(doc)//1024} KB) — {len(decisions)} decisions, {len(leaves)} leaves")
