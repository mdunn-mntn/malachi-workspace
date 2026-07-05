"""Report manifest — one entry per module. The runner (run_report.py) executes each module's
`pulls` (clean single-query .sql in queries_exec/, param-substituted -> outputs/<adv>/<csv>.csv)
then its `chart` command (formatted with the params env). Modules with `pulls: []` reuse another
module's CSVs. Chart command placeholders: {OUT} {ADV} {P1S} {P1E} {P2S} {P2E} {P1L} {P2L}
{WINS} {WINE} {DMS} {DME}.

Each pull: {"csv": "<name>"} -> runs queries_exec/<name>.sql, saves <OUT>/<name>.csv. Optional
"rows" overrides max_rows (default 2000).
"""

SPEC = [
    {"id": "00", "title": "Audience audit (front matter)",
     "pulls": [{"csv": "00_campaign_enum"}, {"csv": "00_all_expressions"},
               {"csv": "00_funnel_sizes"}, {"csv": "00_funnel_hishare"}],
     "chart": 'charts/00_audience_audit.py --outdir {OUT} --adv "{ADV}"'},
    {"id": "00b", "title": "Prospecting reach by score bucket",
     "pulls": [],  # reuses module 00 CSVs
     "chart": 'charts/00b_prospecting_funnel.py --outdir {OUT} --adv "{ADV}"'},
]
