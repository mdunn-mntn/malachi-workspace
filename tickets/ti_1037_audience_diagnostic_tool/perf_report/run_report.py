#!/usr/bin/env python3
"""run_report.py — the run-it-all assembler for the client performance/audience report.

One command regenerates the whole report for any advertiser:
    python run_report.py --params params/bouqs_32147.env
    python run_report.py --params params/kindred_35094.env --only 00,04,12c
    python run_report.py --params params/bouqs_32147.env --charts-only   # skip BQ, re-render

Reads a params .env, then for each module in report_spec.SPEC: runs its pull queries
(queries_exec/<csv>.sql, {{PLACEHOLDER}}-substituted -> outputs/<adv>/<csv>.csv via bq_run.sh),
then its chart command. Finally builds outputs/<adv>/report.html indexing every PNG.
"""
import argparse
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BQ = os.path.join(HERE, "..", "..", "..", ".claude", "scripts", "bq_run.sh")


def parse_env(path):
    env = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def strip_sql(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
    return text.strip().rstrip(";").strip()


def sub(text, env):
    def repl(m):
        k = m.group(1)
        if k not in env:
            raise KeyError(f"param {{{{{k}}}}} missing from env")
        return env[k]
    return re.sub(r"\{\{([A-Z0-9_]+)\}\}", repl, text)


def run_pull(csv, env, outdir, rows=2000):
    sqlf = os.path.join(HERE, "queries_exec", f"{csv}.sql")
    if not os.path.exists(sqlf):
        print(f"    ! missing {sqlf}", file=sys.stderr)
        return False
    sql = sub(strip_sql(open(sqlf).read()), env)
    out = os.path.join(outdir, f"{csv}.csv")
    cmd = ["bash", BQ, "--ticket", "TI-1037", "--label", f"report {csv}",
           "--use_legacy_sql=false", "--format=csv", f"--max_rows={rows}",
           "--project_id=dw-main-silver", sql]
    with open(out, "w") as fh:
        r = subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE, text=True)
    n = sum(1 for _ in open(out)) - 1 if os.path.exists(out) else -1
    ok = r.returncode == 0 and n >= 0
    print(f"    {'✓' if ok else '✗'} {csv}.csv ({n} rows)" + ("" if ok else f"  {r.stderr.strip()[:200]}"))
    return ok


def _month(d, end=False):
    """YYYY-MM-DD -> YYYY-MM. For an EXCLUSIVE end date, step back one day first."""
    if not d:
        return ""
    from datetime import date, timedelta
    y, mo, dy = (int(x) for x in d.split("-"))
    dt = date(y, mo, dy) - (timedelta(days=1) if end else timedelta())
    return f"{dt.year:04d}-{dt.month:02d}"


def fmt_chart(tmpl, env):
    m = {"OUT": env["OUTDIR"], "ADV": env.get("ADV_LABEL", env.get("ADV_NAME", "")),
         "P1S": env.get("P1_START", ""), "P1E": env.get("P1_END", ""),
         "P2S": env.get("P2_START", ""), "P2E": env.get("P2_END", ""),
         "P1L": env.get("P1_LABEL", ""), "P2L": env.get("P2_LABEL", ""),
         "WINS": env.get("WIN_START", ""), "WINE": env.get("WIN_END", ""),
         "DMS": env.get("DELIV_MONTH_START", ""), "DME": env.get("DELIV_MONTH_END", ""),
         "HS": env.get("HOLIDAY_START", ""), "HE": env.get("HOLIDAY_END", ""),
         "P1SM": _month(env.get("P1_START", "")), "P1EM": _month(env.get("P1_END", ""), end=True),
         "P2SM": _month(env.get("P2_START", "")), "P2EM": _month(env.get("P2_END", ""), end=True)}
    return tmpl.format(**m)


def run_chart(tmpl, env):
    cmd = fmt_chart(tmpl, env)
    r = subprocess.run(f"{sys.executable} {cmd}", shell=True, cwd=HERE, capture_output=True, text=True)
    ok = r.returncode == 0
    tail = (r.stdout + r.stderr).strip().splitlines()
    print(f"    {'✓' if ok else '✗'} chart: {cmd.split()[0].split('/')[-1]}"
          + ("" if ok else "  " + " ".join(tail[-3:])[:300]))
    for l in tail:
        if l.startswith("FINDING:"):
            print(f"      {l}")
    return ok


def build_html(env, spec):
    out = env["OUTDIR"]
    # headline order: overview (flags) -> 04 (YoY metrics) -> 05/05b/05c (monthly trends) -> then the rest
    LEAD = ["overview", "04_", "05_", "05b_", "05c_"]

    def prio(p):
        for i, pre in enumerate(LEAD):
            if p.startswith(pre):
                return (i, p)
        return (len(LEAD), p)
    pngs = sorted((f for f in os.listdir(out) if f.endswith(".png")), key=prio)
    adv = env.get("ADV_LABEL", env.get("ADV_NAME", ""))
    parts = [f"<!doctype html><meta charset=utf-8><title>{adv} — report</title>",
             "<style>body{font-family:Helvetica Neue,Arial,sans-serif;background:#FAFAFA;margin:40px;color:#222}"
             "h1{font-size:26px}h2{font-size:16px;color:#27496D;margin-top:34px;border-bottom:1px solid #ddd;padding-bottom:4px}"
             "img{max-width:100%;border:1px solid #eee;margin:8px 0;background:#fff}nav a{margin-right:14px;color:#27496D}</style>",
             f"<h1>{adv} — performance & audience report</h1>",
             "<nav>" + " ".join(f'<a href="#{p}">{p.replace(".png","")}</a>' for p in pngs) + "</nav>"]
    for p in pngs:
        parts.append(f'<h2 id="{p}">{p.replace(".png","")}</h2><img src="{p}">')
    open(os.path.join(out, "report.html"), "w").write("\n".join(parts))
    print(f"\n  built report.html ({len(pngs)} charts) -> {out}/report.html")


def main():
    sys.path.insert(0, HERE)
    from report_spec import SPEC
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    ap.add_argument("--only", default="", help="comma-separated module ids")
    ap.add_argument("--charts-only", action="store_true")
    ap.add_argument("--pulls-only", action="store_true")
    ap.add_argument("--no-html", action="store_true")
    a = ap.parse_args()
    env = parse_env(os.path.join(HERE, a.params) if not os.path.isabs(a.params) else a.params)
    env.setdefault("OUTDIR", f"outputs/{os.path.basename(a.params).replace('.env','')}")
    outdir = os.path.join(HERE, env["OUTDIR"])
    os.makedirs(outdir, exist_ok=True)
    only = set(x.strip() for x in a.only.split(",") if x.strip())

    print(f"REPORT: {env.get('ADV_LABEL', env.get('ADV_NAME'))}  ->  {env['OUTDIR']}")
    fails = []
    for mod in SPEC:
        if only and mod["id"] not in only:
            continue
        print(f"\n[{mod['id']}] {mod['title']}")
        if not a.charts_only:
            for pull in mod["pulls"]:
                if not run_pull(pull["csv"], env, outdir, pull.get("rows", 2000)):
                    fails.append(f"{mod['id']}:{pull['csv']}")
        if not a.pulls_only:
            if not run_chart(mod["chart"], env):
                fails.append(f"{mod['id']}:chart")
    if not a.pulls_only and not a.no_html:
        build_html(env, SPEC)
    print(f"\nDONE. {'FAILURES: ' + ', '.join(fails) if fails else 'all modules ok.'}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
