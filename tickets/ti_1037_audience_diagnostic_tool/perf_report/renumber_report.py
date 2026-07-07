#!/usr/bin/env python3
"""renumber_report.py — render a report, then RENUMBER module outputs into a custom order.

Test-only helper (does not touch run_report.py / report_spec.py). It renders every module
into the params' OUTDIR (reusing existing CSVs), then physically renames each module's output
files (.png/.md) so their leading number reflects a custom display order — and rebuilds
report.html in that order. An optional --cover module is shown first as an unnumbered 00_ cover.

Only the leading module-number changes; the descriptive part of each filename is preserved, so
04_prospecting_yoy_metrics.png -> 01_prospecting_yoy_metrics.png. Input CSVs are left untouched.

Usage:
  python renumber_report.py --params params/bouqs_test.env \
     --order 04,05,05b,05c,00,00b,01,02,06,06b,06c,03,03b,07,07b,08,09,10,11,12,12b,12c \
     --cover overview [--generate]
"""
import argparse
import glob
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_EXTS = (".png", ".md")   # outputs to renumber; .csv inputs are left as-is


def parse_env(path):
    env = {}
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def files_for(outdir, mid):
    """Output files a module wrote: names start with '<mid>_'. The trailing '_' in the glob
    keeps id boundaries clean (mid '05' matches 05_* but NOT 05b_*)."""
    out = []
    for ext in OUT_EXTS:
        out += glob.glob(os.path.join(outdir, f"{mid}_*{ext}"))
    return sorted(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    ap.add_argument("--order", required=True, help="comma-separated module ids in display order")
    ap.add_argument("--cover", default="", help="module id shown first, unnumbered 00_ cover")
    ap.add_argument("--generate", action="store_true", help="render charts first (run_report --charts-only)")
    a = ap.parse_args()

    env = parse_env(os.path.join(HERE, a.params) if not os.path.isabs(a.params) else a.params)
    outdir = os.path.join(HERE, env["OUTDIR"])
    order = [x.strip() for x in a.order.split(",") if x.strip()]

    if a.generate:
        subprocess.run([sys.executable, os.path.join(HERE, "run_report.py"),
                        "--params", a.params, "--charts-only", "--no-html"], cwd=HERE, check=True)

    # old path -> new basename
    mapping, missing, num = {}, [], 0
    if a.cover:
        for f in files_for(outdir, a.cover):
            mapping[f] = f"00_{os.path.basename(f)}"        # cover keeps its full name, gains 00_
    for mid in order:
        fs = files_for(outdir, mid)
        if not fs:
            missing.append(mid)
            continue
        num += 1
        for f in fs:
            rest = os.path.basename(f)[len(mid) + 1:]       # strip "<mid>_"
            mapping[f] = f"{num:02d}_{rest}"

    # two-phase rename so overlapping old/new numbers never collide
    staged = []
    for old, new in mapping.items():
        tmp = os.path.join(outdir, ".renum__" + new)
        os.rename(old, tmp)
        staged.append((tmp, os.path.join(outdir, new)))
    for tmp, new in staged:
        os.rename(tmp, new)

    # drop outputs for EXCLUDED modules (generated but not in --order/--cover) so they don't leak
    # into the report — anything renamed is in `produced`; stray .png/.md are excluded modules.
    produced = set(mapping.values())
    dropped = 0
    for f in os.listdir(outdir):
        if (f.endswith(".png") or f.endswith(".md")) and f not in produced:
            os.remove(os.path.join(outdir, f))
            dropped += 1

    # rebuild report.html — filenames now sort into the intended order
    adv = env.get("ADV_LABEL", "")
    pngs = sorted(f for f in produced if f.endswith(".png"))
    parts = [f"<!doctype html><meta charset=utf-8><title>{adv} — report</title>",
             "<style>body{font-family:Helvetica Neue,Arial,sans-serif;background:#FAFAFA;margin:40px;color:#222}"
             "h1{font-size:26px}h2{font-size:16px;color:#27496D;margin-top:34px;border-bottom:1px solid #ddd;padding-bottom:4px}"
             "img{max-width:100%;border:1px solid #eee;margin:8px 0;background:#fff}nav a{margin-right:14px;color:#27496D}</style>",
             f"<h1>{adv} — performance & audience report</h1>",
             "<nav>" + " ".join(f'<a href="#{p}">{p.replace(".png","")}</a>' for p in pngs) + "</nav>"]
    for p in pngs:
        parts.append(f'<h2 id="{p}">{p.replace(".png","")}</h2><img src="{p}">')
    open(os.path.join(outdir, "report.html"), "w").write("\n".join(parts))

    print(f"renumbered {num} modules"
          + (f" + cover '{a.cover}'" if a.cover else "")
          + (f", dropped {dropped} excluded file(s)" if dropped else "")
          + f" -> {env['OUTDIR']}/report.html")
    if missing:
        print(f"  ! no output files found for: {', '.join(missing)}")


if __name__ == "__main__":
    main()
