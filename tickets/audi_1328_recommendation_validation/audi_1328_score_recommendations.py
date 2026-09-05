#!/usr/bin/env python3
"""Score each shipped optimizer fix: was the quiet attributable, and did the recommendation match the fix that shipped?

Usage:
  audi_1328_score_recommendations.py [--ledger PATH|gs://...] [--repo PATH]
                                     [--min-quiet 3] [--main-ref origin/main]
                                     [--effective-from YYYY-MM-DD] [--out DIR] [--forecast-only]
Exit 0 scored something attributable, 2 nothing eligible yet, 3 eligible but nothing attributable.
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile

DEFAULT_LEDGER = "gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl"
DEFAULT_REPO = "/Users/malachi/Developer/work/mntn/airflow-ti-main"
CONFIG_PATH = "dags/model_task_config.json"
RESOLVE_SWEEPS = 3
STICKY = ("owner_notified", "wont_fix")
HUMAN = (*STICKY, "applied")
NON_FIRING = ("resolved", "observed")
WATCH_KEY = "exec_h"
DARK_RATIO = 0.2
BASELINE_DATES = 5

EQUIVALENT_LEVERS = {
    "spark.sql.shuffle.partitions": {"spark.sql.adaptive.advisoryPartitionSizeInBytes"},
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": {"spark.sql.shuffle.partitions"},
}
SECONDARY_PARAMS = {"spark.speculation.quantile"}
RELATIVE_TARGET = 2.0
TARGET_TOLERANCE = 2.0

ATTRIBUTION_PRIORITY = [
    "pre_fix_quiet",
    "unobserved_window",
    "partial_window",
    "dag_went_dark",
    "detector_went_silent",
    "confounded_by_other_change",
]

SPARK_KEY = re.compile(r"spark\.[A-Za-z0-9_.]*[A-Za-z0-9]")
IMPERATIVE = re.compile(
    r"\b(raise|increase|lower|reduce|set|enable|turn on)\b[^.;]*?(spark\.[A-Za-z0-9_.]*[A-Za-z0-9])",
    re.I,
)
PROHIBITION = re.compile(
    r"\b(do not|don't|never)\b[^.;]*?(spark\.[A-Za-z0-9_.]*[A-Za-z0-9])", re.I
)
TARGET_NUM = re.compile(r"to\s*~?\s*([0-9][0-9,]*)")
BOOL_ASSIGN = re.compile(r"(spark\.[A-Za-z0-9_.]*[A-Za-z0-9])\s*=\s*(true|false)", re.I)
SIZE_LITERAL = re.compile(r"^([0-9.]+)\s*([kmgt])?b?$", re.I)
CONFIG_CALL = re.compile(r'^([+-])\s*\.config\(\s*"([^"]+)"\s*,\s*"?([^",)]+)"?')
MAIN_PY = re.compile(r'"main_python_file_uri":\s*"[^"]*?(models/[^"]+\.py)"')


def git(repo: str, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", repo, *args], capture_output=True, text=True
    ).stdout


def load_ledger(path: str) -> list[dict]:
    if path.startswith("gs://"):
        tmp = os.path.join(tempfile.mkdtemp(), "optimization_ledger.jsonl")
        for tool in (["gcloud", "storage", "cp"], ["gsutil", "-q", "cp"]):
            r = subprocess.run([*tool, path, tmp], capture_output=True, text=True)
            if not r.returncode:
                break
        else:
            sys.exit(f"could not read {path}: {r.stderr.strip()[:400]}")
        path = tmp
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def sweep_dates(rows: list[dict]) -> dict[str, list[str]]:
    """Dates on which a surface's crawl actually ran, which is what resolution counts."""
    by_surface: dict[str, set[str]] = collections.defaultdict(set)
    for r in rows:
        if r.get("state") not in HUMAN and r.get("date"):
            by_surface[r.get("surface") or "spark"].add(r["date"])
    return {k: sorted(v) for k, v in by_surface.items()}


def trailing_quiet(after: list[str], fired: set[str]) -> list[str]:
    """The unbroken run of quiet sweep-dates ending at the newest one, which is what resolve keys on."""
    run: list[str] = []
    for d in reversed(after):
        if d in fired:
            break
        run.append(d)
    return list(reversed(run))


def partial_dates(rows: list[dict]) -> set[str]:
    return {r["date"] for r in rows if r.get("partial")}


def observed_hours(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Executor-hours the crawl actually measured per dag per date; a date absent here was never looked at."""
    seen: dict[str, dict[str, float]] = collections.defaultdict(dict)
    for r in rows:
        if r.get("exec_h") is not None and r.get("state") != "applied":
            seen[r.get("dag_id", "")][r.get("date", "")] = float(r["exec_h"])
    return seen


def detector_activity(rows: list[dict]) -> dict[str, set[str]]:
    """Dates on which each detector fired anywhere in the fleet."""
    active: dict[str, set[str]] = collections.defaultdict(set)
    for r in rows:
        key = r.get("key", "")
        if key and key != WATCH_KEY and r.get("state") not in HUMAN and r.get("state") not in NON_FIRING:
            active[key.split(":")[0]].add(r.get("date", ""))
    return active


def fix_text_for(history: list[dict]) -> str:
    return next((e["fix"] for e in reversed(history) if e.get("fix")), "")


def parse_recommendation(text: str) -> dict:
    primary, warned = [], []
    for _verb, key in IMPERATIVE.findall(text):
        if key not in primary:
            primary.append(key)
    for _verb, key in PROHIBITION.findall(text):
        warned.append(key)
        if key in primary:
            primary.remove(key)
    for key, _val in BOOL_ASSIGN.findall(text):
        if key not in primary:
            primary.append(key)
    mentioned = [k for k in SPARK_KEY.findall(text) if k not in primary and k not in warned]
    targets: dict[str, float] = {}
    for sentence in re.split(r"[.;]", text):
        keys = [k for k in SPARK_KEY.findall(sentence) if k in primary]
        num = TARGET_NUM.search(sentence)
        if keys and num:
            targets[keys[0]] = float(num.group(1).replace(",", ""))
    booleans = {k: v.lower() for k, v in BOOL_ASSIGN.findall(text)}
    return {
        "primary": primary,
        "warned_against": warned,
        "mentioned": [m for m in mentioned if m not in SECONDARY_PARAMS],
        "secondary": [m for m in mentioned if m in SECONDARY_PARAMS],
        "targets": targets,
        "booleans": booleans,
        "relative": bool(re.search(r"\b2x the current\b", text, re.I)),
    }


def merge_commit(repo: str, pr_url: str, main_ref: str) -> tuple[str, str] | None:
    """The commit that actually put this PR on the shipped branch, and the tree it replaced."""
    number = pr_url.rstrip("/").rsplit("/", 1)[-1]
    for pattern in (f"Merge pull request #{number} from", f"(#{number})"):
        out = git(repo, "log", main_ref, "--format=%H%x09%P", "--fixed-strings", "--grep", pattern)
        if out.strip():
            sha, parents = out.strip().split("\n")[0].split("\t")
            return sha, parents.split()[0]
    return None


def task_config(repo: str, tree: str, dag_id: str) -> dict | None:
    raw = git(repo, "show", f"{tree}:{CONFIG_PATH}")
    if not raw.strip():
        return None
    try:
        return json.loads(raw).get(dag_id)
    except json.JSONDecodeError:
        return None


def spark_properties(entry: dict | None) -> dict[str, str]:
    out: dict[str, str] = {}

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "properties" and isinstance(v, dict):
                    for pk, pv in v.items():
                        out[pk.split(":")[-1]] = str(pv)
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(entry or {})
    return out


def model_file(entry: dict | None) -> str | None:
    hit = MAIN_PY.findall(json.dumps(entry or {}))
    return hit[0] if hit else None


def shipped_changes(repo: str, pr_url: str, dag_id: str, main_ref: str) -> dict:
    found = merge_commit(repo, pr_url, main_ref)
    if not found:
        return {"resolvable": False, "changed": {}, "code_changed": False, "head": "", "base": ""}
    head, base = found
    before = spark_properties(task_config(repo, base, dag_id))
    after = spark_properties(task_config(repo, head, dag_id))
    changed = {k: (before.get(k), v) for k, v in after.items() if before.get(k) != v}
    changed.update({k: (v, None) for k, v in before.items() if k not in after})
    path = model_file(task_config(repo, head, dag_id)) or model_file(task_config(repo, base, dag_id))
    diff = git(repo, "diff", base, head, "--", path) if path else ""
    removed = {k: v for sign, k, v in CONFIG_CALL.findall(diff) if sign == "-"}
    for sign, key, value in CONFIG_CALL.findall(diff):
        if sign == "+":
            changed[key] = (removed.get(key), value)
    return {
        "resolvable": True,
        "head": head,
        "base": base,
        "changed": changed,
        "code_changed": bool(diff.strip()),
    }


def competing_commits(repo: str, dag_id: str, start: str, end: str, own: set[str],
                      main_ref: str) -> list[str]:
    """Shipped commits other than the fix that changed this dag's own config inside the quiet window."""
    log = git(
        repo, "log", main_ref, "--format=%H", f"--since={start}", f"--until={end} 23:59:59",
        "--", CONFIG_PATH,
    )
    hits = []
    for sha in [s for s in log.split() if s not in own]:
        if task_config(repo, sha, dag_id) != task_config(repo, f"{sha}^", dag_id):
            hits.append(sha[:8])
    return hits


def numeric(value: str | None) -> float | None:
    if value is None:
        return None
    hit = SIZE_LITERAL.match(str(value).strip())
    if not hit:
        return None
    scale = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    return float(hit.group(1)) * scale.get((hit.group(2) or "").lower(), 1)


def score_alignment(rec: dict, shipped: dict) -> tuple[str, str]:
    changed = shipped["changed"]
    if not shipped["resolvable"]:
        return "unscoreable", "fix_pr merge commit not found in the checkout"
    violated = [k for k in rec["warned_against"] if k in changed]
    if not changed and not shipped["code_changed"]:
        return "no_shipped_change", "the PR changed nothing attributable to this dag"
    hits, misses = [], []
    for key in rec["primary"]:
        if key not in changed:
            misses.append(key)
            continue
        before, after = changed[key]
        want = rec["targets"].get(key)
        want_bool = rec["booleans"].get(key)
        if want_bool is not None:
            hits.append(key) if str(after).lower() == want_bool else misses.append(f"{key}=off_target")
        elif want is not None:
            got = numeric(after)
            ok = got is not None and want / TARGET_TOLERANCE <= got <= want * TARGET_TOLERANCE
            hits.append(key) if ok else misses.append(f"{key}={after}_vs_target_{want:g}")
        elif rec["relative"]:
            b, a = numeric(before), numeric(after)
            ok = b is not None and a is not None and a >= b * RELATIVE_TARGET
            hits.append(key) if ok else misses.append(f"{key}={before}->{after}_under_2x")
        else:
            hits.append(key)
    detail = f"changed={sorted(changed)}"
    if violated:
        detail += f" violates_warning={violated}"
    if hits and not misses:
        return "matched", detail
    if hits:
        return "partially_matched", detail + f" missed={misses}"
    equivalents = {e for k in rec["primary"] for e in EQUIVALENT_LEVERS.get(k, set())} & set(changed)
    if equivalents:
        return "partially_matched", detail + f" equivalent_lever={sorted(equivalents)}"
    if set(rec["secondary"]) & set(changed):
        return "partially_matched", detail + " secondary_parameter_only"
    return "different_fix_worked", detail + f" recommended={rec['primary']}"


def attribution_failures(unit: dict, hours: dict, detectors: dict, torn: set, repo: str,
                         shipped: dict, min_quiet: int, main_ref: str) -> dict[str, str]:
    """Every reason this finding's quiet is not evidence that its recommendation worked."""
    quiet = unit["quiet_date_list"]
    dag, applied, live = unit["dag_id"], unit["applied_date"], unit["watch_from"]
    seen = hours.get(dag, {})
    out: dict[str, str] = {}

    final_pre_fix = unit["last_pre_fix_date"]
    if final_pre_fix and unit["last_fired"] and unit["last_fired"] < final_pre_fix:
        out["pre_fix_quiet"] = (
            f"last fired {unit['last_fired']}, already silent on {final_pre_fix}, the final "
            f"sweep-date before the fix was live from {live} (merged {applied}); "
            "it had stopped on its own and the fix cannot be credited"
        )
    missing = [d for d in quiet if d not in seen]
    if len(quiet) - len(missing) < min_quiet:
        out["unobserved_window"] = (
            f"{len(quiet) - len(missing)} of {len(quiet)} quiet dates measured this dag; "
            f"unmeasured {missing}: absence of a finding is absence of a look"
        )
    torn_hit = [d for d in quiet if d in torn]
    if len(quiet) - len(torn_hit) < min_quiet:
        out["partial_window"] = f"quiet dates on partial sweeps: {torn_hit}"

    before = [h for d, h in sorted(seen.items()) if d < live][-BASELINE_DATES:]
    after = [seen[d] for d in quiet if d in seen]
    if before and after and statistics.median(before) > 0:
        ratio = statistics.median(after) / statistics.median(before)
        if ratio < DARK_RATIO:
            out["dag_went_dark"] = (
                f"median exec_h {statistics.median(before):.1f} before vs "
                f"{statistics.median(after):.1f} after ({ratio:.0%}); the job stopped doing the work"
            )
    unit["exec_h_before"] = round(statistics.median(before), 1) if before else ""
    unit["exec_h_after"] = round(statistics.median(after), 1) if after else ""

    fired_on = detectors.get(unit["detector"], set())
    if quiet and not any(d in fired_on for d in quiet) and any(d in fired_on for d in seen if d < live):
        out["detector_went_silent"] = (
            f"{unit['detector']} fired nowhere in the fleet on {quiet}; "
            "the quiet belongs to the detector, not to this fix"
        )
    own = {shipped.get("head", ""), shipped.get("base", "")}
    competing = competing_commits(repo, dag, applied, quiet[-1], own, main_ref) if quiet else []

    if competing:
        out["confounded_by_other_change"] = (
            f"commits {competing} also changed this dag's config inside the window"
        )
    return out


def analyse(rows: list[dict], repo: str, min_quiet: int, main_ref: str,
            effective_from: str = "") -> dict:
    dates_by_surface = sweep_dates(rows)
    all_dates = sorted({d for v in dates_by_surface.values() for d in v})
    torn = partial_dates(rows)
    hours = observed_hours(rows)
    detectors = detector_activity(rows)
    history: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
    for r in rows:
        history[(r.get("dag_id", ""), r.get("key", ""))].append(r)
    for h in history.values():
        h.sort(key=lambda e: e.get("date", ""))

    units = []
    for (dag_id, key), hist in sorted(history.items()):
        if key == WATCH_KEY:
            continue
        attribution = next((e for e in reversed(hist) if e.get("fix_pr")), None)
        if not attribution or not attribution.get("applied_date"):
            continue
        applied = attribution["applied_date"]
        surface = attribution.get("surface") or "spark"
        firing = [e["date"] for e in hist
                  if e.get("state") not in HUMAN and e.get("state") not in NON_FIRING]
        watch_from = effective_from or applied
        surface_dates = dates_by_surface.get(surface, [])
        after = [d for d in surface_dates
                 if (d >= effective_from if effective_from else d > applied)]
        before = [d for d in surface_dates if d < watch_from]
        quiet = trailing_quiet(after, set(firing))
        outcome = next((e["state"] for e in reversed(hist)
                        if e.get("state") in ("resolved", "fix_not_working")), "watching")
        rec = parse_recommendation(fix_text_for(hist))
        units.append({
            "dag_id": dag_id,
            "finding_key": key,
            "detector": key.split(":")[0],
            "impact": attribution.get("impact", ""),
            "surface": surface,
            "fix_pr": attribution.get("fix_pr", ""),
            "applied_date": applied,
            "watch_from": watch_from,
            "last_pre_fix_date": before[-1] if before else "",
            "last_fired": max(firing) if firing else "",
            "ledger_outcome": outcome,
            "sweep_dates_after_fix": len(after),
            "fired_after_fix": len([d for d in firing if d > applied]),
            "quiet_dates": len(quiet),
            "quiet_date_list": quiet,
            "exec_h_before": "",
            "exec_h_after": "",
            "recommended_keys": ",".join(rec["primary"]),
            "warned_against": ",".join(rec["warned_against"]),
            "_rec": rec,
        })

    cache: dict[tuple[str, str], dict] = {}
    for u in units:
        ck = (u["fix_pr"], u["dag_id"])
        if ck not in cache:
            cache[ck] = shipped_changes(repo, u["fix_pr"], u["dag_id"], main_ref)
        shipped = cache[ck]
        u["shipped_keys"] = ",".join(
            f"{k}:{v[0]}->{v[1]}" for k, v in sorted(shipped["changed"].items())
        )
        u["eligible"] = u["quiet_dates"] >= min_quiet
        if not u["eligible"]:
            u["attribution_failures"] = ""
            u["attributable"] = False
            u["verdict"] = "not_eligible"
            u["verdict_detail"] = (
                f"{u['quiet_dates']} of {min_quiet} consecutive quiet {u['surface']} "
                f"sweep-dates since {u['watch_from']}"
            )
        else:
            failures = attribution_failures(
                u, hours, detectors, torn, repo, shipped, min_quiet, main_ref
            )
            ranked = [r for r in ATTRIBUTION_PRIORITY if r in failures]
            u["attribution_failures"] = ",".join(ranked)
            u["attributable"] = not ranked
            if u["fired_after_fix"] and u["ledger_outcome"] != "resolved":
                u["verdict"] = "fix_not_working"
                u["verdict_detail"] = f"still fired on {u['fired_after_fix']} sweep-dates after the fix"
            elif ranked:
                u["verdict"] = "quiet_unrelated"
                u["verdict_detail"] = f"{ranked[0]}: {failures[ranked[0]]}"
            else:
                u["verdict"], u["verdict_detail"] = score_alignment(u["_rec"], shipped)
        u["quiet_date_list"] = ",".join(u["quiet_date_list"])
        del u["_rec"]
    return {
        "dates": all_dates,
        "dates_by_surface": dates_by_surface,
        "partial_dates": sorted(torn),
        "units": units,
        "min_quiet": min_quiet,
    }


def next_day(date: str) -> str:
    y, m, d = (int(x) for x in date.split("-"))
    return (datetime.date(y, m, d) + datetime.timedelta(days=1)).isoformat()


def scoreable_on(unit: dict, newest: str, need: int) -> str:
    """The ledger date this finding first holds `need` quiet dates, one new sweep-date per day from `newest`."""
    have, date = unit["quiet_dates"], newest
    while have < need:
        date = next_day(date)
        if date >= unit["watch_from"]:
            have += 1
    return date


def forecast(result: dict) -> dict:
    """What the sample can be once the ledger gains the sweep-dates it is still missing."""
    units = result["units"]
    need = result["min_quiet"]
    blind = [u for u in units if not u["sweep_dates_after_fix"]]
    pre_fix = [u for u in units if u["last_pre_fix_date"] and u["last_fired"]
               and u["last_fired"] < u["last_pre_fix_date"]]
    still_firing = [u for u in units if u["fired_after_fix"]]
    live = [u for u in units if u not in pre_fix and u not in still_firing]
    short = max((need - u["quiet_dates"] for u in units if not u["eligible"]), default=0)
    newest = result["dates"][-1] if result["dates"] else ""
    pending = [u for u in (live or units) if not u["eligible"]]
    ledger_date, run_date = "", ""
    if pending and newest:
        ledger_date = min(scoreable_on(u, newest, need) for u in pending)
        run_date = next_day(ledger_date)
    return {
        "no_post_fix_dates": blind,
        "pre_fix_quiet": pre_fix,
        "still_firing": still_firing,
        "best_case_attributable": live,
        "independent_units": len({(u["dag_id"], u["fix_pr"]) for u in live}),
        "sweeps_short": short,
        "earliest_ledger_date": ledger_date,
        "earliest_run_date": run_date,
    }


def table(rows: list[dict], columns: list[str]) -> str:
    if not rows:
        return "  (none)\n"
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    out = ["  " + "  ".join(c.ljust(widths[c]) for c in columns),
           "  " + "  ".join("-" * widths[c] for c in columns)]
    for r in rows:
        out.append("  " + "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns))
    return "\n".join(out) + "\n"


def print_forecast(result: dict, fc: dict) -> None:
    units = result["units"]
    print("SAMPLE FORECAST")
    print(f"  attributed findings                          {len(units)}")
    print(f"  no post-fix sweep-dates observed yet         {len(fc['no_post_fix_dates'])}")
    print(f"  already silent on the last pre-fix sweep     {len(fc['pre_fix_quiet'])}   "
          f"(unattributable however long they stay quiet)")
    print(f"  still firing after their fix                 {len(fc['still_firing'])}   "
          f"(scoreable now as fix_not_working)")
    print(f"  best case attributable once quiet lands      {len(fc['best_case_attributable'])}   "
          f"across {fc['independent_units']} independent (dag, PR) units")
    print(f"  further complete sweep-dates still needed    {fc['sweeps_short']}")
    if fc["earliest_run_date"]:
        print(f"  earliest scoreable ledger date               {fc['earliest_ledger_date']}, "
              f"written by the {fc['earliest_run_date']} 09:00 UTC run")
    by_pr = collections.Counter(u["fix_pr"].rsplit("/", 1)[-1] for u in fc["best_case_attributable"])
    by_det = collections.Counter(u["detector"] for u in fc["best_case_attributable"])
    print(f"  best case by PR                              {dict(by_pr)}")
    print(f"  best case by detector                        {dict(by_det)}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=DEFAULT_LEDGER)
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--min-quiet", type=int, default=RESOLVE_SWEEPS)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs"))
    ap.add_argument("--main-ref", default="origin/main")
    ap.add_argument("--effective-from", default="",
                    help="first ledger date the shipped config was actually live in prod")
    ap.add_argument("--forecast-only", action="store_true")
    args = ap.parse_args()

    rows = load_ledger(args.ledger)
    result = analyse(rows, args.repo, args.min_quiet, args.main_ref, args.effective_from)
    units = result["units"]
    eligible = [u for u in units if u["eligible"]]
    attributable = [u for u in eligible if u["attributable"] or u["verdict"] == "fix_not_working"]
    fc = forecast(result)

    print(f"ledger rows {len(rows)} | sweep dates {len(result['dates'])} "
          f"({result['dates'][0]} .. {result['dates'][-1]})")
    for surface, days in sorted(result["dates_by_surface"].items()):
        print(f"  {surface}: {len(days)} sweep dates, newest {days[-1]}")
    print(f"partial sweep dates: {result['partial_dates'] or 'none recorded'}")
    print(f"attributed findings {len(units)} across "
          f"{len({(u['dag_id'], u['fix_pr']) for u in units})} (dag, PR) units\n")
    print_forecast(result, fc)
    if args.forecast_only:
        return 0

    if not eligible:
        print(f"NOT ENOUGH QUIET DATES YET: 0 of {len(units)} attributed findings have reached "
              f"{args.min_quiet} quiet sweep-dates after their applied_date.")
        print(f"Nothing is scoreable. Re-run after the ledger gains {fc['sweeps_short']} "
              f"more complete sweep-date(s).\n")
        print(table(sorted(units, key=lambda u: (u["dag_id"], u["finding_key"])),
                    ["dag_id", "finding_key", "fix_pr", "applied_date", "last_fired",
                     "quiet_dates", "verdict_detail"]))
        return 2

    os.makedirs(args.out, exist_ok=True)
    csv_path = os.path.join(args.out, f"audi_1328_recommendation_scores_{result['dates'][-1]}.csv")
    columns = [c for c in units[0] if not c.startswith("_")]
    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(units)

    tally = collections.Counter(u["verdict"] for u in eligible)
    print(f"{len(eligible)} eligible findings, {len(attributable)} of them attributable\n")
    print(table([{"verdict": k, "findings": v} for k, v in tally.most_common()],
                ["verdict", "findings"]))
    print()
    print(table(sorted(eligible, key=lambda u: (u["verdict"], u["dag_id"])),
                ["dag_id", "finding_key", "fix_pr", "verdict", "quiet_dates", "last_fired",
                 "exec_h_before", "exec_h_after", "attribution_failures",
                 "recommended_keys", "shipped_keys"]))
    for u in sorted(eligible, key=lambda x: (x["verdict"], x["dag_id"])):
        print(f"\n  {u['dag_id']}/{u['finding_key']}: {u['verdict']}\n    {u['verdict_detail']}")
    print(f"\nwrote {csv_path}")

    if not attributable:
        print("\nNOTHING ATTRIBUTABLE: every eligible finding's quiet has a competing explanation. "
              "No recommendation can be scored right or wrong from this ledger.")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
