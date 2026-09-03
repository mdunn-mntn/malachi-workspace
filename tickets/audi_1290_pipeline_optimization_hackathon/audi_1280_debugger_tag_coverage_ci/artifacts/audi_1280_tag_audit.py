#!/usr/bin/env python3
"""Audit every airflow-ti DAG file's tags against the debugger watch list and the live deployment.

Usage (Python 3.12+, from the workspace root):
  uv run --python 3.12 --no-project --with pytest python artifacts/audi_1280_tag_audit.py [--worktree PATH]
Imports the resolver from tests/dags/test_alerting_tag_coverage.py in the worktree, joins each file to
outputs/audi_1280_live_dags.json on relative_fileloc, and writes outputs/audi_1280_tag_audit.csv plus
outputs/audi_1280_tag_audit.md. Read-only; the live JSON comes from audi_1280_live_dags_pull.py.
THREADED_CHANNELS mirrors the deployment's SLACK_ALERT_CHANNEL (astro deployment variable list, 2026-09-02).
"""

import argparse
import ast
import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

TICKET = Path(__file__).resolve().parent.parent
DEFAULT_WORKTREE = Path(
    "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/"
    "67074af2-5859-4b02-9a41-1fb172083596/scratchpad/wt/audi_1280"
)
THREADED_CHANNELS = {"#alerts-tpa-pipeline": "C08CURMGNMQ", "#monitor-tpa": "C067ZM2EC5S"}
COLUMNS = [
    "dag_id", "file", "team_config", "alert_route", "tags", "watched_at_main", "watched",
    "debugger_reply_location", "is_paused", "live_match", "live_only_tags", "source_only_tags",
]


def watch_list_at_main(worktree: Path) -> list[str]:
    source = subprocess.check_output(
        ["git", "-C", str(worktree), "show", "origin/main:include/airflow_debugger/daily.py"], text=True
    )
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "PAGING_TAGS" for t in node.targets):
            return [e.value for e in node.value.elts if isinstance(e, ast.Constant)]
    raise SystemExit("PAGING_TAGS not found on origin/main")


def reply_location(route: str) -> str:
    if not route.startswith(("slack:", "pagerduty:")):
        return "none"
    return "thread" if any(f"slack:{channel}" in route for channel in THREADED_CHANNELS) else "digest"


def build_rows(files: list, live: dict, watched_main: set[str], watched_now: set[str]) -> tuple[list[dict], list[dict]]:
    live_by_file = {d["relative_fileloc"]: d for d in live["dags"]}
    rows, matched = [], set()
    for info in files:
        live_dag = live_by_file.get(info.path)
        live_tags = set(live_dag["tags"]) if live_dag else set()
        if live_dag:
            matched.add(info.path)
        rows.append({
            "dag_id": live_dag["dag_id"] if live_dag else info.dag_id,
            "file": info.path,
            "team_config": info.team_config or "",
            "alert_route": info.alert_route,
            "tags": " ".join(sorted(info.resolved_tags)),
            "watched_at_main": "yes" if info.resolved_tags & watched_main else "no",
            "watched": "yes" if info.resolved_tags & watched_now else "no",
            "debugger_reply_location": reply_location(info.alert_route),
            "is_paused": ("yes" if live_dag["is_paused"] else "no") if live_dag else "",
            "live_match": "yes" if live_dag else "no live dag",
            "live_only_tags": " ".join(sorted(live_tags - info.resolved_tags)),
            "source_only_tags": " ".join(sorted(info.resolved_tags - live_tags)) if live_dag else "",
            "_alerting": info.alerting,
        })
    orphans = [d for f, d in live_by_file.items() if f not in matched]
    return rows, orphans


def rank(rows: list[dict]) -> list[dict]:
    order = {("yes", "no"): 0, ("yes", "yes"): 1, ("no", "no"): 2, ("no", "yes"): 3}
    key = lambda r: (order[(str(r["_alerting"] and "yes" or "no"), r["watched"])], r["team_config"], r["file"])
    return sorted(rows, key=key)


def write_markdown(rows: list[dict], orphans: list[dict], live: dict, watched_main: list[str], watched_now: list[str], out: Path) -> None:
    alerting = [r for r in rows if r["_alerting"]]
    lines = [
        "# AUDI-1280 tag audit",
        "",
        f"Source: {len(rows)} DAG files (dags/ minus .airflowignore). Live: {live['total_entries']} DAGs fetched {live['fetched_at_utc']}.",
        f"Alerting files: {len(alerting)}. Unwatched at origin/main: {sum(r['watched_at_main'] == 'no' for r in alerting)}. Unwatched on this branch: {sum(r['watched'] == 'no' for r in alerting)}.",
        f"Watch list at origin/main: {watched_main}",
        f"Watch list on this branch: {watched_now}",
        f"Unwatched at main by team config: {dict(Counter(r['team_config'] for r in alerting if r['watched_at_main'] == 'no'))}",
        f"Files with no live DAG: {[r['file'] for r in rows if r['live_match'] != 'yes']}",
        f"Live DAGs with no source file: {[d['dag_id'] for d in orphans]}",
        f"Source tags missing live (resolver over-resolves): {[(r['file'], r['source_only_tags']) for r in rows if r['source_only_tags']]}",
        f"Live tags missing from source (resolver under-resolves, P-tags excluded): {[(r['file'], r['live_only_tags']) for r in rows if set(r['live_only_tags'].split()) - {t for t in r['live_only_tags'].split() if t.startswith('P') and t[1:].isdigit()}]}",
        "",
        "| " + " | ".join(COLUMNS) + " |",
        "|" + "---|" * len(COLUMNS),
    ]
    lines += ["| " + " | ".join(str(r[c]) for c in COLUMNS) + " |" for r in rows]
    out.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worktree", type=Path, default=DEFAULT_WORKTREE)
    args = parser.parse_args()
    sys.path.insert(0, str(args.worktree / "tests" / "dags"))
    import test_alerting_tag_coverage as resolver

    files = resolver.resolve_dag_files(args.worktree)
    live = json.loads((TICKET / "outputs" / "audi_1280_live_dags.json").read_text())
    watched_main = watch_list_at_main(args.worktree)
    watched_now = resolver.read_watch_list(args.worktree / "include" / "airflow_debugger" / "daily.py")
    rows, orphans = build_rows(files, live, set(watched_main), set(watched_now))
    rows = rank(rows)

    csv_path = TICKET / "outputs" / "audi_1280_tag_audit.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    write_markdown(rows, orphans, live, watched_main, watched_now, TICKET / "outputs" / "audi_1280_tag_audit.md")

    alerting = [r for r in rows if r["_alerting"]]
    print(f"files={len(rows)} alerting={len(alerting)} unwatched_at_main={sum(r['watched_at_main'] == 'no' for r in alerting)} unwatched_now={sum(r['watched'] == 'no' for r in alerting)}")
    print("by config (unwatched at main):", dict(Counter(r["team_config"] for r in alerting if r["watched_at_main"] == "no")))
    print("no live dag:", [r["file"] for r in rows if r["live_match"] != "yes"])
    print("live orphans:", [d["dag_id"] for d in orphans])
    print("source-only tags:", [(r["file"], r["source_only_tags"]) for r in rows if r["source_only_tags"]])
    non_p = lambda tags: [t for t in tags.split() if not (t.startswith("P") and t[1:].isdigit())]
    print("live-only tags (non-P):", [(r["file"], non_p(r["live_only_tags"])) for r in rows if non_p(r["live_only_tags"])])
    print("dag_id mismatches:", [(r["file"], r["dag_id"]) for r in rows if r["live_match"] == "yes" and next(f.dag_id for f in files if f.path == r["file"]) not in (r["dag_id"], "<dynamic>")])
    print("paused alerting:", [r["dag_id"] for r in alerting if r["is_paused"] == "yes"])


if __name__ == "__main__":
    main()
