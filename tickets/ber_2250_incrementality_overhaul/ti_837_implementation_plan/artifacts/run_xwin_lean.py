#!/usr/bin/env python3
"""
TI-837 Phase 0c — LEAN cross-window run (2 segments only).

Same as run_xwin_robust.py but uses the LEAN 2-segment SQL (drops `all`
and `stage1` segments). Two prior 4-segment xwin attempts hit BQ's
6-hour query timeout. The lean variant should fit comfortably under it.

Validates the deck's core claim ("retargeting drives lift, prospecting
drives almost none") by reproducing only the `prosp` and `rtg` segments
on the shifted window 2026-04-22 → 2026-04-28.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[4]
TICKET_ROOT = REPO_ROOT / "tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan"

SQL_PATH = TICKET_ROOT / "queries/ti_837_lift_analysis_30adv_7day_v5_xwin_LEAN_2segments.sql"
OUT_PATH = TICKET_ROOT / "outputs/ti_837_lift_30adv_7day_v5_xwin_LEAN_2026_04_22_to_28.json"
STATE_PATH = TICKET_ROOT / "outputs/ti_837_xwin_lean_job_state.json"

PROJECT = "dw-main-silver"


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)


def submit():
    sql = SQL_PATH.read_text()
    client = _client()
    config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        priority=bigquery.QueryPriority.INTERACTIVE,
        labels={"ticket": "ti-837", "label": "phase0c-xwin-lean-2seg"},
    )
    job = client.query(sql, job_config=config, job_id_prefix="ti837_xwin_lean_")
    state = {
        "job_id": job.job_id,
        "project": job.project,
        "location": job.location,
        "submitted_ts": time.time(),
    }
    STATE_PATH.write_text(json.dumps(state, indent=2))
    print(f"[submit] job_id    : {job.job_id}", flush=True)
    print(f"[submit] project   : {job.project}", flush=True)
    print(f"[submit] location  : {job.location}", flush=True)
    print(f"[submit] state     : {STATE_PATH}", flush=True)


def _load_state():
    return json.loads(STATE_PATH.read_text())


def _get_job(state=None):
    if state is None: state = _load_state()
    return _client().get_job(state["job_id"], project=state["project"], location=state["location"])


def status_one():
    state = _load_state()
    job = _get_job(state)
    job.reload()
    elapsed = time.time() - state["submitted_ts"]
    plan = job.query_plan or []
    n_done = sum(1 for s in plan if s.status == "COMPLETE")
    n_running = sum(1 for s in plan if s.status == "RUNNING")
    n_total = len(plan)
    pct = (n_done / n_total * 100) if n_total else 0.0
    bytes_processed = job.total_bytes_processed or 0
    slot_min = (job.slot_millis or 0) / 60000 if hasattr(job, "slot_millis") else 0
    ts = time.strftime("%H:%M:%S")
    print(
        f"[{ts}  +{int(elapsed)}s] state={job.state}  "
        f"stages={n_done}/{n_total} ({pct:.0f}%)  running={n_running}  "
        f"bytes={bytes_processed/1e9:.2f}GB  slot={slot_min:.1f}min",
        flush=True,
    )
    if job.state == "DONE":
        if job.error_result:
            print(f"[{ts}] JOB FAILED: {job.error_result}", flush=True)
        return True
    return False


def poll(interval_s=60):
    while not status_one():
        time.sleep(interval_s)


def fetch():
    state = _load_state()
    job = _get_job(state)
    job.reload()
    if job.state != "DONE":
        print(f"[fetch] job not DONE yet (state={job.state})")
        sys.exit(1)
    if job.error_result:
        print(f"[fetch] job FAILED: {job.error_result}")
        sys.exit(1)
    rows = list(job.result(max_results=2000))
    with OUT_PATH.open("w") as f:
        for r in rows:
            f.write(json.dumps(dict(r), default=str) + "\n")
    print(f"[fetch] wrote {len(rows)} rows / {OUT_PATH.stat().st_size:,} bytes to {OUT_PATH}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["submit", "poll", "status", "fetch"])
    ap.add_argument("--interval", type=int, default=60)
    args = ap.parse_args()
    if args.cmd == "submit": submit()
    elif args.cmd == "poll": poll(args.interval)
    elif args.cmd == "status": status_one()
    elif args.cmd == "fetch": fetch()


if __name__ == "__main__":
    main()
