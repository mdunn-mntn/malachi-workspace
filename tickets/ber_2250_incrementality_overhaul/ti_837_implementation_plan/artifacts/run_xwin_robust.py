#!/usr/bin/env python3
"""
TI-837 Phase 0c — robust BQ cross-window run.

Submits the v5 cross-window SQL via google-cloud-bigquery, saves the job_id
to disk, and polls until DONE. If this script dies, the BQ job keeps
running server-side; restart this script (or any script that knows the
job_id) and it will reattach.

Usage:
    submit:
        python3 run_xwin_robust.py submit

    poll (idempotent — works on any existing job_id from the state file):
        python3 run_xwin_robust.py poll

    fetch results (after DONE):
        python3 run_xwin_robust.py fetch

    do-it-all: submit, poll until DONE, fetch:
        python3 run_xwin_robust.py run
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

SQL_PATH = TICKET_ROOT / "queries/ti_837_lift_analysis_30adv_7day_v5_segments_xwin_2026_04_22_to_28.sql"
OUT_PATH = TICKET_ROOT / "outputs/ti_837_lift_30adv_7day_v5_xwin_2026_04_22_to_28.json"
STATE_PATH = TICKET_ROOT / "outputs/ti_837_xwin_job_state.json"

PROJECT = "dw-main-silver"


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)


def submit() -> str:
    sql = SQL_PATH.read_text()
    client = _client()
    config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        priority=bigquery.QueryPriority.INTERACTIVE,
        labels={"ticket": "ti-837", "label": "phase0c-xwin-robust"},
    )
    job = client.query(sql, job_config=config, job_id_prefix="ti837_xwin_")
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
    return job.job_id


def _load_state() -> dict:
    return json.loads(STATE_PATH.read_text())


def _get_job(state: dict | None = None):
    if state is None:
        state = _load_state()
    client = _client()
    return client.get_job(
        state["job_id"], project=state["project"], location=state["location"]
    )


def status_one() -> bool:
    """Print one status line; return True if job is DONE."""
    state = _load_state()
    job = _get_job(state)
    job.reload()

    elapsed = time.time() - state["submitted_ts"]
    plan = job.query_plan or []
    n_done = sum(1 for s in plan if s.status == "COMPLETE")
    n_running = sum(1 for s in plan if s.status == "RUNNING")
    n_pending = sum(1 for s in plan if s.status == "PENDING")
    n_total = len(plan)

    bytes_processed = job.total_bytes_processed or 0
    slot_ms = (job.slot_millis or 0) if hasattr(job, "slot_millis") else 0

    pct = (n_done / n_total * 100) if n_total else 0.0
    ts = time.strftime("%H:%M:%S")
    print(
        f"[{ts}  +{int(elapsed)}s] state={job.state}  "
        f"stages={n_done}/{n_total} ({pct:.0f}%)  "
        f"running={n_running}  pending={n_pending}  "
        f"bytes={bytes_processed/1e9:.2f}GB  slot={slot_ms/1000/60:.1f}min",
        flush=True,
    )

    if job.state == "DONE":
        if job.error_result:
            print(f"[{ts}] JOB FAILED: {job.error_result}", flush=True)
            return True
        return True
    return False


def poll(interval_s: int = 60):
    """Poll until DONE. Idempotent — can be killed and restarted."""
    while not status_one():
        time.sleep(interval_s)


def fetch():
    state = _load_state()
    job = _get_job(state)
    job.reload()
    if job.state != "DONE":
        print(f"[fetch] job not DONE yet (state={job.state}); aborting.")
        sys.exit(1)
    if job.error_result:
        print(f"[fetch] job FAILED: {job.error_result}")
        sys.exit(1)
    print(f"[fetch] job DONE — fetching results to {OUT_PATH}", flush=True)
    rows = list(job.result(max_results=2000))
    with OUT_PATH.open("w") as f:
        for r in rows:
            f.write(json.dumps(dict(r), default=str) + "\n")
    print(f"[fetch] wrote {len(rows)} rows / {OUT_PATH.stat().st_size:,} bytes", flush=True)


def run_all():
    submit()
    poll()
    fetch()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["submit", "poll", "status", "fetch", "run"])
    ap.add_argument("--interval", type=int, default=60,
                    help="Poll interval seconds (default 60)")
    args = ap.parse_args()
    if args.cmd == "submit":
        submit()
    elif args.cmd == "poll":
        poll(args.interval)
    elif args.cmd == "status":
        status_one()
    elif args.cmd == "fetch":
        fetch()
    elif args.cmd == "run":
        run_all()


if __name__ == "__main__":
    main()
