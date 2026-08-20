#!/usr/bin/env python3
"""Capture every artifact of one real end-to-end debugger run into a JSON evidence file.

The workbook renders this file rather than calling live GCP, so the deliverable regenerates
even when the `dataproc-debug` PAM grant has lapsed or the Cloud Logging freshness window
has rolled past the incident. Re-capture (needs gcloud auth + PAM) with:

    python3 tickets/audi_1191_airflow_spark_debugger/artifacts/audi_1191_capture_worked_example.py

Default subject is INC-013, the case that exercises the whole chain: an Airflow log carrying
NO cause, a batch id, the staging driveroutput fallback, a signature, a past-incident match
with a known fix PR, and a merged prod-verified fix.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import asdict

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, ROOT)

from airflow_debugger.dataproc_rca import driveroutput_uri  # noqa: E402
from airflow_debugger.orchestrate import investigate  # noqa: E402
from airflow_debugger.parse import parse_log, parse_log_file  # noqa: E402
from airflow_debugger.report import code_links  # noqa: E402

AIRFLOW_TI = os.path.expanduser("~/Developer/work/mntn/airflow-ti")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audi_1191_worked_example.json")

SUBJECT = {
    "incident": "INC-013",
    "date": "2026-08-07",
    "log": "on-call/airflow_logs/2026-08-07/"
           "082821__fpa_site_visit_batch_serverless__dsid30_augmentor_log_processing"
           "__try2__failed.log",
    "region": "us-central1",
    "project": "mntn-prj-prod-00",
    "dag_file": "dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py",
    "spark_file": "spark/fpa/dsid30_augmentor_log_processing.py",
    "fix_commit": "478a013",
    "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1179",
}


def _sh(cmd: list[str], timeout: int = 180) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        return f"<unavailable: {e}>"
    return r.stdout if r.returncode == 0 else f"<unavailable: {(r.stderr or '').strip()[:300]}>"


def _git(args: list[str]) -> str:
    return _sh(["git", "-C", AIRFLOW_TI, *args])


def capture() -> dict:
    """Run the whole chain once and record every input and output along the way."""
    log_path = os.path.join(ROOT, SUBJECT["log"])
    with open(log_path, encoding="utf-8", errors="replace") as f:
        airflow_log = f.read()

    # The production entrypoint, so the workbook shows what on-call actually gets.
    result = investigate(log_path, use_llm=False)
    parsed = parse_log_file(log_path)
    body_only = parse_log(airflow_log)
    diag = result["diagnosis"]
    spark = diag.get("spark") or {}
    matches = result["similar_incidents"]

    batch_describe = _sh([
        "gcloud", "dataproc", "batches", "describe", parsed.batch_id or "",
        "--region", SUBJECT["region"], "--project", SUBJECT["project"], "--format", "json",
    ])
    try:
        batch_json = json.loads(batch_describe)
    except json.JSONDecodeError:
        batch_json = {}
    state_message = batch_json.get("stateMessage", "")
    uri = driveroutput_uri(state_message) if state_message else None

    driver_full = ""
    if uri:
        driver_full = _sh(
            ["gsutil", "-o", "GSUtil:check_hashes=never", "cat", uri], timeout=300
        )

    return {
        "subject": SUBJECT,
        "airflow_log": airflow_log,
        "parsed": asdict(parsed),
        "parsed_body_only": asdict(body_only),
        "diagnosis": diag,
        "similar_incidents": matches,
        "confidence": result["confidence"],
        "llm_used": result["llm_used"],
        "batch_state": batch_json.get("state"),
        "batch_state_message": state_message,
        "batch_runtime_s": spark.get("runtime_s"),
        "batch_ttl": spark.get("ttl"),
        "driveroutput_uri": uri,
        "driver_log": driver_full,
        "report": result["report"],
        "troubleshooting": result["troubleshooting"],
        "code_links": code_links(diag),
        "spark_source_before": _git(["show", f"{SUBJECT['fix_commit']}^:{SUBJECT['spark_file']}"]),
        "spark_source_after": _git(["show", f"{SUBJECT['fix_commit']}:{SUBJECT['spark_file']}"]),
        "fix_diff": _git(["show", SUBJECT["fix_commit"], "--", SUBJECT["spark_file"]]),
        "dag_source": _git(["show", f"HEAD:{SUBJECT['dag_file']}"]),
    }


if __name__ == "__main__":
    ev = capture()
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(ev, f, indent=2)
    print(f"wrote {OUT}")
    print(f"  airflow log : {len(ev['airflow_log'].splitlines())} lines")
    print(f"  driver log  : {len(ev['driver_log'].splitlines())} lines")
    print(f"  signature   : {(ev['diagnosis'].get('root_signature') or {}).get('key')}")
    print(f"  similar     : {[m.get('id') for m in ev['similar_incidents'][:3]]}")
    print(f"  code links  : {len(ev['code_links'])}")
