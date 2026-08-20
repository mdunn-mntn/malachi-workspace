"""PHS acquisition: enumerate ipdsc/tpa batches, then read their per-uuid event logs.

The PHS-attached fleet (ipdsc/tpa) does NOT write to the flat spark-events archive; each
batch's log lands at gs://<phs-temp-bucket>/<batch-uuid>/spark-job-history/. Those uuid dirs
are scattered among thousands of empty ones, so a flat prefix scan is infeasible - the crawl
must ENUMERATE batches (dataproc batches list, key-free via ADC) and derive each log path
from the batch uuid. Blocked on standing storage.objectViewer for the temp bucket
(mntn-devops#4724); until it merges, fetches 403 and are skipped with a note.
"""

from __future__ import annotations

import json
import os
import subprocess

PROJECT = "mntn-prj-prod-00"
REGION = "us-central1"
PHS_TEMP_BUCKET = "dataproc-temp-us-central1-995798185124-svhwvc6j"
_GSUTIL_OPTS = [
    "-o", "GSUtil:check_hashes=never",
    "-o", "GSUtil:sliced_object_download_threshold=0",
]


def list_batches(project: str = PROJECT, region: str = REGION, limit: int = 500) -> list[dict]:
    """All recent batches as dicts (newest first per API), [] on CLI failure."""
    r = subprocess.run(
        ["gcloud", "dataproc", "batches", "list", f"--region={region}", f"--project={project}",
         f"--limit={limit}", "--format=json", "--sort-by=~createTime"],
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        return []
    try:
        return json.loads(r.stdout) or []
    except json.JSONDecodeError:
        return []


def phs_succeeded(batches: list[dict]) -> list[dict]:
    """The PHS-attached SUCCEEDED subset - the jobs whose logs live in per-uuid temp dirs."""
    out = []
    for b in batches:
        phs = ((b.get("environmentConfig") or {}).get("peripheralsConfig") or {}).get(
            "sparkHistoryServerConfig"
        )
        if phs and b.get("state") == "SUCCEEDED" and b.get("uuid"):
            out.append(b)
    return out


def log_uri(batch: dict, bucket: str = PHS_TEMP_BUCKET) -> str:
    """The batch's spark-job-history prefix (contains app-*.zstd, possibly .inprogress)."""
    return f"gs://{bucket}/{batch['uuid']}/spark-job-history"


def _strip_top_markers(local: str) -> list[str]:
    """Drop top-level appstatus_*/.crc leftovers and return what remains.

    A PHS batch dir can hold both a flat app-*.zstd and an eventlog_v2_* rolling dir. An
    appstatus_* marker sitting BESIDE them makes crawl._event_logs read the whole uuid dir as
    ONE rolling log, merging unrelated jobs. The marker inside a rolling dir is load-bearing,
    so only the top level is stripped.
    """
    for name in os.listdir(local):
        if name.startswith("appstatus_") or name.endswith(".crc"):
            path = os.path.join(local, name)
            if os.path.isfile(path):
                os.remove(path)
    return os.listdir(local)


def fetch_logs(batches: list[dict], dest: str, bucket: str = PHS_TEMP_BUCKET) -> list[str]:
    """Download each batch's event log; skip unreachable (403/absent) quietly. Returns paths."""
    got = []
    for b in batches:
        local = os.path.join(dest, b.get("uuid", "unknown"))
        os.makedirs(local, exist_ok=True)
        r = subprocess.run(
            # -r or a rolling eventlog_v2_* dir is silently skipped and the batch reads empty.
            ["gsutil", *_GSUTIL_OPTS, "cp", "-r", f"{log_uri(b, bucket)}/*", local + "/"],
            capture_output=True, timeout=600,
        )
        files = _strip_top_markers(local) if r.returncode == 0 else []
        if files:
            got.append(local)
        elif not os.listdir(local):
            os.rmdir(local)
    return got


if __name__ == "__main__":
    import sys

    batches = phs_succeeded(list_batches())
    print(f"{len(batches)} PHS-attached SUCCEEDED batches enumerated")
    for b in batches[:20]:
        bid = (b.get("name") or "").rsplit("/", 1)[-1]
        print(f"  {bid}  ->  {log_uri(b)}")
    if "--fetch" in sys.argv:
        dest = sys.argv[sys.argv.index("--fetch") + 1]
        paths = fetch_logs(batches, dest)
        print(f"fetched {len(paths)}/{len(batches)} (403s skipped pending mntn-devops#4724)")
