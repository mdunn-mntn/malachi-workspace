"""Temp-bucket acquisition: enumerate Dataproc batches, then read their per-uuid event logs.

Most of the fleet does NOT write to the flat spark-events archive. A batch that sets no
spark.eventLog.dir still gets one from Dataproc, at
gs://<temp-bucket>/<batch-uuid>/spark-job-history/ - whether or not a history server is
attached. Of 200 recent prod batches, 13 wrote to the archive and 185 wrote there. Those
uuid dirs are scattered among thousands of empty ones, so a flat prefix scan is infeasible:
the crawl must ENUMERATE batches (dataproc batches list, key-free via ADC) and derive each
log path from the batch uuid.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess

PROJECT = "mntn-prj-prod-00"
REGION = "us-central1"
PHS_TEMP_BUCKET = "dataproc-temp-us-central1-995798185124-svhwvc6j"
ARCHIVE_PREFIX = "gs://mntn-data-archive-prod/spark-events"
TEAM = "ti"  # the batch label that says a job is ours; the project is shared
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


def event_log_dir(batch: dict) -> str:
    """The batch's configured spark.eventLog.dir, or "" when it set none."""
    props = (batch.get("runtimeConfig") or {}).get("properties") or {}
    return props.get("spark:spark.eventLog.dir") or props.get("spark.eventLog.dir") or ""


def phs_succeeded(batches: list[dict], archive: str = ARCHIVE_PREFIX,
                  team: str = TEAM) -> list[dict]:
    """This team's SUCCEEDED batches whose log lands in the temp bucket, not the archive."""
    return [b for b in batches
            if b.get("state") == "SUCCEEDED" and b.get("uuid")
            and (b.get("labels") or {}).get("team", team) == team
            and not event_log_dir(b).startswith(archive)]


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


# A batch dir is a recursive copy of unknown size, so an uncapped fetch fills the worker disk.
MAX_BATCHES = 150
MAX_BYTES = 4 * 1024**3


def _dir_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            with contextlib.suppress(OSError):
                total += os.path.getsize(os.path.join(root, f))
    return total


def fetch_logs(batches: list[dict], dest: str, bucket: str = PHS_TEMP_BUCKET,
               max_batches: int = MAX_BATCHES, max_bytes: int = MAX_BYTES) -> list[str]:
    """Download each batch's event log; skip an unreadable or absent one quietly."""
    got = []
    if len(batches) > max_batches:
        print(f"[phs] capped at {max_batches} of {len(batches)} batches")
        batches = batches[:max_batches]
    for b in batches:
        if _dir_bytes(dest) >= max_bytes:
            print(f"[phs] stopping at {len(got)} batches: download budget "
                  f"({max_bytes // 1024**3} GiB) reached")
            break
        local = os.path.join(dest, b.get("uuid", "unknown"))
        os.makedirs(local, exist_ok=True)
        try:
            r = subprocess.run(
                # -r or a rolling eventlog_v2_* dir is silently skipped, batch reads empty.
                ["gsutil", *_GSUTIL_OPTS, "cp", "-r", f"{log_uri(b, bucket)}/*", local + "/"],
                capture_output=True, timeout=600,
            )
            rc = r.returncode
        except subprocess.TimeoutExpired:
            print(f"[phs] timed out on {b.get('uuid', 'unknown')}")
            rc = 1
        files = _strip_top_markers(local) if rc == 0 else []
        if files:
            got.append(local)
        elif not os.listdir(local):
            os.rmdir(local)
    return got


if __name__ == "__main__":
    import sys

    batches = phs_succeeded(list_batches())
    print(f"{len(batches)} SUCCEEDED batches whose log is in the temp bucket")
    for b in batches[:20]:
        bid = (b.get("name") or "").rsplit("/", 1)[-1]
        print(f"  {bid}  ->  {log_uri(b)}")
    if "--fetch" in sys.argv:
        dest = sys.argv[sys.argv.index("--fetch") + 1]
        paths = fetch_logs(batches, dest)
        print(f"fetched {len(paths)}/{len(batches)}")
