"""Pull the newest Spark event logs out of GCS, preserving rolling-log structure.

The shell entrypoint has done this since the laptop days; this is the same logic in Python so
the sweep can run as an Airflow task with nothing but the package on the worker.

Two rules the download has to respect, both learned the hard way:

* `gcloud storage cp` silently corrupts a `.zstd` event log by routing it through the
  decompressive-transcoding gatekeeper. gsutil with `check_hashes=never` is the only safe path.
* A v2 rolling log is a DIRECTORY of `events_*` parts. Flattened into one download dir, the
  crawler reads every part as ONE merged job (cross-batch spill sums, colliding stage ids) and
  standalone `app-*.zstd` logs beside them are never analysed. Each part keeps its
  `eventlog_v2_*` parent.
"""

from __future__ import annotations

import os
import subprocess

GSUTIL_OPTS = ["-o", "GSUtil:check_hashes=never"]


def dest_for(root: str, obj: str) -> str:
    """Where one object lands: inside its rolling-log dir, or flat at the root."""
    parent = os.path.basename(os.path.dirname(obj))
    return os.path.join(root, parent) if parent.startswith("eventlog_v2_") else root


def newest_logs(prefix: str, cap: int) -> list[str]:
    """The `cap` most recently written finalized logs under `prefix`, oldest first.

    `.inprogress` logs are excluded: the crawler discards them, so including them spends the
    download budget on nothing and can pass a "downloaded > 0" check with an empty report.
    """
    r = subprocess.run(["gsutil", "ls", "-l", f"{prefix.rstrip('/')}/**"],
                       capture_output=True, text=True, timeout=900)
    rows = []
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[-1].endswith(".zstd"):
            rows.append((parts[1], parts[-1]))          # (creation time, object)
    rows.sort()
    return [obj for _, obj in rows[-cap:]]


def download(objects: list[str], dest: str) -> int:
    """Copy each object under `dest`. Returns how many landed; a failed object is skipped."""
    n = 0
    for obj in objects:
        target = dest_for(dest, obj)
        os.makedirs(target, exist_ok=True)
        r = subprocess.run(["gsutil", *GSUTIL_OPTS, "cp", obj, target + "/"],
                           capture_output=True, timeout=600)
        if r.returncode == 0:
            n += 1
    return n
