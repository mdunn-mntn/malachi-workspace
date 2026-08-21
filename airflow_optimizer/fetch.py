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

    Raises on a listing failure rather than returning []. An empty list and a failed list are
    completely different facts downstream: one is a quiet day, the other is a broken sweep that
    would otherwise publish a confident, wrong "nothing to report".
    """
    r = subprocess.run(["gsutil", "ls", "-l", f"{prefix.rstrip('/')}/**"],
                       capture_output=True, text=True, timeout=900)
    if r.returncode != 0:
        raise RuntimeError(f"listing {prefix} failed ({r.returncode}): "
                           f"{(r.stderr or '').strip()[:300]}")
    rows = []
    for line in r.stdout.splitlines():
        parts = line.split()
        # gsutil ls -l ends with a "TOTAL: N objects, M bytes" line; it has no object column.
        if len(parts) >= 3 and parts[-1].endswith(".zstd") and parts[-1].startswith("gs://"):
            rows.append((parts[1], parts[-1]))          # (creation time, object)
    rows.sort()
    return [obj for _, obj in rows[-cap:]]


def download(objects: list[str], dest: str) -> tuple[int, int]:
    """Copy each object under `dest`. Returns (landed, failed).

    The failure count is returned rather than swallowed because a partial download is not a
    small version of a full one: the sweep would read the jobs that landed, see nothing from
    the rest, and report them as having stopped firing.
    """
    landed = failed = 0
    for obj in objects:
        target = dest_for(dest, obj)
        os.makedirs(target, exist_ok=True)
        r = subprocess.run(["gsutil", *GSUTIL_OPTS, "cp", obj, target + "/"],
                           capture_output=True, timeout=600)
        if r.returncode == 0:
            landed += 1
        else:
            failed += 1
            if failed <= 3:                       # enough to diagnose, not a log flood
                print(f"[fetch] failed {obj}: {(r.stderr or b'').decode()[:160]}")
    return landed, failed


def fetch_optional(obj: str, dest: str) -> bool:
    """Fetch one object that is allowed not to exist. True when it landed.

    Distinguishes "absent" from "the copy failed", which a bare download cannot. Callers that
    treat a missing file as empty state need that distinction: for the ledger, believing an
    unreadable object is an absent one destroys the history it is about to republish.
    """
    stat = subprocess.run(["gsutil", *GSUTIL_OPTS, "stat", obj],
                          capture_output=True, timeout=120)
    if stat.returncode != 0:
        return False                              # not there; a fresh start is correct
    os.makedirs(dest, exist_ok=True)
    cp = subprocess.run(["gsutil", *GSUTIL_OPTS, "cp", obj, dest + "/"],
                        capture_output=True, timeout=600)
    if cp.returncode != 0:
        raise RuntimeError(f"{obj} exists but could not be fetched "
                           f"({cp.returncode}): {(cp.stderr or b'').decode()[:200]}")
    return True
