"""Parse one fleet day of flat Spark event logs into stage-metric rows, in batches under the cap.

    python3 artifacts/audi_1317_fleet_backfill.py <worktree> <YYYYMMDD> <out.jsonl> <tmpdir>

Downloads at most BATCH_MIB at a time and deletes each batch after parsing, so the archive's
1.4 GiB day never lands on disk at once. Objects come down through the package's own
`fetch.download` (gcloud token + the GCS JSON API): `gsutil -m cp` wedges on this archive with
every part left at zero bytes, which is the same failure the sweep's downloader was rewritten
to escape.
"""

import json
import os
import shutil
import subprocess
import sys
import time

BUCKET = "gs://mntn-data-archive-prod/spark-events"
BATCH_MIB = 180
GSUTIL = ["gsutil", "-o", "GSUtil:check_hashes=never"]


def listing(day):
    out = subprocess.run([*GSUTIL, "ls", "-l", f"{BUCKET}/app-{day}*"],
                         capture_output=True, text=True).stdout
    objs = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[-1].startswith("gs://"):
            objs.append((parts[-1], int(parts[0])))
    return objs


def batches(objs, cap_bytes):
    batch, size = [], 0
    for name, n in objs:
        if batch and size + n > cap_bytes:
            yield batch
            batch, size = [], 0
        batch.append(name)
        size += n
    if batch:
        yield batch


def main():
    worktree, day, out_path, tmp = sys.argv[1:5]
    sys.path.insert(0, worktree)
    from include.spark_optimizer import fetch, ledger, stage_metrics
    from include.spark_optimizer.crawl import crawl

    sweep_date = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    objs = listing(day)
    print(f"{len(objs)} objects, {sum(n for _, n in objs) / 1048576:.0f} MiB", flush=True)
    total, errors = 0, 0
    with open(out_path, "w") as fh:
        for i, batch in enumerate(batches(objs, BATCH_MIB * 1048576), 1):
            shutil.rmtree(tmp, ignore_errors=True)
            os.makedirs(tmp, exist_ok=True)
            t0 = time.time()
            got, _failed = fetch.download(batch, tmp)
            for r in crawl([tmp]):
                if r.error:
                    errors += 1
                    continue
                for row in stage_metrics.rows_for(r, ledger._dag_id(r), sweep_date):
                    fh.write(json.dumps(row) + "\n")
                    total += 1
            fh.flush()
            print(f"batch {i}: {got}/{len(batch)} logs, {total} rows, {errors} unparsed, "
                  f"{time.time() - t0:.0f}s", flush=True)
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"done: {total} rows, {errors} unparsed", flush=True)


if __name__ == "__main__":
    main()
