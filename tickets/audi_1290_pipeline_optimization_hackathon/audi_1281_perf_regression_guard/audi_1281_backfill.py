"""Download event logs in batches, persist per-stage rows with the AUDI-1281 code, delete the logs.

    python3 audi_1281_backfill.py <batch> [<batch> ...]
    batches: batch1 batch2a batch2b batch3 (intent_score_map) snh_a snh_b (site_network_hourly)

Stops taking new batches once BUDGET_MB of event logs has been downloaded in total.
"""

import datetime
import os
import shutil
import subprocess
import sys
import time

WT = ("/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/"
      "67074af2-5859-4b02-9a41-1fb172083596/scratchpad/wt/audi_1281")
sys.path.insert(0, WT)
from include.spark_optimizer import crawl as crawl_mod  # noqa: E402
from include.spark_optimizer import ledger, stage_metrics  # noqa: E402

TICKET = ("/Users/malachi/Developer/work/mntn/workspace/tickets/"
          "audi_1290_pipeline_optimization_hackathon/audi_1281_perf_regression_guard")
LOGS = f"{TICKET}/outputs/eventlogs"
LOG_FILE = f"{TICKET}/outputs/audi_1281_backfill_log.txt"
BUDGET_FILE = f"{LOGS}/downloaded_bytes.txt"
BUDGET_MB = 700
BUCKET = "gs://mntn-data-archive-prod/spark-events"
GSUTIL = ["gsutil", "-q", "-o", "GSUtil:check_hashes=never"]
TODAY = datetime.date.today().isoformat()
KEEP = {
    "eventlog_v2_batch-42e88a22-6f13-4282-9910-34d2e097ea4e",
    "eventlog_v2_batch-8f1a450a-2ebc-44de-a375-ef5408d27b2f",
}
ISM = f"{TICKET}/outputs/audi_1281_stage_metrics_intent_score_map.jsonl"
SNH = f"{TICKET}/outputs/audi_1281_stage_metrics_site_network_hourly.jsonl"
BATCHES = {
    "batch1": ("intent_score_map", ISM, [
        "eventlog_v2_batch-42e88a22-6f13-4282-9910-34d2e097ea4e",
        "eventlog_v2_batch-8f1a450a-2ebc-44de-a375-ef5408d27b2f",
        "eventlog_v2_batch-66cd15cd-579f-4af4-b71f-1f6c49e3282c",
        "eventlog_v2_batch-6403612f-3fdd-4c13-9c81-32d532df1274",
        "eventlog_v2_batch-51a2d5eb-ca47-446f-9019-8c82ada217a6",
        "eventlog_v2_batch-46993523-7a0c-48e7-add5-ccf76dea0954",
        "eventlog_v2_batch-06dfd454-de04-4aaf-a5a4-1bb4deda235a",
        "eventlog_v2_batch-ddbb2b71-ea37-438d-b60d-84167bd9e311",
    ]),
    "batch2a": ("intent_score_map", ISM, [
        "eventlog_v2_batch-b0e8d3ca-1644-4d03-8516-92628b71bd21",
        "eventlog_v2_batch-cac4f267-d187-458e-b39e-35510309cd1a",
        "eventlog_v2_batch-31f80375-ec39-41aa-aa28-957a9bf6389b",
        "eventlog_v2_batch-1dd66711-3495-4d7a-9783-34db10e54788",
        "eventlog_v2_batch-59987a51-af22-4559-819c-edc89544cbea",
    ]),
    "batch2b": ("intent_score_map", ISM, [
        "eventlog_v2_batch-28680ccd-66fd-4833-b6e6-511ff48c3c12",
        "eventlog_v2_batch-7e1cf930-5533-45b3-852b-95b330327261",
        "eventlog_v2_batch-4081d8ab-98ce-458a-aa1d-fd0f3e2c029a",
        "eventlog_v2_batch-48a1d47b-b1a6-4cb8-bfba-46f5dde2c87b",
        "eventlog_v2_batch-4c03c747-54a0-4f3d-8352-bf6f63d6a6ef",
    ]),
    "batch3": ("intent_score_map", ISM, [
        "eventlog_v2_batch-4651e5fa-23ad-449b-8d94-6dc07d546019",
    ]),
    "snh_a": ("site_network_hourly", SNH, [
        "app-20260901135143132-0605.zstd",
        "app-20260901145138851-0633.zstd",
        "app-20260901205120829-0863.zstd",
    ]),
    "snh_b": ("site_network_hourly", SNH, [
        "app-20260902005143827-0954.zstd",
        "app-20260902115131178-0350.zstd",
        "app-20260902145137894-0461.zstd",
    ]),
}


def log(msg: str) -> None:
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as fh:
        fh.write(line + "\n")


def downloaded_bytes() -> int:
    return int(open(BUDGET_FILE).read().strip() or 0) if os.path.exists(BUDGET_FILE) else 0


def add_downloaded(n: int) -> None:
    os.makedirs(LOGS, exist_ok=True)
    total = downloaded_bytes() + n
    with open(BUDGET_FILE, "w") as fh:
        fh.write(str(total))


def remote_bytes(names: list[str]) -> int:
    total = 0
    for name in names:
        r = subprocess.run(["gsutil", "du", "-s", f"{BUCKET}/{name}"],
                           capture_output=True, text=True)
        total += int(r.stdout.split()[0]) if r.returncode == 0 and r.stdout.strip() else 0
    return total


def local_bytes(path: str) -> int:
    if os.path.isfile(path):
        return os.path.getsize(path)
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


def download(batch_dir: str, names: list[str]) -> int:
    os.makedirs(batch_dir, exist_ok=True)
    fetched = 0
    for name in names:
        local = os.path.join(batch_dir, name)
        if os.path.exists(local):
            continue
        flag = ["-r"] if name.startswith("eventlog_v2_") else []
        t0 = time.time()
        r = subprocess.run([*GSUTIL, "cp", *flag, f"{BUCKET}/{name}", batch_dir],
                           capture_output=True, text=True)
        got = local_bytes(local) if os.path.exists(local) else 0
        fetched += got
        log(f"download {name} rc={r.returncode} {time.time() - t0:.0f}s {got / 1048576:.1f} MiB "
            f"{r.stderr.strip()[-120:]}")
    return fetched


def parse(batch_dir: str, dag: str, out: str) -> None:
    t0 = time.time()
    for r in crawl_mod.crawl([batch_dir]):
        if r.error:
            log(f"skipped {r.source}: {r.error}")
            continue
        resolved = ledger._dag_id(r)
        rows = stage_metrics.rows_for(r, resolved, TODAY)
        spill = {s["stage_id"]: s["disk_spill"] / 1024**3 for s in r.stages}
        fw = {s["stage_id"]: (s["fetch_wait_ms"] / s["run_time_ms"] if s["run_time_ms"] else 0)
              for s in r.stages}
        top_fw = sorted(fw.items(), key=lambda kv: kv[1], reverse=True)[:3]
        log(f"parsed {r.source} app={r.app_name!r} dag={resolved} "
            f"date={rows[0]['date'] if rows else '?'} exec_h={r.exec_h:.1f} "
            f"wall_min={(r.duration_ms or 0) / 60000:.0f} stages={len(r.stages)} "
            f"disk_spill_gib={{{', '.join(f'{k}: {v:.1f}' for k, v in spill.items() if v > 1)}}} "
            f"fetch_wait_top={{{', '.join(f'{k}: {v:.2f}' for k, v in top_fw if v > 0.01)}}}")
        if resolved != dag:
            log(f"ignored {r.source}: resolved dag {resolved} != {dag}")
            continue
        stage_metrics.append(rows, out)
    log(f"parse done in {time.time() - t0:.0f}s; {out} now {len(stage_metrics.read(out))} rows")


def delete(batch_dir: str) -> None:
    for name in sorted(os.listdir(batch_dir)):
        path = os.path.join(batch_dir, name)
        if name in KEEP:
            continue
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
    left = subprocess.run(["du", "-sh", LOGS], capture_output=True, text=True).stdout.split()[0]
    log(f"deleted parsed logs in {batch_dir}; {LOGS} now {left}")


def main() -> None:
    for batch in sys.argv[1:]:
        dag, out, names = BATCHES[batch]
        batch_dir = os.path.join(LOGS, batch)
        have = downloaded_bytes()
        pending = [n for n in names if not os.path.exists(os.path.join(batch_dir, n))]
        need = remote_bytes(pending)
        log(f"== {batch}: {len(names)} logs for {dag}; downloaded so far {have / 1e6:.0f} MB, "
            f"this batch needs {need / 1e6:.0f} MB more")
        if have + need > BUDGET_MB * 1e6:
            log(f"skipped {batch}: would exceed the {BUDGET_MB} MB budget")
            continue
        fetched = download(batch_dir, names)
        if fetched == 0 and not pending:
            fetched = local_bytes(batch_dir)
        add_downloaded(fetched)
        parse(batch_dir, dag, out)
        delete(batch_dir)
    log(f"all batches done; total downloaded {downloaded_bytes() / 1e6:.0f} MB")


if __name__ == "__main__":
    main()
