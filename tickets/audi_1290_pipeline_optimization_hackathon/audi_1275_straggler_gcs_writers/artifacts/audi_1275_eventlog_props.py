#!/usr/bin/env python3
"""Extract committer/speculation properties and per-stage write evidence from Spark event logs.

Usage (from the workspace root):
  python3 artifacts/audi_1275_eventlog_props.py --repo /path/to/airflow-ti-main \
      --ledger outputs/optimization_ledger_live.jsonl --out-dir outputs/ outputs/app-*.zstd

Writes <out-dir>/audi_1275_app_props.csv (one row per app) and
<out-dir>/audi_1275_stage_evidence.csv (one row per stage with more than one task).
"""
import argparse
import csv
import json
import os
import sys
from collections import Counter
from statistics import median

PROP_KEYS = [
    "spark.speculation",
    "spark.speculation.quantile",
    "spark.speculation.multiplier",
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs",
    "spark.sql.sources.commitProtocolClass",
    "spark.sql.parquet.output.committer.class",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
    "spark.hadoop.mapreduce.manifest.committer.validate.output",
    "spark.hadoop.outputCommitCoordination.enabled",
    "spark.sql.shuffle.partitions",
    "spark.dynamicAllocation.maxExecutors",
    "spark.dataproc.scaling.version",
]
JAR_MARKERS = ("gcs-connector", "hadoop-cloud-storage", "spark-hadoop-cloud", "iceberg-spark-runtime")


def ledger_index(path: str) -> dict:
    by_app: dict = {}
    if not path or not os.path.exists(path):
        return by_app
    for line in open(path):
        r = json.loads(line)
        if not r["key"].startswith("straggler:") or not r["app_id"]:
            continue
        by_app.setdefault(r["app_id"].removesuffix(".zstd"), {"dag": r["dag_id"], "keys": set()})
        by_app[r["app_id"].removesuffix(".zstd")]["keys"].add(r["key"])
    return by_app


def scan(path: str, evlog) -> tuple[dict, list]:
    stages: dict = {}
    execs: dict = {}
    app = {"file": os.path.basename(path), "app_id": "", "app_name": "", "spark_version": "",
           "speculative_tasks": 0, "task_end_reasons": Counter(), "jars": {}, "props": {}}

    def stage(sid, attempt=0):
        return stages.setdefault((sid, attempt), evlog.StageMetrics(stage_id=sid))

    def execu(eid):
        return execs.setdefault(eid, evlog.ExecutorInfo(exec_id=eid))

    for e in evlog._read_events(path):
        ev = e.get("Event", "")
        if ev == "SparkListenerLogStart":
            app["spark_version"] = e.get("Spark Version", "")
        elif ev == "SparkListenerApplicationStart":
            app["app_id"] = e.get("App ID") or ""
            app["app_name"] = e.get("App Name") or ""
        elif ev == "SparkListenerEnvironmentUpdate":
            props = e.get("Spark Properties", {}) or {}
            app["props"] = {k: props.get(k, "") for k in PROP_KEYS}
            for entry in (e.get("Classpath Entries", {}) or {}):
                base = os.path.basename(entry)
                for m in JAR_MARKERS:
                    if base.startswith(m):
                        app["jars"][m] = base
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {}) or {}
            st = stage(si.get("Stage ID"), si.get("Stage Attempt ID", 0))
            st.name = si.get("Stage Name", st.name)
            st.num_tasks = max(st.num_tasks, si.get("Number of Tasks", 0))
            if si.get("Failure Reason"):
                st.failure_reason = str(si["Failure Reason"])[:200]
        elif ev == "SparkListenerTaskEnd":
            ti = e.get("Task Info", {}) or {}
            if ti.get("Speculative"):
                app["speculative_tasks"] += 1
            app["task_end_reasons"][(e.get("Task End Reason", {}) or {}).get("Reason", "")] += 1
            evlog._task_end(e, stage, execu)
    return app, evlog._finalize_stages(stages)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--ledger", default="")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("logs", nargs="+")
    a = ap.parse_args()
    sys.path.insert(0, a.repo)
    from include.spark_optimizer import eventlog as evlog

    idx = ledger_index(a.ledger)
    app_rows, stage_rows = [], []
    for path in a.logs:
        app, stages = scan(path, evlog)
        key = os.path.basename(path).removesuffix(".zstd")
        led = idx.get(key, {"dag": "", "keys": set()})
        straggler_ids = {int(k.split(":")[1]) for k in led["keys"]}
        write_stages = [s.stage_id for s in stages if s.output_bytes > 0]
        app_rows.append({
            "dag": led["dag"], "file": app["file"], "app_id": app["app_id"], "app_name": app["app_name"],
            "spark_version": app["spark_version"],
            "ledger_straggler_keys": ";".join(sorted(led["keys"])),
            "write_stages": ";".join(str(s) for s in write_stages),
            "straggler_is_write_stage": any(s in write_stages for s in straggler_ids),
            "speculative_tasks": app["speculative_tasks"],
            "task_end_reasons": ";".join(f"{k}={v}" for k, v in sorted(app["task_end_reasons"].items())),
            **{k: app["props"].get(k, "") for k in PROP_KEYS},
            **{f"jar_{m}": app["jars"].get(m, "") for m in JAR_MARKERS},
        })
        for s in stages:
            if s.num_tasks <= 1:
                continue
            durs = s.task_durs
            stage_rows.append({
                "dag": led["dag"], "app_id": app["app_id"], "stage_id": s.stage_id, "name": s.name[:80],
                "tasks": s.num_tasks, "succeeded": s.succeeded, "failed": s.failed,
                "input_gb": round(s.input_bytes / 1e9, 2), "output_gb": round(s.output_bytes / 1e9, 2),
                "shuffle_read_gb": round(s.shuffle_read_bytes / 1e9, 2),
                "shuffle_write_gb": round(s.shuffle_write_bytes / 1e9, 2),
                "skew_ratio": round(s.skew_ratio, 1), "data_skew_ratio": round(s.data_skew_ratio, 1),
                "median_task_s": round(median(durs) / 1000, 1) if durs else "",
                "max_task_s": round(max(durs) / 1000, 1) if durs else "",
                "is_write": s.output_bytes > 0,
                "is_ledger_straggler": s.stage_id in straggler_ids,
            })
        print(f"parsed {app['file']} dag={led['dag']} spark={app['spark_version']} "
              f"stages={len(stages)} write_stages={write_stages} straggler_keys={sorted(led['keys'])}")

    for name, rows in (("audi_1275_app_props.csv", app_rows), ("audi_1275_stage_evidence.csv", stage_rows)):
        with open(os.path.join(a.out_dir, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)


if __name__ == "__main__":
    main()
