"""Apply the AUDI-1270 verdict rules to the parsed stage metrics and write the per-DAG verdict table.

Usage: python3 audi_1270_build_verdict.py <outputs_dir>
Reads audi_1270_stage_metrics.csv + audi_1270_spark_props.csv, writes audi_1270_verdict_table.csv.
"""

import csv
import math
import os
import sys

MIB = 1024**2
GIB = 1024**3
DISK_FLOOR_GIB, MEM_FLOOR_GIB = 2.0, 32.0
AQE_MERGE_THRESHOLD_MIB = 32
DRIVER_CAP = 5000

DAGS = {
    "fangorn_prospecting_scoring": ("models/audience_intent/fangorn_prospecting_scoring.py", "builder", "repartition(2048) write only"),
    "ipdsc_ds_17": ("models/ipdsc/ipdsc_ds_17.py", "none", "repartition(79, ip)"),
    "ipdsc_46_monitor": ("models/monitoring/ipdsc_46_monitor.py", "both", "coalesce(1) write only"),
    "ipdsc_14_monitor": ("models/monitoring/ipdsc_14_monitor.py", "both", "coalesce(1) write only"),
    "ipdsc_49_monitor": ("models/monitoring/ipdsc_49_monitor.py", "both", "coalesce(1) write only"),
    "ipdsc_ds_13": ("models/ipdsc/ipdsc_ds_13.py", "none", "repartition(35, ip)"),
    "ipdsc_ds_14": ("models/ipdsc/ipdsc_ds_14.py", "none", "repartition(12, ip)"),
    "ipdsc_ds_47": ("models/ipdsc/ipdsc_ds_47.py", "decorator", "none"),
    "fangorn_predictions_vertical": ("models/machine_learning/fangorn_predictions_vertical.py", "builder", "none"),
    "fangorn_household_predictions_vertical": ("models/machine_learning/fangorn_household_predictions_vertical.py", "builder", "none"),
    "vertical_size_monitor": ("models/monitoring/vertical_size_monitor.py", "both", "coalesce(1) write only"),
    "aug_log_ip": ("models/feature_store/feature_group_1_source/aug_log_ip.py", "builder", "repartition(8, ip)"),
    "guid_log_advertiser_id_dsc_id": ("models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py", "none", "coalesce(min(current, target))"),
    "guid_log_pivot_household_id_vertical_id": ("models/feature_store/feature_group_3_pivoted/guid_log_pivot_household_id_vertical_id.py", "decorator", "repartition(target_partitions, household_id)"),
    "advertiser_join": ("models/audience_intent/advertiser_join.py", "builder", "coalesce(14000)"),
}
OWNED_ELSEWHERE = {"guid_log_advertiser_id_dsc_id": "conflict_1269"}


def mib(size):
    n, unit = float(size.rstrip("gGmM")), size[-1].lower()
    return n * 1024 if unit == "g" else n


def round_up_100(n):
    return int(math.ceil(n / 100.0) * 100)


def newest_log_per_dag(props):
    by_dag = {}
    for p in props:
        dag = p["app_name"].removeprefix("Populate ").split(".")[0]
        if dag in DAGS and (dag not in by_dag or p["app_id"] > by_dag[dag]["app_id"]):
            by_dag[dag] = p
    return by_dag


def verdict_for(dag, p, st):
    parts = int(p["spark.sql.shuffle.partitions"])
    tasks, sread, mem = int(st["num_tasks"]), float(st["shuffle_read_gib"]), float(st["mem_spill_gib"])
    disk, inp = float(st["disk_spill_gib"]), float(st["input_gib"])
    exec_mib = mib(p["spark.executor.memory"]) * 0.6 / int(p["spark.executor.cores"])
    row = {"spill_side": "", "knob_owns_stage": "", "target_partitions": "", "per_task_after_mib": "",
           "exec_mem_per_task_mib": round(exec_mib), "driver_mem": p["spark.driver.memory"], "verdict": ""}
    if disk < DISK_FLOOR_GIB and mem < MEM_FLOOR_GIB:
        row["verdict"] = "no_spill"
        row["spill_side"] = "shuffle" if sread > 0 else ("input" if inp > 0 else "source")
        return row
    if sread == 0:
        row["spill_side"] = "input" if inp > 0 else "source"
        row["verdict"] = "1273" if inp > 0 else "code"
        return row
    row["spill_side"] = "shuffle"
    row["knob_owns_stage"] = "yes" if tasks == parts else ("aqe_coalesced" if tasks < parts else "code")
    if row["knob_owns_stage"] != "yes":
        row["verdict"] = "1274" if row["knob_owns_stage"] == "aqe_coalesced" else "code"
        return row
    bytes_target = round_up_100(math.ceil(sread * GIB / (256 * MIB)))
    mem_target = round_up_100(math.ceil(mem * GIB / (256 * MIB)))
    target = max(bytes_target, mem_target, parts)
    aqe_cap = int(sread * GIB / (AQE_MERGE_THRESHOLD_MIB * MIB))
    row["target_partitions"] = target
    row["per_task_after_mib"] = round(mem * 1024 / target)
    row["verdict"] = OWNED_ELSEWHERE.get(dag, "change")
    if target > aqe_cap:
        row["verdict"] += "+aqe_cap"
    if target > DRIVER_CAP and p["spark.driver.memory"] in ("", "9600m"):
        row["verdict"] += "+D3"
    return row


def main(out_dir):
    props = list(csv.DictReader(open(os.path.join(out_dir, "audi_1270_spark_props.csv"))))
    stages = list(csv.DictReader(open(os.path.join(out_dir, "audi_1270_stage_metrics.csv"))))
    out = []
    for dag, (model_file, site, constant) in DAGS.items():
        p = newest_log_per_dag(props)[dag]
        app_stages = [s for s in stages if s["app_id"] == p["app_id"]]
        spilling = [s for s in app_stages if float(s["disk_spill_gib"]) >= DISK_FLOOR_GIB or float(s["mem_spill_gib"]) >= MEM_FLOOR_GIB]
        chosen = spilling or [max(app_stages, key=lambda s: float(s["disk_spill_gib"]))]
        for st in chosen:
            out.append({"dag": dag, "model_file": model_file, "log_app_id": p["app_id"], "stage": st["stage_id"],
                        "stage_tasks": st["num_tasks"], "input_gib": st["input_gib"],
                        "shuffle_read_gib": st["shuffle_read_gib"], "shuffle_write_gib": st["shuffle_write_gib"],
                        "mem_spill_gib": st["mem_spill_gib"], "disk_spill_gib": st["disk_spill_gib"],
                        "effective_partitions": p["spark.sql.shuffle.partitions"], "config_site": site,
                        "aqe_coalesce": p["spark.sql.adaptive.coalescePartitions.enabled"] or "default(true)",
                        "repartition_constant": constant, **verdict_for(dag, p, st)})
    with open(os.path.join(out_dir, "audi_1270_verdict_table.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    for r in out:
        print(f"{r['dag']:<40} st{r['stage']:>3} tasks={r['stage_tasks']:>5} in={r['input_gib']:>6} shR={r['shuffle_read_gib']:>6} "
              f"shW={r['shuffle_write_gib']:>6} mem={r['mem_spill_gib']:>7} disk={r['disk_spill_gib']:>6} parts={r['effective_partitions']:>5} "
              f"{r['spill_side']:<7} own={r['knob_owns_stage']:<3} tgt={r['target_partitions']!s:>5} after={r['per_task_after_mib']!s:>4} "
              f"exec={r['exec_mem_per_task_mib']:>5} drv={r['driver_mem']:<6} {r['verdict']}")


if __name__ == "__main__":
    main(sys.argv[1])
