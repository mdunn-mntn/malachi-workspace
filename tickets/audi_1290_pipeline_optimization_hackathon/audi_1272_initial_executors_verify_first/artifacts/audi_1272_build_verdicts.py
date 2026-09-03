"""Join the spread-check rows to the ten ticket DAGs and stamp the per-DAG verdict into outputs/audi_1272_verdicts.csv.

Usage: python3 audi_1272_build_verdicts.py <outputs_dir>
"""

from __future__ import annotations

import csv
import sys

APP_TO_DAG = {
    "Populate advertiser_mid.AdvertiserMid": "advertiser_mid",
    "Populate ipdsc_42_monitor.IPDSC42Monitor": "ipdsc_42_monitor",
    "Populate tpa_export_enrich.TpaExportEnrich": "tpa_export_enrich",
    "Populate tpa_mntn_id_export.TpaMntnIdExport": "tpa_mntn_id_export",
    "Populate ipdsc_ds_46.DS46": "ipdsc_ds_46",
    "Populate aug_log_ip_hourly.AugLogIpHourly": "aug_log_ip_hourly",
    "Populate vertical_size_monitor.VerticalSizeMonitor": "vertical_size_monitor",
    "Populate guid_log_derived_household_id_vertical_id.GuidLogDerivedHouseholdId": "guid_log_derived_household_id_vertical_id",
    "Populate site_visit_signal_derived_advertiser_id_dsc_id.SiteVisitSignalDerivedAdvertiserIdDscId": "site_visit_signal_derived_advertiser_id_dsc_id",
}
DS46_LOG_TO_DAG = {
    "app-20260901212248899-0756": "fangorn_14day_lookback",
    "app-20260826000057091-0058": "fangorn_14day_lookback",
    "app-20260901213302742-0181": "fangorn_household_14day_lookback",
    "app-20260826003036565-0444": "fangorn_household_14day_lookback",
}
VERDICT = {
    "advertiser_mid": ("confirmed, server count: map stages 3 and 15 run on the 25 starting executors, the run reaches 90", 90),
    "ipdsc_42_monitor": ("confirmed, concentration: the 20-task map stage runs on 2 to 3 executors, hottest holds 40 to 42%", 7),
    "tpa_export_enrich": ("not confirmed, spread: map output on all 120 executors, hottest 1.1%", None),
    "tpa_mntn_id_export": ("not confirmed, spread: map output on all 150 executors, hottest 1 to 4%", None),
    "fangorn_14day_lookback": ("not confirmed, spread: map output on all 180 executors, hottest 0.7 to 0.9% (owner change, not this PR)", None),
    "fangorn_household_14day_lookback": ("not confirmed, split verdict and cost rule: 3.4 exec-h boot against 1.8 exec-h wait on 09-01 (owner change, not this PR)", None),
    "ipdsc_ds_46": ("not confirmed, cost rule: 0.43 exec-h boot of 77 to 95 extra executors against 0.14 to 0.16 exec-h wait", None),
    "aug_log_ip_hourly": ("not confirmed: initial 100 already equals the peak; fleet trimmed to 50 at 315 s, map at 2482 s", None),
    "vertical_size_monitor": ("not confirmed, split verdict (08-26 no fetch-wait stage) and cost rule: 0.33 exec-h boot against 0.12 exec-h wait", None),
    "guid_log_derived_household_id_vertical_id": ("not confirmed: no stage over the 20% fetch-wait floor in either run; 09-03 stage 50 at 15.5% is spread on 20 of 20", None),
    "site_visit_signal_derived_advertiser_id_dsc_id": ("not confirmed, spread: map output on all 100 executors, initial 100 already equals the peak", None),
}
STAGE_NOTE = {("advertiser_mid", "24"): "spread: stage 19 output on 75 to 90 executors, hottest 1.9 to 2.0%, wait stays"}
QUIET_LOGS = [
    ("guid_log_derived_household_id_vertical_id", "app-20260902011537309-0445", "10", "20", "10", "20"),
    ("vertical_size_monitor", "app-20260826010837114-0307", "2", "1000", "2", "87"),
    ("site_visit_signal_derived_advertiser_id_dsc_id", "app-20260902014754984-0379", "100", "200", "100", "100"),
]
FIELDS = [
    "dag_id", "app_id", "stage", "fetch_wait_pct", "fetch_wait_exec_h", "feeding_stage", "map_output_gib",
    "executors_holding_output", "executors_holding_90pct", "hottest_share_pct", "executors_live_at_map_start",
    "executors_registered_peak", "current_initial", "max", "first_removal_s", "extra_idle_exec_h", "spread_class",
    "verdict", "target_initial",
]


def dag_of(row: dict) -> str:
    return DS46_LOG_TO_DAG.get(row["log"].replace(".zstd", ""), APP_TO_DAG.get(row["app_name"], row["app_name"]))


def convert(row: dict) -> dict:
    dag = dag_of(row)
    verdict, target = VERDICT[dag]
    note = STAGE_NOTE.get((dag, row["stage"]))
    return {
        "dag_id": dag,
        "app_id": row["log"].replace(".zstd", ""),
        "stage": row["stage"],
        "fetch_wait_pct": row["fetch_wait_pct"],
        "fetch_wait_exec_h": row["fetch_wait_exec_h"],
        "feeding_stage": row["feeding_stage"],
        "map_output_gib": row["map_output_gib"],
        "executors_holding_output": row["executors_with_output"],
        "executors_holding_90pct": row["executors_holding_90pct"],
        "hottest_share_pct": row["hottest_share_pct"],
        "executors_live_at_map_start": row["map_live_at_first_task"],
        "executors_registered_peak": row["peak_registered"],
        "current_initial": row["start_count"],
        "max": row["dyn_max"] or "1000 (serverless default)",
        "first_removal_s": row["first_removal_s"] or "none",
        "extra_idle_exec_h": row["extra_idle_exec_h"],
        "spread_class": row["spread_class"],
        "verdict": note or verdict,
        "target_initial": target if target else "no change",
    }


def main(out_dir: str) -> None:
    rows = [convert(r) for r in csv.DictReader(open(f"{out_dir}/audi_1272_spread_all.csv"))]
    seen = {(r["app_id"], r["stage"]) for r in rows}
    for r in csv.DictReader(open(f"{out_dir}/audi_1272_spread_lowfloor.csv")):
        c = convert(r)
        if (c["app_id"], c["stage"]) not in seen:
            c["verdict"] = "below the 20% fetch-wait floor; " + c["verdict"]
            rows.append(c)
    for dag, app, cur, cap, live, peak in QUIET_LOGS:
        rows.append({**{k: "" for k in FIELDS}, "dag_id": dag, "app_id": app, "stage": "none over 10% wait and 60 s run time",
                     "current_initial": cur, "max": cap, "executors_live_at_map_start": live, "executors_registered_peak": peak,
                     "verdict": VERDICT[dag][0], "target_initial": VERDICT[dag][1] or "no change"})
    order = list(VERDICT)
    rows.sort(key=lambda r: (order.index(r["dag_id"]), r["app_id"], int(r["stage"]) if r["stage"].isdigit() else 999))
    with open(f"{out_dir}/audi_1272_verdicts.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    with open(f"{out_dir}/audi_1272_verdicts.md", "w") as f:
        f.write("# AUDI-1272 verdict table (same rows as audi_1272_verdicts.csv, which .gitignore excludes)\n\n")
        f.write("| " + " | ".join(FIELDS) + " |\n|" + "---|" * len(FIELDS) + "\n")
        for r in rows:
            f.write("| " + " | ".join(str(r[k]) for k in FIELDS) + " |\n")
    for r in rows:
        print(f"{r['dag_id']:48} {r['app_id'][-4:]} s{r['stage']:<4} wait {r['fetch_wait_pct']:>5}% {r['fetch_wait_exec_h']:>7} exec-h "
              f"| out on {r['executors_holding_output']:>3} (90% on {r['executors_holding_90pct']:>3}, hottest {r['hottest_share_pct']:>5}%) "
              f"| live {r['executors_live_at_map_start']:>3} peak {r['executors_registered_peak']:>3} | {r['target_initial']}")


if __name__ == "__main__":
    main(sys.argv[1])
