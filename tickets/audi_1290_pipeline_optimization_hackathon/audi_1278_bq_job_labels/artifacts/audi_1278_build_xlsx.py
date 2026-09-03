#!/usr/bin/env python3
"""Union the daily unlabeled-job fingerprints, map each query to its source, write the CSV and the branded xlsx.

    python3 tickets/audi_1290_pipeline_optimization_hackathon/audi_1278_bq_job_labels/artifacts/audi_1278_build_xlsx.py
"""

import glob
import json
import os
import re
import shutil
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook

TICKET_DIR = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1290_pipeline_optimization_hackathon/audi_1278_bq_job_labels"
OUT = f"{TICKET_DIR}/outputs"
QUERIES = f"{TICKET_DIR}/queries"
DRIVE_TICKETS = os.path.expanduser("~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/Tickets")
DRAFT = "DRAFT - NOT FINAL"
PERIOD = "2026-08-26 to 2026-09-01"
DAYS = 7

CAMPERBID_SA = "airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com"
TI_SA = "airflow-ti-prod@mntn-prj-prod-00.iam.gserviceaccount.com"
SUBMITTER = {CAMPERBID_SA: "camperbid Airflow", TI_SA: "airflow-ti"}
LABEL_REGEX = re.compile(r"^[\w-]{0,63}$")

CONNECTOR = "Spark-BigQuery connector (Dataproc batch)"
CLIENT = "python client inside the task"
NOT_FOUND = "not found by code search"

SOURCES = [
    (r"^SELECT DATE_TRUNC\(sp\.time, MINUTE\) AS created_at , sp\.camp", "spark_scripts/bos/campaign_flight_end_cost.py", "bos__spend", "tables.campaign_flight_end_cost.create", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT DATE_TRUNC\(sp\.time, MINUTE\) AS created_at , c\.campa", "spark_scripts/bos/campaign_group_flight_end_cost.py", "bos__spend", "tables.campaign_group_flight_end_cost.create", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT pmd\.private_marketplace_deal_id", "spark_scripts/bos/sum_by_private_marketplace_by_hour.py", "bos__spend", "tables.sum_by_private_marketplace_by_hour.create", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT cil\.campaign_id , DATE_TRUNC\(cil\.time, HOUR\)", "spark_scripts/bos/campaign_utc_yesterday_costs_impressions_by_hour.py", "bos__spend", "tables.campaign_utc_yesterday_costs_impressions_by_hour.create", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT campaign_id, SUM\(bids\) AS bids", "spark_scripts/win_rate_hourly/spark_pipeline.py", "win_rate_hourly", "", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT campaign_id, term_id, COUNT\(DISTINCT IF\(impress", "spark_scripts/win_rate_hourly/spark_pipeline.py", "win_rate_hourly", "", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT b\.campaign_id , b\.bucket_id", "spark_scripts/intent_score_threshold_v3/pipeline.py, _v4/pipeline.py", "intent_score_threshold_v3, intent_score_threshold_v4", "", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT f\.campaign_id , f\.campaign_group_id , f\.domain", "spark_scripts/media_plan_analytics/pipeline.py", "media_plan_analytics", "", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT date, hh, campaign_id, term_id, (impressions|bids) FROM `dw-main-silver\.aggregates", "spark_scripts/win_rate_bq/spark_pipeline.py", "win_rate_bq", "", "pacing + performance-ml", CONNECTOR),
    (r"perml\.network_performance_metrics", "spark_scripts/network_performance_metrics_sync/spark_coredb_sync.py", "network_performance_metrics_sync", "", "pacing + performance-ml", CONNECTOR),
    (r"AUTOTOF|perml\.campaign_(tightening_gates|signals|progressions|preset_decisions|gates|eligibility|frequency_presets)", "olympus autotof kedro pipeline (run via spark_scripts/autotof/run_kedro_command.py)", "autotof_morning_pass", "morning_pass", "performance-ml", "python client inside the kedro job"),
    (r"hhst_v#__campaign_bucket", NOT_FOUND, "", "", "pacing + performance-ml", CLIENT),
    (r"^SELECT `rpt_day`", NOT_FOUND + " (connector view read, column-list form)", "", "", "pacing + performance-ml", CONNECTOR),
    (r"^SELECT `advertiser_id`,`campaign_group_id`,`flight_id`", NOT_FOUND + " (connector view read, column-list form)", "", "", "pacing + performance-ml", CONNECTOR),
    (r"attr\.url_paths_#_#d_#` (WHERE|ORDER)", "dags/attribution/url_pattern_pipeline.py iter_destination_rows", "url_pattern_identification", "discover_and_load", "attribution (ours)", CLIENT),
    (r"^CREATE OR REPLACE TABLE `dw-main-silver\.attr\.url_paths", "dags/attribution/url_pattern_pipeline.py run_query_to_destination", "url_pattern_identification", "materialize_windows", "attribution (ours)", CLIENT),
    (r"attr\.dlv_events_#_#d_#", "dags/attribution/url_pattern_pipeline.py iter_destination_rows", "dlv_pattern_identification", "load_patterns", "attribution (ours)", CLIENT),
    (r"^CREATE TEMP FUNCTION parse_dlv", "dags/attribution/url_pattern_pipeline.py run_query_to_destination", "dlv_pattern_identification", "materialize_windows", "attribution (ours)", CLIENT),
    (r"core_advertisers_x_feat", "dags/attribution/tmobile_blocked_ip_workflow.py, tmobile_blocked_guids_workflow.py fetch_advertiser_ids", "tmobile_blocked_ip_export_dataproc, tmobile_blocked_guids_export_dataproc", "fetch_tmobile_advertisers_task", "attribution (ours)", CLIENT),
]

AFTER_FIX = [
    ("bos__spend", "tables.campaign_flight_end_cost.create", "camperbid hand-off"),
    ("bos__spend", "tables.campaign_group_flight_end_cost.create", "camperbid hand-off"),
    ("bos__spend", "tables.sum_by_private_marketplace_by_hour.create", "camperbid hand-off"),
    ("bos__spend", "tables.campaign_utc_yesterday_costs_impressions_by_hour.create", "camperbid hand-off"),
    ("bos__spend", "tables.campaign_performance.create", "camperbid hand-off"),
    ("bos__spend", "tables.campaign_group_performance.create", "camperbid hand-off"),
    ("url_pattern_identification", "materialize_windows", "airflow-ti PR"),
    ("url_pattern_identification", "discover_and_load", "airflow-ti PR"),
    ("dlv_pattern_identification", "materialize_windows", "airflow-ti PR"),
    ("dlv_pattern_identification", "load_patterns", "airflow-ti PR"),
    ("tmobile_blocked_ip_export_dataproc", "fetch_tmobile_advertisers_task", "airflow-ti PR"),
    ("tmobile_blocked_guids_export_dataproc", "fetch_tmobile_advertisers_task", "airflow-ti PR"),
    ("set_gaclid_enabled_flag", "fetch_aids", "airflow-ti PR"),
    ("blocked_ip_addresses_export", "fetch_icloud_ip_addresses", "airflow-ti PR"),
    ("blocked_guids_export", "fetch_icloud_guids", "airflow-ti PR"),
]


def classify(row: pd.Series) -> tuple[str, str, str, str, str]:
    if row["job_type"] == "LOAD":
        return NOT_FOUND, "", "", "pacing + performance-ml", "load job"
    head = "" if pd.isna(row["query_head"]) else str(row["query_head"])
    for pattern, source, dag, task, owner, path in SOURCES:
        if re.search(pattern, head):
            return source, dag, task, owner, path
    owner = "attribution (ours)" if row["user_email"] == TI_SA else "pacing + performance-ml"
    return NOT_FOUND, "", "", owner, "unknown"


def load_fingerprints() -> pd.DataFrame:
    frames = []
    for path in sorted(glob.glob(f"{OUT}/audi_1278_unlabeled_fingerprint_2026_*.csv")):
        day = pd.read_csv(path)
        day.insert(0, "date", os.path.basename(path)[-14:-4].replace("_", "-"))
        frames.append(day)
    df = pd.concat(frames, ignore_index=True)
    mapped = df.apply(classify, axis=1, result_type="expand")
    mapped.columns = ["source_file", "dag_id", "task_id", "owner", "submit_path"]
    return pd.concat([df, mapped], axis=1)


def ledger_dags() -> set[str]:
    with open(f"{OUT}/prod_optimization_ledger.jsonl") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    return {r["dag_id"] for r in rows if r.get("surface") == "bq"}


def report_unattributed() -> pd.DataFrame:
    rows = []
    for path in sorted(glob.glob(f"{OUT}/optimizer_bq_*.md")):
        for line in open(path):
            if line.startswith("|") and "`unattributed`" in line:
                cells = [c.strip().strip("`") for c in line.strip().strip("|").split("|")]
                rows.append({"date": path[-13:-3], "report_jobs": int(cells[2].replace(",", "")), "report_slot_h": float(cells[3].replace(",", ""))})
    return pd.DataFrame(rows)


def by_pipeline(df: pd.DataFrame, tracked: set[str]) -> pd.DataFrame:
    df = df.copy()
    df["pipeline"] = df["dag_id"].where(df["dag_id"] != "", "unknown")
    g = df.groupby(["user_email", "pipeline", "owner", "submit_path"], as_index=False)[["jobs", "slot_h", "tib_billed"]].sum()
    total_slot_h = g["slot_h"].sum()
    out = pd.DataFrame(
        {
            "Submitter": g["user_email"].map(SUBMITTER),
            "Pipeline (Airflow DAG)": g["pipeline"],
            "Owner": g["owner"],
            "How the job is submitted": g["submit_path"],
            "Jobs (7 days)": g["jobs"].astype(int),
            "Jobs per day": g["jobs"] / DAYS,
            "Slot-hours (7 days)": g["slot_h"],
            "Slot-hours per day": g["slot_h"] / DAYS,
            "Share of unlabeled slot-hours": g["slot_h"] / total_slot_h,
            "TiB billed (7 days)": g["tib_billed"],
            "Tracked by the optimizer today": g["pipeline"].map(lambda d: "Yes" if any(x.strip() in tracked for x in d.split(",")) else "No"),
        }
    )
    return out.sort_values(["Slot-hours (7 days)", "Jobs (7 days)"], ascending=False).reset_index(drop=True)


def fingerprints(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["query_head"] = df["query_head"].fillna("(load job, no query text)")
    df["statement_type"] = df["statement_type"].fillna("LOAD")
    g = df.groupby(["user_email", "statement_type", "query_head", "source_file", "dag_id", "task_id"], as_index=False)[["jobs", "slot_h", "tib_billed"]].sum()
    out = pd.DataFrame(
        {
            "Submitter": g["user_email"].map(SUBMITTER),
            "SQL statement": g["statement_type"],
            "Query start (first 90 characters, numbers masked)": g["query_head"],
            "Source file": g["source_file"],
            "Pipeline (Airflow DAG)": g["dag_id"].replace("", "unknown"),
            "Task": g["task_id"].replace("", "unknown"),
            "Jobs (7 days)": g["jobs"].astype(int),
            "Slot-hours (7 days)": g["slot_h"],
            "TiB billed (7 days)": g["tib_billed"],
        }
    )
    return out.sort_values(["Slot-hours (7 days)", "Jobs (7 days)"], ascending=False).reset_index(drop=True)


def daily(df: pd.DataFrame, report: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(["date", "user_email"], as_index=False)[["jobs", "slot_h", "tib_billed"]].sum()
    wide = g.pivot(index="date", columns="user_email", values=["jobs", "slot_h"]).fillna(0)
    out = pd.DataFrame(
        {
            "Date": wide.index,
            "camperbid jobs": wide[("jobs", CAMPERBID_SA)].astype(int).values,
            "camperbid slot-hours": wide[("slot_h", CAMPERBID_SA)].values,
            "airflow-ti jobs": wide[("jobs", TI_SA)].astype(int).values,
            "airflow-ti slot-hours": wide[("slot_h", TI_SA)].values,
        }
    )
    out["All unlabeled jobs"] = out["camperbid jobs"] + out["airflow-ti jobs"]
    out["All unlabeled slot-hours"] = out["camperbid slot-hours"] + out["airflow-ti slot-hours"]
    out = out.merge(report, left_on="Date", right_on="date", how="left").drop(columns=["date"])
    out = out.rename(columns={"report_jobs": "Daily optimizer report, unattributed jobs", "report_slot_h": "Daily optimizer report, unattributed slot-hours"})
    return out.sort_values("Date", ascending=False).reset_index(drop=True)


def after_fix() -> pd.DataFrame:
    rows = []
    for dag, task, lands_in in AFTER_FIX:
        dag_label = dag.lower()
        task_label = task.lower().replace(".", "-")
        rows.append(
            {
                "Pipeline (Airflow DAG)": dag,
                "Task": task,
                "airflow-dag label": dag_label,
                "airflow-task label": task_label,
                "Task label length (characters, limit 63)": len(task_label),
                "Label passes the 63-character rule": "Yes" if LABEL_REGEX.match(dag_label) and LABEL_REGEX.match(task_label) else "No",
                "Change lands in": lands_in,
            }
        )
    return pd.DataFrame(rows).sort_values("Task label length (characters, limit 63)", ascending=False).reset_index(drop=True)


def build() -> None:
    df = load_fingerprints()
    tracked = ledger_dags()
    report = report_unattributed()

    csv_cols = ["date", "user_email", "job_type", "statement_type", "query_head", "dest_dataset", "source_file", "dag_id", "task_id", "owner", "submit_path", "jobs", "slot_h", "tib_billed"]
    df[csv_cols].sort_values(["date", "slot_h"], ascending=[True, False]).to_csv(f"{OUT}/audi_1278_unlabeled_jobs_by_submitter.csv", index=False)

    pipeline_tab = by_pipeline(df, tracked)
    fingerprint_tab = fingerprints(df)
    daily_tab = daily(df, report)
    after_tab = after_fix()

    camperbid_h = df.loc[df.user_email == CAMPERBID_SA, "slot_h"].sum() / DAYS
    ti_h = df.loc[df.user_email == TI_SA, "slot_h"].sum() / DAYS
    ti_jobs = df.loc[df.user_email == TI_SA, "jobs"].sum() / DAYS
    bos_h = df.loc[df.dag_id == "bos__spend", "slot_h"].sum() / DAYS
    bos_share = bos_h / (camperbid_h + ti_h)

    wb = MntnWorkbook(
        title="Unlabeled BigQuery Jobs",
        ticket="AUDI-1278",
        subtitle="Who submits the fleet's unlabeled BigQuery jobs, and where the airflow-dag and airflow-task labels get added",
        period=f"{PERIOD} · {DRAFT}",
        generated="2026-09-03",
    )
    wb.table(
        "Unlabeled jobs by pipeline",
        pipeline_tab,
        finding=f"{bos_share:.0%} of unlabeled slot-hours are bos__spend Spark jobs reading BigQuery through the connector",
        method="Jobs with no airflow-dag label from the two fleet service accounts, dw-main-bronze job history, 7 days. Query text mapped to source by code search. See Read me.",
        formats={"Jobs (7 days)": FMT.INT, "Jobs per day": FMT.NUM1, "Slot-hours (7 days)": FMT.NUM1, "Slot-hours per day": FMT.NUM1, "Share of unlabeled slot-hours": FMT.PCT1, "TiB billed (7 days)": FMT.NUM2},
        heat={"Slot-hours (7 days)": "high"},
        kind="headline",
        widths={"Pipeline (Airflow DAG)": 34, "How the job is submitted": 36, "Owner": 24},
        toc="Unlabeled jobs and slot-hours by pipeline and owner",
        query="audi_1278_unlabeled_fingerprint_1d.sql",
    )
    wb.table(
        "Query fingerprints",
        fingerprint_tab,
        finding=f"{len(fingerprint_tab)} distinct query shapes; the four bos__spend reads carry {bos_h:,.0f} slot-hours a day",
        method="One row per submitter, statement and query start (first 90 characters, numbers masked). Source file from code search of airflow-camperbid and airflow-ti main.",
        formats={"Jobs (7 days)": FMT.INT, "Slot-hours (7 days)": FMT.NUM1, "TiB billed (7 days)": FMT.NUM2},
        heat={"Slot-hours (7 days)": "high"},
        widths={"Query start (first 90 characters, numbers masked)": 60, "Source file": 48, "Pipeline (Airflow DAG)": 30, "Task": 30},
        toc="Every unlabeled query shape mapped to its source file, pipeline and task",
        query="audi_1278_unlabeled_fingerprint_1d.sql",
    )
    wb.table(
        "Daily totals",
        daily_tab,
        finding=f"camperbid averages {camperbid_h:,.0f} unlabeled slot-hours a day, airflow-ti {ti_h:.1f} across {ti_jobs:.0f} jobs",
        method="Per day, both fleet service accounts. The last two columns are the unattributed row of the daily optimizer report for the same day, where one exists.",
        formats={"camperbid jobs": FMT.INT, "camperbid slot-hours": FMT.NUM1, "airflow-ti jobs": FMT.INT, "airflow-ti slot-hours": FMT.NUM1, "All unlabeled jobs": FMT.INT, "All unlabeled slot-hours": FMT.NUM1, "Daily optimizer report, unattributed jobs": FMT.INT, "Daily optimizer report, unattributed slot-hours": FMT.NUM1},
        toc="Day by day totals, reconciled to the daily optimizer report",
        query="audi_1278_unlabeled_fingerprint_1d.sql",
    )
    wb.table(
        "Labels after the fix",
        after_tab,
        finding="Every task label fits the 63-character limit; the longest bos__spend task renders at 62",
        method="The label pair each job will carry, using the same transform as the Airflow BigQuery operator: lower case, dots in the task id become dashes.",
        formats={"Task label length (characters, limit 63)": FMT.INT},
        widths={"Task": 44, "airflow-task label": 44, "Pipeline (Airflow DAG)": 34},
        toc="What the optimizer report will show once both changes land",
    )
    wb.glossary(
        "Read me",
        intro=f"{DRAFT}. How to read this workbook.",
        rows=[
            ("Unlabeled job", "A BigQuery job with no airflow-dag label. The daily optimizer report shows these as one unattributed row."),
            ("Fleet service account", "The two identities Airflow runs as: airflow-ti-prod and airflow-camperbid-prod. Only their jobs are in scope."),
            ("Slot-hours", "BigQuery compute consumed, from total_slot_ms in the job history divided by 3,600,000."),
            ("TiB billed", "Bytes billed for the job, in tebibytes."),
            ("Pipeline (Airflow DAG)", "The Airflow workflow the job belongs to, found by matching the query text to a source file."),
            ("Spark-BigQuery connector", "The library a Dataproc Spark job uses to read BigQuery. It runs the query job itself, so Airflow never labels it."),
            ("python client inside the task", "Code in an Airflow task calling google-cloud-bigquery or pandas-gbq directly. Labels must be passed by hand."),
            ("Tracked by the optimizer today", "Yes when the pipeline already has labeled tasks in the optimizer ledger, so the fix moves cost into an existing row."),
            ("Query start", "The first 90 characters of the SQL with digits replaced by # so daily runs group together."),
        ],
    )
    wb.sql_dir("Queries", QUERIES, note=f"{DRAFT}. BigQuery SQL run once per day with the date literals swapped; each day scans 4.86 GB.")
    wb.notes(
        "Method & caveats",
        intro=f"{DRAFT}. Method and caveats.",
        blocks=[
            ("Scope", "Jobs in dw-main-bronze job history from the two fleet service accounts with no airflow-dag or airflow-task label, 2026-08-26 to 2026-09-01. Everything else in the project (dagctl, cds-dpp, Mode, people) is out of scope."),
            ("Source mapping", "Query starts were matched to files on airflow-camperbid main 707d739 and airflow-ti main 825b07e by GitHub code search. Rows marked not found had no match; their daily cost is under 1 slot-hour."),
            ("Reconciliation", "The per-day totals match the unattributed row of the daily optimizer report for 2026-08-28 to 2026-09-01 to within rounding, so both read the same population."),
            ("Why bos__spend dominates", "Its four Spark jobs read spend logs through the connector every 15 minutes. The connector submits the query itself, so the operator's automatic labels never reach it."),
            ("What the fixes change", "Attribution only. Labels move cost from the unattributed row into the owning pipeline; total slot-hours do not change."),
            ("Measurement", "Before and after numbers come from the daily optimizer report in GCS, not the Mode dashboard, which reads only the findings ledger."),
        ],
    )
    wb.cover(
        takeaways=[
            f"{camperbid_h:,.0f} of {camperbid_h + ti_h:,.0f} unlabeled slot-hours a day are camperbid Spark jobs reading BigQuery through the connector; {bos_share:.0%} are bos__spend.",
            f"airflow-ti submits {ti_jobs:.0f} unlabeled jobs a day but only {ti_h:.1f} slot-hours; the airflow-ti PR labels all 8 python-client call sites.",
            "The camperbid fix is two Spark properties in dag_utils/google.py; the hand-off with the exact diff is drafted for the pacing and performance-ml owners.",
        ]
    )
    local = wb.save_local(f"{OUT}/audi_1278_unlabeled_bq_jobs.xlsx")
    print("wrote", local)

    if os.path.isdir(DRIVE_TICKETS):
        existing = sorted(d for d in os.listdir(DRIVE_TICKETS) if d == "AUDI-1278" or d.startswith("AUDI-1278 "))
        folder = os.path.join(DRIVE_TICKETS, existing[0] if existing else "AUDI-1278 BQ Job Labels")
        os.makedirs(folder, exist_ok=True)
        drive_path = os.path.join(folder, "AUDI-1278 Unlabeled BigQuery Jobs.xlsx")
        shutil.copyfile(local, drive_path)
        print("wrote", drive_path)

    print(pipeline_tab.to_string())
    print(daily_tab.to_string())
    print(after_tab.to_string())


if __name__ == "__main__":
    build()
