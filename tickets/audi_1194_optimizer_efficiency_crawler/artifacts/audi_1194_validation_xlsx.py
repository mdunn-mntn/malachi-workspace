"""Build the AUDI-1194 optimizer validation workbook."""
import json
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

SW = "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/1e1ddf5c-5fa7-4d0c-b846-f111b4c43a5b/scratchpad/fullsweep"
A = json.load(open(f"{SW}/analysis.json"))

CHECK = {
    "shuffle_fetch_wait": ("Tasks sit idle waiting for shuffle blocks to arrive", "working"),
    "disk_spill": ("Tasks spill to disk because memory ran out", "working"),
    "idle_reserved_executors": ("Executors are held and billed but run no tasks", "working"),
    "shuffle_partition_sizing": ("Shuffle partitions far off the 256 MiB target", "working"),
    "straggler": ("One task runs far longer than the median on even data", "working"),
    "skew": ("One task handles far more data than the median", "working"),
    "gc_pressure": ("Garbage collection eats a large share of task time",
                    "working, nothing to report"),
    "spot_preemption_cost": ("Reclaimed spot executors force work to be redone",
                             "working, nothing to report"),
    "shuffle_fetch_instability": ("Shuffle blocks fail to fetch and stages retry",
                                  "working, nothing to report"),
    "cache_ineffective": ("A cached dataset is dropped and has to be recomputed", "never exercised"),
    "missing_statistics": ("Table has no size estimate, so the planner guesses the join",
                           "cannot run on this input"),
    "broadcast_candidate": ("A join side small enough to broadcast is being shuffled",
                            "cannot run on this input"),
    "window_full_sort": ("A window function sorts the whole table", "cannot run on this input"),
    "repeated_scan": ("The same table is read more than once in one query",
                      "cannot run on this input"),
}
EVIDENCE = {
    "gc_pressure": "Garbage-collection time is recorded on 295 of 300 sampled runs; the highest "
                   "share of task time seen was 4.3%, and the check reports at 10%.",
    "spot_preemption_cost": "Executor removal reasons are recorded; none of 300 sampled runs "
                            "was reclaimed. The fleet runs Dataproc Serverless without spot.",
    "shuffle_fetch_instability": "Zero failed shuffle fetches across 300 sampled runs.",
    "cache_ineffective": "No run in the sample cached a dataset at all, so the check has never "
                         "had an opportunity to fire and is unproven either way.",
}
PLAN_WHY = ("Needs the table-size annotations Databricks writes into a query plan. Spark writes "
            "the plan text (4.7 MB across 295 sampled runs) but no size estimates, so no table "
            "node is readable and the check cannot run.")


def jobs_df(cap=45):
    rows = []
    for r in A["task_rows"][:cap]:
        rows.append({
            "Spark job": r["job"],
            "Airflow DAG": r["dag_id"] or "no DAG defines this name",
            "Runs": r["runs"],
            "Executor-hours": r["exec_h_total"],
            "Worst single run": r["exec_h_worst"],
            "Findings": r["findings"],
            "High impact": r["high"],
            "Worst finding": r["top_finding"] or "no finding",
        })
    return pd.DataFrame(rows)


def checks_df():
    rows = []
    for key, n in A["detector_counts"].items():
        plain, status = CHECK[key]
        ex = A["detector_examples"].get(key) or {}
        note = EVIDENCE.get(key, PLAN_WHY if status == "cannot run on this input" else "")
        rows.append({
            "What the check looks for": plain,
            "Times it reported": n,
            "Verdict": status,
            "Example it produced": ex.get("title", ""),
            "Fix it recommended": ex.get("fix", ""),
            "How we know": note,
        })
    rows.sort(key=lambda r: (-r["Times it reported"], r["What the check looks for"]))
    return pd.DataFrame(rows)


def gaps_df():
    dark = A["dags_dark"]
    rows = [
        {"Blind spot": "Spark jobs whose log is written to the Dataproc temp bucket, "
                       "not the archive the sweep reads",
         "How many": f"{dark} of 30 DAGs with a Spark task",
         "Can it be closed": "yes, already proven",
         "What closes it": "Enumerate Dataproc batches and read each one's per-run log path. "
                           "Verified on 14 batches; one of them is a DAG the sweep has never seen."},
        {"Blind spot": "Checks that need a query plan with table-size annotations",
         "How many": "4 of 14 checks",
         "Can it be closed": "yes, needs Databricks",
         "What closes it": "Run EXPLAIN COST on Databricks and feed the plan in. The read access "
                           "that blocked this was granted 2026-08-25."},
        {"Blind spot": "Airflow DAGs with no Spark task at all",
         "How many": "35 of 65 active DAGs",
         "Can it be closed": "no",
         "What closes it": "Nothing to read. They run on Databricks, Vertex, or no engine at all, "
                           "and are reported as out of scope on every sweep."},
        {"Blind spot": "Spark jobs whose name matches no Airflow task",
         "How many": f"{A['distinct_jobs'] - A['resolved_jobs']} of {A['distinct_jobs']} jobs",
         "Can it be closed": "no",
         "What closes it": "The job and its task are named differently in source. Their findings "
                           "still appear, without a link to the DAG page."},
        {"Blind spot": "Days of history available to read",
         "How many": "23 days, not 30",
         "Can it be closed": "no, only forward",
         "What closes it": "Archiving to the bucket began 2026-08-04. There is no expiry rule, "
                           "so the window grows by a day each day."},
    ]
    return pd.DataFrame(rows)


def unresolved_df():
    return pd.DataFrame([{"Spark job": n, "Why there is no DAG link": w}
                         for n, w in A["unresolved_detail"]])


def main():
    logs, jobs = A["logs_total"], A["distinct_jobs"]
    resolved, findings = A["resolved_jobs"], A["findings_total"]
    working = sum(1 for k in A["detector_counts"] if CHECK[k][1].startswith("working"))
    top10 = sum(r["exec_h_total"] for r in A["task_rows"][:10])
    wb = MntnWorkbook(
        title="Spark optimizer validation",
        ticket="AUDI-1194",
        subtitle="Every Spark job in the archive, what the tool found, and where it is still blind",
        period="2026-08-04 to 2026-08-26",
        generated="2026-08-26",
    )
    wb.table("Jobs by cost", jobs_df(),
             finding=f"{jobs} Spark jobs ran in the window and the 10 costliest hold "
                     f"{top10 / max(A['exec_h_total'], 1):.0%} of all executor-hours",
             method=f"All {logs:,} archived event logs parsed. Executor-hours = time executors "
                    "were held and billed, summed over runs. See Read me.",
             formats={"Executor-hours": FMT.NUM1, "Worst single run": FMT.NUM1},
             heat={"Executor-hours": "high"},
             kind="headline", toc="Every Spark job, ranked by the compute it holds")
    wb.table("Checks", checks_df(),
             finding=f"{working} of 14 checks ran correctly on real logs; 4 need a Databricks "
                     "query plan and 1 has never had a case to judge",
             method="One row per check, with the first result it produced and the fix it "
                    "recommended. Verdicts are from a 300-run sample of the same window.",
             heat={"Times it reported": "high"},
             toc="Every check, whether it ran, and what it produced")
    wb.table("Blind spots", gaps_df(),
             finding="One blind spot is worth closing and already proven: most Spark jobs write "
                     "their log where the sweep does not look",
             method="Each row states what is missed, how much of the fleet it is, and whether "
                    "anything can close it.",
             kind="headline", toc="What the tool still cannot see, and what would fix it")
    wb.table("Jobs with no DAG link", unresolved_df(),
             finding=f"{jobs - resolved} of {jobs} Spark jobs could not be matched to an Airflow "
                     "task, so their findings appear without a link",
             method="A job matches when one form of its name belongs to exactly one DAG. The "
                    "reason each of these did not is given per row.",
             kind="detail", toc="The jobs with no DAG link, one reason each")
    wb.glossary("Read me", intro="How to read this workbook.", rows=[
        ("Spark job", "The name Spark records for a run. It is the table the job populates."),
        ("Airflow DAG", "The scheduled workflow whose task submits that Spark job."),
        ("Executor-hours",
         "Time the run held its executors, billed whether or not a task was running."),
        ("Finding", "One improvement a check reported, with the measurement behind it."),
        ("High impact", "The check's own tier, set from a measured threshold, not judgement."),
        ("Event log", "The record Spark writes for each run. The only thing this tool reads."),
        ("Query plan", "Spark's description of how it will run a query. Needed by 4 checks."),
    ])
    wb.notes("Method and caveats", blocks=[
        ("Every archived log in the window was read, not a sample",
         f"All {logs:,} event logs from 2026-08-04 to 2026-08-26 were parsed and every check run "
         f"on each. {findings:,} findings across {jobs} jobs. Nothing was skipped or capped."),
        ("Three checks reported nothing because there was nothing to report",
         "Each was confirmed to be reading real data and finding the fleet healthy on that axis, "
         "not silently broken. The evidence is on the Checks tab."),
        ("Executor-hours, not dollars",
         "A committed-use discount means cut executor-hours may not cut the bill by the same "
         "share, so cost is stated in the unit measured rather than converted to money."),
        ("The DAG link is now derived, not guessed",
         "A job is linked only when one form of its name belongs to exactly one DAG. A name two "
         "DAGs share is left unlinked rather than sent to the wrong owner."),
    ])
    wb.cover(takeaways=[
        f"All {logs:,} archived event logs parsed, covering {jobs} Spark jobs and "
        f"{findings:,} findings over 23 days.",
        f"{working} of 14 checks ran correctly; 4 need a Databricks query plan and 1 has never "
        "had a case to judge.",
        f"The largest blind spot is closable today: {A['dags_dark']} of 30 Spark DAGs write their "
        "log where the sweep does not look.",
    ])
    print(wb.save_drive("AUDI-1194", "Spark Optimizer Validation"))


if __name__ == "__main__":
    main()
