#!/usr/bin/env python3
"""AUDI-1194 optimizer explainer + proof deliverable.

Builds the branded multi-sheet .xlsx for the SUCCESS-SWEEP optimizer (O1-O4): how it works,
the detector catalog, and real prod findings (the 242x skew etc.). The failure DEBUGGER has
its own workbook under AUDI-1191.

Regenerate: python3 tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_how_it_works_xlsx.py
"""

import os
import sys

import pandas as pd
from openpyxl.styles import Font
from openpyxl.worksheet.hyperlink import Hyperlink

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from lib.mntn_xlsx import BRAND, FMT, MntnWorkbook

GEN = "2026-08-06"
GH = "https://github.com/mdunn-mntn/malachi-workspace/blob/main/"

wb = MntnWorkbook(
    title="Airflow/Spark Efficiency Optimizer",
    ticket="AUDI-1194",
    subtitle="Reads jobs that SUCCEEDED and finds waste: skew, spill, GC pressure, spot churn, missing stats. Ranks a fleet backlog worst-first.",
    period="Built Aug 2026",
    generated=GEN,
    status="Building",
)

# ---------------------------------------------------------------- 1. How it works (step map)
STEPS = [
    # (step, name, what/how, code display, code repo-path#anchor, test command, proven)
    ("O1", "Get the job's Spark data",
     "Runs on SUCCEEDED jobs. Dataproc: the .zstd Spark event log from gs://mntn-data-archive-{env}/spark-events "
     "(fleet-enabled by PR #1169) or the per-batch history-server folders for ipdsc/tpa (temp bucket, needs the "
     "standing read grant). Databricks: the EXPLAIN COST plan + Spark metrics from jobs get-run-output.",
     "oncall_weekly_optimizer.sh", ".claude/scripts/oncall_weekly_optimizer.sh",
     "bash .claude/scripts/oncall_weekly_optimizer.sh",
     "Agent-tested 2026-08-06: 360 objects read, parsed clean. Gap: eventlog_v2 rolling parts (IMP-029)"),
    ("O2", "Parse 7 surfaces",
     "Full Spark event-log parse into structured metrics: jobs, stages, tasks, executors, environment, SQL node "
     "metrics, storage.",
     "eventlog.py:147", "airflow_optimizer/eventlog.py#L147",
     "python3 -m airflow_optimizer.tests.test_eventlog",
     "Agent-tested 2026-08-06: 4 jobs/9 stages/34 tasks/3 SQL parsed from fixture"),
    ("O3", "Detect waste",
     "Plan detectors (missing stats, broadcast candidate, shuffle sizing, window full-sort, repeated scan) + run "
     "detectors (skew, spill, GC pressure, spot-preemption cost, fetch instability), each with real numbers and a "
     "concrete fix. See the Detector catalog tab.",
     "optimizations.py:97", "airflow_optimizer/optimizations.py#L97",
     "python3 -m airflow_optimizer.tests.test_optimizations",
     "Found the 242x prod skew; agent re-ran 2026-08-06 (12.2x fixture skew)"),
    ("O4", "Rank the fleet backlog",
     "Crawls every event log, ranks jobs worst-first, groups each finding CODE / INFRA / FAILURE so the owner "
     "knows the kind of fix. This is the 'check every DAG automatically' mode.",
     "crawl.py:54", "airflow_optimizer/crawl.py#L54",
     "python3 -m airflow_optimizer.crawl <dir-or-glob>",
     "2026-08-04 prod crawl (13 jobs, 34 findings); agent re-ran 2026-08-06"),
]
steps_df = pd.DataFrame(
    [{"Step": s, "Name": n, "What it does and how": w, "Code": d, "Test it": t, "Proven": p}
     for (s, n, w, d, _, t, p) in STEPS]
)
ws_steps = wb.table(
    "How it works",
    steps_df,
    finding="Four small steps: acquire, parse, detect, rank — all plain code, no LLM anywhere",
    method="Sweeps jobs that SUCCEEDED (the failure debugger is AUDI-1191, its own workbook). Code links to the exact "
           "source line on GitHub (mdunn-mntn/malachi-workspace). Test it = the command that exercises just that step.",
    kind="headline",
    widths={"Step": 6, "Name": 22, "What it does and how": 64, "Code": 24, "Test it": 44, "Proven": 26},
    toc="The step map — every chunk, how it's done, the code link, and how to test it",
)
for i, (_, _, _, disp, repo_path, _, _) in enumerate(STEPS, 1):  # data rows start at 5
    c = ws_steps.cell(row=4 + i, column=list(steps_df.columns).index("Code") + 1)
    c.hyperlink = Hyperlink(ref=c.coordinate, target=GH + repo_path, display=disp)
    c.font = Font(name=c.font.name, size=10, color=BRAND["LINK"], underline="single")

# ---------------------------------------------------------------- 2. Real findings
finds = pd.DataFrame([
    {"Job": "Update Vertical Categorization", "Engine": "Dataproc", "Group": "CODE",
     "Finding (real numbers)": "Stage 0 skewed up to 242x (max vs median task) on one run and 10-20x on every other run checked; GC pressure alongside. One partition holds nearly all the data.",
     "Fix": "Salt the skewed group/join key or enable AQE skew join. A plain repartition will not fix a value-skewed key.",
     "Status": "Flagged to the model owner (IMP-024)"},
    {"Job": "Prepare HTML Content", "Engine": "Dataproc", "Group": "CODE",
     "Finding (real numbers)": "Stage skew 18.4x (max vs median task).",
     "Fix": "Same class: salt the key or AQE skew join.",
     "Status": "In the crawl backlog"},
    {"Job": "keyword_ddp_reporting / targeted_signal", "Engine": "Databricks", "Group": "CODE",
     "Finding (real numbers)": "Missing table stats on product_categorization (13.5B rows scanned) so the optimizer defaults to full sorts; shuffles of 768 / 72 / 182 GiB at the default partition count.",
     "Fix": "ANALYZE TABLE COMPUTE STATISTICS; set spark.sql.shuffle.partitions (~256 MiB per partition) or enable AQE coalesce.",
     "Status": "Demoed from the INC-009 job's plan + metrics"},
    {"Job": "keyword_ddp_reporting / targeted_signal", "Engine": "Databricks", "Group": "INFRA",
     "Finding (real numbers)": "161 task re-runs from 7 spot-reclaimed executors; 168 FetchFailed tasks (shuffle instability).",
     "Fix": "Raise first_on_demand or add on-demand fallback; reduce shuffle block size.",
     "Status": "Demoed from the INC-009 job's metrics"},
    {"Job": "6 of 13 prod jobs", "Engine": "Dataproc", "Group": "—",
     "Finding (real numbers)": "Clean: no findings above thresholds.",
     "Fix": "None needed.",
     "Status": "2026-08-04 prod crawl"},
])
wb.table(
    "Real findings",
    finds,
    finding="The first prod crawl found a 242x skew nobody was looking for — on jobs that all show green",
    method="From the 2026-08-04 crawl of 13 real prod event logs (13 jobs, 34 findings, 10 high-impact) plus the Databricks demo on the INC-009 job. Every number is from a real run.",
    kind="headline",
    widths={"Job": 30, "Engine": 11, "Group": 9, "Finding (real numbers)": 52, "Fix": 42, "Status": 26},
    toc="What it has already found on real prod jobs",
)

# ---------------------------------------------------------------- 3. Detector catalog
dets = pd.DataFrame([
    {"Detector": "skew", "Reads": "event log (run)", "Flags": "One task far slower than its stage's median (max-vs-median ratio, per stage).", "Typical fix": "Salt the skewed key / AQE skew join."},
    {"Detector": "disk spill", "Reads": "event log (run)", "Flags": "Stages writing shuffle data to disk because memory ran out.", "Typical fix": "More partitions, more memory, or cache less."},
    {"Detector": "GC pressure", "Reads": "event log (run)", "Flags": "High share of task time spent in garbage collection (memory-starved).", "Typical fix": "Raise executor memory / fewer, larger partitions."},
    {"Detector": "spot preemption cost", "Reads": "event log (run)", "Flags": "Task re-runs caused by reclaimed spot executors.", "Typical fix": "first_on_demand / on-demand fallback."},
    {"Detector": "shuffle fetch instability", "Reads": "event log (run)", "Flags": "FetchFailed tasks (executors losing shuffle blocks).", "Typical fix": "Reduce shuffle block size; steadier nodes."},
    {"Detector": "missing statistics", "Reads": "plan text", "Flags": "Tables scanned without stats, so the optimizer guesses sizes and picks bad joins/sorts.", "Typical fix": "ANALYZE TABLE COMPUTE STATISTICS."},
    {"Detector": "shuffle partition sizing", "Reads": "plan text", "Flags": "Very large shuffles at a default/low partition count (huge partitions).", "Typical fix": "Set shuffle partitions (~256 MiB each) / AQE coalesce."},
    {"Detector": "broadcast candidate", "Reads": "plan text", "Flags": "A small table joined the expensive way when it could be broadcast.", "Typical fix": "Broadcast hint / raise the broadcast threshold."},
    {"Detector": "window full-sort", "Reads": "plan text", "Flags": "A window function forcing a full sort of the data.", "Typical fix": "Partition the window; pre-bucket the data."},
    {"Detector": "repeated scan", "Reads": "plan text", "Flags": "The same source read multiple times in one job.", "Typical fix": "Cache the reused frame."},
])
wb.table(
    "Detector catalog",
    dets,
    finding="Ten detectors: five read how the job RAN, five read what the plan INTENDED",
    method="Hand-maintained list matching airflow_optimizer/optimizations.py (analyze_run + analyze_plan). Each finding carries the real measured numbers and a concrete fix.",
    kind="data",
    widths={"Detector": 22, "Reads": 16, "Flags": 52, "Typical fix": 40},
    toc="What each detector looks for and the fix it recommends",
)

# ---------------------------------------------------------------- 4. Worked example: Dataproc
wb.notes(
    "Ex — Dataproc",
    intro="Reading a Spark event log to find a concrete speed-up (the Update Vertical Categorization job).",
    blocks=[
        ("Input", "A finished job's Spark event log (.zstd) from the archive bucket. No failure needed — this runs on healthy jobs to find waste."),
        ("Parse (7 surfaces)", "Parses jobs, stages, tasks, executors, environment, SQL per-node metrics, and storage from the event log into structured metrics."),
        ("Detect", "The skew detector compares max-vs-median task time per stage. Stage 0's slowest task ran 242x the median: one partition held nearly all the data, with GC pressure alongside."),
        ("Recommend (actual output)",
         "[high] Stage 0 skewed 242.1x (max vs median task). Why: one partition holds most of the data. Fix: salt the skewed group/join key or enable AQE skew join (spark.sql.adaptive.skewJoin.enabled); a plain repartition will not fix a value-skewed key."),
        ("Crawl (check every DAG)", "The same run across a directory of event logs ranks a cross-job backlog worst-first. The 2026-08-04 prod crawl scanned 13 jobs and surfaced 34 findings, 10 high-impact, led by this 242x skew."),
        ("Outcome", "Flagged to the model owner as a concrete wall-clock and cost win. First real optimization target found autonomously by the tool."),
    ],
)

# ---------------------------------------------------------------- 5. Worked example: Databricks
wb.notes(
    "Ex — Databricks",
    intro="The same detectors on Databricks, on a real job (targeted_signal in keyword_ddp_reporting). Databricks runs ~66 dbt models plus a handful of PySpark jobs.",
    blocks=[
        ("Input", "A Databricks job's EXPLAIN COST plan (from jobs get-run-output) plus its Spark job metrics (stage shuffle sizes, task failures, executor events). No GCS event log needed."),
        ("Parse", "The plan gives per-node operators and optimizer statistics; the metrics give stage shuffle sizes, failed tasks, and executor removals."),
        ("CODE fixes (actual output)",
         "Missing table stats on product_categorization (13.5B rows scanned) so the optimizer defaults to full sorts, fix ANALYZE TABLE COMPUTE STATISTICS. Wide shuffles 768/72/182 GiB at the default partition count, fix set spark.sql.shuffle.partitions (~256 MiB each) or enable AQE coalesce."),
        ("INFRA / FAILURE fixes (actual output)",
         "161 task re-runs from 7 spot-reclaimed executors, fix raise first_on_demand or add on-demand fallback. 168 FetchFailed tasks (shuffle instability), route as infra and reduce shuffle block size."),
        ("Grouping", "Each finding is tagged CODE (a query/config PR), INFRA (a cluster change), or FAILURE (route it) so the owner knows the kind of fix."),
        ("Outcome", "Same detectors as Dataproc, different acquisition (the plan plus Spark metrics instead of the GCS event log). This is what proves it works on both engines."),
    ],
)

# ---------------------------------------------------------------- 6. Status + next
wb.notes(
    "Status + next",
    intro="Where the build stands (2026-08-06) and what remains before it runs fully unattended.",
    blocks=[
        ("Working today", "All four steps run and are agent-tested. The weekly sweep (oncall_weekly_optimizer.sh) pulls the newest archive event logs and ranks the backlog. Fleet event logs flow since the 2026-08-04 eventLog change (PR #1169)."),
        ("1. Standing read grant", "The ipdsc/tpa jobs keep their history-server logs in the Dataproc temp bucket, which needs a standing object-viewer grant (request to devops). Today's workaround is a 1-hour self-service PAM grant, which cannot serve an unattended cron."),
        ("2. History-server crawl", "Those logs sit in per-batch folders (thousands, most empty), so the crawler must enumerate batches via the Dataproc API first, then fetch each batch's folder — not a flat scan."),
        ("3. Databricks live pull", "The EXPLAIN COST path is demoed from captured output; wire the live jobs get-run-output pull into the sweep."),
        ("4. Cadence", "Measure one full sweep's cost, then schedule: daily if cheap, weekly if expensive."),
        ("5. Rolling logs", "Newer jobs write multi-part rolling event logs (eventlog_v2 folders); the crawler reads single files today (IMP-029)."),
    ],
)

# ---------------------------------------------------------------- 7. Read me / glossary
wb.glossary(
    "Read me",
    intro="What this tool is, and the terms used across the tabs.",
    rows=[
        ("What it is", "An efficiency sweep for Airflow/Spark jobs that SUCCEEDED. It reads each finished job's Spark data, finds waste (skew, spill, GC pressure, spot churn, missing stats), and ranks a fleet backlog worst-first."),
        ("Why it exists", "Green jobs still burn money. Nobody has time to open every job's Spark UI looking for waste; this checks every job automatically and points at the worst first."),
        ("The other half", "The failure DEBUGGER (fires on failed tasks, returns a root-cause report) is a separate tool and workbook: AUDI-1191, in the same Drive folder set."),
        ("", ""),
        ("Event log", "A file Spark writes as a job runs, recording 7 layers: jobs, stages, tasks, executors, environment, SQL node metrics, storage. The optimizer reads all 7."),
        ("Skew", "One partition holds far more data than the others, so one task runs much longer than the rest. Measured as max-vs-median task time per stage."),
        ("Spill", "A stage ran out of memory and wrote shuffle data to disk, which is much slower."),
        ("AQE", "Adaptive Query Execution: Spark features that fix skew and partition sizing at runtime when enabled."),
        ("EXPLAIN COST", "A Databricks plan dump that includes the optimizer's size estimates; how the tool reads Databricks jobs without a GCS event log."),
        ("CODE / INFRA / FAILURE", "Each finding's fix type: a code or config change, a cluster change, or something to route to the owning team."),
        ("Code links", "The Code column on 'How it works' links to the exact source line on GitHub (mdunn-mntn/malachi-workspace). Repo access required; the file:line shown still works as the reference."),
    ],
)

# ---------------------------------------------------------------- Cover (LAST)
wb.cover(
    takeaways=[
        "Checks every Spark job that ran GREEN for hidden waste, on both Dataproc and Databricks, and ranks a fleet backlog worst-first.",
        "First prod crawl found a real 242x skew (plus missing stats and spot churn on Databricks) — concrete cost and wall-clock wins on jobs nobody was looking at.",
        "All plain code, key-free, agent-tested step by step; remaining work is access + scheduling, not capability.",
    ]
)

scratch = os.environ.get("CLAUDE_SCRATCH", "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad")
local = wb.save_local(os.path.join(scratch, "audi_1194_how_it_works.xlsx"))
print("saved local :", local)
try:
    drive = wb.save_drive("AUDI-1194", "Optimizer How It Works")
    print("saved drive :", drive)
except Exception as e:
    print("drive save skipped:", e)

from openpyxl import load_workbook  # noqa: E402
print("tabs        :", load_workbook(local).sheetnames)
