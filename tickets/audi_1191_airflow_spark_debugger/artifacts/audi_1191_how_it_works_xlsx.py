#!/usr/bin/env python3
"""AUDI-1191 explainer + proof deliverable.

Builds the branded multi-sheet .xlsx that (1) explains how the automated Airflow/Spark
failure-debugger + optimizer works step by step, and (2) proves it on real DAGs across
every use case (Dataproc RCA, Databricks RCA, the optimizer). The signature-taxonomy sheet
is built live from airflow_debugger.signatures so it never drifts.

Regenerate: python3 tickets/audi_1191_airflow_spark_debugger/artifacts/audi_1191_how_it_works_xlsx.py
"""

import os
import sys

import pandas as pd
from openpyxl.styles import Font
from openpyxl.worksheet.hyperlink import Hyperlink

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from airflow_debugger import signatures as sig
from lib.mntn_xlsx import BRAND, FMT, MntnWorkbook

GEN = "2026-08-06"
GH = "https://github.com/mdunn-mntn/malachi-workspace/blob/main/"

wb = MntnWorkbook(
    title="Airflow/Spark Failure-Debugger + Optimizer",
    ticket="AUDI-1191",
    subtitle="How it works, and proof on real DAGs across every use case (Dataproc RCA, Databricks RCA, optimization).",
    period="Validated Jul-Aug 2026",
    generated=GEN,
    status="Working",
)

# ---------------------------------------------------------------- 1. How it works (step map)
# One row per testable chunk. D = debugger (fires on failures), O = optimizer (sweeps successes, AUDI-1194).
STEPS = [
    # (step, name, what/how, code display, code repo-path#anchor, test command, proven)
    ("D1", "Detect failed tasks",
     "Queries the Airflow REST API on Astronomer directly (no Slack): POST /dags/~/dagRuns/~/taskInstances/list, "
     "windowed on the UTC day, state=failed. Auth is the short-lived astro SSO login. The Slack alert is only how "
     "a human notices; the tool finds the same failure itself from the API.",
     "airflow_api.py:203", ".claude/scripts/airflow_api.py#L203",
     "bash .claude/scripts/airflow_pull.sh --date <D> --state failed",
     "Live 2026-08-06 (5 failed tasks). Gap found: a failed try mid-retry needs watch-mode, not the day-dump"),
    ("D2", "Download the log",
     "For each failed try: GET the task-instance log (structured JSON/NDJSON), render to plain text, save as "
     "<time>__<dag>__<task>__try<N>__<state>.log plus a _manifest.jsonl row (the pass/fail grid).",
     "airflow_api.py:294", ".claude/scripts/airflow_api.py#L294",
     "same command; logs land in on-call/airflow_logs/<date>/",
     "INC-010 + INC-011 pulls"),
    ("D3", "Parse + route",
     "Reads the log for identity (dag, task, run, logical date), routes by operator classpath to the engine "
     "(Dataproc vs Databricks), and extracts the downstream Spark job id (batch id / run id).",
     "parse.py:51", "airflow_debugger/parse.py#L51",
     "python3 -m airflow_debugger.tests.test_parse",
     "Unit tests green"),
    ("D4", "Engine RCA",
     "Dataproc: batches describe + Cloud Logging traceback + structural TTL check (dataproc_rca.py:156). "
     "Databricks: jobs get-run + get-run-output on the TASK run id + cluster events (databricks_rca.py:74). "
     "Turns the boilerplate Airflow error into the real underlying cause.",
     "dataproc_rca.py:156", "airflow_debugger/dataproc_rca.py#L156",
     "python3 -m airflow_debugger.dataproc_rca <batch_id>",
     "INC-005 (TTL) · INC-009 (pod-evict) · INC-012 (ruled out TTL in one call)"),
    ("D5", "Signature match",
     "25 regex fingerprints classify the failure; a high-confidence match returns a cached verdict with NO LLM "
     "call. Each signature's programmatic-fix flag separates a fixable root cause from a downstream symptom.",
     "signatures.py:28", "airflow_debugger/signatures.py#L28",
     "python3 -m airflow_debugger.tests.test_signatures",
     "19 cases incl INC-011 skip-vs-fail + INC-012 GCS list timeout"),
    ("D6", "Past-incident match",
     "Lexical matcher over the local incident corpus (on-call/incident_log.jsonl) attaches the most similar past "
     "incidents to the verdict, so a repeat is recognized instantly.",
     "incident_match.py:34", "airflow_debugger/incident_match.py#L34",
     "python3 -m airflow_debugger.tests.test_incident_match",
     "Surfaces the INC-009 twin"),
    ("D7", "Report",
     "BLUF/STAR report under 500 characters: root cause + confidence + affected file:line where known + whether a "
     "code fix is possible + a deep link to the batch/run.",
     "report.py:50", "airflow_debugger/report.py#L50",
     "python3 -m airflow_debugger.report <log file>",
     "INC-005 / 009 / 010 / 011 / 012"),
    ("D8", "LLM fallback",
     "ONLY when no signature matched: one bounded LLM synthesis call over the distilled evidence (synth.py:29). "
     "Everything before this is plain deterministic code, so known failures cost nothing and are instant.",
     "orchestrate.py:16", "airflow_debugger/orchestrate.py#L16",
     "python3 -m airflow_debugger.orchestrate <log>  (--no-llm to force it off)",
     "Unknown-signature path only"),
    ("O1", "Get the job's Spark data",
     "Runs on SUCCEEDED jobs. Dataproc: the .zstd Spark event log from gs://mntn-data-archive-{env}/spark-events "
     "(fleet-enabled by PR #1169) or the PHS per-batch dirs (ipdsc/tpa). Databricks: the EXPLAIN COST plan + Spark "
     "metrics from jobs get-run-output.",
     "oncall_weekly_optimizer.sh", ".claude/scripts/oncall_weekly_optimizer.sh",
     "bash .claude/scripts/oncall_weekly_optimizer.sh",
     "Prod event logs flowing since 2026-08-04"),
    ("O2", "Parse 7 surfaces",
     "Full Spark event-log parse into structured metrics: jobs, stages, tasks, executors, environment, SQL node "
     "metrics, storage.",
     "eventlog.py:147", "airflow_optimizer/eventlog.py#L147",
     "python3 -m airflow_optimizer.tests.test_eventlog",
     "Real prod event-log fixtures"),
    ("O3", "Detect waste",
     "Plan detectors (missing stats, broadcast candidate, shuffle sizing, window full-sort, repeated scan) + run "
     "detectors (skew, spill, GC pressure, spot-preemption cost, fetch instability), each with real numbers and a "
     "concrete fix.",
     "optimizations.py:97", "airflow_optimizer/optimizations.py#L97",
     "python3 -m airflow_optimizer.tests.test_optimizations",
     "Found the 242x prod skew"),
    ("O4", "Rank the fleet backlog",
     "Crawls every event log, ranks jobs worst-first, groups each finding CODE / INFRA / FAILURE so the owner "
     "knows the kind of fix.",
     "crawl.py:54", "airflow_optimizer/crawl.py#L54",
     "python3 -m airflow_optimizer.crawl <dir-or-glob>",
     "2026-08-04 prod crawl: 13 jobs, 34 findings"),
]
steps_df = pd.DataFrame(
    [{"Step": s, "Name": n, "What it does and how": w, "Code": d, "Test it": t, "Proven": p}
     for (s, n, w, d, _, t, p) in STEPS]
)
ws_steps = wb.table(
    "How it works",
    steps_df,
    finding="Every step is a small, separately testable chunk of plain code",
    method="D = debugger (fires on failures) · O = optimizer (sweeps successes, AUDI-1194). Code links to the exact "
           "source line on GitHub (mdunn-mntn/malachi-workspace). Test it = the command that exercises just that step.",
    kind="headline",
    widths={"Step": 6, "Name": 22, "What it does and how": 64, "Code": 22, "Test it": 44, "Proven": 26},
    toc="The step map — every chunk, how it's done, the code link, and how to test it",
)
for i, (_, _, _, disp, repo_path, _, _) in enumerate(STEPS, 1):  # data rows start at 5 (title block 1-3, header 4)
    c = ws_steps.cell(row=4 + i, column=list(steps_df.columns).index("Code") + 1)
    c.hyperlink = Hyperlink(ref=c.coordinate, target=GH + repo_path, display=disp)
    c.font = Font(name=c.font.name, size=10, color=BRAND["LINK"], underline="single")

# ---------------------------------------------------------------- 2. Use cases proven
cases = pd.DataFrame([
    {"Use case": "Failure RCA — perf / TTL", "Engine": "Dataproc",
     "Example DAG / task": "tpa_mntn_id_export", "What it read": "batch describe + event log",
     "Tool verdict (output)": "Cancelled at 10800s TTL (perf regression). Profile the event log for spill/skew; a TTL bump alone rarely fixes it.",
     "Confidence": "high", "Validated vs": "INC-005 (owner PR #1161)"},
    {"Use case": "Failure RCA — code bug", "Engine": "Dataproc",
     "Example DAG / task": "tpa_ipdsc_export / ipdsc_ds_67", "What it read": "batch describe + Cloud Logging traceback",
     "Tool verdict (output)": "Invalid GCS bucket name: write_location passed as a method, not called. Root cause at ipdsc_ds_67.py:73.",
     "Confidence": "high", "Validated vs": "ds67 (owner fix a008b2e)"},
    {"Use case": "Failure RCA — late / missing data", "Engine": "Dataproc",
     "Example DAG / task": "tpa_ipdsc_export / wait_ds17_src", "What it read": "Airflow sensor log + GCS",
     "Tool verdict (output)": "Mandatory partner feed (ShareThis / DS17) missing; the existence sensor hard-timed out.",
     "Confidence": "high", "Validated vs": "INC-010 (owner day-1 fix)"},
    {"Use case": "Failure RCA — orchestration", "Engine": "Databricks",
     "Example DAG / task": "keyword_ddp_reporting / write_targeted_signal_ds_19", "What it read": "jobs get-run + get-run-output + cluster events",
     "Tool verdict (output)": "Databricks job SUCCEEDED; K8s pod evicted mid-run (orchestration-only). Not a code fix.",
     "Confidence": "high", "Validated vs": "INC-009"},
    {"Use case": "Optimization — skew", "Engine": "Dataproc",
     "Example DAG / task": "Update Vertical Categorization", "What it read": "Spark event log (7 surfaces)",
     "Tool verdict (output)": "Stage 0 skewed 242x (max vs median task). Salt the skewed key or enable AQE skew join.",
     "Confidence": "high", "Validated vs": "prod crawl 2026-08-04"},
    {"Use case": "Optimization — stats + shuffle", "Engine": "Databricks",
     "Example DAG / task": "keyword_ddp_reporting / targeted_signal", "What it read": "EXPLAIN COST plan + Spark job metrics",
     "Tool verdict (output)": "Missing table stats (ANALYZE), 768/72/182 GiB shuffles under-partitioned, 161 spot-kill re-runs. Code + infra fixes.",
     "Confidence": "high", "Validated vs": "INC-009 job / screenshots"},
])
wb.table(
    "Use cases proven",
    cases,
    finding="Validated on real DAGs across both engines and both purposes (RCA and optimization)",
    method="Each row is a real failed task or job the tool was run on. Tool verdict is the actual output; Validated vs is the incident or owner action that confirmed it.",
    kind="headline",
    widths={"Use case": 26, "Engine": 11, "Example DAG / task": 34, "What it read": 30, "Tool verdict (output)": 60, "Confidence": 10, "Validated vs": 24},
    toc="The proof — one row per real use case, tool output vs ground truth",
)

# ---------------------------------------------------------------- 3. Worked example: Dataproc RCA
wb.notes(
    "Ex — Dataproc RCA",
    intro="A perf failure on a Dataproc batch (INC-005, tpa_mntn_id_export), traced step by step.",
    blocks=[
        ("Input", "Airflow alert: tpa_mntn_id_export FAILED. The Airflow log is boilerplate ('Batch job was cancelled') and does not say why."),
        ("Parse + route", "Identified DAG tpa_mntn_id_export; operator type routes to Dataproc; pulled the batch id."),
        ("Analyze", "Dataproc batch describe stateHistory: 'Cancelling batch as ttl exceeded' after running the full 10804s against a 10800s TTL. The structural TTL check flags it ran to the wall."),
        ("Signature", "ttl_exceeded (class ttl/wall-clock, programmatic_fix = sometimes)."),
        ("Report (actual output)",
         "RCA [high]: tpa_mntn_id_export - ttl/wall-clock. Cancelled at its 10800s TTL (ran 10804s). Usually a perf regression. Profile the Spark event log for spill/skew/uncached recompute; a TTL bump alone rarely fixes it. Code fix possible; verify first. + a deep link to the batch."),
        ("Outcome", "Matches the incident ground truth: the real fix was a Spark perf fix (cache the reused frame, raise shuffle partitions), shipped by the owner as PR #1161, not a TTL bump."),
    ],
)

# ---------------------------------------------------------------- 4. Worked example: Databricks RCA
wb.notes(
    "Ex — Databricks RCA",
    intro="An orchestration failure on a Databricks job (INC-009, write_targeted_signal_ds_19), traced step by step.",
    blocks=[
        ("Input", "Airflow alert: write_targeted_signal_ds_19 FAILED with an empty log ('No exception message found')."),
        ("Parse + route", "Operator type (DbxDbtOperator / KubernetesPodOperator) routes to Databricks; pulled the Databricks run id."),
        ("Analyze", "Databricks jobs get-run + get-run-output: the underlying job run SUCCEEDED and wrote its data. The Airflow-side pod was evicted / lost mid-run (a 404 in the pod check)."),
        ("Signature", "pod_evicted_404 (class orchestration/pod-evicted, programmatic_fix = no)."),
        ("Report (actual output)",
         "RCA [high]: keyword_ddp_reporting/write_targeted_signal_ds_19 - orchestration/pod-evicted. Downstream databricks job SUCCEEDED, orchestration-only failure. K8s pod evicted or lost mid-run (the job may have succeeded and written data). Not a code fix (compute/infra or upstream). + a deep link to the run."),
        ("Outcome", "Matches ground truth: the owner confirmed the data was complete and marked the task success; the durable fix is anti-eviction, not a code change. The tool correctly separated the successful job from the failed orchestration."),
    ],
)

# ---------------------------------------------------------------- 5. Worked example: Optimizer
wb.notes(
    "Ex — Optimizer (Dataproc)",
    intro="The efficiency half on Dataproc: reading a Spark event log to find a concrete speed-up (the Update Vertical Categorization job).",
    blocks=[
        ("Input", "A finished job's Spark event log (.zstd) from the archive bucket. No failure needed — this runs on healthy jobs to find waste."),
        ("Parse (7 surfaces)", "Parses jobs, stages, tasks, executors, environment, SQL per-node metrics, and storage from the event log into structured metrics."),
        ("Detect", "The skew detector compares max-vs-median task time per stage. Stage 0's slowest task ran 242x the median: one partition held nearly all the data, with GC pressure alongside."),
        ("Recommend (actual output)",
         "[high] Stage 0 skewed 242.1x (max vs median task). Why: one partition holds most of the data. Fix: salt the skewed group/join key or enable AQE skew join (spark.sql.adaptive.skewJoin.enabled); a plain repartition will not fix a value-skewed key."),
        ("Crawl (check every DAG)", "The same optimizer runs across a directory of event logs and ranks a cross-job backlog worst-first. The 2026-08-04 prod crawl scanned 13 jobs and surfaced 34 findings, 10 high-impact, led by this 242x skew."),
        ("Outcome", "Flagged to the model owner (DDP) as a concrete wall-clock and cost win. First real optimization target found autonomously by the tool."),
    ],
)

# ---------------------------------------------------------------- 5b. Worked example: Databricks optimizer
wb.notes(
    "Ex — Optimizer (Databricks)",
    intro="The same efficiency half on Databricks, on a real job (targeted_signal, in keyword_ddp_reporting). Databricks is used by ~66 dbt models plus a handful of PySpark jobs across the mntn_match and DDP DAGs.",
    blocks=[
        ("Input", "A Databricks job's EXPLAIN COST plan (from jobs get-run-output) plus its Spark job metrics (stage shuffle sizes, task failures, executor events). No GCS event log needed here."),
        ("Parse", "The plan gives per-node operators and optimizer statistics; the metrics give stage shuffle sizes, failed tasks, and executor removals."),
        ("CODE fixes (actual output)",
         "Missing table stats on product_categorization (13.5B rows scanned) so the optimizer defaults to full sorts, fix ANALYZE TABLE COMPUTE STATISTICS. Wide shuffles 768/72/182 GiB at the default partition count, fix set spark.sql.shuffle.partitions (~256 MiB each) or enable AQE coalesce."),
        ("INFRA / FAILURE fixes (actual output)",
         "161 task re-runs from 7 spot-reclaimed executors, fix raise first_on_demand or add on-demand fallback. 168 FetchFailed tasks (shuffle instability), route as infra and reduce shuffle block size."),
        ("Grouping", "Each finding is tagged CODE (a query/config PR), INFRA (a cluster change), or FAILURE (route it) so the owner knows the kind of fix."),
        ("Outcome", "Same detectors as the Dataproc optimizer, different acquisition (the plan plus Spark metrics instead of the GCS event log). This is what proves the optimizer works on both engines."),
    ],
)

# ---------------------------------------------------------------- 6. Signature taxonomy (live)
CLASS_MAP = {  # human label for the fix flag
    "yes": "Yes", "sometimes": "Sometimes", "no": "No",
}
tax = pd.DataFrame([
    {"Signature": s.key.replace("_", " "),
     "Engine": s.engine,
     "Class": s.sig_class,
     "Likely cause": s.likely_cause,
     "Auto-fix possible?": CLASS_MAP.get(s.programmatic_fix, s.programmatic_fix)}
    for s in sig.SIGNATURES
])
wb.table(
    "Signature taxonomy",
    tax,
    finding=f"{len(tax)} failure fingerprints — a match returns a cached verdict with no LLM",
    method="Built live from airflow_debugger.signatures. Engine 'any' applies to both. Auto-fix possible separates a fixable root cause (Yes) from a symptom or infra issue (No).",
    kind="data",
    widths={"Signature": 26, "Engine": 11, "Class": 22, "Likely cause": 62, "Auto-fix possible?": 16},
    toc=f"Coverage — the {len(tax)} fingerprints the deterministic classifier knows",
)

# ---------------------------------------------------------------- 7. Read me / glossary
wb.glossary(
    "Read me",
    intro="What this tool is, and the terms used across the tabs.",
    rows=[
        ("What it is", "An automated debugger for failed Airflow/Spark tasks (Dataproc and Databricks). It returns a root-cause report with the affected file and a confidence level. A second mode reads healthy jobs to find efficiency wins."),
        ("Why it exists", "On-call debugging of a failed task was slow and expert-dependent, and the framework author left. This removes the single-person dependency and cuts time-to-diagnosis."),
        ("", ""),
        ("Deterministic-first", "The fetch, routing, signature match, and report are plain code. The LLM is used only as a fallback when no known signature matches, so most cases cost nothing and are instant."),
        ("Key-free", "No stored API tokens and no Slack bot. Data access uses short-lived SSO / CLI tokens (astro, gcloud, Databricks OAuth). Required by MNTN security policy."),
        ("BLUF / STAR", "Bottom Line Up Front. The report leads with the answer (root cause) in one line, then the supporting detail, kept under 500 characters."),
        ("RCA", "Root-cause analysis: naming the actual underlying cause, not just that the task failed."),
        ("Event-log surfaces", "A Spark event log records 7 layers (jobs, stages, tasks, executors, environment, SQL node metrics, storage). The optimizer reads all 7 to find skew and spill."),
        ("Skew", "One partition holds far more data than the others, so one task runs much longer than the rest. Measured as max-vs-median task time."),
        ("Signature", "A regex fingerprint of a known failure. A high-confidence match returns a cached verdict with no LLM call."),
        ("Code links", "The Code column on 'How it works' links to the exact source line on GitHub (mdunn-mntn/malachi-workspace). Repo access required; without it, the file:line shown is still the reference."),
        ("Programmatic-fix flag", "Per signature: whether an automated code fix is even possible. It separates a fixable root cause from a downstream symptom or an infra issue."),
    ],
)

# ---------------------------------------------------------------- Cover (LAST)
wb.cover(
    takeaways=[
        "One automated tool turns a failed Airflow task into a root-cause report with the affected file, for both Dataproc and Databricks.",
        "Validated on real DAGs across every use case: Dataproc and Databricks RCA, plus the optimizer on both engines (a 242x Dataproc skew, and Databricks missing-stats + spot churn).",
        "Deterministic-first and key-free: most cases are instant with no LLM and no stored tokens.",
    ]
)

scratch = os.environ.get("CLAUDE_SCRATCH", "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad")
local = wb.save_local(os.path.join(scratch, "audi_1191_how_it_works.xlsx"))
print("saved local :", local)
try:
    drive = wb.save_drive("AUDI-1191", "Failure-Debugger How It Works")
    print("saved drive :", drive)
except Exception as e:
    print("drive save skipped:", e)

from openpyxl import load_workbook  # noqa: E402
print("tabs        :", load_workbook(local).sheetnames)
