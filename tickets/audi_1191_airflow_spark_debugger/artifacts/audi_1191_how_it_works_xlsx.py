#!/usr/bin/env python3
"""AUDI-1191 failure-debugger explainer + proof deliverable.

Builds the branded multi-sheet .xlsx that (1) explains how the automated Airflow/Spark
failure DEBUGGER works step by step (D1-D8), and (2) proves it on real incidents
(INC-005/009/010/011/012). The signature-taxonomy sheet is built live from
airflow_debugger.signatures so it never drifts. The optimizer (success-sweep) half has its
own deliverable under AUDI-1194.

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
    title="Airflow/Spark Failure Debugger",
    ticket="AUDI-1191",
    subtitle="How the failure debugger works, step by step, and proof on 5 real prod incidents (Dataproc + Databricks).",
    period="Validated Jul-Aug 2026",
    generated=GEN,
    status="Working",
)

# ---------------------------------------------------------------- 1. How it works (step map)
# One row per testable chunk. Fires on FAILURES only; the success-sweep optimizer is AUDI-1194.
STEPS = [
    # (step, name, what/how, code display, code repo-path#anchor, test command, proven)
    ("D1", "Detect failed tasks",
     "Queries the Airflow REST API on Astronomer directly (no Slack): POST /dags/~/dagRuns/~/taskInstances/list, "
     "windowed on the UTC day, state=failed. Auth is the short-lived astro SSO login. The Slack alert is only how "
     "a human notices; the tool finds the same failure itself from the API.",
     "airflow_api.py:203", ".claude/scripts/airflow_api.py#L203",
     "bash .claude/scripts/airflow_pull.sh --date <D> --state failed",
     "Agent-tested 2026-08-06 (6 failed tasks). Mid-retry tries need watch-mode, not the day-dump"),
    ("D2", "Download the log",
     "For each failed try: GET the task-instance log (structured JSON/NDJSON), render to plain text, save as "
     "<time>__<dag>__<task>__try<N>__<state>.log plus a _manifest.jsonl row (the pass/fail grid).",
     "airflow_api.py:294", ".claude/scripts/airflow_api.py#L294",
     "same command; logs land in on-call/airflow_logs/<date>/",
     "Agent-tested 2026-08-06 (3 logs + manifest verified) · INC-010/011/012 pulls"),
    ("D3", "Parse + route",
     "Reads the log for identity (dag, task, run, logical date), routes by operator classpath to the engine "
     "(Dataproc vs Databricks), and extracts the downstream Spark job id (batch id / run id).",
     "parse.py:51", "airflow_debugger/parse.py#L51",
     "python3 -m airflow_debugger.tests.test_parse",
     "Adversarial review 2026-08-06: 9 parse defects fixed; real-log sweep 64/64 identity, 33/33 job ids"),
    ("D4", "Engine RCA",
     "Dataproc: batches describe + Cloud Logging traceback + structural TTL check (dataproc_rca.py:156); "
     "when Cloud Logging has no error text it reads the staging driveroutput named in the batch stateMessage "
     "(needs the dataproc-debug PAM grant; notes the 403 and the unblock step without it). "
     "Databricks: jobs get-run + get-run-output on the TASK run id + cluster events (databricks_rca.py:74).",
     "dataproc_rca.py:156", "airflow_debugger/dataproc_rca.py#L156",
     "python3 -m airflow_debugger.tests.test_dataproc_rca",
     "INC-005/009/012. Driveroutput fallback (IMP-028) tested on the real INC-012 driver text: full gcs_list_timeout verdict"),
    ("D5", "Signature match",
     f"{len(sig.SIGNATURES)} regex fingerprints classify the failure; a high-confidence match returns a cached verdict with NO LLM "
     "call. Each signature's programmatic-fix flag separates a fixable root cause from a downstream symptom.",
     "signatures.py:28", "airflow_debugger/signatures.py#L28",
     "python3 -m airflow_debugger.tests.test_signatures",
     "27 cases + order-integrity test; INC-012 mixed driver blob classifies correctly"),
    ("D6", "Past-incident match",
     "Lexical matcher over the local incident corpus (on-call/incident_log.jsonl) attaches the most similar past "
     "incidents to the verdict, so a repeat is recognized instantly.",
     "incident_match.py:34", "airflow_debugger/incident_match.py#L34",
     "python3 -m airflow_debugger.tests.test_incident_match",
     "Agent-tested 2026-08-06: INC-012 query -> INC-012 top match (0.605)"),
    ("D7", "Report",
     "BLUF/STAR report under 500 characters: root cause + confidence + affected file:line where known + whether a "
     "code fix is possible + a deep link to the batch/run.",
     "report.py:50", "airflow_debugger/report.py#L50",
     "python3 -m airflow_debugger.report <log file>",
     "2 real-log replays <=500 chars; truncation now URL-safe (never emits a cut link)"),
    ("D8", "LLM fallback",
     "ONLY when no signature matched: one bounded LLM synthesis call over the distilled evidence (synth.py:29). "
     "Everything before this is plain deterministic code, so known failures cost nothing and are instant.",
     "orchestrate.py:16", "airflow_debugger/orchestrate.py#L16",
     "python3 -m airflow_debugger.orchestrate <log>  (--no-llm to force it off)",
     "Deterministic report never replaced by an LLM error stub; unknown-sig fallback verified"),
]
steps_df = pd.DataFrame(
    [{"Step": s, "Name": n, "What it does and how": w, "Code": d, "Test it": t, "Proven": p}
     for (s, n, w, d, _, t, p) in STEPS]
)
ws_steps = wb.table(
    "How it works",
    steps_df,
    finding="Every step is a small, separately testable chunk of plain code",
    method="Fires on failures only (the success-sweep optimizer is AUDI-1194, its own workbook). Code links to the "
           "exact source line on GitHub (mdunn-mntn/malachi-workspace). Test it = the command that exercises just that step.",
    kind="headline",
    widths={"Step": 6, "Name": 22, "What it does and how": 64, "Code": 22, "Test it": 44, "Proven": 26},
    toc="The step map — every chunk, how it's done, the code link, and how to test it",
)
for i, (_, _, _, disp, repo_path, _, _) in enumerate(STEPS, 1):  # data rows start at 5 (title block 1-3, header 4)
    c = ws_steps.cell(row=4 + i, column=list(steps_df.columns).index("Code") + 1)
    c.hyperlink = Hyperlink(ref=c.coordinate, target=GH + repo_path, display=disp)
    c.font = Font(name=c.font.name, size=10, color=BRAND["LINK"], underline="single")

# ---------------------------------------------------------------- 2. Incidents proven
cases = pd.DataFrame([
    {"Incident": "INC-005", "Failure class": "Perf / TTL", "Engine": "Dataproc",
     "DAG / task": "tpa_mntn_id_export", "What it read": "batch describe + event log",
     "Tool verdict (output)": "Cancelled at 10800s TTL (perf regression). Profile the event log for spill/skew; a TTL bump alone rarely fixes it.",
     "Confirmed by": "Owner perf fix PR #1161"},
    {"Incident": "ds67", "Failure class": "Code bug", "Engine": "Dataproc",
     "DAG / task": "tpa_ipdsc_export / ipdsc_ds_67", "What it read": "batch describe + Cloud Logging traceback",
     "Tool verdict (output)": "Invalid GCS bucket name: write_location passed as a method, not called. Root cause at ipdsc_ds_67.py:73.",
     "Confirmed by": "Owner fix a008b2e, exact file/line"},
    {"Incident": "INC-009", "Failure class": "Orchestration only", "Engine": "Databricks",
     "DAG / task": "keyword_ddp_reporting / write_targeted_signal_ds_19", "What it read": "jobs get-run + get-run-output + cluster events",
     "Tool verdict (output)": "Databricks job SUCCEEDED; K8s pod evicted mid-run (orchestration-only). Not a code fix.",
     "Confirmed by": "Owner marked success; data verified in GCS"},
    {"Incident": "INC-010", "Failure class": "Late / missing partner data", "Engine": "Dataproc",
     "DAG / task": "tpa_ipdsc_export / wait_ds17_src", "What it read": "Airflow sensor log + GCS",
     "Tool verdict (output)": "Mandatory partner feed (ShareThis / DS17) missing; the existence sensor hard-timed out. Also caught: the unblock copy was previous-day data.",
     "Confirmed by": "Owner root cause matched + day-1 fallback shipped"},
    {"Incident": "INC-011", "Failure class": "Skip treated as failure", "Engine": "n/a (sensor)",
     "DAG / task": "hashed_email_ds_26_signals / wait_fpa", "What it read": "sensor log + external task state + producer short-circuit log",
     "Tool verdict (output)": "False alarm: producer SUCCEEDED and correctly skipped (no Predactiv file that hour); the sensor counts a skip as a failure.",
     "Confirmed by": "Fix PR #1175 merged (skipped_states, 2 DAGs)"},
    {"Incident": "INC-012", "Failure class": "GCS list timeout", "Engine": "Dataproc",
     "DAG / task": "materialize_mntn_select / materialize", "What it read": "batch describe + staging driveroutput",
     "Tool verdict (output)": "Both tries died listing augmentor_log: the region glob lists every file (~17M) to find one hour. 'Lost executors' was a red herring (idle scale-downs).",
     "Confirmed by": "Fix PR #1176 merged (literal paths); corrected the thread's preemption theory"},
])
wb.table(
    "Incidents proven",
    cases,
    finding="Six real prod incidents diagnosed correctly, including two where the tool corrected the first human read",
    method="Each row is a real failure the tool was run on. Tool verdict is the actual output; Confirmed by is the owner action or merged fix that proved it right.",
    kind="headline",
    widths={"Incident": 10, "Failure class": 20, "Engine": 12, "DAG / task": 34, "What it read": 30, "Tool verdict (output)": 58, "Confirmed by": 30},
    toc="The proof — one row per real incident, tool output vs ground truth",
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

# ---------------------------------------------------------------- 5. Worked example: INC-012
wb.notes(
    "Ex — GCS list timeout",
    intro="The newest live incident (INC-012, materialize_mntn_select): the evidence overruled two plausible human theories, and the fix merged the same day.",
    blocks=[
        ("Input", "Airflow alert: 'Dataproc Agent reports job failure' (boilerplate). Two tries died at the same ~19 minutes. The team's thread read it as lost executors, then spot preemption."),
        ("Analyze", "batches describe ruled out the TTL class in one call (ran 1121s of a 14400s TTL). The driver output showed both tries died on the same timeout while listing gs://.../augmentor_log/, and the 'lost executor' lines were idle scale-downs with no preempt or kill messages."),
        ("Mechanism", "The path used region={east,west}, a pattern. Expanding it makes the GCS connector list every file under augmentor_log (~17M) just to find one hour. When that listing runs slow, the job dies. Measured: the exact-folder list is 18K names in 7s."),
        ("Verdict", "Not preemption, not lost executors: a fragile full-prefix listing hitting variable GCS latency. A re-run passes (latency varies), so it would keep paging until fixed."),
        ("Fix + outcome", "PR #1176: point the job at the two exact region folders instead of the pattern (plus a crash guard in the shared helper). Merged same day by the owning team. The hour hole was backfilled; signature gcs_list_timeout added so the next one is recognized instantly."),
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
        ("What it is", "An automated debugger for failed Airflow/Spark tasks (Dataproc and Databricks). On a failure it returns a root-cause report with the affected file and a confidence level, in under 500 characters."),
        ("Why it exists", "On-call debugging of a failed task was slow and expert-dependent, and the framework author left. This removes the single-person dependency and cuts time-to-diagnosis."),
        ("The other half", "The efficiency sweep (reads jobs that SUCCEEDED and finds waste like skew and spill) is a separate tool and workbook: AUDI-1194, in the same Drive folder set."),
        ("", ""),
        ("Deterministic-first", "The fetch, routing, signature match, and report are plain code. The LLM is used only as a fallback when no known signature matches, so most cases cost nothing and are instant."),
        ("Key-free", "No stored API tokens and no Slack bot. Data access uses short-lived SSO / CLI tokens (astro, gcloud, Databricks OAuth). Required by MNTN security policy."),
        ("BLUF / STAR", "Bottom Line Up Front. The report leads with the answer (root cause) in one line, then the supporting detail, kept under 500 characters."),
        ("RCA", "Root-cause analysis: naming the actual underlying cause, not just that the task failed."),
        ("Signature", "A regex fingerprint of a known failure. A high-confidence match returns a cached verdict with no LLM call."),
        ("Code links", "The Code column on 'How it works' links to the exact source line on GitHub (mdunn-mntn/malachi-workspace). Repo access required; without it, the file:line shown is still the reference."),
        ("Programmatic-fix flag", "Per signature: whether an automated code fix is even possible. It separates a fixable root cause from a downstream symptom or an infra issue."),
    ],
)

# ---------------------------------------------------------------- Cover (LAST)
wb.cover(
    takeaways=[
        "A failed Airflow task goes in; a root-cause report with the affected file comes out, for both Dataproc and Databricks.",
        "Proven on six real prod incidents, including two where the tool corrected the first human diagnosis (INC-011 skip-vs-fail, INC-012 list timeout); both fixes merged.",
        "Deterministic-first and key-free: known failures resolve instantly with no LLM call and no stored tokens.",
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
