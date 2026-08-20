"""The exhaustive worked-example tabs: one real failure, every line, nothing cut.

Renders `audi_1191_worked_example.json` (written by audi_1191_capture_worked_example.py) into
the explainer workbook. Every log is shown in FULL, one line per row, with a column naming
exactly which step consumed that line. The point of the walkthrough is the signal-to-noise
ratio: a 28-line Airflow log where 4 lines matter and the cause is not in the file at all.

Imported by audi_1191_how_it_works_xlsx.py; not run directly.
"""

from __future__ import annotations

import json
import os
import re

import pandas as pd

_NEVER = re.compile(r"(?!x)x")  # matches nothing; placeholder when a value is absent
EVIDENCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audi_1191_worked_example.json")


def load() -> dict:
    """The captured end-to-end run. Regenerate with audi_1191_capture_worked_example.py."""
    with open(EVIDENCE, encoding="utf-8") as f:
        return json.load(f)


def _short(v: object, cap: int = 30000) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= cap else s[: cap - 1] + "…"


# --------------------------------------------------------------------- per-line annotation
def _airflow_annotations(ev: dict) -> list[tuple[re.Pattern, str, str]]:
    """(matcher, step, what the debugger takes) built from the captured values, so it can't drift."""
    p, d = ev["parsed"], ev["diagnosis"]
    sig = d.get("airflow_signature") or {}
    return [
        (re.compile(re.escape(f"dag_id='{p['dag_id']}'")), "D3 identity",
         f"dag_id={p['dag_id']}, task_id={p['task_id']}, run_id={p['run_id']}, "
         f"try_number={p['try_number']}"),
        (re.compile(r"DataprocCreateBatchOperator\s+Starting batch"), "D3 route + job id",
         f"Operator name routes the engine to Dataproc; the batch id {p['batch_id']} is the "
         "handle for every later step"),
        (re.compile(r"The batch .* was created"), "D3 confirm",
         "The batch really was submitted, so a failure means the job ran and died, not that "
         "submission failed (contrast INC-020)"),
        (re.compile(r"Waiting for the completion of batch job"), "-",
         "Present in EVERY healthy Dataproc log too, so it is NOT usable as a failure "
         "fingerprint. This line cost 325 false positives before it was removed from a signature"),
        (re.compile(r"\[error\] task Task failed with exception"), "D5 attempt",
         "The only [error] line, and it carries NO cause. This is why the chain must go one "
         "layer down to the Dataproc driver output"),
        (re.compile(re.escape(sig["matched_on"])) if sig.get("matched_on")
         else _NEVER, "D5 signature",
         f"Matched the Airflow-layer signature {sig.get('key')} on "
         f"'{sig.get('matched_on')}' (superseded by the driver-layer verdict at D5)"),
    ]


def _driver_annotations(ev: dict) -> list[tuple[re.Pattern, str, str]]:
    root = ev["diagnosis"].get("root_signature") or {}
    link = (ev["code_links"] or [["", ""]])[0]
    return [
        (re.compile(r"^Traceback \(most recent call last\)"), "D4 driver text",
         "Start of the Python traceback. Everything above is container boot noise"),
        (re.compile(r'File "/var/dataproc/tmp/.*\.py", line \d+'), "D7 code link",
         f"Frame in the deployed job script. Resolved against the airflow-ti tree to "
         f"{link[1]} and rendered as the exact #L link. Framework frames (pyspark, py4j, "
         "/usr/lib) are filtered out"),
        (re.compile(r"^\s+df = spark\.read"), "D7 the failing statement",
         "The exact line of code that raised, quoted from the driver output"),
        (re.compile(re.escape(root["matched_on"])) if root.get("matched_on")
         else _NEVER, "D5 signature MATCH",
         f"This is the verdict. Signature {root.get('key')} matched on "
         f"'{root.get('matched_on')}' and returns a cached root cause with NO LLM call"),
        (re.compile(r"SocketTimeoutException: Read timed out"), "D5 corroboration",
         "The underlying timeout. The signature requires it to be GCS-bound; a bare socket "
         "timeout with no gs:// nearby is a different class"),
        (re.compile(r"Lost executor|spark scale down"), "-",
         "Benign idle decommission. This is the line a human read as the cause on INC-012; "
         "the signature order deliberately keeps gcs_list_timeout ahead of executor_lost"),
    ]


def _log_frame(text: str, ann: list[tuple[re.Pattern, str, str]]) -> pd.DataFrame:
    rows = []
    for i, line in enumerate(text.splitlines(), 1):
        # a single line can carry several signals at once (the slack_messages dump holds the
        # identity AND the signature text), so collect every hit rather than the first
        hits = [(s, u) for pat, s, u in ann if pat.search(line)]
        step = " + ".join(dict.fromkeys(s for s, _ in hits))
        used = "  ||  ".join(dict.fromkeys(u for _, u in hits))
        rows.append({
            "#": i,
            "Log line (verbatim, nothing removed)": _short(line),
            "Step": step,
            "What the debugger takes from this line": used,
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------- sheets
def add_sheets(wb: object, github_base: str = "") -> None:
    """Append the worked-example tabs to an open MntnWorkbook."""
    ev = load()
    s, p, d = ev["subject"], ev["parsed"], ev["diagnosis"]
    root = d.get("root_signature") or {}
    matches = ev["similar_incidents"]
    a_lines = ev["airflow_log"].splitlines()
    dr_lines = ev["driver_log"].splitlines()

    # ---- 1. When it runs -------------------------------------------------
    trig = pd.DataFrame([
        {"Situation": "An Airflow task reaches state failed", "Debugger runs?": "Yes",
         "What starts it": "The daily pull selects state=failed / upstream_failed, then runs the "
                           "chain on each log it downloaded",
         "Where": ".claude/scripts/oncall_daily_rca.sh"},
        {"Situation": "A task fails a try and will retry", "Debugger runs?": "Yes, in watch mode",
         "What starts it": "Per-try terminal transitions. The day-dump cannot see a failed try "
                           "while the task instance is still running",
         "Where": "airflow_pull.sh --watch --diagnose"},
        {"Situation": "A human pastes one log", "Debugger runs?": "Yes, on demand",
         "What starts it": "You run it against a single file",
         "Where": "python3 -m airflow_debugger.orchestrate <log>"},
        {"Situation": "A task succeeds", "Debugger runs?": "No",
         "What starts it": "Nothing. Success is never a trigger",
         "Where": "-"},
        {"Situation": "A task is skipped or is still running", "Debugger runs?": "No",
         "What starts it": "Nothing. Only terminal failure states are selected",
         "Where": "-"},
        {"Situation": "Finding waste in jobs that SUCCEEDED", "Debugger runs?": "No, different tool",
         "What starts it": "A weekly sweep over Spark event logs, entirely separate",
         "Where": "AUDI-1194 airflow_optimizer/"},
    ])
    wb.table(
        "When it runs", trig,
        finding="The debugger only ever fires on a failed task. Success is not a trigger",
        method="A green run is never diagnosed and never posted about. The efficiency sweep that "
               "does read successful jobs is a separate tool with its own schedule and workbook.",
        kind="headline",
        widths={"Situation": 44, "Debugger runs?": 18, "What starts it": 62, "Where": 42},
        toc="The trigger rule - failure only, and what each entry point is",
    )

    # ---- 2. The code that failed ----------------------------------------
    before = ev["spark_source_before"].splitlines()
    b_hit = next((i for i, ln in enumerate(before, 1) if "basePath" in ln), 0)
    code = pd.DataFrame([
        {"What": "The DAG", "File": s["dag_file"],
         "Detail": "Builds one Dataproc task per enabled vendor id from DSID_CONFIG. Data source "
                   "30 maps to the script below with task suffix augmentor_log_processing, which "
                   "is how the task id in the alert is formed."},
        {"What": "The task that failed", "File": f"{s['dag_file']} (dsid 30 entry)",
         "Detail": f"{p['task_id']} in DAG {p['dag_id']}, run {p['run_id']}, try {p['try_number']}."},
        {"What": "The job script", "File": s["spark_file"],
         "Detail": f"Runs on Dataproc Serverless as batch {p['batch_id']}."},
        {"What": "The line that raised (BEFORE the fix)",
         "File": f"{s['spark_file']}:{b_hit}",
         "Detail": next((ln.strip() for ln in before if "basePath" in ln), "")},
        {"What": "Why that line is dangerous", "File": "-",
         "Detail": "region={east,west} is a glob and basePath points at the bucket root. The GCS "
                   "connector resolves both by listing the entire multi-year augmentor_log prefix "
                   "(millions of objects) before filtering down to one hour."},
        {"What": "The fix (AFTER)", "File": f"{s['spark_file']} @ {s['fix_commit']}",
         "Detail": "Two literal region paths, no glob, no basePath, plus an existence guard so a "
                   "genuinely missing hour raises a clear error instead of timing out."},
    ])
    wb.table(
        "Example 1 - the code", code,
        finding=f"{s['incident']}: a one-line read in {os.path.basename(s['spark_file'])} that "
                "asks GCS to list millions of objects to find one hour",
        method=f"Source quoted from the airflow-ti repo at fix commit {s['fix_commit']} and its "
               f"parent. Fix PR: {s['fix_pr']}",
        kind="headline",
        widths={"What": 34, "File": 46, "Detail": 96},
        toc="Start here - the DAG, the task, and the exact line of code that failed",
    )

    diff = pd.DataFrame([
        {"#": i, "Diff line (verbatim)": _short(ln),
         "Meaning": ("removed - the dangerous read" if ln.startswith("-") and "basePath" in ln
                     else "added - the fix" if ln.startswith("+") and not ln.startswith("+++")
                     else "removed" if ln.startswith("-") and not ln.startswith("---")
                     else "")}
        for i, ln in enumerate(ev["fix_diff"].splitlines(), 1)
    ])
    wb.table(
        "Example 1 - the fix diff", diff,
        finding="The whole fix is two lines out, twelve lines in",
        method=f"git show {s['fix_commit']}. Merged, then prod-verified the same morning: the "
               "retry ran about 6 min against ~19-min deaths.",
        kind="data",
        widths={"#": 5, "Diff line (verbatim)": 118, "Meaning": 30},
        toc="The merged fix, line by line",
    )

    # ---- 3. The Airflow log, in full ------------------------------------
    af = _log_frame(ev["airflow_log"], _airflow_annotations(ev))
    used = int((af["Step"] != "").sum())
    wb.table(
        "Example 1 - Airflow log", af,
        finding=f"All {len(a_lines)} lines of the real log. {used} carry signal, and NONE of them "
                "contain the cause",
        method="Verbatim, nothing removed. Step names which step consumed the line; blank means "
               "ignored. The one [error] line says only 'Task failed with exception'.",
        kind="data",
        widths={"#": 5, "Log line (verbatim, nothing removed)": 104, "Step": 20,
                "What the debugger takes from this line": 70},
        toc=f"The input - every one of the {len(a_lines)} log lines on-call actually sees",
    )

    # ---- 4. The chain, step by step, on THIS case -----------------------
    chain = pd.DataFrame([
        {"Step": "D1", "What it does": "Find the failed task",
         "Input (actual)": f"Airflow REST API, day {s['date']}, state=failed",
         "Output (actual)": f"{p['dag_id']} / {p['task_id']}, try {p['try_number']}"},
        {"Step": "D2", "What it does": "Download the log",
         "Input (actual)": "Task-instance log endpoint",
         "Output (actual)": f"{os.path.basename(s['log'])} ({len(a_lines)} lines)"},
        {"Step": "D3", "What it does": "Parse identity, route engine, extract job id",
         "Input (actual)": "The 28 lines above",
         "Output (actual)": f"engine={p['engine']}, batch_id={p['batch_id']}, "
                            f"run_id={p['run_id']}"},
        {"Step": "D4a", "What it does": "Ask Dataproc what happened to the batch",
         "Input (actual)": f"gcloud dataproc batches describe {p['batch_id']}",
         "Output (actual)": f"state={ev['batch_state']}, ran {ev['batch_runtime_s']}s of "
                            f"ttl {ev['batch_ttl']}. Not a TTL kill, so the TTL class is ruled "
                            "out structurally"},
        {"Step": "D4b", "What it does": "Get the driver text",
         "Input (actual)": "Cloud Logging first",
         "Output (actual)": "Empty for this batch (outside the freshness window). "
                            "Recorded as a note, not a silent blank"},
        {"Step": "D4c", "What it does": "Fall back to the staging driveroutput",
         "Input (actual)": "The driveroutput URI named in the batch stateMessage",
         "Output (actual)": f"{len(dr_lines)} lines of real driver output (needs the "
                            "dataproc-debug PAM grant; without it the tool says so and names "
                            "the unblock step)"},
        {"Step": "D5", "What it does": "Match a signature",
         "Input (actual)": "The driver text",
         "Output (actual)": f"{root.get('key')} ({root.get('sig_class')}), matched on "
                            f"'{root.get('matched_on')}'. No LLM call"},
        {"Step": "D6", "What it does": "Find similar past incidents",
         "Input (actual)": "Signature class + error text + dag/task",
         "Output (actual)": "; ".join(
             f"{m.get('inc')} ({m.get('score')})" for m in matches) or "none"},
        {"Step": "D7", "What it does": "Build the report and troubleshooting pack",
         "Input (actual)": "Verdict + matches + traceback frames",
         "Output (actual)": f"BLUF report ({len(ev['report'])} chars), known-fix PR from the "
                            f"matched incident, and {len(ev['code_links'])} exact #L code link"},
        {"Step": "D8", "What it does": "LLM fallback",
         "Input (actual)": "-",
         "Output (actual)": f"SKIPPED. A signature matched, so llm_used={ev['llm_used']} and "
                            f"confidence={ev['confidence']}. Cost: nothing"},
    ])
    wb.table(
        "Example 1 - the chain", chain,
        finding="Every step with its real input and real output, ending in a verdict the Airflow "
                "log never contained",
        method="Reproduced end to end in about 9 seconds by one command (see 'Run it live'). "
               "Nothing here is illustrative; each cell is captured from the actual run.",
        kind="headline",
        widths={"Step": 7, "What it does": 38, "Input (actual)": 52, "Output (actual)": 82},
        toc="The walkthrough - what each step received and produced on this real failure",
    )

    # ---- 5. The driver log, in full -------------------------------------
    dv = _log_frame(ev["driver_log"], _driver_annotations(ev))
    dused = int((dv["Step"] != "").sum())
    wb.table(
        "Example 1 - driver log", dv,
        finding=f"All {len(dr_lines)} lines of the Dataproc driver output. The cause is here, and "
                f"{dused} lines carry it",
        method="Read from the staging driveroutput named in the batch stateMessage. Verbatim, "
               "nothing removed. This is the layer the Airflow log points at but does not carry.",
        kind="data",
        widths={"#": 5, "Log line (verbatim, nothing removed)": 104, "Step": 22,
                "What the debugger takes from this line": 70},
        toc=f"Where the answer actually lives - all {len(dr_lines)} driver lines",
    )

    # ---- 6. The answer ---------------------------------------------------
    ans_rows = [
        {"Detail": "Verdict", "What it says": f"{root.get('sig_class')} ({root.get('key')})"},
        {"Detail": "Confidence", "What it says": ev["confidence"]},
        {"Detail": "LLM used", "What it says": str(ev["llm_used"])},
        {"Detail": "Matched on", "What it says": _short(root.get("matched_on"))},
        {"Detail": "Root cause", "What it says": _short(root.get("likely_cause"))},
        {"Detail": "Auto-fix possible", "What it says": root.get("programmatic_fix")},
        {"Detail": "Affected file", "What it says": (ev["code_links"] or [["", ""]])[0][0]},
        {"Detail": "Known fix (from the matched incident)",
         "What it says": next((m.get("fix_pr") for m in matches if m.get("fix_pr")), "-")},
        {"Detail": "Similar incidents",
         "What it says": "; ".join(f"{m.get('inc')} score {m.get('score')}" for m in matches) or "-"},
        {"Detail": "Ground truth",
         "What it says": f"Correct. {s['fix_pr']} merged 2026-08-07 16:22Z and prod-verified the same "
                  "morning: the 15Z retry succeeded in about 6 minutes against ~19-minute deaths. "
                  "A repo-wide sweep off this verdict found 2 more unfixed readers, one already "
                  "shipping a green run with zero rows."},
    ]
    ans_rows += [{"Detail": f"REPORT line {i}", "What it says": _short(ln)}
                 for i, ln in enumerate(ev["report"].splitlines(), 1)]
    ans_rows += [{"Detail": f"PACK line {i}", "What it says": _short(ln)}
                 for i, ln in enumerate(ev["troubleshooting"].splitlines(), 1) if ln.strip()]
    wb.table(
        "Example 1 - the answer", pd.DataFrame(ans_rows),
        finding="The output on-call receives, verbatim, plus the ground truth that proved it right",
        method="REPORT rows are the under-500-character BLUF exactly as emitted. PACK rows are the "
               "--troubleshoot output exactly as emitted. Neither is paraphrased.",
        kind="headline",
        widths={"Detail": 40, "What it says": 128},
        toc="The output - the verbatim report, the pack, and how we know it was right",
    )

    # ---- 7. Slack reply --------------------------------------------------
    slack_body = ev["report"]
    slack = pd.DataFrame([
        {"Part": "Status", "Content":
            "PROPOSED, NOT LIVE. Posting is Phase 3 and is on hold. Today the same text is "
            "written to a file beside the log (<log>.rca.md) and read during triage."},
        {"Part": "Where it would post", "Content":
            "As a threaded reply under the existing failure alert in #airflow-ti-alerts, over "
            "the SlackNotifier connection airflow-ti already uses to send that alert. No new bot, "
            "no new token, no new app."},
        {"Part": "Trigger", "Content":
            "Only the failure callback that already fires to send the alert. If no alert was "
            "posted, no reply is posted."},
        {"Part": "Guardrail", "Content":
            "One reply per failed task instance, in-thread only, never a new top-level message, "
            "and never a channel the alert did not already go to."},
        {"Part": "Feature flag", "Content":
            "Airflow Variable DEBUGGER_AUTOFIRE, default off, enabled one team at a time."},
        {"Part": "", "Content": ""},
        {"Part": "Reply text", "Content": "--- begin ---"},
    ] + [
        {"Part": f"  line {i}", "Content": _short(ln)}
        for i, ln in enumerate(slack_body.splitlines(), 1)
    ] + [
        {"Part": "  link", "Content": (ev["code_links"] or [["", ""]])[0][0]},
        {"Part": "  known fix",
         "Content": next((m.get("fix_pr") for m in matches if m.get("fix_pr")), "-")},
        {"Part": "Reply text", "Content": "--- end ---"},
        {"Part": "", "Content": ""},
        {"Part": "What it will never do", "Content":
            "Open a pull request, push a branch, commit code, or change any prod resource. The "
            "debugger is read-only and stays read-only. GitHub access is for reading the repo to "
            "resolve a traceback frame to a file link, nothing else."},
    ])
    wb.table(
        "Example 1 - Slack reply", slack,
        finding="The reply is a thread comment on the alert that already exists, and it is read-only",
        method="Body is the same verbatim report from the previous tab. Marked PROPOSED because "
               "the posting step is Phase 3 and has not been built or approved.",
        kind="headline",
        widths={"Part": 26, "Content": 136},
        toc="The last mile - the exact Slack text, where it posts, and what it will never do",
    )

    # ---- 8. Run it live --------------------------------------------------
    live = pd.DataFrame([
        {"#": 1, "Do this": "Authenticate (once, before the room)",
         "Command": "gcloud auth login   #  plus: astro login, databricks auth login -p malachi@mountain.com",
         "You should see": "An account listed by: gcloud auth list"},
        {"#": 2, "Do this": "Show the raw log first",
         "Command": f"cat {s['log']}",
         "You should see": f"{len(a_lines)} lines, one [error] line, no cause. Ask the room what "
                           "they would do next"},
        {"#": 3, "Do this": "Run the debugger on it",
         "Command": f"python3 -m airflow_debugger.orchestrate {s['log']} --no-llm --troubleshoot",
         "You should see": f"RCA [high] ... {root.get('sig_class')}, the {s['fix_pr'].rsplit('/', 1)[-1]} "
                           "known fix, and the #L30 code link. Takes about 9 seconds"},
        {"#": 4, "Do this": "Show it is deterministic, not a guess",
         "Command": "python3 -m airflow_debugger.tests.test_signatures",
         "You should see": "The classifier cases and the order-integrity test passing"},
        {"#": 5, "Do this": "Show the coverage claim is measured",
         "Command": "python3 -m airflow_debugger.sweep",
         "You should see": "Rates per outcome across every log on disk, and the remaining "
                           "unclassified clusters"},
        {"#": 6, "Do this": "Diagnose a failure from today instead",
         "Command": "bash .claude/scripts/oncall_daily_rca.sh $(date -u +%F)",
         "You should see": "Each failed task pulled and an .rca.md written beside its log"},
        {"#": 7, "Do this": "If step 3 returns a thin answer",
         "Command": "-",
         "You should see": "The driver text for this batch comes from the staging bucket, which "
                           "needs the dataproc-debug PAM grant. Without it the tool says so and "
                           "names the unblock step. Grant PAM first, or demo a batch from the "
                           "last few days where Cloud Logging still has the text"},
    ])
    wb.table(
        "Run it live", live,
        finding="Seven copy-paste steps to run the whole thing in front of the team",
        method="Run from the workspace root. Step 7 is the one failure mode worth rehearsing: the "
               "driver text for an older batch is PAM-gated.",
        kind="headline",
        widths={"#": 5, "Do this": 42, "Command": 78, "You should see": 78},
        toc="The demo script - exact commands and what each should print",
    )
