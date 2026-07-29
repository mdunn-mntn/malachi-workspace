---
name: feedback-verify-edit-scripts
description: "Gate 'shipped' claims on git evidence — an assert-abort in a batch-edit script leaves ZERO edits applied while the prepared commit/success message claims otherwise"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [verify_edit_scripts, verify, edit, scripts, gate, shipped, claims, evidence]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-20
---
Twice in one TI-1037 session (2026-07-08), a python multi-replace heredoc raised AssertionError on a
string-count check BEFORE its single end-of-script write. Nothing was applied — but the pre-written commit
ran anyway (5510ec6 swept only a stray perf-log line under a message describing feature edits) and the
clipboard was loaded with an UNMODIFIED file, so Malachi was told fixes shipped that hadn't.

**Why:** assert-early/write-late is correct for atomicity, but its failure mode is silent: the turn's
narrative (commit message, "on your clipboard", "done") was authored before the evidence existed.

**How to apply:** after any batch-edit script, confirm `git diff --stat` / the commit's file list matches
the intended files BEFORE committing or claiming success; if a count-assert fires, say so plainly, fix the
anchor, and rerun — never let a prepared success message survive a failed script. [[reference_mode_dashboard_porting]]

**2026-07-14 recurrence (opposite flavor):** unasserted `str.replace()` with a print AFTER the call —
anchor missed, file unchanged, "updated" printed anyway; the failure then CASCADED for 3 days because
later edits anchored on the missing text. Rule: every scripted replace must `assert old in s` (abort
loudly), and after the commit, check `git show --stat` lists every file you claimed to edit — a file
missing from the commit = a lie in the log. Grep-verify the new text exists post-write.

**2026-07-20 (new failure mode — SYNTAX, not application):** a git-diff check does NOT catch a script
that applied cleanly but is syntactically BROKEN. An `Edit` to `bq_run.sh` put an apostrophe (`it's`)
in a jq comment INSIDE the single-quoted jq program → closed the bash quote early → `bash -n` syntax
error. The diff looked perfect and committed fine (bafa6bf), but the wrapper was UNRUNNABLE on `main`
for hours; the queue-crawl agents silently fell back to raw `bq query` (no perf-logging). Rule: after
editing ANY script, **parse-check before committing** — `bash -n <file>` for shell, `python3 -m
py_compile <file>` for python — and run a one-line smoke test that exercises the changed path. Watch
apostrophes/backticks inside single-quoted jq/awk heredocs. Committing ≠ working.
