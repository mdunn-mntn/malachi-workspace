#!/usr/bin/env bash
# hooks_selftest.sh — exercise all 9 Claude Code harness hooks with synthetic inputs and assert
# (exit code, output substring). Answers "are all the hooks working?" — run inside verify.sh (full mode)
# and workflow_audit.sh §11, NOT per-commit. Read-only except the two hooks whose contract is to append
# to a local/gitignored ledger; those are exercised only on their NO-OP path (harness-marker / non-bq
# command) so the self-test never pollutes real state.
#
# Exit 0 if all pass, 1 if any hook misbehaves. Never mutates tracked files.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
H="$ROOT/.claude/hooks"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "  ok    $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL  $1 — $2"; }

# assert_exit <label> <want_exit> <runner> <hookfile>   (stdin piped by caller via heredoc)
echo "hooks self-test (9 harness hooks):"

# 1. enforce_bq_wrapper.sh — the ONLY blocker: raw `bq query` => exit 2; wrapper/dry-run => exit 0
out=$(bash "$H/enforce_bq_wrapper.sh" 2>&1 <<'J'
{"tool_input":{"command":"bq query 'SELECT 1'"}}
J
); rc=$?
{ [ $rc -eq 2 ] && grep -qi "bq_run" <<<"$out"; } && ok "enforce_bq_wrapper: raw 'bq query' blocked (exit 2)" || bad "enforce_bq_wrapper[block]" "exit=$rc want 2, out='$out'"
out=$(bash "$H/enforce_bq_wrapper.sh" 2>&1 <<'J'
{"tool_input":{"command":".claude/scripts/bq_run.sh --ticket T --label x --project_id dw-main-silver 'SELECT 1'"}}
J
); rc=$?
[ $rc -eq 0 ] && ok "enforce_bq_wrapper: bq_run.sh wrapper allowed (exit 0)" || bad "enforce_bq_wrapper[allow]" "exit=$rc want 0"
out=$(bash "$H/enforce_bq_wrapper.sh" 2>&1 <<'J'
{"tool_input":{"command":"bq query --dry_run 'SELECT 1'"}}
J
); rc=$?
[ $rc -eq 0 ] && ok "enforce_bq_wrapper: --dry_run allowed (exit 0)" || bad "enforce_bq_wrapper[dryrun]" "exit=$rc want 0"

# 2. comms_lint_precheck.sh — advisory (exit 0); a non-Jira command is a silent no-op
out=$(bash "$H/comms_lint_precheck.sh" 2>&1 <<'J'
{"tool_name":"Bash","tool_input":{"command":"ls -la"}}
J
); rc=$?
[ $rc -eq 0 ] && ok "comms_lint_precheck: non-Jira no-op (exit 0)" || bad "comms_lint_precheck" "exit=$rc want 0"

# 3. flag_net_new_tables.sh — PostToolUse; a non-bq_run command is a no-op (exit 0), no queue write
out=$(bash "$H/flag_net_new_tables.sh" 2>&1 <<'J'
{"tool_input":{"command":"cat knowledge/START_HERE.md"}}
J
); rc=$?
[ $rc -eq 0 ] && ok "flag_net_new_tables: non-bq_run no-op (exit 0)" || bad "flag_net_new_tables" "exit=$rc want 0"

# 4. memory_recall.py — fires on a strong keyword match; silent on a harness re-invocation.
# Use a NON-hot-tier topic (hot-tier memories are deliberately excluded from the recall map).
out=$(python3 "$H/memory_recall.py" 2>&1 <<'J'
{"prompt":"who owns MNTN frequency capping and what is the counter key"}
J
); rc=$?
{ [ $rc -eq 0 ] && grep -q "memory-recall" <<<"$out"; } && ok "memory_recall: injects on keyword match" || bad "memory_recall[hit]" "exit=$rc, out='$out'"
out=$(python3 "$H/memory_recall.py" 2>&1 <<'J'
{"prompt":"<system-reminder>background task has completed</system-reminder>"}
J
); rc=$?
{ [ $rc -eq 0 ] && [ -z "$out" ]; } && ok "memory_recall: silent on harness re-invocation" || bad "memory_recall[skip]" "exit=$rc, out='$out'"

# 5. log_request.py — exercised on its SKIP path (harness marker) so no local-log write happens
out=$(python3 "$H/log_request.py" 2>&1 <<'J'
{"prompt":"<task-notification>background task has completed</task-notification>"}
J
); rc=$?
[ $rc -eq 0 ] && ok "log_request: harness-marker skip (exit 0, no write)" || bad "log_request" "exit=$rc want 0"

# 6. session_start_routing.sh — prints the orientation banner (no stdin)
out=$(bash "$H/session_start_routing.sh" 2>&1 </dev/null); rc=$?
{ [ $rc -eq 0 ] && grep -qiE "AI Workflow Kit|Retrieval|Coverage" <<<"$out"; } && ok "session_start_routing: prints orientation banner" || bad "session_start_routing" "exit=$rc, out head='$(head -1 <<<"$out")'"

# 7. capture_reminder.sh — advisory Stop hook (exit 0)
out=$(bash "$H/capture_reminder.sh" 2>&1 </dev/null); rc=$?
[ $rc -eq 0 ] && ok "capture_reminder: advisory (exit 0)" || bad "capture_reminder" "exit=$rc want 0"

# 8. comms_cap_reminder.sh — advisory Stop hook (exit 0)
out=$(bash "$H/comms_cap_reminder.sh" 2>&1 </dev/null); rc=$?
[ $rc -eq 0 ] && ok "comms_cap_reminder: advisory (exit 0)" || bad "comms_cap_reminder" "exit=$rc want 0"

# 9. oncall_triage_reminder.sh — advisory Stop hook (exit 0)
out=$(bash "$H/oncall_triage_reminder.sh" 2>&1 </dev/null); rc=$?
[ $rc -eq 0 ] && ok "oncall_triage_reminder: advisory (exit 0)" || bad "oncall_triage_reminder" "exit=$rc want 0"

echo "hooks self-test: $PASS passed, $FAIL failed."
[ $FAIL -eq 0 ]
