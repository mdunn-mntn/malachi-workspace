---
name: feedback_validated_is_not_correct
description: "Validated on N incidents is not correct on the fleet — before trusting a parser/classifier, sweep it against the full real corpus, verify review claims by execution, and guard precedence lists with an order-integrity test"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [validated is not correct, happy path validation, full corpus sweep, adversarial code review, execution-verified claims, reproduce before it counts, order-integrity test, first-match-wins precedence, precedence rot, regression test per fix, classifier taxonomy review, fleet correctness, shared-resource sweep, call-site sweep, incomplete fix, fix recurrence, INC-012 v2]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-06
---
**"Validated on N incidents" is NOT "correct on the fleet."** A tool that has passed every live case it met can still be riddled with defects — the AUDI-1191 debugger had passed 6 live incidents and still held **40 execution-confirmed defects** when reviewed against the whole corpus (2026-08-06; 37 fixed same day).

**Why:** happy-path validation samples only the paths that already worked. Live incidents arrive one at a time and each exercises one route through the code; the routes those incidents never took (other log shapes, other signature orderings, other engine branches) stay untested forever unless you deliberately sweep them. Passing N real cases proves those N routes, nothing more.

**How to apply — the discriminating technique (all three legs):**
1. **Review each module against the WHOLE corpus of real prod artifacts** (for the debugger: all 64 real logs in `on-call/airflow_logs/`), not the incidents it already passed. The corpus is the fleet in miniature; the incidents are a biased sample of it.
2. **Adversarial verification where a skeptic must REPRODUCE each claim by running code before it counts.** A review finding that hasn't been executed is a hypothesis (40 of 41 claims survived execution; 1 was refuted — the refutation is as valuable as the confirmations).
3. **Per-module fixers with a mandatory regression test per fix** — a fix without a test is the next silent regression.

**Sub-lesson — any first-match-wins taxonomy needs an ORDER-INTEGRITY test:** run every case text through the FULL ordered list and assert the expected key wins, or precedence silently rots as entries are added. Concretely: `executor_lost` stole `gcs_list_timeout` on the real INC-012 driver blob — meaning the tool would have repeated the exact human misdiagnosis ("lost executors") it exists to prevent. The guard is `test_order_integrity` in `airflow_debugger/tests/test_signatures.py`.

Before calling a parser/classifier "working": sweep it against the full real corpus, add order-integrity tests to precedence lists, and verify review findings by execution — not by plausibility.

**Addendum — INC-012 fix v2 (2026-08-06): a fix validated against the traceback's call site is not a fix for the resource.** Fix v1 (literal region paths, airflow-ti#1176) was necessary but not sufficient — the next prod run failed identically ON the new code, because the read's `basePath` option statted the same huge root prefix through a different call site. The miss was not asking "what OTHER call sites touch the failing resource"; a shared-resource sweep at v1 review would have caught `basePath`. Counterpoint worth keeping: the recurrence was diagnosed in ~10 minutes because the evidence pipeline was already proven (deployed-script timestamp check + `driveroutput.*` under PAM + batch describe) — a wrong fix costs little when re-diagnosis is cheap. Mechanism detail: [[reference_airflow_ti]].

Related: [[feedback_self_qa_before_shipping]] (mechanize enforcement, verify the render), [[feedback_hold_evidenced_verdict]] (test-first verdicts), [[feedback_adversarial_workflow_authoring]] (multi-agent verify mechanics), [[project_airflow_debugger]] (the case).
