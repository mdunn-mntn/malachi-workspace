---
name: feedback_test_must_not_share_code_assumption
description: "A test that encodes the same assumption as the code cannot fail the way the code fails. Model the host you don't control from observed behavior, and make the harness go RED on the live symptom before fixing anything"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [test shares code assumption, harness models the host, jsdom harness, innerHTML replace vs append, innerHTML assigned scripts never execute, reproduce the failure first, red before green, green test while user still sees the bug, duplicate root element, mode layout injection, integration harness realism, environment assumption, AUDI-1213 mode port]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-09-03
---
**A test that encodes the same assumption as the code cannot fail the way the code fails.** When the code under test hands off to a host you do not control (a BI platform, a browser, a rendering surface, an orchestrator), the harness's model of that host is a hypothesis about it. If that hypothesis is the same one the code makes, the suite stays green on exactly the failure the user is looking at.

**The case that earned it (2026-09-03, the AUDI-1213 MDE calculator port to Mode).** The jsdom harness replayed Mode's layout injection as `host.innerHTML = frag`, which REPLACES and leaves exactly one root element, and then hand-recreated every `<script>` so it would run. Both halves were assumptions about Mode and both were wrong. Mode can APPEND a re-injected layout, so the document holds two copies of the root, duplicate ids make `document.getElementById` resolve to the stale FIRST copy, and the visible second copy never boots. And the failure the user actually kept reporting was the other half: Mode re-renders the report body by ASSIGNING HTML, so scripts inserted that way never execute and a Refresh leaves fresh placeholder markup with no JavaScript running at all. A harness that re-creates the scripts by hand can never see that, so it passed clean through three rounds. Fixed by running three cases: a first load with scripts executing, a re-render that appends into a wrapper div, and a re-render where the scripts are NOT re-created, which must still render via the watchdog installed on first execution. Mode-side mechanics: [[reference_mode_dashboard_porting]].

**How to apply:**
- **Write down the host assumption before writing the harness** (replace vs append, one instance vs many, event ordering, timing, idempotence) and derive it from OBSERVED host behavior (dump the real DOM, log, or output), not from how your code expects the host to act.
- **Make the harness go RED on the reported symptom before fixing anything.** A harness that cannot reproduce the live failure is not testing it, whatever it says when it passes.
- **Assert the invariant the host can break** (here: the surviving root count, and that the page still renders when the host does not re-execute the scripts), not only the happy path.
- **Signature to watch for: green tests plus a symptom the user still sees, twice or more.** At that point stop editing the code and go audit the harness's model of the environment.

Related: [[feedback_validated_is_not_correct]] (a fixture built from the answer you want proves the invented shape only), [[feedback_self_qa_before_shipping]] (verify the render, not the object), [[feedback_hold_evidenced_verdict]].
