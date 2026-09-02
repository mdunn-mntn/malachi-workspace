---
name: feedback_verify_agent_findings_before_relaying
description: "Adversarial review agents produce confident false blockers even after a default-refute verifier; the main session must check load-bearing claims against source before relaying them to a human."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [adversarial review, subagent hallucination, false blocker, pr gauntlet, refuter, verify before relaying, workflow findings, code review agents, import error claim, load-bearing claim]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-09-02
---
**Never relay a subagent's finding to a human without checking the load-bearing ones against source
yourself.** A default-refute verifier reduces the false-positive rate; it does not get it to zero.

**Evidence (2026-09-02, airflow-ti PR #1194).** A 10-lens adversarial review produced 98 raw findings;
after dedupe and an independent refuter pass, 82 survived as "confirmed", 12 of them blockers. Working
through them by hand cut that to 7 worth sending. The follow-up run on Sean's fix commits then returned a
**blocker that was simply false**: it claimed `from mntn_graph import GraphConfig, IdType` would raise
ImportError on Dataproc and take down two pre-existing production jobs. One `cat` of
`mntn_graph/__init__.py` showed `IdType` re-exported and listed in `__all__`. Relaying it would have sent
the user chasing a production outage that could not happen — right after they had approved the PR.

A second agent claim in the same run was subtler and also wrong: that a coverage metric was tautological
because its denominator came from the resolver's own output. Reading the library showed resolve mode
left-joins, so unmatched inputs survive with a null `household_id` and the denominator is the true input
count.

**Why:** these agents reason from plausible structure. "Symbol imported from a package root that other
call sites import from a submodule" and "denominator derived from output" are both *shapes* of real bugs,
and an agent that cannot open the artifact will confirm the shape. The cost is asymmetric — a missed real
bug costs a follow-up comment, a relayed false blocker costs the reviewer's credibility with the author.

**How to apply:**
- Rank findings by consequence, then personally verify every one you intend to send. The count that
  survives your own check is the real count; the workflow's "confirmed" tally is a candidate list.
- **Fetch the artifact the claim depends on.** When a finding turns on a third-party library's behavior,
  get the library. `gcloud storage cp` the zip and read it — that one step settled fan-out, tiebreak,
  shared-ID default, the `IdTypeFamily` state, and the staleness-guard reachability in a single pass, and
  it flipped several agent conclusions in both directions.
- Tell agents when an artifact is unreachable; they will hedge appropriately. Better, reach it first and
  put it in their context.
- State plainly in the writeup what you attacked and could not break. A "checked and clean" section is
  what makes the findings you *do* send credible.

Related: [[feedback_hold_evidenced_verdict]] [[feedback_background_work_liveness]]
[[project_fangorn_on_mntn_id]]
