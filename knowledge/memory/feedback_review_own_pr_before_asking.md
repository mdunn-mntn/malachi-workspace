---
name: feedback_review_own_pr_before_asking
description: "Adversarially review your own PR before asking a human. Grep the diff for personal paths INCLUDING inside binary fixtures, and check that CI actually runs the tests you claim pass."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [PR review, adversarial review, self review, personal paths, home dir leak, environment assumptions, green tests wrong environment, hardcoded profile, service account not copied, wiz finding not ignore, page_limit page_offset, run it live once, binary fixture, zstd fixture, committed test data, unenforced tests, CI path filter, workflow paths, vendored package, ruff config, swallowed failure, green run empty report]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-08-24
---
**Before asking anyone to review a PR, review it adversarially yourself.** On airflow-ti#1212
(AUDI-1194) a multi-reviewer pass raised 87 findings, 47 of which survived an independent attempt
to refute them, on a branch two humans had already commented on and one had approved.

**Why:** reviewers catch what is visible in a diff. The three worst defects were not.

**How to apply.** Three checks that each caught something no human reviewer would have:

**1. Grep the diff for your own environment, and decompress anything binary first.**
`/Users/<you>`, `$HOME`, `/tmp/...`, session UUIDs, your username, your email, paths to files that
exist only in your workspace. Ryan Kleck spotted one in a `.py` fixture generator. The same
identity was **also inside both committed `.zstd` test fixtures** — home dir, `user.name`, Java
extension paths, and a scratchpad path with a session UUID — invisible in a diff because they are
compressed, and shipped into a shared prod repo. Sanitize by decompressing, substituting, and
**recompressing with the same frame count**: real Spark logs are many concatenated zstd frames,
and collapsing to one silently stops exercising the `read_across_frames` path.
```bash
grep -rIl "$USER\|/Users/\|/tmp/" <paths>            # text
for f in **/*.zstd; do zstd -dc "$f" | grep -c "$USER"; done   # binary
```

**2. Check that CI actually runs the tests the PR claims pass.** Read the workflows' `paths:`
filters against `git diff --name-only origin/main...HEAD`. #1212 said "49 tests pass"; **no
workflow ran them** — `pr_model.yaml` filters on `models/**` and the repo's `tests/` suite is
never invoked. A vendored package also needs its **lint config vendored with it**, or CI lints it
against the host repo's rules; that gap only appeared once a job existed to expose it.

**3. Ask what each error path does when it fails, not whether it can fail.** Every high finding
was the same defect in a different place: **a failure was swallowed and the job published a
confident wrong answer.** A failed GCS listing returned `[]` (a 403 read as a quiet day); a failed
ledger fetch would have overwritten the published history with a one-day file; a partial download
made the digest announce never-scanned jobs as "Stopped firing". That is the exact failure mode
the ticket exists to fix — "a stale token gives a green run and an empty report" — rebuilt inside
the fix. **Silence is the dangerous outcome, not a crash.**

Run it as a workflow of independent reviewers plus one refuter per finding: 40 of 87 claims were
refuted from source, including a confident "the ledger orders history by date" that is simply not
what the code does. **An unverified review finding is a hypothesis**, same as
[[feedback_hold_evidenced_verdict]] in the other direction.

Related: [[feedback_branch_from_origin_not_local_main]] (the other thing a diff hides),
[[reference_gcs_iam_creator_vs_user]] (two IAM defects from the same PR).

## It recurred on the sibling PR one day later (AUDI-1191 #1214, 2026-08-21)

Same class, same week, on the failure-debugger PR — and the personal path went in **one day after
I wrote a document telling other people not to do it**. The user caught it by reading the diff
("that's an embarrassing rookie mistake"). Two leaks, both into a shared prod repo:

- `PROFILE = "malachi@mountain.com"` in `databricks_rca.py` — a named human's OAuth profile.
- `Path.home() / "Developer" / "work" / "mntn" / "airflow-ti"` in `report.py`, as a *fallback that
  never fires*, which is worse: dead code whose only content is a personal directory layout.

**All five blocking defects were ENVIRONMENT assumptions, and that is the pattern.** Not logic
errors — assumptions about what identity it runs as, what filesystem it sits in, what binaries
exist, and what the scheduler passes in:

| Defect | Assumption |
|---|---|
| Hardcoded profile | a person's credential is available |
| `Path.home()` fallback | it runs from a laptop checkout |
| SA had no `logging.viewer`, zero bindings in the second project | "copies the optimizer" implies the same access |
| `external_task_rca` imported a workspace script and shelled to `astro` | the repo tree and CLI are present |
| `data_interval_start.subtract(days=1)` | the scheduler passes what I assumed |

**Ruff, 106 tests and `compileall` were all green.** None of them can catch any of the five,
because tests exercise the code against the developer's environment — the one environment it will
never run in. Green locally is evidence about the laptop, not about prod.

**Two techniques that actually work, in order of cost:**

1. **A grep over the diff, as a CI step rather than a habit.** It takes a minute and catches leaks
   1, 2 and 4 mechanically:
   `grep -rnE 'Path\.home\(\)|/Users/|Developer/work|@mountain\.com|\.databrickscfg|\.zshrc'`
   A habit failed twice in one week; the CI step cannot.
2. **Get the real credential and run it once.** The first live run with a real Astro token found a
   422 in thirty seconds — the POST body takes `page_limit`/`page_offset`, not `limit`/`offset`,
   so that call had **never worked**. 106 tests could not find it because none of them touch a
   live API. One real invocation beats any amount of mocking for environment defects.

**On identity specifically: "it copies X's service account" is not a check.** The optimizer reads
event logs from one bucket; the debugger reads Cloud Logging and a second GCP project. Verified
against live IAM, the shared SA had **only** `dataproc.viewer` and **zero** bindings in the
targeting project. The DAG would have run green and published a report where every finding read
`driver log fetch failed` — thin results, not a visible break, which is the worst failure mode.
**Ask what the workload READS, then read the live policy.** See
[[reference_gcs_iam_creator_vs_user]] and [[reference_mntn_devops_permissions]].

**Also: a security scanner finding is a defect to fix, not a rule to suppress.** Wiz flagged
colleagues' names in the vendored incident corpus (`resolved_by`, `note`, `action`). The fix was
to project the corpus onto the nine fields the matcher actually reads, so the names are *gone*,
plus a test enforcing the allowlist. `#wiz_ignore` was available and would have been wrong.
