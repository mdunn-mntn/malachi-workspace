---
name: feedback_gauntlet_findings_not_fixes
description: "Take the pr_gauntlet's findings, never its fixer's diff on faith — it deleted four working features to satisfy style findings; an ambiguity guard over a truncated list is not a guard; and a fixer that crashes mid-run leaves HALF-APPLIED edits in the tree (git diff before building on any post-gauntlet state)."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [pr_gauntlet, gauntlet fixer over-reach, THRASH verdict, FAIL_MAX_ROUNDS, adversarial review, deleted working features, ambiguity guard truncated list, MAX_REFUTERS_PER_ROUND, MAX_ROUNDS, gauntlet cost tuning, review findings vs fixes, monitoring.metricReader nonexistent, bogus iam role, external identifier verification, refuter confirmed wrong finding, mntn-devops 5224, crashed fixer half-applied edits, api server error mid-run, ERROR verdict findings unapplied, gauntlet resume cached agents, PR 1259]
domain: [workflow]
lifecycle: active
last_verified: 2026-09-01
---
**Take the gauntlet's FINDINGS. Do not take its fixer's diff on faith.** On airflow-ti#1217 (2026-08-25) the fixer deleted four of the five features the PR existed to ship, plus a whole module and its tests — working, tested code, removed to satisfy style findings about unused surface. The correct response was `git checkout <commit> -- <paths>` to restore the tested state, then re-applying ONLY the confirmed defect. Read the fixer's diff the way you would read a stranger's PR, because that is what it is.

**Why:** the reviewers and refuters are adversarial and evidence-bound, so their findings are usually right. The fixer is not adversarial — it optimises for making findings go away, and deleting the code a finding points at always does.

**How to apply:** on PASS, still diff the fixer's changes before committing. On THRASH or FAIL_MAX_ROUNDS, expect to restore and hand-apply.

**The two defects it caught were both the same shape, and both were mine:** a guard that looks sound and is not.
- `_run_holding` scanned `ranked[:12]` while its docstring promised "two candidates name neither". A second candidate past index 11 left ONE hit that read as unambiguous, so the wrong run's culprit was reported as fact. **An ambiguity guard over a truncated list is not a guard.**
- `notify.find_alert_ts` matched an alert on `dag_id` + `task_id`, with a docstring claiming "never wrong". The wrong matches are the common ones: a daily sweep diagnoses a closed day, so a task failing again today has a NEWER alert with the same names.

**Both had a docstring asserting the safety property the code did not have.** A confident docstring over an unverified guard is the tell.

**Cost tuning (superseded 2026-08-26 by tiers, kept for the reasoning).** First runs took 41 min / 30 agents / 1.6M tokens. Round 1 confirmed 10 findings; round 2 confirmed 0. Retuning to `MAX_ROUNDS = 2` cut a run to 23 min / 17 agents and still caught a blocker.

**Tiers replaced the single global setting (2026-08-26).** `fast` = 1 round, skeptic only, 3 refuters, effort `medium` (~13 min / 5 agents). `medium` = 2 rounds, both reviewers, 4 refuters (the old default). `thorough` = 3 rounds, 6 refuters, must converge clean. The tier is the first word of the `/pr_gauntlet` args and rides in `args.tier`.

**The bug the tiers fixed is the one worth remembering: the last round used to throw its own work away.** At the round cap the loop returned `FAIL_MAX_ROUNDS` with the round's confirmed findings UNFIXED, so a run that found anything in its final round always ended with open work and a wasted review. `fast` and `medium` now apply those fixes and return the new verdict **`FIXED_UNVERIFIED`** — the fixes are real, but no fresh agent has re-read them, so run the tests and the mechanical gate yourself before shipping. `thorough` still refuses to end that way. **Do not read `FAIL_MAX_ROUNDS` in an old transcript as "the code is bad"** — until 2026-08-26 it also meant "the loop hit its cap".

**Recurred 2026-08-28 (medium tier, airflow-ti #1245):** the fixer ran `ruff format` across ENTIRE
files, including files outside the review set (`test_ledger.py`, `test_sweep.py`), and replaced a
rich module docstring with one line. Both times the diff-review-like-a-stranger rule caught it;
the docstring was restored and only the behavioral fixes kept. Formatting sweeps and docstring
truncation are fixer over-reach shapes to expect alongside deletion.

**A fixer edit that names an EXTERNAL identifier must be verified against the owning system
before shipping (2026-09-01, mntn-devops PR 5224).** The fixer swapped `roles/monitoring.viewer`
for `roles/monitoring.metricReader` — a role that does not exist in GCP (the IAM API 404s on
it) — and the refuter CONFIRMED the finding instead of refuting it. Reviewers and refuters argue
from the diff and each other; none of them queries the system the identifier lives in. IAM
roles, API field names, env var names: check them against the real system (an API call, the
docs, the console) before shipping, the same way you would a stranger's PR that claims a new
permission exists.

**A fixer that CRASHES mid-run leaves HALF-APPLIED edits in the working tree (2026-09-01, twice in one evening).** Two gauntlet runs on airflow-ti #1259 (the pod point-order fix) died on API server errors mid-fixer before a third converged; one crash left an unused helper function, the other a dangling call to a function that was never written. **`git diff` the tree before building on ANY post-gauntlet state**, crashed or clean. **An ERROR verdict means the findings stand UNAPPLIED** — it is neither a pass nor a fail, and the confirmed findings still need hand-applying or a re-run. A resume of a crashed run replays its cached agents free, so re-running costs little.

Related: [[project_airflow_debugger]], [[feedback_validated_is_not_correct]], [[feedback_hold_evidenced_verdict]].
