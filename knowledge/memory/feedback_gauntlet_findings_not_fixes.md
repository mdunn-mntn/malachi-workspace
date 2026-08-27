---
name: feedback_gauntlet_findings_not_fixes
description: "Take the pr_gauntlet's findings, never its fixer's diff on faith — it deleted four working features to satisfy style findings. And an ambiguity guard over a truncated list is not a guard."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [pr_gauntlet, gauntlet fixer over-reach, THRASH verdict, FAIL_MAX_ROUNDS, adversarial review, deleted working features, ambiguity guard truncated list, MAX_REFUTERS_PER_ROUND, MAX_ROUNDS, gauntlet cost tuning, review findings vs fixes]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-25
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

Related: [[project_airflow_debugger]], [[feedback_validated_is_not_correct]], [[feedback_hold_evidenced_verdict]].
