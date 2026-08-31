# Missed debugger replies + open failures, 2026-08-29

## Un-replied alerts: root cause is the PAGING_TAGS filter

The rapid sweep only considers failures from DAGs tagged `tpa` or `Machine Learning`
(`include/airflow_debugger/daily.py` PAGING_TAGS). Both missed alerts came from DAGs whose tags
fall outside that list:

| Alert (channel, PT time) | DAG | Its tags | Why missed |
|---|---|---|---|
| monitor-tpa 08-29 07:03 | `fetch_common_crawl` | `common_crawl_content`, `vertical_categorization` | not in PAGING_TAGS |
| monitor-tpa 08-28 14:40 | `audience_intent_scoring_household_14day_lookback` | `audience_intent_scoring`, `ipdsc`, `ml`, `fangorn` | `ml` != `Machine Learning` |

The rapid runs themselves were healthy (every 15 min, all success; windows covered both failure
times). The failures were simply never candidates. Fix: broaden PAGING_TAGS (add `ml`,
`vertical_categorization`, `common_crawl_content`, `audience_intent_scoring`) or drop the tag
filter and let the channel search decide. Small PR in airflow-ti.

Note: alerts-tpa-pipeline had NO un-replied failures since delivery went live; the two 08-29
alerts there got replies (8:16, 10:31). The un-replied pair are both monitor-tpa.

## Matt/Sean's incrementals chain: NOT fixed

All three 08-28 runs still failed: `mntn_match_incrementals_submit` (batch_submit failed try 5),
`mntn_match_incrementals_fetch` (batch_post.openai_batch_joined failed try 10 + upstream_failed
fan-out), `keyword_ddp_reporting` (sensor). batch_submit try-5 log:

    ValueError: Inconsistent state between openai_batch_submissions and
    openai_batch_input_formatted locations. Expected 1035 == 1035 + 1102

Sean's "try 1 succeeded but retries failed": try 1 submitted and recorded 1102 batches before
the task died, so every retry's consistency check sees submissions ahead of the formatted-input
count and refuses to resubmit. This is a guard doing its job, not flakiness. Recovery follows
Matt's order but step 1 cannot just be "rerun submit": the inconsistent state must be cleared
first (batch_cleanup_1 / delete_all_storage_files wipes the OpenAI file storage, then
batch_prep rebuilds), i.e. clear the submit DAG run from batch_cleanup_1, not from batch_submit.
Then fetch from batch_transition, then clear keyword_ddp_reporting
(reference_mntn_matched_batch_pipeline: submit-D before fetch-(D+1)).

## Debugger-channel failures (own-channel digest)

- `vertical_classification_api/response_tests` — 5 timeout diagnoses in one day, all identical:
  killed at 45m limit, successes run 39-42m. Chronic, not incident. The debugger's own fix is
  right: raise execution_timeout 45m -> 68m; one-line PR in airflow-ti. Latest run succeeded, so
  it flaps until the limit moves. The same task also tripped a dbt test once (route to model
  owner) and lost a pod once (recovered).
- `ipdsc_monitor/monitor_ipdsc_42` (unclassified) and
  `fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline` (unclassified) — both DAG
  runs later succeeded; transient. Worth pulling their logs into synth.py to add signatures so
  the class is named next time.
- `mntn_match_verticals_precache_v1_1/pre_cache_verticals` — pod never reached Running in 120s
  budget, then a pod-eviction match; later runs success. Cluster capacity blip, no action.

## After-action plan (user directive 2026-08-29, run once the recovery chain completes)

1. Ticket per issue, filed per Bryce Wagg's conventions (one spike per multi-item
   investigation, feedback_one_spike_multi_item): debugger improvements (AUDI, covers items
   below); flag the OpenAI batch pipeline hardening (dead-cohort detection, resubmit runbook)
   for Matt's team rather than filing on their board uninvited. Write the incident + fixes up
   in Confluence, and prep talking points: the team discusses these issues Monday 2026-08-31.
2. Debugger PR round 2: new signatures for the openai inconsistent-state guard and the
   dead-cohort PATH_NOT_FOUND; cross-DAG root-cause walk (IMP-096, the 10:31 reply stopped one
   layer short); signatures for the two unclassified (ipdsc_monitor, fangorn_hhid challenger);
   rapid-sweep lookback watermark (IMP-095); the 4 reply-clarity fixes below.
3. Verify #1248 after merge: next missed-tag failure gets a reply.
4. Recovery chain state at write time: submit-08-27 resubmission running under orchestrator
   (delete of submissions ledger done, Matt-approved in thread); fetch/keyword_ddp automated.

## Reply clarity (user feedback: Fix/Why not clear)

Weak spots seen today, all fixable in the signature texts / slack_block.py rendering:
1. "How it failed: matched on \"istio check\"" / "matched on \"2 of 4 FAIL 11620\"" — internal
   match strings leak as the explanation. Render "matched a known failure pattern
   (<plain name>)" and keep the match string out of the reader path.
2. The external-task signature (10:31 reply) is one 90-word sentence carrying three INC
   references the reader can't resolve. Split into: state check first, then the two branches as
   numbered steps, drop INC ids from Slack (keep in runbook).
3. "Fix: Route to the model owner: either the source data is wrong or the test bound is" — name
   the owner when known (model -> owner map), else say where to find them.
4. Unclassified white-circle replies say "Open the task log" without linking the log line-range
   it already fetched. It has the log; attach the tail.

## 2026-08-30 update: the OpenAI failure is systemic, not one dead cohort

Session died overnight (orchestrator killed with it). State at 15:00 UTC:
- submit-08-27 resubmission succeeded (1067 batches, submitted 06:59-08:16 UTC 08-30).
- Today's scheduled fetch-08-29 ALSO failed on missing `openai_batch_results/dt=2026-08-28`.
- Tracking flags: dt=2026-08-28 0/1102 submitted after its transition ran 22h post-submit
  (same dead pattern as 08-27); dt=2026-08-29 0/971 (submitted 10:45-11:54 UTC today, too
  early to judge); dt=2026-08-27 resubmission 0/1067 (transition-only probe launched to check).
- 08-26 was the last healthy cohort. Every cohort submitted since 08-28 morning UTC dies at
  OpenAI after creation: batches get ids (creation succeeds, so this is NOT the INC-007 file
  storage quota), then never reach in_progress/completed. Org-level cause at OpenAI (billing /
  enqueued-token limit / project limits) is the leading hypothesis; only the API key holder can
  read the batch error field. Resubmitting more cohorts is pointless and costs money until that
  is read. No further resubmits.
- Sample ids for the owner to retrieve: dt=08-27 `batch_6a93d4e7cedc819089960bc1e6e172fa`,
  dt=08-28 `batch_6a92b590d1c8819088c7b793924afb2e`, dt=08-29 `batch_6a9409b205408190b3224cd5c9d115ab`.
- Pod stdout ([base] lines) is not captured in the transition/fetch task logs, so per-batch
  statuses never reach Airflow. Debugger improvement: have the batch runner print each batch's
  status + error on transition (shopper_graph change, owner's repo).
- Probe result 15:30 UTC: the resubmitted 08-27 batches are 0/1067 in_progress-or-completed 7h
  after submit (validation takes minutes, so they are failing, not queued). Resubmission is
  confirmed dead too. Blocker is entirely OpenAI-side; awaiting the key holder's status read.
- 2026-08-31 16:10 UTC check: four dead cohorts now (08-27 through 08-30, 0 transitioned of
  1067/1102/971/906). Outage ongoing; with the OpenAI reps via Alyson since 08-30. No resubmits.

## 2026-08-31 17:50 UTC: the error text arrived (screenshot readable at last)

Every failed batch shows one error, e.g. batch_6a9569157a248190a9cd78bba9e2813b (created
08-31 07:44 PT, failed 07:45, 0 total requests):

    Cannot find file file-25XZLo4rMGap7MD1aeXCcu, or organization
    org-ldKlX0Pr81MhoY05W9t6oB1V does not have access to it.

Code-side facts (shopper_graph, read 2026-08-31): batch_base builds ONE client,
OpenAI(api_key=os.getenv("OPENAI_API_KEY")), no organization/project set anywhere.
create_batch does files.create then batches.create back-to-back on that client, so the
file exists under the key's own scope seconds before the batch references it. delete_file
removes only the LOCAL temp file (os.remove), never the OpenAI file. Submitter last
changed 2025-12-17, base 2025-10-01: our code did not change when the outage started
(08-28 06:00 PT).

Diagnosis: OpenAI-side org/project scoping break. The same key uploads the file and
creates the batch, yet async validation ~50s later says org-ldKlX0Pr81MhoY05W9t6oB1V
cannot see the file. Either the key was rotated ~08-27/28 to one with mismatched
org/project bindings, or OpenAI changed the org/project mapping (Ryan Kleck's MNTN org
needing reauthentication is consistent with an org-level event).

Discriminating asks for the OpenAI reps / key holder:
1. Under org-ldKlX0Pr81MhoY05W9t6oB1V, retrieve file-25XZLo4rMGap7MD1aeXCcu (GET /v1/files).
   Found -> validation bug on their side. Not found -> the upload landed in a different
   org/project: ask which org/project the key sk-...(pod OPENAI_API_KEY) belongs to.
2. Was the key or the org/project structure changed on 08-27/28? Anything in their audit
   log at 08-28 06:00 PT onset?
3. If scoping is the cause: fix = re-issue one key whose default project both stores
   files and runs batches; then we resubmit all dead cohorts (08-27..30) per the
   documented recovery.

## 2026-08-31 18:20 UTC: dashboard access confirms OpenAI-side fault

Malachi now has org access (Okta enterprise SSO; the Google-auth path fails with "Could not
access the organization"). Verified in the UI, org MNTN / Default project:
- File batch_requests_2026-08-31_9wvi6hey.jsonl (file-GxCYLt544LNshMxVX1es4c) EXISTS,
  status Ready, created 08-31 06:04, expires 09-30.
- The batch created 06:04:41 referencing that exact id failed 06:05:42 with "Cannot find
  file file-GxCYLt544LNshMxVX1es4c, or organization org-ldKlX0Pr81MhoY05W9t6oB1V does not
  have access to it."
- File visible and Ready in the same org+project the batch runs in, yet validation denies
  access: the fault is inside OpenAI's batch validation, not our upload scoping. The org
  hypothesis narrows: not a key/org mismatch on our side.
- That 06:04 batch is a MANUAL test (name batch_requests_*, not our part-*-tid-* naming),
  so a second producer hits the identical failure: org-wide.
- Our pipeline's failed batches sit in the same Default project (part-* files visible in
  Storage alongside it).

Escalation line for the OpenAI reps: file exists and is Ready in org
org-ldKlX0Pr81MhoY05W9t6oB1V / Default project, batch created 40s later in the same
project fails claiming the org cannot access it; every batch since 08-28 06:00 PT fails
identically. Request: audit-log check for the org at that onset + fix; nothing to change
on our side.
