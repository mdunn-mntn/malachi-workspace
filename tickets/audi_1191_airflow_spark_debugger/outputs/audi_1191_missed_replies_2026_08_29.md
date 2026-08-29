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
