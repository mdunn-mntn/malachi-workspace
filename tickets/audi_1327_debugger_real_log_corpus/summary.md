---
doc_type: ticket
title: "AUDI-1327: Pin the debugger replies to real logs, then fix the downstream-cause parser"
status: in_progress
date: 2026-09-04
summary: "Real-log fixture corpus per signature class, then fix ordering, collapsed stacks, db_unreachable"
result: "PR 1287 open, gauntlet PASS"
question: "Can the debugger's replies be pinned to logs in the shape production emits, so the 2026-09-03 conversion_signal_backfill failure renders its real cause and a fixture-shape regression fails CI rather than Slack?"
framing_state: locked
---

# AUDI-1327: Pin the debugger replies to real logs, then fix the downstream-cause parser

**Jira:** https://mntn.atlassian.net/browse/AUDI-1327
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Can the debugger's replies be pinned to logs in the shape production
  actually emits, such that the 2026-09-03 `conversion_signal_backfill_workflow/submit_batch_dsid_21`
  failure renders its real cause and a fixture-shape regression fails CI rather than Slack?
- **Goal (why / the decision):** PR #1285 shipped with 53 of 53 tests green and does nothing on the
  exact production failure it was written for. Every reply defect to date was caught by a person
  seeing a bad Slack post. Until the corpus exists, the test suite certifies the wrong thing, and
  nothing downstream (AUDI-1328's validation, AUDI-1329's coverage, the AUDI-1325 LLM layer) can
  trust a reply.
- **Objective (done-when):** That failure renders `java.net.SocketTimeoutException: Connect timed
  out`, raised through `org.postgresql.util.PSQLException`, at `spark_read_host.py:27`, from the
  captured real log, pinned in CI; and every signature class that has a real prod example carries
  one fixture and one golden rendered reply.
- **Approach (how):** Corpus first, parser second, so the same class of defect cannot ship green
  again. Sources: 44 signature classes in `include/airflow_debugger/signatures.py`, ~600 real task
  logs already pulled under `on-call/airflow_logs/` (432 on 2026-09-03), and Cloud Logging for the
  Dataproc driver logs. Assumptions to resolve empirically first: how many of the 44 classes have a
  real prod example at all, and whether Cloud Logging's shape (descending order, tab-joined stacks)
  is stable across engines and fetch paths or varies by API call.
- **What would change the answer:** If most of the 44 classes have no real prod example, the corpus
  cannot be complete and the deliverable becomes "every class that fires in practice" plus a named
  gap list, not "every class". If the descending order turns out to be a flag on our own fetch call
  rather than the API's behaviour, the ordering half is a one-line fix and the ticket shrinks to the
  tab-collapsed stacks plus the corpus.

## 1. Introduction
`include/airflow_debugger/` diagnoses FAILED Airflow tasks and posts the reply to Slack. PR #1285
(2026-09-03) was meant to make a wrapper failure name the exception its downstream job ended on. It
does not, on the exact case it was written for, while 53 of 53 tests pass. This ticket builds the
real-log corpus first, then fixes the parser under it.

## 2. The Problem

**The defect is wider than #1285.** `error_region` is destroying the signature on every real driver
log, not just the acceptance case. Over the 5 real Dataproc driver logs still retrievable:
`classify(error_region(text))` returns `None` for 5 of 5, while `classify(full_text)` returns a
signature for 2 of 5. `analyze_batch` (`dataproc_rca.py:478`) classifies `state_message +
error_region(logs)`, so the whole Spark-side taxonomy is being fed a window that has walked away
from the error.

**Why the window walks away.** Cloud Logging is fetched with `--order desc`, so entries arrive
newest first, and it had already split the Python traceback across entries. In the real 14,960-char
acceptance slab, `Traceback (most recent call last):` occurs once at index 9892, while the three
strings we need sit at 5812 (`org.postgresql.util.PSQLException`), 8416
(`java.net.SocketTimeoutException`) and 9763 (`spark_read_host.py`) — all BEFORE the anchor. So
`text[9892:11892]` runs forward into older entries. **The fix is to reverse the ENTRY LIST before
joining, not to swap `rfind` for `find`:** the frames precede their own header because the entries
were reversed, not the characters.

**The stack is tab-joined, so the chain parser never matches.** The real text reads
`...Thread.run(Thread.java:840)Caused by: java.net.SocketTimeoutException: Connect timed out\tat
java.base/sun.nio.ch...` — no newline before `Caused by:`, frames glued with `\t` (84 tabs across
81 lines). `parse.py:112` `_CAUSED_BY` is `^\s*Caused by:` with `re.MULTILINE`: of 5 `Caused by:`
occurrences only 1 starts a line, and it is the wrong one (a `SparkException` awaitResult wrapper).
`_EXC_LINE`'s `[^\n]*` then swallows the whole tab-joined frame run.

**`db_unreachable` is not a signature bug.** `classify()` on the FULL driver text returns
`db_unreachable` correctly; it returns `None` only on `error_region`'s output. Fixing the ordering
plausibly fixes this with no change to `signatures.py`. Prove that before touching the taxonomy.

**Driver logs live 7 days, and the code assumes 45.** `gcloud logging buckets list
--project=mntn-prj-prod-00` shows `_Default` `retentionDays=7` (the 400-day bucket is `_Required`,
admin audit only). `dataproc_rca.py:40` sets `_LOG_FRESHNESS="45d"`, 6.4x wider than the data that
exists, so its "no driver log via Cloud Logging (check freshness window)" note misdirects. Of 27
distinct failed Dataproc batches probed, only 5 returned any text; the oldest survivor is
2026-08-29. **This caps the corpus permanently**, and it is why the Spark-side half of the taxonomy
(OOM, shuffle, executor loss, TTL, GCS listing) has zero retrievable real examples today.

**The taxonomy is 48 emitted keys, not 44.** `signatures.py` holds 44, and `external_task_rca.py`
emits four more that are not in `SIGNATURES`: `external_task_target_failed` (fired 6x in 13 days of
published prod RCA), `external_task_target_skipped`, `external_task_target_unfinished`,
`external_task_window_mismatch`. A corpus scoped to "the 44" silently omits four live classes.

**All 44 signatures carry `engine="any"`**, so `classify()`'s engine filter (`signatures.py:651-652`)
is dead code. Any per-engine corpus split has to come from the log, not the signature.

## 3. Plan of Action
1. Framing locked 2026-09-04.
2. Design: four probes (inventory, corpus harness, parser fixes, CI gate), three adversarial reviews
   each. Done: 12 reviews, 3 refuted, ~36 required corrections.
3. Build the corpus with redaction, then fix the parser under it, then the CI gate.
4. `/pr_gauntlet`, then PR.

## 4. Investigation & Findings

**Corpus scope, from scanning 3,627 real logs across 33 dates (2026-07-28 to 2026-09-04) with the
package's own `parse_log()` + `classify()`.** States: 3,068 success, 245 failed, 173 skipped, 137
upstream_failed. Of the 245 failed, `classify()` matched 215; 30 unmatched, 22 of which carry error
text and are therefore genuine taxonomy gaps.

| Bucket | Count | Meaning |
|---|---|---|
| (i) real example available now | 28 | fixture + golden reply |
| (ii) fires in prod, no artifact retained | 7 | named waiver; all live in a driver log or Vertex payload, never the Airflow task log |
| (iii) never observed in any source | 9 | named waiver |

Bucket (ii): `gcs_list_timeout`, `ttl_exceeded`, `quota_exhaustion`, `model_alias_not_found`,
`vertex_param_contract`, `executor_oom_yarn`, `driver_oom` — each named in an incident record
(INC-012/013, INC-005, INC-008, INC-024, INC-003, INC-016, INC-018) with the exact text, but with no
retained log. Bucket (iii) includes `max_distinct_paths_guard`, whose regex matches a string MNTN
prod code actually emits (`dags/attribution/url_pattern_pipeline.py:779-780`), so it is reachable
but unfired.

Engine split of the 245 failed logs: databricks 153, dataproc 31, other 28, vertex 26, unknown 7.

**The framing's kill criterion partly fired.** "Every signature class carries a fixture" is not
achievable: 16 of 48 have no retrievable example, 7 of them because of a 7-day retention we do not
control. The deliverable becomes every class in bucket (i), plus a CI-read waiver list naming the
other 16 so an unfixtured class is a recorded gap rather than an omission.

**Corrections the adversarial pass forced, carried into implementation:**
- The corpus must vendor the raw Cloud Logging **entry list** (`gcloud ... --format json`), not the
  `value()`-joined string. Defect (a) lives in the fetch (`dataproc_rca.py:240-247`); a frozen join
  captures the bug rather than the input.
- **Real logs carry secrets.** The acceptance log line 73 is
  `CURRENT vault https://vault.prod.in.mountain.com`, and it carries internal `10.140.4.x` addresses.
  Redaction is a gate on committing anything, not a nicety.
- The acceptance assertion must sit on the surface that renders the strings
  (`slack_block.why` / `followed_cause`), not on the parser alone.
- `error_region` has four call sites in two modules (`dataproc_rca.py:426,442` and
  `vertex_rca.py:150,194`), so a Vertex corpus entry is needed to lock the existing reversal in
  `vertex_rca._messages_text` against a shared-helper refactor.
- The 2000-char window is fragile: a sweep over the real slab returns the correct root only between
  4000 and 6000 chars, and 7000 gives the wrong `SparkException`. The bound must derive from the
  failure, not be a magic number.
- `exception_chain`'s "deepest = last" assumption breaks after reversal: post-reversal the last
  `Caused by:` in the window is the newest event, not the deepest cause.
- `inc012_driveroutput.log` (19,577 bytes, commit `c4ca3da`) is already in the repo and is a real
  ascending, newline-separated driver log. The corpus must pin BOTH shapes.
- The `gcs://dataproc-staging/driveroutput.*` fallback path (`dataproc_rca.py:434-444`) was not
  probed before declaring 22 of 27 driver logs lost. Probe it before finalising bucket (ii).
- `_JDBC_DRIVER_LOG` (`tests/test_parse.py:527`) is a synthetic reconstruction of this exact batch in
  the shape prod never emits. Delete or quarantine it rather than leaving it beside the corpus.
- The proposed `slack_block` gate widening was refuted on a real case: on
  `tpa_ipdsc_export/ipdsc_ds_65 try2` it appends a wrong message. Fire `followed_cause` only when the
  Spark dive itself produced the root.
- The proposed CI secret grep goes red on `origin/main` today: `gserviceaccount\.com` matches
  `dags/airflow_debugger_daily.py:37` and `dags/airflow_debugger_rapid.py:25`.

## 5. Solution
PR [SteelHouse/airflow-ti#1287](https://github.com/SteelHouse/airflow-ti/pull/1287), branch
`audi-1327-real-log-corpus`, 6 commits, 30 files, 1,976 lines (source +143, the rest corpus and tests).

| Commit | What |
|---|---|
| `d5618ba` | Five real prod failures vendored as the entry list their API returned, redacted, with a golden reply per case; `capture_case.py` records the fetch command |
| `41c3bf4` | `dataproc_rca._logging_messages` returns the driver log in emission order; `exception_chain` reads the failure's own stack via `stack_block`, and `unfold_stack` handles tab-joined frames |
| `ebee895` | Regold the three replies the parser fix moves |
| `0f04953` | `followed_cause` fires when the root is a wrapper OR the dive itself produced the root, pinned on the rendered reply |
| `ee797e7` | Coverage and shape gates: all 48 emitted keys captured or waived, a typed or reordered fixture fails the build |
| `807082e` | One-line docstrings on the code this branch adds |

**Acceptance met, on the rendered surface.** Driving `orchestrate.investigate` -> `slack_block.render`
with every external call served from the case's own capture, the Why line reads: "(followed
downstream) The job never reached the database. The socket timed out before any handshake, so this
is reachability, not a credential or a query... The dataproc job failed with
`java.net.SocketTimeoutException: Connect timed out`, raised through
`org.postgresql.util.PSQLException: The connection attempt failed`, at `spark_read_host.py:27` in
`pixel_isolation_advertiser_ids_df`."

**Verification.** All four CI steps reproduced locally: ruff clean, **332 tests** (baseline 282),
personal-paths grep clean, `compileall` OK.

**Recurrence proof, three ways.** Revert only the ordering fix: 9 tests red, including one that
prints the actual defect, a Why line ending mid-frame at `conversion_signal_backfill_process.py",
line 44,` — #1285's exact failure mode. Revert only the `exception_chain` half: 1 red, expected
`SocketTimeoutException`, got `ConnectException: Connection refused` (the driver-teardown exception
that fires after the real failure). Retype a fixture into ascending newline-separated shape: the
shape gate goes red.

**Secrets.** Every vendored byte was read and scanned against 23 patterns. Internal hostnames ->
`redacted-host.invalid` (13 occurrences; the `CURRENT vault https://...` line is present and
defanged), private IPv4 -> `203.0.113.1` (RFC 5737). Zero `mountain.com` anywhere in the diff.

**Gauntlet.** PASS on a clean round at `thorough`: skeptic 0, stylist 4, all 4 refuted. I re-checked
each refutation by hand: `_is_stack_line` and `_verified` are private, so `D103` does not apply;
`_one_line`'s word-boundary truncation drops at most one partial word from a safety collapse; the
description finding counted the workflow's argument string, not the linted PR body.

## 6. Questions Answered
- **Q:** Is the #1285 defect confined to the acceptance case?
  **A:** No. `error_region` destroys the signature on 5 of 5 retrievable real driver logs.
  `classify(full_text)` finds a signature on 2 of 5; `classify(error_region(text))` finds none on any.
- **Q:** Is `db_unreachable`'s regex wrong?
  **A:** No. `classify()` on the full driver text returns it correctly. It failed only on
  `error_region`'s output, so `signatures.py` needed no change.
- **Q:** Can every signature class carry a fixture?
  **A:** No, and this is permanent. Cloud Logging `_Default` retention is 7 days while
  `dataproc_rca.py:40` sets `_LOG_FRESHNESS="45d"`. 28 of 48 classes are captured, 16 are waived by a
  CI-read list with a review date, 4 are the `external_task_rca` keys that were outside `SIGNATURES`.
- **Q:** Is the fix a simple `rfind` -> `find`?
  **A:** No. Cloud Logging split the traceback across ENTRIES and `--order desc` reversed the entries,
  so frames precede their own header. The reversal has to happen on the entry list, below the
  `--order` flag, which is also where the replay seam had to sit for a fetch-order change to be
  observable in CI.

## 7. Data Documentation Updates
Routed by `/capture` to `project_airflow_debugger` (the Cloud Logging shape finding and the 7-day
retention ceiling) and `reference_pr_gauntlet`.

## 8. Open Items / Follow-ups
- **The corpus cannot be refreshed after 2026-09-10.** The acceptance capture's source expires with
  the 7-day retention, and CI has no gcloud auth, so nothing can compare the vendored bytes to prod.
  There is no drift detection.
- Waiver expiry warns rather than fails unless `DEBUGGER_WAIVER_EXPIRY=1`, which CI does not set, so
  all 43 waivers will warn past their review dates with nothing forcing the review.
- CGNAT worker addresses (`100.64.0.0/10`, RFC 6598) are vendored unredacted: not routable, not a
  credential, but the decision is undocumented.
- No size ceiling on the corpus. It adds 141,878 bytes across 17 files and `.dockerignore` does not
  exclude `include/`, so every capture ships inside the Astro prod image.
- `test_the_application_id_does_not_depend_on_the_fetch_order` is thinner than it looks: none of the
  three captures carries more than one `MCP_EVENT_LOGGING_CONFIG_BASE64` breadcrumb.
- `_LOG_FRESHNESS="45d"` still misstates the real 7-day window, so the "check freshness window" note
  misdirects. Not fixed here.

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
