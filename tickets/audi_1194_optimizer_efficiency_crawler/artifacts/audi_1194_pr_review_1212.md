# AUDI-1194 — adversarial review of airflow-ti#1212, 2026-08-21

Six reviewers over six dimensions, then one independent agent per finding told to **refute** it.
**87 findings raised, 47 survived, 40 refuted.** 93 agents. The refuted 40 are listed at the end
because what a review gets *wrong* is as useful as what it gets right.

The confirmed findings share one shape, and it is the shape this whole project exists to fix:
**a failure is swallowed and the task publishes confident wrong output instead of crashing.**
The PR description says the laptop cron's problem was that "a stale token produces a green run
and an empty report". The DAG reproduces that defect in four new places.

---

## HIGH (13)

### The append-only ledger in GCS is overwritten with a truncated copy whenever the pre-download silently fails

`dags/spark_optimizer_daily.py:66` · found by the **dag** reviewer · verifier confidence high

**Failure:** A transient GCS 503 or a 600s timeout hits the single ledger `gsutil cp` on line 66. n=0 is returned and ignored. record() classifies all of today's findings as state="new", streak=1. append() writes a fresh 40-line file. publish() copies it over the accumulated ledger. Every historical entry, every hand-set owner_notified/wont_fix decision, and every fix_pr attribution is destroyed in one command. The next day's digest reports the entire fleet as brand-new findings and nobody can tell it happened, because the task succeeded.

**Fix:** Distinguish "object absent" from "download failed": `gsutil stat` the ledger first and raise if it exists but the copy returned non-zero. Better, stop overwriting a single blob: write date-partitioned ledger objects (optimizer/ledger/dt=<date>.jsonl) and reconstruct history by reading the prefix, which also removes the read-modify-write race entirely.

<details><summary>verifier</summary>

Reproduced from source. dags/spark_optimizer_daily.py:66 discards fetch.download's return; fetch.py:55 skips any object whose gsutil cp exits non-zero and returns a count without raising or logging. ledger.read (ledger.py:75-76) treats the missing local file as an empty ledger, classify marks every finding new/streak=1, and append (ledger.py:156, "a" mode) creates a one-sweep file. sweep.run passes ledger_path into publish (sweep.py:108), which does a plain `gsutil cp` to {prefix}/{basename} (sweep.py:50-51) - a fixed object name, hence a full-object overwrite of gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl, permitted by the SA's optimizer/-conditioned storage.objectUser. That file is the only store for owner_notified/wont_fix/fix_pr: set_state (ledger.py:246) and mark_applied (ledger.py:266) merely append lines to it, and only backlog/coverage/digest get dated names, so there is no second copy. No guard exists: no -n flag, no generation precondition, no non-empty check, and the `n + phs_n == 0` early return (DAG:76-79) only covers a total event-log download failure, not a ledger-only one, so the task exits green. One correction to the rationale: the 600s-timeout branch is wrong - subprocess.run(timeout=600) at fetch.py:53-54 raises TimeoutExpired, which propagates out of the unwrapped line 66 and fails the task before publish; only the non-zero-exit path (missing/renamed object, IAM condition change, retries exhausted) is silent, which is sufficient. Severity corrected to high rather than blocker: it silently destroys the artifact the feature exists to produce and misreports the fleet as new, but it breaks no production pipeline, corrupts no upstream data, and leaks nothing.

</details>

---

### ds is not the date the data is selected by, so retries and cleared runs publish wrong-dated reports and corrupt ledger streaks

`dags/spark_optimizer_daily.py:51` · found by the **dag** reviewer · verifier confidence high

**Failure:** An operator clears the run for 2026-08-10 to re-test after a fix (routine on a shared deployment). The task re-runs today, downloads today's newest 200 logs, and (a) overwrites gs://.../optimizer/optimizer_backlog_2026-08-10.md and optimizer_digest_2026-08-10.md with content from 2026-08-21, and (b) appends ledger entries dated 2026-08-10 for findings observed today. ledger.classify (include/spark_optimizer/ledger.py:98-118) orders history by the `date` field: `past` now contains an out-of-order entry, `entry.streak = last["streak"] + 1` is computed off the backdated row, and `after_fix = [e for e in past if e["date"] > applied_date]` mis-counts, flipping keys to "fix_not_working" or resetting chronic keys to "new". The ledger's whole purpose ("how long has this been true") is silently corrupted with no error.

**Fix:** Select logs by a time window derived from data_interval_start/data_interval_end rather than "newest N", so the run is a pure function of its interval. If that is not feasible, stop stamping artifacts and ledger rows with ds: use the actual execution timestamp, and guard the task so a run whose data_interval is not the most recent one fails fast instead of publishing over a past date.

<details><summary>verifier</summary>

Half real, and the load-bearing half is fabricated. Severity drops from blocker to low.

VERIFIED (the kernel): fetch.newest_logs (include/spark_optimizer/fetch.py:30-44) sorts every .zstd in the archive by creation time and takes the newest 200 — no date filter, no relation to ds. dags/spark_optimizer_daily.py:51,84 passes ds as the artifact/ledger stamp, sweep.py:69,104 bakes it into optimizer_backlog_<date>.md / optimizer_digest_<date>.md, and publish (sweep.py:50-51) gsutil-cp's to a fixed basename, which overwrites. So clearing an old run does republish a historical dated report containing current-day content. Real, but it is a mislabeled regenerable markdown file in the DAG's own prefix: no prod breakage, no leak, no data loss. Low. Note catchup=False + start_date 2026-08-21 means this needs a deliberate operator clear; on the scheduled path ds (day N-1, per CREATE_CRON_DATA_INTERVALS) roughly matches the ~24h of logs fetched at 09:00 on day N, which is what the docstring intends.

REFUTED (everything the reviewer built the severity on):
1. "retries ... corrupt ledger streaks" — flatly false. ledger.py:103 filters `e.get("date") != date`, a deliberate same-date idempotency guard. Simulated a retry against the real module: the second attempt at 2026-08-14 reproduced state=chronic streak=5 identically, and the next sweep got 6, exactly correct. Retries also overwrite only their own same-day artifacts.
2. "ledger.classify orders history by the `date` field" — false, and this is the premise the rest rests on. _history (ledger.py:90-95) does setdefault().append() over read() in FILE order, i.e. append order. No sort by date exists anywhere in classify.
3. "resetting chronic keys to new" — impossible. The "new" branch (ledger.py:104-106) fires only when `past` is empty; a backdated row never empties it. Simulation of a cleared 2026-08-12 run after 8 sweeps: state stayed `chronic` throughout.
4. "flipping keys to fix_not_working" via after_fix — unreachable from this DAG. Line 114/117 are both gated on `entry.applied_date`, and grep confirms fix_pr/applied_date are written ONLY by mark_applied (ledger.py:250-267), a manual CLI (`__main__`, line 368). Nothing in dags/ or sweep.py calls mark_applied or set_state, so after_fix is always [] on the automated path.
5. The dcu_h before/after mis-attribution that a backdated row could cause (ledger.py:301-304) is also dead: the DAG's record() call passes no `dcu`, so dcu_h is always None.

ACTUAL measured ledger impact of a cleared run: one duplicate out-of-date row, and the key's streak inflated by exactly 1 going forward (2026-08-18 came out 10 instead of 9). State classification is unaffected. That is drift in a counter, not "the ledger's whole purpose silently corrupted."

</details>

---

### PHS half is uncapped (up to 500 recursive batch-dir downloads) and its "403, skipped quietly" premise is already stale

`include/spark_optimizer/phs.py:74` · found by the **runtime** reviewer · verifier confidence high

**Failure:** First run: `gcloud dataproc batches list --limit=500` returns 500 SUCCEEDED PHS batches. `fetch_logs` runs 500 recursive downloads into the same tempdir as the 1.6 GB of flat logs. At even 20 MB per batch dir that is another 10 GB on pod ephemeral storage; the pod is evicted or the disk fills mid-write, leaving truncated logs that then parse as "clean" (see the eventlog.py:141 finding). Wall clock is also unbounded: 500 x 600 s of `timeout=` is 83 hours worst case.

**Fix:** Add an explicit cap and a date filter to the PHS half — take only batches created in the sweep window, cap the count the same way LOG_CAP caps the archive, and enforce a byte/disk budget across both halves. Then delete or correct the stale "403 until mntn-devops#4724" comments at spark_optimizer_daily.py:71-72 and phs.py:7-8, which currently tell the reviewer this code path is inert when it is not.

<details><summary>verifier</summary>

Could not refute; the mechanism reproduces exactly from source. dags/spark_optimizer_daily.py:74 calls phs.fetch_logs(phs.phs_succeeded(phs.list_batches()), tmp) with no slice and no limit override, so phs.py:26's limit=500 default governs. phs.py:77-84 then loops over every surviving batch doing `gsutil cp -r gs://<temp-bucket>/<uuid>/spark-job-history/*` — a recursive directory copy per batch, sequential, timeout=600 each — into the SAME tmp dir as the 200 flat logs (DAG:69). LOG_CAP=200 (DAG:32) bounds only fetch.newest_logs, so the DAG's own comment at :30-31 ("the download... is the task's only real resource cost") is false once the PHS half runs. There is no wall-clock guard anywhere: default_args is only {retries:1, retry_delay:30m} and include/job_config/job_config.py:124-147 make_default_args returns just failure callbacks plus kwargs — no execution_timeout, no dagrun_timeout — and retries:1 repeats the whole download pass. The staleness half also holds: phs.py:7-8 ("Blocked on standing storage.objectViewer for the temp bucket (mntn-devops#4724); until it merges, fetches 403 and are skipped") and DAG:71-72 ("costs one list call until the grant lands") both assume a grant the optimizer SA already has on gs://dataproc-temp-us-central1-995798185124-svhwvc6j, so the first prod run takes the expensive path the author documented as blocked, and there is no dedup/state so it repeats every day.

Three corrections to the reviewer's framing, none of which rescue the code: (1) "83 hours" is only reachable if all 500 calls finish just under 600s — a call that actually hits the timeout raises TimeoutExpired, which escapes fetch_logs (no internal try) into the DAG's except at :75, sets phs_n=0 and continues, so a hung download aborts the PHS loop rather than compounding; unbounded wall clock is still real, that specific number is not. (2) "500 SUCCEEDED PHS batches" and "20 MB per batch dir" are estimates unverifiable from the repo — phs_succeeded filters the page, and only the ~7.9 MB flat-log figure is sourced; the code bound is <=500. (3) The "truncated logs parse as clean" chain is weaker than stated: a truncated .zstd raises ValueError at eventlog.py:170-171, crawl.py:79-80 records it as JobReport(error=...), and sweep.py:64 excludes errored reports from scored — disk exhaustion surfaces as a failed or degraded run, not a silent clean bill; eventlog.py:147-148's JSONDecodeError-continue only swallows one malformed line.

Severity high rather than blocker: an uncapped recursive-download loop with no execution_timeout on a shared prod worker, masked by a comment asserting it is a no-op, is a genuine co-tenant ephemeral-storage and slot risk that recurs daily — but nothing leaks, the try/except plus max_active_runs=1 contains the blast radius, and the actual batch count and directory sizes are unproven, so it is not demonstrably a prod-breaker on contact.

</details>

---

### One silent ledger-download failure truncates the append-only ledger and overwrites the GCS copy

`dags/spark_optimizer_daily.py:66` · found by the **runtime** reviewer · verifier confidence high

**Failure:** Day 40. GCS returns a transient 503 on the ledger cp. The task logs nothing about it, appends 40 fresh `new` entries, and overwrites 39 days of history in GCS with a 40-line file. Every hand-set `owner_notified` / `wont_fix` / `applied` row is destroyed — exactly the rows the module docstring (ledger.py:16-22) says are sticky because they record a human decision. The digest that morning reports every chronic finding as brand new and nobody can tell it happened.

**Fix:** Distinguish "object does not exist" (fresh start, fine) from "download failed" (fatal) — check the return code and stderr, and raise on anything that is not a 404. Additionally, do not overwrite a single mutable object: append a dated shard (`optimization_ledger_<ds>.jsonl`) and reduce on read, or enable object versioning on the bucket so a bad overwrite is recoverable.

<details><summary>verifier</summary>

Reproduced every link, could not refute it. (1) dags/spark_optimizer_daily.py:66 discards fetch.download's return value. (2) fetch.py:53-56 runs gsutil with capture_output=True and only counts rc==0 — a nonzero rc is neither logged nor raised, and stderr is swallowed, so "object absent" (first run) and "403 / exhausted-retry 5xx / auth failure" are indistinguishable to the caller. (3) fetch.dest_for returns outdir for that object (verified by running it), so the local path is exactly the `ledger` var at dag:62. (4) ledger.read (ledger.py:74-76) returns [] for a missing file. (5) I ran the real modules over a 3-line history containing owner_notified and wont_fix rows: with the file absent, record() classified the finding as state=new/streak=1, _mark_resolved was a no-op, and append (ledger.py:153-159, mode "a") created a 1-line file. (6) sweep.py:108 passes ledger_path into publish, which gsutil-cp's it to {gcs_prefix}/optimization_ledger.jsonl (sweep.py:50-51) — the same object it read from — and the optimizer SA's storage.objectUser on the "optimizer/" prefix permits the overwrite. The task exits success, so retries:1 never fires and nothing in the log says the pull failed. Worse variant the reviewer did not name: a zero-finding sweep still creates an empty local file, so publish's os.path.exists guard (sweep.py:48) does not save it — history is replaced by a 0-byte object. Two corrections to the reviewer's framing that do not change the verdict: gsutil retries transient 503/429 internally, so the realistic triggers are impersonation/token 403, a 5xx window longer than the retry budget, or a network partition rather than a single blip; and the timeout=600 path raises TimeoutExpired, which fails loudly. Severity high, not blocker: it silently destroys durable prod-bucket state including the sticky human decisions the docstring (ledger.py:16-22) promises survive replay, and unless the bucket has object versioning those rows are unrecoverable, but the blast radius is the optimizer's own auxiliary artifact — no pipeline breaks, nothing leaks, no other team's data is touched.

</details>

---

### newest_logs never checks the gsutil return code; any listing failure yields an empty list and a green task

`include/spark_optimizer/fetch.py:36` · found by the **runtime** reviewer · verifier confidence high

**Failure:** The impersonation or the underlying gcloud identity is not what the DAG assumes (see the CLOUDSDK finding). Every gsutil call 403s. `newest_logs` returns []; `phs.list_batches` returns [] on its own rc!=0 branch (phs.py:35-36); `n + phs_n == 0`; the task logs one WARNING line and exits 0. Because failure alerting is `on_failure_callback` only (include/job_config/job_config.py:128-145), no Slack fires. The DAG is green every morning for weeks while producing nothing, and the last good report stays in GCS looking current.

**Fix:** Raise on a non-zero return code from the listing and include `r.stderr` in the exception. Separately, make the zero-work path at spark_optimizer_daily.py:79 fail rather than return — a sweep that scanned nothing is a broken sweep, not an idle one. If a genuinely empty day is possible, assert a floor (e.g. fewer than N logs is a failure) so "no data" and "no access" are distinguishable.

<details><summary>verifier</summary>

Could not refute it — every step reproduces from source.

1. `include/spark_optimizer/fetch.py:36-44`: `r = subprocess.run(["gsutil","ls","-l",...], capture_output=True, text=True, timeout=900)` then only `r.stdout` is iterated. `r.returncode` and `r.stderr` are never touched, and there is no `check=True`. gsutil exits 1 and writes to stderr on `AccessDeniedException: 403` and on `CommandException: One or more URLs matched no objects`, leaving stdout empty → `rows` empty → `return []`. Non-zero exit does not raise in Python, so nothing propagates.

2. This is the only subprocess call in the vendored package that ignores rc. Every sibling checks it: `fetch.py:55`, `phs.py:33`, `phs.py:85`, `sweep.py:52`, `coverage.py:116`. That internal inconsistency makes it an omission, not a deliberate design.

3. DAG path confirms the swallow. `dags/spark_optimizer_daily.py:68-69`: `objects = fetch.newest_logs(...)` → `[]`; `fetch.download([], tmp)` loops zero times and returns `0`. Lines 73-77 wrap the PHS half in `try/except` → any raise becomes `phs_n = 0`; and `phs.list_batches` has its own `if r.returncode != 0: return []` at `phs.py:33-34`, so a 403 there also yields `phs_n = 0` with no raise. Lines 79-81: `n + phs_n == 0` → one `logger.warning` → `return {"scanned": 0, "findings": 0, "high": 0}`. Task exits 0.

4. Alerting is failure-only, as claimed. `JobConfig.make_default_args` (`include/job_config/job_config.py:124-147`) attaches callbacks solely under `args["on_failure_callback"]`. `JobTeamConfig.TARGETING` (`include/job_config/job_team_config.py:151-157`) sets `task_fail_email`/`task_fail_slack` but no `dag_success_slack`, and the DAG calls `TEAM.make_dag_args(...)` at `spark_optimizer_daily.py:41-44` without `severity`, so severity defaults to 5 and the `severity == 0` PagerDuty branch (`job_config.py:138`) never registers. Green task = zero notifications of any kind.

5. No test covers the failure path. `include/spark_optimizer/tests/test_phs.py:118-130` is the only `newest_logs` test and its stub hardcodes `{"stdout": listing, "returncode": 0}`.

Two minor overstatements in the reviewer's narrative, neither affecting the defect: (a) `sweep.publish` writes date-stamped `optimizer_backlog_<date>.md` / `optimizer_digest_<date>.md`, so a careful observer browsing GCS could notice no new file — it is the fixed-name `optimization_ledger.jsonl` that actually goes stale while looking current; (b) an entirely missing `gsutil` binary would raise `FileNotFoundError` and fail loudly, so the silent path requires gsutil present but unauthorized, which is precisely the 403 scenario described.

Severity corrected from blocker to high. The claimed consequence is real and would persist undetected for weeks, but nothing in production breaks and nothing leaks: the task is read-only apart from its own `optimizer/` report prefix, no downstream DAG consumes its output, and the blast radius is this new DAG silently producing nothing. That is a serious silent-failure/observability defect, not a production-breaking or data-exposing one. Fix is one line: check `r.returncode` and raise (or at minimum log `r.stderr`) at `fetch.py:36`.

</details>

---

### collect_local queries the metadata DB from inside a task; on Airflow 3 this fails and the failure is swallowed into a one-line note

`include/spark_optimizer/coverage.py:175` · found by the **runtime** reviewer · verifier confidence high

**Failure:** Every run, forever. The coverage report reads `Could not enumerate DAGs: Session must be set before!` (or `no such table: dag`), the digest headline reads `DAG coverage unknown (...)`, and the digest ships a Slack link for every job name whether or not it is a real dag_id. Nobody notices because the task is green and the message is buried in a file. Note that include/spark_optimizer/tests/test_coverage.py:51-54 only asserts the error branch works — the happy path in a real Airflow task is untested.

**Fix:** Do not touch the metadata DB from a task. Either drop the paused filter and read pause state from the Airflow REST API using the existing `collect(base, date, token)` path, or get the paused set through the Task SDK / API server rather than SQLAlchemy. Whichever path is chosen, make a coverage failure loud — log it at WARNING in the task and put it in the task's return value, not only in the markdown.

<details><summary>verifier</summary>

Confirmed by reproduction, though the reviewer's mechanism is wrong in detail. It is not a sqlite fallback: airflow/sdk/execution_time/supervisor.py:375 calls block_orm_access() inside _fork_main before the task callable runs, and that function (supervisor.py:268-310) sets settings.Session = BlockedDBSession, deletes engine/NonScopedSession, and overwrites AIRFLOW__DATABASE__SQL_ALCHEMY_CONN with "airflow-db-not-allowed:///". BlockedDBSession.__init__ (supervisor.py:251) raises RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0"), so create_session() at coverage.py:174 raises on Session() instantiation. The re-exec path fails too: settings.py:615-618 skips configure_orm() when _AIRFLOW__REEXECUTED_PROCESS=="1", leaving Session None and create_session raising "Session must be set before!" (utils/session.py:38). Both paths dead; the reviewer hedged "whatever the exact error", so the substance stands. Live repro in the repo venv (calling block_orm_access() exactly as _fork_main does, then collect_local): error='Direct database access via the ORM is not allowed in Airflow 3.0', dags=0, headline='DAG coverage unknown (...)', and dag_link('materialize_mntn_select_16', known=None) emits a link. Prod path reaches it: dags/spark_optimizer_daily.py:86 passes airflow_base="local" -> sweep.py:83 collect_local; swallow at coverage.py:192-193; sweep.py:85 yields known=None (empty set `or None`), disabling the _TRAILING_INDEX disambiguation at ledger.py:228-231 and the guard at digest.py:45, which then links every normalised app name unconditionally, the exact dead-link behaviour its docstring claims to prevent. tests/test_coverage.py:34-46 monkeypatches _load_bag_and_paused away and :51-54 asserts only the error branch, so the real path is untested. Two overstatements that do not change the verdict: the sqlite/"no such table: dag" story is wrong, and the digest is a markdown file in GCS, never posted to Slack (the DAG returns only scanned/findings/high/published). Verified against Airflow 3.0.3 in the repo .venv; prod is runtime 3.1-9, where AIP-72 task/DB isolation is unchanged. Severity is high, not blocker: the sweep still writes backlog/ledger/digest, the task legitimately goes green, nothing leaks or breaks in prod, but the coverage feature never works on any run and silently degrades ledger identity and digest links.

</details>

---

### A failed ledger download silently clobbers the published ledger with a one-day file

`dags/spark_optimizer_daily.py:66` · found by the **auth** reviewer · verifier confidence high

**Failure:** Day 30. The ledger object is 40k lines. A transient GCS 503, a DNS blip, or the 600s timeout at fetch.py:54 makes the single `gsutil cp` return non-zero. n is discarded, ledger.read() returns [], every finding classifies as `new` (ledger.py:104-105), and sweep.publish() overwrites the 40k-line object with today's ~200 lines. Every `owner_notified` / `wont_fix` human decision, every fix_pr attribution in shipped(), and every streak is destroyed. Overwrite is unrecoverable without object versioning on the archive bucket. The task returns SUCCESS and no alert fires.

**Fix:** Make the ledger fetch fail loud and separable from 'absent': `gsutil stat gs://.../optimization_ledger.jsonl` first, raise on any non-zero download rc when stat succeeded, and refuse to publish a ledger with fewer lines than the one that was pulled. Do the upload with `gsutil -h x-goog-if-generation-match:<gen>` from the stat so a concurrent/stale writer 412s instead of clobbering.

<details><summary>verifier</summary>

Could not refute; every link reproduces in source. (1) fetch.download (include/spark_optimizer/fetch.py:47-57) runs `gsutil cp` with capture_output, counts only returncode==0, and never raises, logs, or inspects r.stderr — a non-zero exit is indistinguishable from "object absent". (2) dags/spark_optimizer_daily.py:66 discards the return value entirely; the comment at 63-65 assumes only the absent case. dest_for() sends the ledger to outdir/optimization_ledger.jsonl, the same path passed as ledger_path at line 88, so a failed fetch leaves no local file. (3) ledger.read() returns [] for a missing file (ledger.py:74-76); classify() then sets state="new", streak=1 for every finding (ledger.py:104-106) and _mark_resolved emits nothing since hist is empty. (4) ledger.append (ledger.py:153-159) opens "a", creating a fresh file containing only today's entries (or a 0-byte file if there were no findings). (5) sweep.run always passes ledger_path into publish (sweep.py:108), which does a bare `gsutil cp` to {prefix}/optimization_ledger.jsonl (sweep.py:51) with no generation precondition, no size or line-count guard, and returns without raising. The optimizer SA holds storage.objectUser conditioned on the "optimizer/" prefix, so it has the create permission to overwrite. Task returns SUCCESS; retries:1 never fires. No test or caller guards this path. Two corrections to the reviewer's write-up: (a) the 600s timeout at fetch.py:54 is NOT one of the triggers — subprocess.run raises TimeoutExpired on timeout, which propagates and fails the task loudly, so no overwrite occurs there; the real triggers are any non-zero exit (impersonation/token-creation error, 403 after an IAM condition edit, retries exhausted on 5xx/network). (b) gsutil retries transient 503s internally, so this needs a durable-ish failure, not any blip. Severity corrected blocker -> high: the loss is real, silent and irrecoverable absent bucket versioning, but it is confined to this PR's own optimizer/ prefix — no shared prod pipeline breaks and nothing leaks. Fix is one line: check the return of fetch.download at line 66 and fail the task (or skip publishing the ledger) when it is 0 and the object exists per `gsutil stat`.

</details>

---

### collect_local parses all 99 DAG files inside the worker task process, then always throws the result away

`include/spark_optimizer/coverage.py:173` · found by the **auth** reviewer · verifier confidence high

**Failure:** Every daily run, inside a worker slot shared with 2 concurrent tasks, the task imports all 99 files in dags/ — executing their module-level code, including the dozens of Variable.get() calls at parse time (e.g. dags/gcp_page_view_signal_backfill_workflow.py:38-39, dags/gcp_hashed_phone_backfill_workflow.py:18-30), each an RPC to the API server — burning CPU and hundreds of MB of RSS. It then raises on create_session, so cov.error is set, `known` is None, the ledger loses its run-index-vs-data-source-id disambiguation (ledger.py:227-230), and the published coverage report says "Could not enumerate DAGs: Direct database access via the ORM is not allowed in Airflow 3.0" every single day. The stated benefit of airflow_base="local" (dags/spark_optimizer_daily.py:86) is never delivered.

**Fix:** Drop the DagBag+ORM path. Get the paused set from the Task SDK's API client, or read the deployment's DAG list through the existing API path with an injected token, or simply reorder so the DB read happens first and DagBag is never constructed when it will be discarded. At minimum pass a scoped dag_folder and construct DagBag with collect_dags=False until the paused set is confirmed available.

<details><summary>verifier</summary>

Reproduced end to end from source; could not refute. coverage.py:173 constructs DagBag(dag_folder=None) — dagbag.py:120,157-161 show collect_dags defaults True and dag_folder falls back to settings.DAGS_FOLDER, and collect_dags (dagbag.py:563-620) takes no session and swallows per-file errors, so all 99 files under dags/ are imported before anything else. coverage.py:174 then calls create_session(), which does Session = getattr(settings, "Session"); Session(). supervisor.py:375 calls block_orm_access() in the forked child immediately before target(); supervisor.py:291 sets settings.Session = BlockedDBSession and supervisor.py:250-252 raises RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0") in __init__. supervisor.py:309 also unconditionally sets AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///, so no fallback path exists. The raise is caught at coverage.py:192-194, so cov.error is set and cov.dags is empty; sweep.py:84-85 then yields known=None, ledger.py:227-230 skips run-index-vs-data-source-id disambiguation, and coverage.py:218 publishes "Could not enumerate DAGs: ..." every day. The parse cost is real: 99 .py files under dags/ and 67 module-level `X = Variable.get(...)` assignments; airflow/models/variable.py get() checks SUPERVISOR_COMMS and routes to the Task SDK API-server path inside a task, so those are RPCs as claimed, and _load_modules_from_file re-imports rather than reusing sys.modules. Tests never exercise the path: tests/test_coverage.py:39 monkeypatches _load_bag_and_paused away, and :51-54 passes only because create_session raises, not because the folder is nonexistent. Two unverified embellishments (hundreds of MB RSS; 2 concurrent tasks in the slot) are color, not load-bearing. Severity corrected to high rather than blocker: it silently kills the advertised airflow_base="local" feature and degrades the ledger on every run, but breaks no other DAG, corrupts no data, and leaks nothing.

</details>

---

### A failed ledger download silently erases the whole history and republishes a one-day file

`dags/spark_optimizer_daily.py:66` · found by the **data** reviewer · verifier confidence high

**Failure:** A transient 503/network blip on the single `gsutil cp gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` at 09:00. Reproduced with the real module: a ledger holding 16 entries across 4 sweeps drops to 4 entries, every finding is classified `state="new"`, and the digest published to GCS reads "*New today*" listing the entire fleet. Months of streak/chronic/owner_notified/fix_pr history are gone and nothing in the task log says so — the task goes green.

**Fix:** Make `fetch.download` return per-object success/failure and inspect gsutil's stderr. Treat only a definitive "No URLs matched"/404 as "first run"; on any other failure raise before `sweep_mod.run` is called. Independently, enable object versioning on the ledger object and have the sweep refuse to publish a ledger with fewer lines than the one it downloaded.

<details><summary>verifier</summary>

Every link in the chain is in the source and I reproduced the wipe with the real modules.

1. dags/spark_optimizer_daily.py:66 — `fetch.download([f"{REPORT_PREFIX}/{LEDGER_NAME}"], outdir)` discards the return value; no existence check on `ledger` (line 62) follows.
2. include/spark_optimizer/fetch.py:53-56 — `if r.returncode == 0: n += 1` with no else, no log, no raise. "Object absent (first run)" and "cp failed" both return 0, so even a caller that checked could not distinguish them.
3. include/spark_optimizer/ledger.py:74-77 — missing file returns `[]`; `classify` (line 103-105) then marks every key `new`, and `append` (line 153-158) opens `"a"` on a nonexistent path, creating a one-day file.
4. include/spark_optimizer/sweep.py:108 + :51 — `ledger_path` is in the publish list and `publish` does a bare `gsutil cp f gs://…/optimizer/optimization_ledger.jsonl`. No `-n`, no `ifGenerationMatch`, no backup copy, no row-count floor. grep for version/backup/generation/no-clobber across include/spark_optimizer/ and the DAG returns nothing. The optimizer SA's `storage.objectUser` condition ("optimizer/" prefix) permits exactly this overwrite, so the clobber succeeds.

Repro (real ledger module, 4 sweeps + one `set_state`): prior file 17 entries / 4813 bytes, latest states chronic×3 + owner_notified. With the local ledger absent, `record` produced 4 entries / 1110 bytes, all `state="new"`, delta new=4 chronic=0 notified=0. The human-set `owner_notified` is gone. `sweep.run` returns normally and the task goes green; the only trace is nothing at all, since `download` logs no failure.

Two corrections to the reviewer's framing that do not change the verdict: a `subprocess` timeout (fetch.py:54, timeout=600) raises `TimeoutExpired` and would fail the task loudly, so the silent path needs a non-zero exit rather than a hang; and if the network is down broadly, the `n + phs_n == 0` early return (DAG line 79-81) skips the publish. The dangerous window is exactly the one claimed: the single ledger `cp` fails while the ~200 event-log `cp`s succeed.

Severity: high, not blocker. It silently destroys the job's only durable state including hand-set `owner_notified`/`wont_fix`/`fix_pr`, and recovery depends on bucket object versioning that nothing in the repo establishes. But it breaks no production pipeline, corrupts no shared dataset, and leaks nothing; blast radius is this job's own `optimizer/` prefix.

</details>

---

### A partial download makes the digest declare never-scanned jobs "Stopped firing"

`dags/spark_optimizer_daily.py:79` · found by the **data** reviewer · verifier confidence high

**Failure:** Reproduced: a 4-job fleet with 4 sweeps of history, followed by 3 sweeps where the download partially fails and only `job_a` lands. Sweep 3 publishes `*Stopped firing* — job_b, job_c, job_d` and writes `resolved` rows for all three, with the note "stopped firing after 2026-08-04". Those jobs never stopped firing; they were never read. An owner reading the digest concludes three defects are fixed, and the ledger will report them as `new` when the download recovers.

**Fix:** Check `r.returncode` in `newest_logs` and raise on non-zero. Have `download` return the failure count and abort the sweep when the landed/listed ratio falls below a threshold. Gate `_mark_resolved` on the sweep being complete: skip resolution entirely on any run whose scanned count is materially below the prior run's, and record `dag_id`s that were actually scanned so a key is only resolvable when its job was observed.

<details><summary>verifier</summary>

Reproduced end-to-end against the real modules. Every link in the chain is in the source:

1. `dags/spark_optimizer_daily.py:79` — `if n + phs_n == 0:` is the ONLY acquisition guard. `n` vs `len(objects)` is computed at line 85 but only interpolated into the backlog's provenance string; it gates nothing. Confirmed by grep: no minimum-coverage constant, no prior-sweep scan-count comparison anywhere in `include/spark_optimizer/`.
2. `include/spark_optimizer/fetch.py:36-44` — `newest_logs` runs `gsutil ls -l` with `capture_output=True` and parses `r.stdout` without ever reading `r.returncode`. A truncated or 403'd listing silently yields a short object list. Separately `fetch.py:53-56` (`download`) increments `n` only on rc==0 and silently drops every object that fails, so a partial download is a first-class path even when the listing is perfect.
3. `include/spark_optimizer/ledger.py:129-150` — `_mark_resolved` keys purely on absence: any (dag_id, key) not in this sweep and not present in the last `RESOLVE_SWEEPS-1` ledger dates gets an appended `state="resolved"` row. It never consults download counts or coverage.

Repro (real `ledger.classify`/`digest.render`, 4 jobs, 4 healthy sweeps then 3 sweeps where only job_a's log lands):
  2026-08-07 digest → "Stopped firing — job_b, job_c, job_d"
  ledger rows → job_b/c/d `state=resolved`, `note="stopped firing after 2026-08-04"`
Byte-identical to the reviewer's claimed output. The ledger is then uploaded to GCS (`sweep.py:108` publishes `ledger_path`), so the false rows are permanent in an append-only record.

Two corrections to the claim, neither of which refutes it:
- On recovery the keys return as `recurring` with `streak` reset to 1, not `new`. `recurring` is rendered by NO digest section (`digest.py:88-99` only emits new/chronic/notified/resolved), so job_b/c/d vanish from the digest entirely for two more sweeps and their real multi-week streak is destroyed. Worse than the reviewer said, not better.
- A TOTAL acquisition failure accidentally self-protects: `seen_dates` (ledger.py:101) is derived from ledger entries, not the calendar, so a zero-entry sweep never advances the grace window. Only the PARTIAL failure is dangerous — which is precisely the case filed.

Trigger frequency is higher than "download breaks three days running." `LOG_CAP=200` counts OBJECTS, and `newest_logs` lists `prefix/**`, so each `events_*` part of a v2 rolling log eats a slot against a stated ~160/day fleet — a busy day silently truncates the oldest jobs. And any DAG that does not run for three consecutive sweeps (weekly/monthly schedules, a paused-then-resumed DAG) has no log to read and gets "stopped firing" with zero infrastructure failure at all.

Severity: high, not blocker. It changes nothing in the Airflow fleet, leaks nothing, and the DAG's only write is its own report prefix. What it does is publish a false all-clear to owners and permanently corrupt the append-only ledger that is the entire point of the feature.

</details>

---

### A coverage failure silently rekeys the ledger, producing false "New today" and then false "Stopped firing"

`include/spark_optimizer/sweep.py:85` · found by the **data** reviewer · verifier confidence high

**Failure:** Reproduced: three sweeps record `materialize_mntn_select` while coverage works. On day 4 `DagBag`/`create_session` throws (bundle parse error in an unrelated DAG, DB unreachable from the task). `known=None`, so the same job records as `materialize_mntn_select_16` with `state="new", streak=1`; the digest reports it as new. Three sweeps later `materialize_mntn_select` has no history in the window and `_mark_resolved` publishes "Stopped firing — materialize_mntn_select". One transient coverage failure yields a false new finding and a false fix claim for the same job.

**Fix:** Make the ledger step depend explicitly on `known`: if prior ledger rows exist and `known` is None this run, skip `ledger_mod.record` and say so in the digest rather than writing rows under a different key namespace. Have `collect_local` propagate the failure to sweep (or check `cov.error`) instead of returning a silently-empty Coverage that reads as "zero active DAGs".

<details><summary>verifier</summary>

Could not refute; reproduced both variants by running the vendored modules directly. Chain confirmed in source: coverage.py:190-194 catches every exception in collect_local and returns Coverage(dags=[]) rather than raising, so sweep.py:86's except is dead for that path and sweep.py:85's `or None` sets known=None; ledger.py:227 `if known and name not in known` then short-circuits and _TRAILING_INDEX never strips. tests/test_ledger.py:154-168 asserts this exact dependence on `known`. Repro 1 (transient): days 1-3 record materialize_mntn_select (chronic, streak 3); day 4 with known=None records materialize_mntn_select_16 state="new" streak=1 and digest.py:92 prints "*New today*"; on day 7 _mark_resolved (ledger.py:129-150) fires and digest.py:98 prints "*Stopped firing* - materialize_mntn_select_16". Repro 2 (failure persists) plus a mark_applied PR: day 6 emits resolved carrying fix_pr, and ledger.shipped() reports outcome="resolved" for that PR while the finding still fires daily under the rekeyed name - false credit for a fix that did not work. The reviewer actually understated the trigger surface: no exception is needed at all. coverage.py:197-198 excludes paused DAGs and DagBag collects per-file import errors without raising, so pausing one DAG or an import error in one DAG file drops it from `known` and rekeys that job silently, with the coverage report still looking healthy. The ledger is append-only in gs://mntn-data-archive-prod/optimizer/, so bad rows are permanent and a re-run cannot repair them. Severity high, not blocker: no prod pipeline breaks and nothing leaks, but the tool's primary output (what is new, what got fixed) is wrong and it fabricates savings attribution.

</details>

---

### test_collect_local_reports_a_broken_bundle_instead_of_raising is vacuous — it passes with Airflow uninstalled

`include/spark_optimizer/tests/test_coverage.py:51` · found by the **tests** reviewer · verifier confidence high

**Failure:** `create_session()` at `coverage.py:174` raises in the Airflow 3.1.5 task runtime (no DB session configured for a task-SDK worker process). `collect_local` swallows it, `cov.error` is set, `cov.dags` is empty, so `known` is `None` at `sweep.py:85`. `ledger._dag_id` then loses its known-DAG set, and per `test_run_stamps_are_stripped_but_data_source_ids_are_not` (test_ledger.py:151-165) `materialize_mntn_select_16` stops collapsing to `materialize_mntn_select` — the ledger forks one standing finding into a new "new today" entry every run, forever, and the coverage report reads "DAG coverage unknown". Every test still passes.

**Fix:** Split the assertion so it proves the specific behavior: assert `"/nonexistent/bundle" in cov.error` or that the error text names a path/DagBag problem, and add a test that monkeypatches `_load_bag_and_paused` to raise a specific sentinel exception and asserts that exact message reaches `cov.error`. Separately, add `apache-airflow` to the test dependency group so the real path is importable, or narrow the `except Exception` at `coverage.py:192` so an `ImportError` is not silently equivalent to a bundle error.

<details><summary>verifier</summary>

Reproduced, and worse than claimed. Ran the real call in the repo's own venv (Airflow 3.0.3 IS installed at .venv, so "Airflow uninstalled" is not even required): `coverage.collect_local("2026-08-21", dag_folder="/nonexistent/bundle")` returns `error='(sqlite3.OperationalError) unable to open database file'`, `dags=[]`. DagBag logged "Filling up the DagBag from /nonexistent/bundle" and did NOT raise — a nonexistent folder yields an empty bag. So the assertion at include/spark_optimizer/tests/test_coverage.py:51-55 (`cov.error and not cov.dags`) is satisfied by a metadata-DB failure, never by the broken bundle the test name claims. It pins only the bare `except Exception` at coverage.py:192-194; the session query and classification loop at coverage.py:174-177 / 196-209 have zero coverage, because the sibling test (test_coverage.py:31) monkeypatches `_load_bag_and_paused` away.

The production consequence is confirmed in the installed SDK, not speculation. Task subprocesses enter via `_fork_main`, which calls `block_orm_access()` (airflow/sdk/execution_time/supervisor.py:375); that sets `settings.Session = BlockedDBSession`, whose `__init__` raises `RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0")` (supervisor.py:248-252), and rewrites `sql_alchemy_conn` to `airflow-db-not-allowed:///` (supervisor.py:309). `create_session()` (airflow/utils/session.py:33-39) instantiates `settings.Session`, so coverage.py:174 raises on every run. dags/spark_optimizer_daily.py:86 passes `airflow_base="local"` → sweep.py:83 `collect_local` → error swallowed → sweep.py:85 `known = set() or None` → None. ledger._dag_id (ledger.py:227-230) then never strips a trailing index, so `materialize_mntn_select_16` stays forked from `materialize_mntn_select` exactly as test_ledger.py:151-165 documents, and coverage.py:84-85 emits "DAG coverage unknown" forever. Aggravating: CI only runs `pytest tests/models` (.github/workflows/pr_model.yaml:94), so include/spark_optimizer/tests never runs at all.

Only overstatement: the real DagBag code IS executed by this test; it is the session/classification half that is untested. Does not change the verdict.

Severity corrected to high, not blocker: the task still succeeds, nothing leaks, and backlog/digest/ledger still publish. But the coverage pass is guaranteed dead on 100% of prod runs, silently, and it permanently corrupts the ledger's new-vs-chronic distinction, which is the feature's headline output.

</details>

---

### sweep.run and sweep.publish — the DAG's actual call path and the only GCS write — have zero tests

`include/spark_optimizer/sweep.py:59` · found by the **tests** reviewer · verifier confidence high

**Failure:** `publish` (line 51) invokes `gsutil cp` for `[backlog, digest_path, cov.report_path or "", ledger_path]`. Under the optimizer SA's `storage.objectUser` grant, conditioned to objects prefixed `optimizer/`, a `gsutil` invocation is a subprocess whose auth comes from `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` set at `spark_optimizer_daily.py:54`. If impersonation is not honored by the vendored gsutil in the Astro image, every `cp` returns non-zero, `publish` prints to stdout and returns `[]` (line 56, never raises), `run` returns `published: []`, the task logs "Scanned N jobs..." and succeeds green. The ledger is never uploaded, so the next day's `fetch.download` of the ledger finds nothing, every finding is re-recorded as `state="new"`, and the digest reports a fabricated fleet-wide regression. No test would have caught any of it.

**Fix:** Add `include/spark_optimizer/tests/test_sweep.py` that monkeypatches `sweep.subprocess.run` and asserts: (a) `publish` returns `[]` and does not raise when `returncode != 0`; (b) `run()` writes all three markdown files and appends to the ledger given a tmp_path of fixture logs; (c) `run()` still produces a backlog when `collect_local` raises. At minimum, assert that a non-empty `gcs_prefix` with a failing upload is surfaced in the returned dict so the DAG can log or fail on it.

<details><summary>verifier</summary>

Could not refute — reproduced end to end.

COVERAGE CLAIM IS EXACT. `grep -rn "sweep" include/spark_optimizer/tests/*.py tests/ dags/` returns only 8 docstring-prose hits plus the DAG itself; no test file imports the module (`from include.spark_optimizer import ...` lines cover coverage, ledger, digest, crawl, optimizations, optimize, eventlog, phs, fetch — never sweep), and nothing imports sweep transitively (sweep imports the others, not the reverse). So `sweep.run` (sweep.py:59) and `sweep.publish` (sweep.py:38) are at 0% coverage — provably, since a function in a never-imported module cannot execute. Test count is 51 (16+14+7+6+4+4), matching. `fetch.download` (fetch.py:47), called at spark_optimizer_daily.py:66 and :69, is likewise untested; test_phs.py:108-130 covers only `dest_for` and `newest_logs`.

FAILURE CHAIN CONFIRMED, each link read in source and executed:
- sweep.py:51 `subprocess.run([*_GSUTIL,"cp",f,dest], capture_output=True, timeout=300)`; :54-55 on non-zero it only `print`s; :56 `return landed`. No raise. Ran it with a stubbed 403: printed `[sweep] upload failed ... AccessDeniedException: 403` and returned `[]`.
- sweep.py:95 passes `[backlog, digest_path, cov.report_path if cov else "", ledger_path]` — publish is the only write to gs://mntn-data-archive-prod/optimizer/ (the other gsutil sites, fetch.py:53 and phs.py:82, are downloads).
- spark_optimizer_daily.py:92-96 logs `out["published"]` and returns; nothing asserts it is non-empty. Task goes green.
- ledger.py:74-76 `read()` returns `[]` for a missing file; classify (:103-106) then sets `state="new", streak=1` for every finding. Executed three simulated days: with the ledger persisted, day1 `new/1` → day2 `recurring/2`; with it lost, all three days emit `new/1`, `chronic=0`, and digest.render prints the whole fleet under "New today" every day forever. Nothing ever reaches chronic or resolved — the ledger's entire stated purpose (ledger.py:1-8) is dead, silently.

UNDERSTATED, not overstated. Two amplifications the reviewer missed: (1) `publish` uses `gsutil cp`, which overwrites — so one failed download at spark_optimizer_daily.py:66 followed by a successful publish replaces the full remote history with a single day's file. Loss is permanent absent bucket versioning. (2) The return value of that ledger `fetch.download` is discarded entirely at line 66 (contrast line 69, which binds `n`), so a 404 or 403 on the ledger pull is indistinguishable from a first run.

Only speculative element is the reviewer's chosen trigger ("if impersonation is not honored by the vendored gsutil"). Irrelevant to validity: the swallow path is trigger-agnostic and fires on any non-zero exit — the conditional `storage.objectUser` binding rejecting a path, a transient 5xx, quota. (The IAM condition itself is fine: publish writes `optimizer/<basename>`, satisfying the "optimizer/" prefix. And `timeout=300` raises TimeoutExpired uncaught, so a hang does fail loudly — it is specifically the permission/quota class that goes green.)

Severity high, not blocker: no leak, no corruption outside the DAG's own report prefix, no impact on other pipelines in the shared deployment. But it is silent (green task), self-perpetuating, and irreversible, with no test or assertion anywhere that would surface it. Minimum fix: have `run` raise when `gcs_prefix` is set and `publish` returns fewer paths than it was handed, plus a monkeypatched-subprocess test on `publish` and on the ledger round-trip, in the shape test_phs.py:46-64 already establishes.

</details>

---

## MEDIUM (17)

### Committed binary fixture embeds the author's username, home dir, and a session-UUID scratchpad path

`include/spark_optimizer/tests/fixtures/eventlog.zstd:1` · found by the **leakage** reviewer · verifier confidence high

**Failure:** Merge lands. `zstd -dc include/spark_optimizer/tests/fixtures/eventlog.zstd | grep malachi` on any engineer's checkout returns 37 hits including a full local filesystem layout and a Claude Code session UUID. A future `git filter-repo` to remove it rewrites every SHA on a repo that gates prod Airflow deploys.

**Fix:** Regenerate both fixtures with the personal fields neutralised, or post-process the JSONL before recompressing: drop the `System Properties` and `Classpath Entries` blocks from SparkListenerEnvironmentUpdate, blank `"User"` on ApplicationStart, and set `spark.eventLog.dir` to a relative placeholder. The parser tests only read App Name/ID, Spark Properties, stages, tasks, executors and SQL, so nothing in the test suite depends on the leaked fields.

<details><summary>verifier</summary>

CONFIRMED — reproduced by decompressing the actual file, not inferred. `zstd -dc include/spark_optimizer/tests/fixtures/eventlog.zstd` (77,105 bytes, added by this PR) yields 114 JSON lines containing every string the reviewer named, exactly:

- SparkListenerApplicationStart: `"User": "malachi"`
- SparkListenerEnvironmentUpdate System Properties: `user.name = malachi`, `user.home = /Users/malachi`, `user.dir = /private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad`, `java.library.path = /Users/malachi/Library/Java/Extensions:...`
- Spark Properties: `spark.eventLog.dir = file:///tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad/spark-events` — session UUID matches the claim character-for-character
- `.../scratchpad/out` (in the physical plan + InsertIntoHadoopFsRelationCommand) and `.../scratchpad/spark-warehouse` both present
- Occurrence count of `malachi` = 37, exactly as claimed

`git grep -il malachi origin/main` returns nothing, so this PR genuinely introduces these strings into the repo tree; the file is status `A`, so it is permanent in history at merge.

REVIEWER UNDERSTATED SCOPE: the sibling fixture `include/spark_optimizer/tests/fixtures/eventlog_cache.zstd` (263,347 bytes, also added by this PR) carries 27 more occurrences, including `/Users/malachi/Developer/work/mntn/workspace` and `file:/Users/malachi/Developer/work/mntn/workspace/spark-warehouse` — disclosing the path of a separate private workspace repo. Total is 64 occurrences across two files, and any fix must cover both.

SEVERITY CORRECTED TO MEDIUM, not blocker. A blocker must break prod or leak a secret; this does neither. No credential, token, key, or customer data is exposed. The username is already public in this repo's commit metadata (`Malachi Dunn <malachi@mountain.com>` appears in origin/main's author list), `/Users/malachi` is trivially derived from it, and a Claude Code session UUID is a local identifier with no auth value. The DAG runs identically either way. What justifies medium rather than low is irreversibility plus cost asymmetry, which the reviewer got right: pre-merge the fix is trivial, post-merge it needs a `git filter-repo` rewriting every SHA on the repo that gates prod Airflow deploys.

FIX IS CHEAP — verified, not assumed: no test asserts on any leaked string (`grep` over `tests/test_eventlog.py` for `malachi|user.name|User|eventLog.dir|scratchpad` returns nothing), and `eventlog.py` never reads the `User`/`user.home`/`eventLog.dir` fields. Tests bind only to `FIXTURE`/`CACHE_FIXTURE` paths and `reports[0].source.endswith("eventlog.zstd")` (test_eventlog.py:17-18, 256). So the fixtures can be regenerated under a neutral `EVENTLOG_FIXTURE_DIR` — `gen_eventlog.py` already supports that env override — or the identity fields scrubbed in place, with zero test churn.

Minor wording nit, not a defect in the claim: the reviewer's literal command `grep malachi` returns 16 matching lines, not 37; 37 is the occurrence count (`grep -o | wc -l`). Substance unaffected.

</details>

---

### No execution_timeout and no dagrun_timeout: a stalled download holds a worker and the only DAG slot indefinitely

`dags/spark_optimizer_daily.py:43` · found by the **dag** reviewer · verifier confidence high

**Failure:** GCS throttles or a gsutil transfer stalls below the per-call timeout on the 09:00 UTC run. The task keeps running past 24h. Because max_active_runs=1 (line 39), tomorrow's run is queued and never starts, and the day after that. The task never fails, so the Slack failure callback never fires and nobody is told. The DAG stops producing reports for days while permanently occupying one default_pool slot and one worker/pod on a deployment shared with attribution, ML and TPA pipelines.

**Fix:** Add `"execution_timeout": timedelta(minutes=45)` to default_args and `dagrun_timeout=timedelta(hours=1)` inside the make_dag_args call (it is a documented parameter there, not a DAG kwarg). Add an aggregate deadline inside the sweep so the download loop stops at a wall-clock budget rather than 200x600s, and register the DAG in WATCHDOG_RULES.

<details><summary>verifier</summary>

Core defect verified, but the reviewer's headline scenario and two supporting facts are wrong, so it is a hardening finding, not a blocker.

VERIFIED: dags/spark_optimizer_daily.py:41-44 passes only `tags` and `default_args={"retries":1,"retry_delay":30m}` to make_dag_args — no `dagrun_timeout`, no `execution_timeout`. include/job_config/job_config.py:192-193 sets `dagrun_timeout` only when explicitly passed, and make_default_args (job_config.py:124-145) injects no execution_timeout. No deployment-level default either: the Dockerfile sets only AIRFLOW__SCHEDULER__CREATE_{CRON,DELTA}_DATA_INTERVALS. max_active_runs=1 at line 39 with catchup=False, so the next run queues behind a long one. WATCHDOG_RULES (dags/monitoring/dag_run_duration_watchdog.py:531-538) contains exactly one rule (audience_intent); this DAG is not in it. Every network call is individually bounded (60/300/600/900s) and nothing bounds the total, so a sustained-slowness day has no wall-clock stop and no alert.

WRONG #1 — the stated trigger. "A stalled download holds a worker indefinitely" is the one case that IS bounded. fetch.download (include/spark_optimizer/fetch.py:53-54) does not catch TimeoutExpired, so a genuinely hung `gsutil cp` raises at 600s, kills the child, fails the task, and fires the Slack task-failure callback from make_default_args. Only transfers that are slow but complete under 600s accumulate silently — a much narrower trigger than "GCS throttles or a transfer stalls."

WRONG #2 — the 83-hour PHS ceiling. dags/spark_optimizer_daily.py:73-77 wraps the entire PHS half in `try/except Exception`, so the FIRST 600s timeout aborts the whole loop (phs_n=0, sweep continues). It cannot compound 500 x 600s. Also only the PHS-attached SUCCEEDED subset of the 500 listed batches reaches fetch_logs (phs.py:39-49), not all 500.

WRONG #3 — the convention claim. Only 5 files in dags/ set `dagrun_timeout` (attribution/ga4.py:25, marketo_data_export.py:25, augmentor_daily_gcs.py:59, tmobile_blocked_ip_workflow.py:72, tmobile_blocked_guids_workflow.py:72). dlv_pattern_identification.py:198 is `execution_timeout`, not `dagrun_timeout`. Across 63 DAG-defining files, 13 set any timeout at all — good practice, not an enforced repo standard.

SEVERITY: medium, not blocker. Nothing breaks on merge and nothing leaks. Blast radius is this DAG's own run slot plus 1 of default_pool's slots (default 128), not deployment-wide starvation of attribution/ML/TPA. The real cost is the silent-failure mode: a degraded run produces no reports and pages nobody. One-line fix — `execution_timeout: timedelta(hours=2)` in default_args at line 43.

</details>

---

### A completely broken run reports success: every acquisition and publish error is swallowed

`dags/spark_optimizer_daily.py:79` · found by the **dag** reviewer · verifier confidence high

**Failure:** roles/iam.serviceAccountTokenCreator on spark-optimizer@mntn-prj-prod-00 is revoked, or the objectUser condition on the "optimizer/" prefix is edited. Every `gsutil cp` returns non-zero. download() returns 0 with no log line. list_batches returns []. The task logs "Listed 4231 objects but downloaded none. Idling." and returns success. The DAG shows a green run every morning while publishing nothing, indefinitely, and no Slack message is ever sent.

**Fix:** Raise AirflowException when `n + phs_n == 0` after objects were listed, log r.stderr on every non-zero gsutil returncode in fetch.download and phs.fetch_logs, and make sweep.publish return a failure signal the task raises on when len(published) < len(files_written).

<details><summary>verifier</summary>

Confirmed in source; I could not refute it. Every link in the chain is real and no code path in between raises.

- `dags/spark_optimizer_daily.py:79-81`: `if n + phs_n == 0: logger.warning(...); return {...}` — a run that acquired zero logs returns a dict, so the task ends SUCCESS.
- `include/spark_optimizer/fetch.py:36-44` (`newest_logs`): `subprocess.run` without `check=True`; on a non-zero `gsutil ls` it parses empty stdout and returns `[]`. No raise, no log.
- `include/spark_optimizer/fetch.py:53-56` (`download`): counts only `returncode == 0`; `capture_output=True` captures stderr and it is never read or logged. Failed objects are invisible.
- `include/spark_optimizer/phs.py:33-38` (`list_batches`): `if r.returncode != 0: return []`, plus `[]` on JSONDecodeError. The DAG additionally wraps the PHS half in `except Exception` at line 75-77.
- `include/spark_optimizer/sweep.py:38-56` (`publish`): docstring says "never raises"; failures `print()` and are dropped from the returned list, so `run()` returns `published: []` and the DAG (line 94-96) simply logs nothing.

Alerting: `TEAM.make_dag_args(...)` with default `severity=5` yields only the Slack `task_fail_slack` `on_failure_callback` from `include/job_config/job_config.py:126-146` (no PagerDuty, since PD requires `severity == 0`). That callback fires only on task failure, so it can never fire in either scenario.

Both failure modes hold:
1. Impersonation revoked → every gcloud/gsutil call non-zero → `n = 0`, `phs_n = 0` → line 80 warning → green task, every morning, indefinitely, no Slack.
2. `objectUser` "optimizer/" condition edited → reads still succeed, `sweep_mod.run` produces reports, `publish` fails on all four files and returns `[]` → task logs "Scanned N jobs..." with no published lines → green, publishing nothing, no Slack.

Two corrections to the reviewer's write-up, neither of which defeats the finding: (a) under a tokenCreator revocation `gsutil ls` fails too, so the line reads "Listed 0 objects", not "Listed 4231" — the ls-succeeds/cp-fails combination requires a read-scoped permission change rather than a full impersonation break; (b) the stronger case is PARTIAL download failure — if some `cp` calls fail, `n > 0`, the `n + phs_n == 0` branch is skipped entirely and there is not even a warning, so the report silently covers a subset of the fleet. Also `fetch.download` at DAG line 66 discards its return value, so a failed ledger pull silently restarts the ledger and the digest reports every standing finding as "new".

Severity correction: not a blocker. Nothing in production breaks, no data is corrupted, nothing leaks — the DAG is read-mostly and its only write is its own report prefix. The impact is an undetectably dead scheduled job producing a permanently green run. Medium (defensible as high if the team treats an unalertable scheduled job as a merge blocker).

</details>

---

### Task code re-parses the entire DAG bundle and opens a metadata-DB session; the guaranteed failure is swallowed so coverage is permanently dead

`include/spark_optimizer/coverage.py:173` · found by the **dag** reviewer · verifier confidence high

**Failure:** Every scheduled run pays the cost of importing all ~34 DAG modules inside the task, then hits the session error, prints one line to stdout, and continues with cov=None. The optimizer_coverage_<date>.md file is never written, the digest permanently reads "DAG coverage unknown", and `known` stays None so ledger._dag_id (include/spark_optimizer/ledger.py:174) loses the active-DAG set it needs to distinguish a run index from a data-source id, mislabelling ledger keys. The feature is dead on arrival and the only evidence is a stdout print in a task log nobody reads.

**Fix:** Remove the local path. Use collect() against the Task Execution API / REST surface, or drop the coverage pass from the DAG entirely. If it stays, do not call DagBag or create_session from task code, and let the exception fail the task instead of printing at sweep.py:87 so a permanently broken feature is visible.

<details><summary>verifier</summary>

Core defect confirmed, reviewer's mechanism and three of four downstream claims wrong.

CONFIRMED. coverage.py:173 `DagBag(dag_folder=None, ...)` defaults `collect_dags=True` (airflow/models/dagbag.py:128,161) and `dag_folder or settings.DAGS_FOLDER` (dagbag.py:143), so every DAG module in the bundle is imported and executed inside the task's forked process. coverage.py:174 `create_session()` then fails on every run, guaranteed. I reproduced the whole path locally by calling `airflow.sdk.execution_time.supervisor.block_orm_access()` (what `_fork_main` calls before task code, supervisor.py:376) and then `coverage.collect_local(...)`: the DagBag parse ran (traceback showed dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py:17 executing `Variable.get("ENV")` at import), and the session call raised. So coverage is dead on arrival in the deployment, and the task pays a 34-module bundle parse daily for nothing.

WRONG EXCEPTION. It is not `RuntimeError("Session must be set before!")` from an unconfigured ORM. Airflow 3 deliberately blocks task-process DB access: block_orm_access (supervisor.py:268-310) deletes `settings.Session` and reassigns it to `BlockedDBSession`, so `create_session` passes the `Session is None` check (utils/session.py:37-40) and dies one line later at `Session()` with `RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0")` (supervisor.py:246-249). The failure is more certain than the reviewer argued, for a different reason.

WRONG CATCH SITE. sweep.py:86-88 never fires. `collect_local` catches it itself at coverage.py:192-194 and returns a Coverage with `.error` set. `cov` is never None. The author's own test asserts this (tests/test_coverage.py:51-54).

WRONG: "coverage .md never written." sweep.py:97 `if cov is not None:` is True, so optimizer_coverage_<date>.md IS written and IS in the publish list (sweep.py:107), containing "Could not enumerate DAGs: Direct database access via the ORM is not allowed in Airflow 3.0" and "Treat its completeness as unknown."

WRONG: "only evidence is a stdout print nobody reads." The error is the digest headline (digest.py:87-88 -> `unprofiled_line()`) and the published coverage report in GCS, i.e. the DAG's primary deliverables.

PARTLY WRONG on the ledger. `known` is None (sweep.py:85: `set() or None`), but `_dag_id` guards with `if known and name not in known` (ledger.py:227), so the trailing-index strip is skipped entirely. Effect is under-merging, not mislabelling: `materialize_mntn_select_16` / `_17` mint separate keys each sweep so run-indexed jobs never accrue a streak. No key is given the wrong identity.

MINOR HEDGE, not a defect. On Astro image-based deploys the dags folder is baked in at $AIRFLOW_HOME/dags, so settings.DAGS_FOLDER does resolve to it. The bundle-path speculation did not materialize.

SEVERITY. Not a blocker: the DAG still succeeds, backlog/digest/ledger still publish, the failure self-reports in the published artifacts, nothing leaks. A shipped feature is dead plus a wasted daily parse of 34 DAG modules inside a task container (with the import-time `Variable.get` side effects that entails). Medium.

</details>

---

### No execution_timeout or dagrun_timeout, with max_active_runs=1: one hung run blocks the DAG forever and pins shared quota

`dags/spark_optimizer_daily.py:36` · found by the **runtime** reviewer · verifier confidence high

**Failure:** GCS throttles or the network degrades and downloads run near their per-object timeout. The task keeps running past 24 hours. `max_active_runs=1` means the next day's run never starts, and the day after that — the sweep silently stops producing reports while the DAG shows "running", which is a state nobody pages on. Meanwhile the pod holds its slice of the deployment's 10 CPU / 20Gi quota for days, so other teams' tasks queue behind it.

**Fix:** Set `execution_timeout` on the task (e.g. 60 minutes) and `dagrun_timeout` via `TEAM.make_dag_args(dagrun_timeout=...)` — the repo's job_config docstring at include/job_config/job_config.py:14-16 documents that parameter explicitly. Also add an overall wall-clock budget inside `sweep()` so the download loops stop and publish a partial report rather than running to the per-object timeouts.

<details><summary>verifier</summary>

Confirmed in source. dags/spark_optimizer_daily.py:36-45 passes only tags and default_args={"retries":1,"retry_delay":30m} to TEAM.make_dag_args; include/job_config/job_config.py:192-193 only emits dagrun_timeout when explicitly passed, so there is none. The @task at :49 sets no execution_timeout, and Airflow's core.default_task_execution_timeout defaults to empty/None (.venv/.../config_templates/config.yml:351-355) with no override in Dockerfile, .astro/config.yaml, or .env.example. No aggregate wall-clock bound exists.

The only bounds are per-subprocess and the reviewer's cites are accurate: fetch.py:37 (900s list), fetch.py:54 (600s per object, looped over LOG_CAP=200 at :50-56), phs.py:83 (600s per batch, looped over up to 500 batches from list_batches limit=500), sweep.py:51 (300s per upload). Up to ~706 unbounded-in-aggregate subprocess calls. Worst case is 900 + 200x600 + 500x600 = 420,900 s = 117 h, not the 58 h claimed; the arithmetic is wrong in the direction that understates the problem.

Alerting gap confirmed: severity defaults to 5, so make_default_args (job_config.py:138-142) attaches no PagerDuty; JobTeamConfig.TARGETING (job_team_config.py:151-157) sets no dag_success_slack, so there is no daily success message whose absence would be noticed; and WATCHDOG_RULES (dags/monitoring/dag_run_duration_watchdog.py:532-539) is an explicit opt-in tuple containing only audience_intent. A run that keeps running emits nothing. With max_active_runs=1 the next day's run stays queued.

Two corrections to the reviewer's framing. (1) "One hung run blocks the DAG forever" overstates it: TimeoutExpired from fetch.newest_logs (:68), fetch.download (:66, :69) and sweep publish is uncaught in the task, so a genuinely hung gsutil fails the task within 600-900s and fires the Slack task-failure callback. Only the phs call is wrapped (:73-77). The unbounded path requires many operations each slow-but-under-timeout, which is exactly the throttling scenario described. (2) Severity: this is a real robustness gap that violates the repo's own documented options (job_config.py:14-29) and the pattern in 11 other DAGs, but it does not break production or leak anything. Blast radius is this DAG's own advisory markdown reports going silent plus one held worker slot; "pins shared quota" for other teams is overstated. Medium, fix before merge: add execution_timeout on the task and either dagrun_timeout via make_dag_args or a WATCHDOG_RULES entry.

</details>

---

### Per-object download failures are counted but never surfaced; 199 of 200 failing is indistinguishable from success

`include/spark_optimizer/fetch.py:53` · found by the **runtime** reviewer · verifier confidence high

**Failure:** The optimizer SA can list `gs://mntn-data-archive-prod/spark-events` but a subset of objects is in a different bucket, is a `.gstmp` remnant, or hits a rate limit. 199 downloads fail, 1 succeeds. `n=1`, the task proceeds, the backlog header reads `Source: ... (newest 1 of 200)` buried in a markdown file in GCS, the digest says "1 Spark job scanned", and the task is green. The ledger then marks 199 jobs' findings as `resolved` after RESOLVE_SWEEPS=3 such days (ledger.py:129-150), reporting fixed problems that were never fixed.

**Fix:** Log each failure with its stderr, and fail the task when the success ratio falls below a threshold (e.g. `n < 0.8 * len(objects)`). Put the scanned/listed ratio into the task's return value and its log line at spark_optimizer_daily.py:92 rather than only into a markdown header.

<details><summary>verifier</summary>

Reproduced in source. fetch.py:53-56 runs gsutil with capture_output and increments n only on returncode==0; r.stderr is discarded and nothing is logged. The return value is consumed at exactly two places: dags/spark_optimizer_daily.py:79 (`if n + phs_n == 0`) and :85 (the provenance string), so any n>=1 proceeds identically to n=200 and the task exits green. The cascade holds: ledger.py:129-150 `_mark_resolved` skips a key only while an entry of its exists in the last RESOLVE_SWEEPS-1=2 seen dates (line 132/139), so 3 consecutive degraded sweeps append `state="resolved"` for every key that stopped appearing, and digest.py:95-98 prints them as "Stopped firing", while ledger.shipped() (line 290) flips those rows' outcome to `resolved` — a fix reported as verified by absence of data rather than absence of the defect. Two of the reviewer's example causes are wrong and do not matter: objects all come from one listed prefix (fetch.py:36) so "different bucket" is impossible, and `.gstmp` remnants are filtered by the `.zstd` suffix test at fetch.py:41. The premise still stands on real partial-failure modes (worker ephemeral disk exhaustion mid-loop, sustained 429/5xx past gsutil's retries, per-object timeout=600 at line 54, an object rolled/deleted between list and cp), and the identical swallow exists at phs.py:71. Two accuracy corrections to the claim's framing: the degraded count is not literally invisible — `scanned` is logged and returned as XCom at dags/spark_optimizer_daily.py:92-96, and the day-3 digest names every falsely-resolved DAG — but nothing compares it to expectation, fails the task, or alerts, and stderr is gone so the cause is undiagnosable. Severity is medium, not blocker: no production pipeline, dataset, or credential is affected; the only corrupted artifact is the optimizer's own ledger under gs://mntn-data-archive-prod/optimizer/, it requires three consecutive degraded sweeps, and it self-corrects on the next healthy sweep (classify() at ledger.py:103-124 re-derives the key as `recurring`, and shipped() overwrites the outcome with the later `fix_not_working`). Concrete fix: log r.stderr per failure and fail the task when n < some fraction of len(objects), so a degraded sweep cannot be laundered into a resolve.

</details>

---

### LOG_CAP slices objects, not logs, so a v2 rolling log can be cut in half and parsed as a complete run

`include/spark_optimizer/fetch.py:44` · found by the **runtime** reviewer · verifier confidence high

**Failure:** An hourly job writes 40 rolling parts. The cap boundary lands mid-log and only parts 18-40 are downloaded. The parse never sees `SparkListenerApplicationStart`, so `app_name` and `duration_ms` are None, stage totals cover only the tail, and the detectors score partial shuffle/spill numbers as if they were the whole run. The backlog reports those numbers as fact. Separately, one job with more than 200 parts consumes the entire day's budget and every other job is silently dropped.

**Fix:** Group listed objects by their `eventlog_v2_*` parent before applying the cap, and take whole logs. Then have `_read_events` reject a rolling directory whose part indices do not start at 1 and run contiguously, so a sliced log surfaces as an error rather than as clean metrics.

<details><summary>verifier</summary>

Reproduced end to end against the source; I could not refute it.

Mechanism, verified line by line:
- fetch.py:36-44 lists every `.zstd` OBJECT under the prefix (`ls -l ... /**`), sorts by creation time, and returns `rows[-cap:]`. Rolling-log parts are separate objects with separate creation times, so the window boundary can fall between parts of one app. Running `newest_logs` with a faked listing (app-A parts 1-4 interleaved with 4 single-file logs, cap=5) returned only `events_3_app-A.zstd` and `events_4_app-A.zstd`.
- fetch.py:24-27 `dest_for` puts both surviving parts back under `eventlog_v2_app-A/`, so the truncated set looks exactly like a complete rolling log on disk.
- crawl.py:54-56 treats any `eventlog_v2_*` dir as ONE log. eventlog.py:126 globs `events_*`, sorts by `_part_order`, and concatenates; nothing checks that index 1 is present or that indices are contiguous.
- Parsed the same dir with and without part 1: full = app_name 'nightly_rollup', duration_ms 8000, jobs 1, stages [1,2,3], mem_spill 126,000,000. Partial = app_name None, duration_ms None, jobs 0, stages [2,3], mem_spill 96,000,000, and `spark_props` EMPTY (SparkListenerEnvironmentUpdate lives in part 1 too, eventlog.py:226). No exception; crawl.py:79 never fires, so JobReport.error is None and sweep counts it in `scored` (sweep.py:64).

Two consequences beyond what the reviewer named:
- Empty `spark_props` flips config-conditioned evidence: optimizations.py:222 computes `spec_off = props.get("spark.speculation","false") != "true"` and prints "spark.speculation is OFF, so nothing re-ran it" as fact for a job that may have it on.
- ledger.py:222 falls back to `source` when app_name is None, so the truncated run's identity becomes the per-run dir `eventlog_v2_app-<stamp>` (verified: `_dag_id` returns it unchanged). That key is unique every run, so its findings always read NEW and the real job's prior findings read RESOLVED — the digest delta is polluted, not just one row.

Caveats that do not rescue the code: the boundary truncates at most one log per sweep at the lower edge; the reviewer's second scenario (one job with >200 parts eating the whole budget) is structurally true but unverified against the real bucket — a 200-part log at Spark's default 128MB roll size is ~25GB of events. The primary defect does not depend on it.

Severity: correcting to medium, not blocker. The DAG is read-only apart from its own `optimizer/` report prefix, nothing in production breaks, nothing leaks. It is a silent-wrong-output defect: one job per sweep can be scored on partial totals and published as fact (in my repro it rendered as "clean"). Cheap fix: group the listing by rolling-log parent and cap on LOGS, taking whole prefixes.

</details>

---

### DagBag parses the entire 99-file bundle inside the task, triggering a module-level Variable.get storm

`include/spark_optimizer/coverage.py:173` · found by the **runtime** reviewer · verifier confidence high

**Failure:** Once per day, the sweep task imports 99 DAG modules — pulling in the google, databricks, cassandra and kubernetes providers — and issues 100+ Variable lookups against the API server from a worker pod, adding several hundred MB of RSS on top of the parse and download memory (against a 0.5Gi default limit) and 1-3 minutes of wall clock. Any DAG module that does I/O at import blocks the sweep. A DAG file with an import error is silently dropped from the coverage denominator, so coverage under-reports without saying so.

**Fix:** Do not DagBag the bundle from a task. Read the DAG/task inventory from the Airflow REST API (the existing `collect()` path), or precompute the operator inventory with a static AST pass over `dags/` — the repo already uses that technique in tests/dags/test_crm_match_rate_config.py:47-53 — which needs neither imports nor Variables nor the DB.

<details><summary>verifier</summary>

Reproduced from source, not speculation. coverage.py:173 constructs DagBag(dag_folder=None); airflow/models/dagbag.py:143 (apache-airflow 3.0.3 in the repo .venv) resolves that to settings.DAGS_FOLDER and :161-166 calls collect_dags() from __init__, importing every discovered file in-process via process_file (:281). The path is live in prod: dags/spark_optimizer_daily.py:86 passes airflow_base="local" -> sweep.py:83 collect_local -> coverage.py:191 -> :173. The Variable mechanism is confirmed at airflow/models/variable.py:146: when SUPERVISOR_COMMS is present (true in a task process) Variable.get routes through the Task SDK to the API server, one round trip per call. The silent-undercount sub-claim is also real: _load_bag_and_paused returns bag.dags only and never reads bag.import_errors, and render() (:216) disclaims only when cov.error is set, which import errors do not set, so an unimportable DAG drops out of the denominator unannounced. Two number corrections: dags/.airflowignore plus safe-mode filtering reduces the import set to 73 files, not 99 (reproduced with Airflow's own list_py_file_paths), and module-scope Variable.get across those 73 is 147 calls in 50 files (tpa_export/tpa_ipdsc_export.py has 13 alone), so "100+ lookups" is if anything understated. Unverifiable from the repo: the 0.5Gi limit and the 1-3 minute wall clock (no executor/resource config in .astro/config.yaml). "Any DAG module doing I/O at import blocks the sweep" is generic rather than instantiated: the only import-time I/O present is the Variable.get calls; the requests.get and BigQueryHook hits (targeting/fetch_common_crawl.py:63, attribution/tmobile_blocked_ip_workflow.py:79) sit inside @task bodies. Not a blocker: it is wrapped in try/except at sweep.py:86 and coverage.py:192, runs once daily in one isolated task, breaks no other pipeline and leaks nothing; worst case the sweep OOMs and retries once. Also worth noting for the author, create_session() at coverage.py:174 needs metadata-DB access a worker normally lacks in Airflow 3, so the task can pay the whole 73-module parse cost and still emit "coverage unknown".

</details>

---

### subprocess timeouts raise TimeoutExpired, contradicting the "never raises" / "skipped" contracts in four places

`include/spark_optimizer/sweep.py:51` · found by the **runtime** reviewer · verifier confidence high

**Failure:** The sweep completes the full crawl, writes the backlog, coverage and digest, appends the ledger — then the first `publish` upload hangs on a slow connection and hits the 300 s timeout. `TimeoutExpired` propagates out of `run()`, out of the task, and the `TemporaryDirectory` context manager at spark_optimizer_daily.py:59 deletes every output. Nothing is published, the ledger append is lost with the tempdir, and 40 minutes of work is discarded. The retry starts from scratch.

**Fix:** Wrap each `subprocess.run` in `try/except subprocess.TimeoutExpired` and handle it the way each docstring already promises — skip the object, or record the upload as failed and continue to the next file. Publish the ledger first so its append survives a later upload failure.

<details><summary>verifier</summary>

Reproduced from source; could not refute. `subprocess.TimeoutExpired` is confirmed a subclass of `Exception` (verified by execution), and `subprocess.run(timeout=N)` re-raises it after killing the child.

Uncaught sites, 3 of the 4 claimed are live:
- include/spark_optimizer/sweep.py:51 `timeout=300`, no try/except; docstring line 39 says "Returns what landed; never raises". `publish()` is called at sweep.py:108 and `sweep_mod.run(...)` at dags/spark_optimizer_daily.py:83 is unguarded, so it propagates out of the task.
- include/spark_optimizer/fetch.py:37 `timeout=900` on `gsutil ls -l <prefix>/**` (full recursive listing of gs://mntn-data-archive-prod/spark-events) — unguarded at DAG:68. Most realistic hang candidate.
- include/spark_optimizer/fetch.py:54 `timeout=600` per object x LOG_CAP=200 objects; docstring line 48 says "a failed object is skipped" — unguarded at DAG:69, so one hung object kills the loop after up to 199 successful downloads.

The failure scenario checks out and is slightly worse than described: the ledger is LAST in the publish list (sweep.py:108 `[backlog, digest_path, coverage, ledger_path]`), and `ledger = os.path.join(outdir, LEDGER_NAME)` (DAG:62) lives inside the `TemporaryDirectory` at DAG:59, whose only persistence is that upload. A timeout on any earlier file therefore drops the ledger append while backlog/digest may already be in GCS.

Reviewer overcounted one site: phs.py:83 (and phs.py:31) do violate the "skip unreachable quietly" docstring at line 75, but the caller wraps `phs.fetch_logs(...)` in `except Exception` at DAG:73-77, so TimeoutExpired there is caught and logged. coverage.py:114 is likewise contained by sweep.py:86.

Severity corrected down from blocker to medium. Nothing leaks and no prod pipeline breaks: this is one daily report DAG with `retries: 1`. The work is recoverable — the GCS ledger is never mutated on the failing path, and `ledger.classify` filters prior entries with `e.get("date") != date` (ledger.py:103), so a retry on the same `ds` is idempotent. Real cost is a failed task in a shared deployment plus a wasted ~1.6 GB re-download.

Related and arguably worse, though outside this claim: a plain rc!=0 upload failure at sweep.py:52-55 only prints and continues, so the task returns SUCCESS with `published: []` while the tempdir silently discards backlog, digest, coverage and the ledger append.

</details>

---

### A zero-byte or truncated download parses to an empty run and is reported as a clean job, not an error

`include/spark_optimizer/eventlog.py:141` · found by the **runtime** reviewer · verifier confidence high

**Failure:** Disk fills mid-sweep (see the PHS finding) or gsutil leaves a truncated/zero-length object. Those logs report as `clean` in the backlog and contribute zero findings. The ledger then sees their previously-firing keys go absent and, after RESOLVE_SWEEPS=3 such sweeps, appends `resolved` entries (ledger.py:129-150) claiming the problems stopped firing. The digest announces fixes that never happened.

**Fix:** Treat an empty or event-free parse as an error: raise `ValueError` from `_read_events` on a zero-byte part, and have `parse_eventlog` raise when it saw no `SparkListenerApplicationStart` and no events at all, so `crawl` records it as SKIPPED rather than clean.

<details><summary>verifier</summary>

REAL, reproduced empirically. I created a 0-byte file named app-20260820-0001.zstd and ran crawl() on its directory: output was "- app-20260820-0001.zstd: clean" with error=None, findings=0, and it was counted in the "1 job scanned" headline. Mechanism is exactly as described: eventlog.py:139-141 reads 4 magic bytes, b"" != b"\x28\xb5\x2f\xfd", so it falls to _plain_lines, yields zero events, parse_eventlog returns a default SparkRun; crawl.py:78 builds JobReport(findings=[], app_name=None, error=None); crawl.py:96-97 renders "clean"; sweep.py:64 (scored = [r for r in reports if not r.error]) counts it as a scanned job. The author's own docstring at eventlog.py:122-123 states the opposite contract, and the test at tests/test_eventlog.py:174 asserts that contract only for the zstd-magic-plus-garbage case, never for the empty file.

TWO CORRECTIONS to the reviewer's framing.

(1) "Truncated" is wrong for the normal case. In the same experiment, a file truncated to 3000 bytes from the real fixture (magic intact) routed to _zstd_lines and raised ValueError at eventlog.py:161, surfacing as "SKIPPED". Only a 0-byte file, a file truncated below 4 bytes, or an empty/short plain-JSON log reaches the clean path.

(2) The ledger consequence is overstated. _mark_resolved (ledger.py:132,139) skips any key seen in the last RESOLVE_SWEEPS-1 distinct sweep dates, so a false "resolved" requires the SAME job's log to parse empty on three consecutive daily sweeps. A one-off empty download yields silent under-reporting, not a phantom fix announcement. Moreover, false "resolved" is already reachable without this defect: LOG_CAP=200 (dags/spark_optimizer_daily.py:32) means any job whose logs age out of the cap goes absent and resolves identically, so the digest consequence is not exclusive to this bug.

Reachability is genuine, not speculative: fetch.download (fetch.py:53-55) only counts rc==0 downloads while crawl globs the whole temp dir, so bytes left by a failed copy are still scanned; phs.fetch_logs (phs.py:86-89) sets files=[] on rc!=0 but only rmdirs when the dir is empty, leaving a partial -r copy's fragments in place for the crawl.

SEVERITY: medium, not blocker or high. This is a read-only reporting DAG whose only write is its own gs://mntn-data-archive-prod/optimizer/ prefix. The defect silently under-reports and inflates its own "N jobs scanned" headline; it does not break a production pipeline, corrupt shared data, or leak anything. The fix is small: raise in _read_events when a part yields zero parsed events (or when the part is zero-length), so crawl records it as an error instead of "clean".

</details>

---

### Any auth or permission regression produces a permanently green DAG that does nothing

`dags/spark_optimizer_daily.py:79` · found by the **auth** reviewer · verifier confidence high

**Failure:** roles/iam.serviceAccountTokenCreator on spark-optimizer@mntn-prj-prod-00 is removed during an IAM cleanup, or storage.objectViewer on gs://mntn-data-archive-prod lapses. Every gsutil call 403s. objects=[], n=0, phs_n=0, the task logs "Listed 0 objects but downloaded none. Idling." and returns {"scanned": 0, "findings": 0, "high": 0}. No failure callback, no PagerDuty, no Slack. The DAG stays green indefinitely and the team reads the absence of findings as a clean fleet.

**Fix:** Raise on non-zero rc in newest_logs and on the ledger download; make download return (ok, failed) and fail the task when failed/total exceeds a threshold. Add a hard floor: if n + phs_n is below an expected minimum (the DAG already asserts ~160 logs/day at line 30-31), raise instead of returning.

<details><summary>verifier</summary>

Reproduced end to end in source. fetch.newest_logs (include/spark_optimizer/fetch.py:36-44) runs `gsutil ls` and never checks r.returncode; a 403 puts the error on stderr, leaves stdout empty, and the `.zstd` suffix filter drops any stray text, so it returns []. fetch.download (fetch.py:47-57) increments n only when rc==0 and has no failure branch. phs.list_batches (phs.py:33-34) returns [] on any non-zero rc, so phs_succeeded/fetch_logs cascade to phs_n=0. dags/spark_optimizer_daily.py:79-81 then logs a warning and returns {"scanned":0,...}; a task function that returns normally is SUCCESS. No alert can fire: the DAG passes no `severity` to make_dag_args, so severity defaults to 5 and job_config.py:136-142 skips PagerDuty, while the Slack callback at job_config.py:126-133 is an on_failure_callback that never runs on success. Nothing verifies the impersonation took effect or that a plausible number of logs arrived. The same vendored library raises on non-zero rc in coverage.py:116-117, so the acquisition path's silence is an oversight, not a stated design. Two corrections to the reviewer's write-up: (1) on a total 403 the early return skips sweep_mod.run entirely, so no dated digest is published at all — the GCS reports go stale rather than showing a fabricated clean fleet; (2) the partial regression is worse than the one claimed — with n>0 but a fraction of the fleet, ledger._mark_resolved (ledger.py:128-150) appends state="resolved" rows for every key absent RESOLVE_SWEEPS=3 sweeps, writing false "stopped firing" history into the append-only ledger republished to gs://mntn-data-archive-prod/optimizer/. Severity corrected down from blocker: the DAG is read-only except its own optimizer/ prefix, breaks no production pipeline and leaks nothing; the ledger false-resolve path keeps it above low.

</details>

---

### `fix_not_working` never reaches the digest — it renders as "No change since the last sweep"

`include/spark_optimizer/ledger.py:346` · found by the **data** reviewer · verifier confidence high

**Failure:** Reproduced twice. (a) A fix is marked applied for `site_network_hourly/shuffle_fetch_wait:9` on 2026-08-02; the detector keeps firing 08-03/04/05. The ledger correctly computes `state="fix_not_working"`, and the digest published to GCS says "No change since the last sweep." (b) `job_a` resolves on 08-04, regresses on 08-05 and 08-06; the digest does not mention `job_a` on either day. In both cases the ledger holds the right answer and the artifact people read states the opposite.

**Fix:** Add `fix_not_working` and `regressed` fields to `Delta`, bucket them in `delta()`, and render them as their own sections in `digest.render` (before "New today" — a shipped fix that did not work is the highest-value line in the digest). In `classify`, when `past[-1]["state"] == "resolved"` set `state="regressed"` and restart the streak rather than falling through to `recurring`.

<details><summary>verifier</summary>

CONFIRMED by execution, not inspection. `Delta` (include/spark_optimizer/ledger.py:336-343) declares exactly four fields and `delta()` (ledger.py:346-357) buckets only new / chronic / resolved / STICKY. `classify` can emit two states that match no branch: `fix_not_working` (ledger.py:119) and `recurring` (ledger.py:124). Neither reaches `digest.render`, and `digest.py:100` prints "No change since the last sweep." when all four buckets are empty. Reproduced with the real modules: applying a fix on 2026-08-02 for site_network_hourly/shuffle_fetch_wait:9 and letting it keep firing yields digest "No change since the last sweep." on 08-03 (recurring) and again on 08-05 (fix_not_working), with only 08-04 (chronic) visible. The perverse part the reviewer understates: the finding is rendered while `chronic` and goes SILENT on the exact sweep it becomes `fix_not_working` — marking a fix applied makes the unfixed defect disappear from the daily report. Even if it were bucketed, `digest._line` (digest.py:62) annotates only owner_notified/wont_fix, so no line would say the fix failed. Regression half also reproduced: job_a resolves 08-04 (Entry streak=0, ledger.py:148), refires 08-05/08-06 → `classify` inherits streak 0 (ledger.py:108) giving 1 then 2 → `recurring` (ledger.py:121-124), absent from the digest both days; it returns 08-07 mislabeled "Chronic — day 3" rather than as a regression. Two framing corrections. (1) The literal string "No change since the last sweep." only appears when the whole sweep has no other new/chronic entry; in a 200-log production sweep the digest renders its other sections and silently omits the fix_not_working line, which is the same defect with a less dramatic symptom. (2) The data is not lost: `shipped()`/`render_shipped` (ledger.py:270-325) correctly reports outcome=fix_not_working, and the raw ledger JSONL is published to GCS (sweep.py:108, dags/spark_optimizer_daily.py:89) — but `render_shipped` is only reachable from the `__main__` CLI (ledger.py:373), never from sweep.run, so no published markdown carries the state. Not a blocker: nothing breaks, crashes, or leaks, the ledger holds the right answer, and the still-firing finding remains in the backlog (unattributed). Medium: the primary human-read artifact contradicts the ledger on the one state the module docstring (ledger.py:19-22) says the register exists to produce. Suppressing `recurring` on its own is defensible noise control; suppressing `fix_not_working` is not.

</details>

---

### Ledger read-modify-write has no compare-and-swap, so any concurrent writer is silently clobbered

`include/spark_optimizer/sweep.py:51` · found by the **data** reviewer · verifier confidence high

**Failure:** An engineer runs `python -m include.spark_optimizer.ledger applied site_network_hourly shuffle_fetch_wait:9 https://github.com/.../pull/412 2026-08-22` against a local copy at 09:05 and uploads it. The 09:00 DAG run finishes its crawl at 09:20 and publishes the copy it downloaded at 09:00. The `applied` row is gone, `fix_pr` is never carried forward by `classify` (ledger.py:109-112), `shipped()` returns no row for the fix, and the finding keeps being reported as `chronic` forever. The same clobber deletes `owner_notified`, which ledger.py:16 promises is sticky.

**Fix:** Publish the ledger with a generation precondition (`gsutil -h x-goog-if-generation-match:<gen>` using the generation recorded at download) and retry the full download-append-upload on 412. Better: keep human decisions in a separate `decisions.jsonl` object that the sweep only ever reads, so the two writers never share an object.

<details><summary>verifier</summary>

Confirmed against source and reproduced end to end.

Mechanism verified:
- `sweep.publish` (include/spark_optimizer/sweep.py:51) runs `["gsutil","-o","GSUtil:check_hashes=never","cp", f, dest]` with no `-o GSUtil:...if-generation-match`, no precondition, no ETag. `grep -rn "generation|precondition|etag|lock"` over include/spark_optimizer/ returns nothing. Every GCS write in the package is a plain `cp`.
- `dest` (sweep.py:50) is `basename(f)` under the prefix, and `ledger_path` is in the published list (sweep.py:108), so the object written is exactly `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`.
- The read side is `fetch.download([REPORT_PREFIX/LEDGER_NAME], outdir)` at dags/spark_optimizer_daily.py:66; `fetch.dest_for` (fetch.py:26-27) resolves parent `optimizer`, which is not `eventlog_v2_*`, so it lands at `outdir/optimization_ledger.jsonl` = the `ledger` path at dag:62. `ledger.append` (ledger.py:156) opens it `"a"`. So the uploaded blob is (object as of download) + this sweep's lines. Anything appended to the object in between is destroyed. Classic lost update, last writer wins, in both directions (a human uploading after the DAG's upload clobbers the DAG's entries too).

Human-writer path is real, not hypothetical: `set_state`/`mark_applied` (ledger.py:234-267) and the `__main__` CLI (ledger.py:364-372) only ever touch a local path, and inside Airflow the ledger lives in a `TemporaryDirectory` (dag:59-62) that is deleted on task exit. There is no GCS write path for a human decision anywhere in the package, so download → CLI → upload is the only way to set `owner_notified`/`wont_fix`/`applied`. `max_active_runs=1` (dag:40) bounds only run-vs-run.

Reproduction (real modules, no mocks): built three sweeps → state `chronic`; snapshotted the file as the DAG's 09:00 download; ran `mark_applied` + `set_state` on the object copy; then appended the 09:21 sweep to the stale snapshot and copied it over the object, which is precisely what sweep.py:51 does. Result:
  before clobber: shipped() rows = 1, state = owner_notified
  after clobber:  shipped() rows = 0, state = chronic, fix_pr = ''
Every element of the claimed failure holds: `classify` (ledger.py:109-112) finds no prior entry carrying `fix_pr`, so attribution is not carried forward; the sticky scan (ledger.py:113) finds no STICKY row, so `owner_notified` — promised sticky at ledger.py:16 — is gone; `shipped()` (ledger.py:279) skips every entry with no `fix_pr` and returns nothing; the finding re-reports as chronic indefinitely.

Severity corrected to medium, not blocker. It destroys data and silently violates the module's two stated guarantees ("sticky", "the register"), with no detection and no recovery unless the bucket has object versioning (not configured in this diff). But it breaks no production pipeline, leaks nothing, touches no business data — the blast radius is one advisory report object — and the trigger is a human editing inside a once-daily task window. Recoverable by re-running the CLI if anyone notices.

Worth flagging to the author as the same root cause with a far likelier trigger: the same unconditional `cp` means a transient failure of the ledger download at dag:66 (fetch.download returns 0 and is never checked — its return value is discarded) publishes a ledger containing only today's entries, wiping the entire history in one run with no error raised.

</details>

---

### `ds` labels every artifact but never selects the logs, so reports are attributed to the wrong day

`dags/spark_optimizer_daily.py:68` · found by the **data** reviewer · verifier confidence high

**Failure:** Two ways this goes wrong. (a) `optimizer_backlog_2026-08-21.md` is published containing findings from jobs that ran on 2026-08-22; anyone correlating a finding against that DAG's 08-21 run looks at the wrong execution. (b) The fleet grows past 200 logs/day, or one backfill floods the prefix. The oldest jobs in the window silently fall off the newest-200 slice, are never scanned, and three sweeps later `_mark_resolved` publishes "Stopped firing" for every one of them — with no signal anywhere that the cap truncated the input.

**Fix:** Filter `newest_logs` by the run's real window (`data_interval_start`/`data_interval_end` from the context) rather than newest-N, label the artifacts with the window actually scanned, and raise (or at minimum emit a loud warning that suppresses `_mark_resolved`) when the number of objects in the window exceeds `LOG_CAP`.

<details><summary>verifier</summary>

Reproduced end to end; the refutation attempt failed on every leg.

1. ds is the previous calendar day. dags/spark_optimizer_daily.py:51 takes ds from the context and passes it as `date` (:84). Dockerfile:2 pins AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS="True", so `schedule="0 9 * * *"` (:37) resolves to CronDataIntervalTimetable, whose logical_date is data_interval_start; airflow/sdk/execution_time/task_runner.py:219-230 derives ds from logical_date. make_dag_args (include/job_config/job_config.py:161-193) sets no timetable or timezone, and start_date is UTC. The 09:00 run on 08-22 is therefore stamped ds=2026-08-21.

2. Nothing selects by that date. fetch.newest_logs (include/spark_optimizer/fetch.py:30-44) runs `gsutil ls -l <prefix>/**`, sorts on the creation-time string and returns rows[-cap:]. No predicate. The PHS half is the same shape (phs.py:26-36: newest 500 batches, no date). So the scanned window is the ~200 newest objects ending at 09:00 on 08-22, labelled 08-21 in the backlog filename and H1 (sweep.py:69-71), the coverage filename (:98), the digest filename (:104-105) and its header (digest.py:80), and on every Entry.date (ledger.py:176-181).

3. The truncation is genuinely silent, and worse than stated. sweep.py:85 prints "(newest {n} of {len(objects)})", but `objects` is already capped by rows[-cap:], so the "of" figure can never exceed 200 and never reveals how many were listed. Additionally a v2 rolling log is a directory of many `events_*` parts, each its own GCS object (fetch.py:24-27, crawl.py:43-64), so 200 objects buys well under 200 jobs; the "~160/day so 200 has headroom" comment at dag:30-31 is an unverified assumption that the object-vs-job distinction undercuts. README.md:26-28 does claim the sweep "pulls a full day of event logs."

4. The false-resolve path is real. ledger._mark_resolved (:129-150) grace window is the last RESOLVE_SWEEPS-1 = 2 recorded dates; a key absent from three consecutive sweeps gets an appended state="resolved" with "stopped firing after ...", and digest.py:96-98 prints "*Stopped firing* — <dags>". Consistently truncated early-in-window jobs are absent every sweep, so they qualify. After that, re-firing yields streak 1 → "recurring", not "chronic", so the chronic signal is lost too.

One consequence the reviewer missed: mark_applied (ledger.py:250-267) takes a human-typed real calendar date, while sweep entries are stamped one day behind. classify's after_fix comparison (:114) and shipped()'s dcu_h before/after split (:294-304) both compare those two date spaces directly, so the skew also misattributes fix outcomes.

Severity correction: not a blocker. It breaks nothing in production, writes only to the optimizer's own prefix, and leaks nothing; the damage is a mislabelled internal report plus degraded state in the tool's own ledger under fleet growth. Medium.

</details>

---

### A task retry double-appends the day's rows to the append-only ledger

`include/spark_optimizer/ledger.py:184` · found by the **data** reviewer · verifier confidence high

**Failure:** Reproduced: recording the same date twice leaves 2 of 3 lines carrying `date="2026-08-02"`. State is not double-advanced (classify drops same-date rows at ledger.py:103, and next-day streak was still 3/chronic), but the file is no longer one-row-per-key-per-sweep: `shipped()`'s dcu scan (ledger.py:294-304) and the `__main__` summary (ledger.py:377) both read rows as distinct sweeps, and any future consumer counting sweeps from the ledger is wrong by however many retries have occurred.

**Fix:** Make the append idempotent for a given `date`: read the ledger, drop existing rows whose `(date, dag_id, key)` matches what this sweep is about to write, and rewrite the file — or key each row with a run id and dedupe on read.

<details><summary>verifier</summary>

Confirmed by execution, not reading. record() at ledger.py:183-184 calls classify() then append() unconditionally; append (ledger.py:153-159) opens "a" with no key or same-date guard. Repro against the real module: record(reports,"2026-08-01"), record(reports,"2026-08-02"), then a re-record of "2026-08-02" yields 3 lines, two dated 2026-08-02, both state=recurring streak=2. The window is real: sweep.py:108 uploads ledger_path as the LAST of four gsutil cps, and the DAG (dags/spark_optimizer_daily.py:59-96) still has tempdir teardown, two logger.info calls and the XCom return after that; a worker eviction there, or the routine ops case of a human clearing the task, re-runs sweep(), re-downloads the already-published ledger at dag:66 into the same outdir path (fetch.dest_for sends it to the root since its GCS parent is not eventlog_v2_*), and appends the day again.

Two of the reviewer's claimed consequences do not hold. shipped()'s dcu scan (ledger.py:294-304) is unaffected: it is last-write-wins assignment, not a count, so identical duplicate rows overwrite with the same value, and in this deployment it never runs at all because sweep.py:92 calls record() with no dcu map, so dcu_h is None on every row and line 296 skips them. ledger.py:377 is a __main__ debug print that never executes in Airflow.

But the reviewer understated it. classify() DOES have a row-counting consumer they missed: after_fix at ledger.py:114 is a list of rows, and lines 117/120 test len(after_fix)+1 >= RESOLVE_SWEEPS. Repro with mark_applied on 2026-08-02 and a duplicated 2026-08-03 sweep: fix_not_working fires on 2026-08-04 instead of 2026-08-05, with the note "still firing 3 sweeps after <PR>" when only two sweeps have run. So a retry silently makes the ledger declare a shipped fix broken a full sweep early and misstate the count in the note, which is exactly the "did the fix work" question the module docstring (ledger.py:1-23) exists to answer. The author names the invariant himself: _dedup's docstring at ledger.py:189 and test_one_entry_per_key_per_sweep (tests/test_ledger.py:81) enforce one row per key per sweep, but only within a single record() call.

Severity medium, not blocker: nothing crashes, no leak, the write stays inside the objectUser condition on optimizer/, and the day's backlog/digest/coverage outputs are correct. It is silent history corruption in an append-only file with no versioning, repairable only by hand-deduping the JSONL.

</details>

---

### Ledger identity is scraped from detector prose, so rewording a title silently resets every streak

`include/spark_optimizer/ledger.py:65` · found by the **data** reviewer · verifier confidence high

**Failure:** Someone reworks a detector message to "Stages 5 and 9 spend 73% ..." or "Stage-9 spends ...". `finding_key` no longer extracts a stage, so `shuffle_fetch_wait:9` becomes `shuffle_fetch_wait` for every affected job. Every one of those findings is published as `*New today*` with `streak=1`, and three sweeps later `_mark_resolved` publishes "Stopped firing" for the old keys — a fleet-wide false new-and-fixed report caused by an edit to a string, with no test to catch it.

**Fix:** Add an explicit `stage: int | None` field to `OptFinding`, set it where `s.stage_id` is already in scope, and have `finding_key` read that field. Keep the title parse only as a fallback, and add a test asserting the key for each stage-scoped detector.

<details><summary>verifier</summary>

Could not refute it; it reproduces exactly. include/spark_optimizer/ledger.py:65-71 rebuilds ledger identity by splitting `finding.title` on whitespace and taking the token after a literal `stage`. OptFinding (optimizations.py:53-62) has only key/title/impact/evidence/fix/rec_type — no stage field — while the stage-scoped detectors format it into prose at optimizations.py:215, 225, 240, 258, 272, so the structural id is available at the emit site and thrown away.

Reproduced end to end. I changed only the fetch-wait title at optimizations.py:272 from `f"Stage {s.stage_id} spends ..."` to `f"Stage-{s.stage_id} spends ..."` and ran the vendored suite: 51/51 still pass, so nothing catches it (test_ledger.py:26 `test_key_uses_the_stage_not_every_number` asserts against its own hand-written FETCH literal at test_ledger.py:15, not against detector output; test_eventlog.py only asserts `f.key`/`rec_type`/`evidence`, never title shape). Then I drove ledger.record over six sweeps of the identical defect:
  08-18/19/20 title "Stage 9 spends 73%..." -> key shuffle_fetch_wait:9, new -> recurring -> chronic streak 3
  08-21 reworded title -> key shuffle_fetch_wait, state new, streak 1, digest prints "*New today*"
  08-23 -> the new key is "chronic day 3" AND the old key is appended state=resolved, note "stopped firing after 2026-08-20", digest prints "*Stopped firing* — site_network_hourly"
So one sweep publishes the same stall as both a fresh chronic finding and a fixed one, fleet-wide, from a string edit. Reverted the mutation; git status clean.

Two things the reviewer understated. (1) The key is also the handle for human state: set_state/mark_applied (ledger.py:242, 258) match on (dag_id, key), so a reword silently orphans owner_notified/wont_fix stickiness and every fix_pr attribution in shipped(). (2) There is a second, inconsistent parser of the same prose — optimize.py:41 `re.search(r"Stage (\d+)", f.title)` for stage-aware dedup. It disagrees with the ledger's word-split on inputs like "stage 9" (ledger lowercases, the regex does not) and "Stage 9:" (regex matches, `isdigit()` on "9:" does not), so the two can silently key the same finding differently.

Severity, not blocker: nothing breaks on merge and nothing leaks — the trigger is a future edit to a detector string, and the blast radius is report/ledger correctness, not the pipeline. Medium. Fix is small: add `stage: int | None = None` to OptFinding, set it at the five emit sites, and have finding_key prefer the field with the title parse as fallback for old entries.

</details>

---

### No CI workflow runs include/spark_optimizer/tests/ — the "49 tests pass" claim is unenforced

`.github/workflows/pr_model.yaml:8` · found by the **tests** reviewer · verifier confidence high

**Failure:** A contributor edits `include/spark_optimizer/ledger.py` to change the recurring→chronic threshold and breaks `test_states_walk_new_recurring_chronic`. The PR is opened, `pr_model.yaml` does not trigger (no matching path), trufflehog passes, all checks green, PR merges. The daily 09:00 UTC sweep then writes a wrong-state ledger to `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, which is append-only, so the corrupted state persists into every subsequent sweep's digest.

**Fix:** Add a `spark-optimizer` job to a PR workflow triggered on `paths: [include/spark_optimizer/**, dags/spark_optimizer_daily.py, requirements.txt]` that installs the test group plus `zstandard` and runs `python -m pytest include/spark_optimizer/tests -v`. Until that job exists and is green on this branch, treat the test count in the PR description as unverified.

<details><summary>verifier</summary>

Could not refute; every assertion reproduces from source. .github/workflows/ has 7 files, only pr_model.yaml and trufflehog-scan.yaml trigger on pull_request (deploy_dev/deploy_prod are push, deploy_gcs/deploy_model_to_gcs are workflow_call, sync-confluence is workflow_dispatch). pr_model.yaml:8 is `paths:` with the quoted filter on 9-15; the PR touches only dags/spark_optimizer_daily.py, include/spark_optimizer/**, and requirements.txt, none of which match (`model_*.py` is a GitHub glob where * does not cross /, so root-level only). `grep -rn pytest .github/` returns exactly one hit, pr_model.yaml:94 `python -m pytest tests/models -v`. The diff adds zero .github/ files. Count is 51 test functions (4+14+16+4+7+6), no parametrize or Test classes, so the reviewer's 51-vs-49 correction is right. test_states_walk_new_recurring_chronic is at include/spark_optimizer/tests/test_ledger.py:32. Additional corroboration the reviewer omitted: no .pre-commit-config.yaml, tox.ini, Makefile, pytest.ini, root conftest.py, or [tool.pytest.ini_options] in pyproject.toml, so nothing else collects these tests; .astro/test_dag_integrity_default.py exists but no workflow runs it, so the DAG has no import-parse gate either; README.md:58's documented manual invocation only works for test_eventlog.py:261 and test_optimizations.py:69, leaving 33 of 51 tests (the whole ledger state machine) unrunnable that way; and zstandard was added to requirements.txt:21 only, not the pyproject `test` group, so a CI job following the repo's existing `uv export --only-group test` pattern would fail on fixture import. Severity corrected from blocker to medium: nothing breaks on day one and nothing leaks, since the merged code is what the author ran locally. The exposure is future regression only. It is not merely cosmetic, because the ledger target gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl is append-only, so a later state-machine break persists into every subsequent digest, but that is second-order, not a production break at merge.

</details>

---

## LOW (17)

### Second binary fixture leaks the same identity plus the author's private workspace path

`include/spark_optimizer/tests/fixtures/eventlog_cache.zstd:1` · found by the **leakage** reviewer · verifier confidence high

**Failure:** Anyone auditing the prod Airflow image (the fixture ships in it, see the gen_eventlog finding) can read the directory layout of an unrelated private repo off a production container.

**Fix:** Same treatment as eventlog.zstd, or drop this fixture entirely and assert the storage surface from a hand-written 20-line JSON event log with `SparkListenerBlockUpdated` events, which is all `test_parse_storage_surface_real` actually checks (rdd_cached_blocks > 0, cached_rdd_bytes > 0).

<details><summary>verifier</summary>

Reproduced exactly, but the severity framing is inflated.

VERIFIED (zstd -dc on include/spark_optimizer/tests/fixtures/eventlog_cache.zstd, 263,347 bytes): the SparkListenerEnvironmentUpdate record carries System Properties/user.name = "malachi", user.home = "/Users/malachi", user.dir = "/Users/malachi/Developer/work/mntn/workspace", java.library.path = "/Users/malachi/Library/Java/Extensions:...", SparkListenerApplicationStart "User":"malachi", and 22 copies of "spark.sql.warehouse.dir":"file:/Users/malachi/Developer/work/mntn/workspace/spark-warehouse". Total "malachi" count is 27, matching the claim precisely. It is a distinct file from eventlog.zstd, and the path is a different repo than this one (this checkout is .../mntn/airflow-ti).

SHIPS IN THE IMAGE: confirmed. .dockerignore excludes astro/.git/.env/models/utils_*/etc. but not include/, and dags/spark_optimizer_daily.py:56 imports include.spark_optimizer, so include/ must be present in the deployment; tests/fixtures/ rides along. git grep on origin/main finds zero pre-existing "malachi" or "/Users/<name>" strings, so this branch is the first to introduce them.

SEVERITY CORRECTION — low, not a blocker or high:
1. What leaks is a macOS username plus ONE absolute path (a repo root and a Spark-created spark-warehouse sibling). "Directory layout of an unrelated private repo" overstates it: no file listing, no repo contents, no structure is enumerated.
2. The username is already in the branch's own commit metadata (Malachi Dunn <malachi@mountain.com> on every commit), so the incremental disclosure is the path string alone.
3. I scanned the decompressed fixture for AIza*/ya29.*/PRIVATE KEY/*.iam.gserviceaccount.com/gs://*/secret|token|password|credential — zero hits. The log is a synthetic local spark.range() job (818 TaskStart/TaskEnd, App Name "storagefx"); no credentials, bucket names, hostnames, or MNTN data.
4. Nothing breaks. The DAG's runtime behavior is unaffected; the parser never reads user.name/user.dir (eventlog.py:226 handles EnvironmentUpdate only for spark_props, and test_eventlog.py asserts only on shuffle.partitions).
5. The audience is people who already hold private-repo read or Astronomer deploy/exec access.

Real and worth fixing as commit hygiene (regenerate with user.dir/user.home neutralized, or scrub the System Properties block), but it is not a production leak. Side note for the reviewer's triage, not this finding: the sibling eventlog.zstd is the worse of the two, still embedding the full Claude Code scratchpad session path — commit ad0a0a7 removed personal paths from gen_eventlog.py only, never from the committed binaries.

</details>

---

### Cache fixture is unreproducible: no committed script generates it

`include/spark_optimizer/tests/fixtures/eventlog_cache.zstd:1` · found by the **leakage** reviewer · verifier confidence high

**Failure:** A future engineer needs to regenerate the fixture (e.g. to strip the personal paths above, or to cover a Spark 4.x event-format change). They run the only committed generator, get an event log named `audi1191_eventlog_fixture` with different block-update behaviour, and `test_parse_storage_surface_real` fails with no way to recover the original input.

**Fix:** Extend `gen_eventlog.py` to emit both fixtures (add the cache/eviction workload and a second SparkSession with `spark.eventLog.logBlockUpdates.enabled=true`), or replace the binary with a synthetic JSON fixture as described above.

<details><summary>verifier</summary>

Could not refute; reproduced from source and the failure is structural, not just a name mismatch.

Facts verified:
1. /Users/malachi/Developer/work/mntn/airflow-ti/include/spark_optimizer/tests/fixtures/gen_eventlog.py:27 sets appName "audi1191_eventlog_fixture". Decompressing the fixtures: eventlog.zstd's SparkListenerApplicationStart carries App Name "audi1191_eventlog_fixture"; eventlog_cache.zstd carries "storagefx". `grep -rn storagefx` over the repo returns nothing outside the binary itself, so no committed script produces it.

2. The two fixtures are different workloads, not one script run twice: eventlog.zstd = 114 events / 4 jobs / 34 tasks / 0 SparkListenerBlockUpdated; eventlog_cache.zstd = 1742 events / 6 jobs / 818 tasks / 33 SparkListenerBlockUpdated.

3. The generator provably cannot recreate the cache fixture even if renamed. gen_eventlog.py:25-34 never sets `spark.eventLog.logBlockUpdates.enabled=true`, which include/spark_optimizer/eventlog.py:106 documents as required ("storage surface (needs spark.eventLog.logBlockUpdates.enabled=true, else zeros)"). Running parse_eventlog on both:
   eventlog.zstd  -> rdd_cached_blocks=0, cached_rdd_bytes=0
   eventlog_cache.zstd -> rdd_cached_blocks=4, cached_rdd_bytes=13739
   So the committed generator's output fails test_eventlog.py:38-41 `test_parse_storage_surface_real` (asserts both > 0) despite gen_eventlog.py:44 calling `fact.cache()`. The reviewer's "different block-update behaviour" is understated: it is zero block-update events, guaranteed by a missing config.

4. The regeneration motive is live, not hypothetical: eventlog_cache.zstd still embeds `/Users/malachi/Developer/work/mntn/workspace/spark-war...` and `"User":"malachi"`, so the earlier personal-path cleanup did not reach inside the binary fixture.

Note on test_fleet_crawl_ranks_worst_first (test_eventlog.py:249-258): its assertion `reports[0].source.endswith("eventlog.zstd")` is satisfied by the non-cache fixture, so that test depends on eventlog_cache.zstd only as the second crawl input; the hard dependency is test_parse_storage_surface_real.

Severity correction: the reviewer filed this under "leakage" implying high/blocker. It is neither. Nothing in dags/spark_optimizer_daily.py reads the fixtures, no credential or customer data is exposed, and prod cannot break from it. It is a test-reproducibility defect that blocks future maintenance (Spark upgrade, stripping the embedded personal paths). Fix is two lines: add `.config("spark.eventLog.logBlockUpdates.enabled", "true")` and a second session/appName in gen_eventlog.py.

</details>

---

### Module docstring documents a Kubernetes CronJob/ExternalSecret deployment that is not the one being merged

`include/spark_optimizer/coverage.py:10` · found by the **leakage** reviewer · verifier confidence high

**Failure:** An engineer debugging an empty coverage report reads this docstring, goes hunting for the ExternalSecret that should be injecting `AIRFLOW_TI_API_TOKEN` into the deployment, and burns an hour before discovering the DAG never takes that path.

**Fix:** Rewrite the docstring to describe the one path that ships: inside Airflow the DAG bundle is on disk, `collect_local()` parses it, and no credential exists. Delete the two-auth-paths paragraph along with the code it describes.

<details><summary>verifier</summary>

Could not refute — the reviewer's reasoning reproduces exactly from source, and the defect is actually broader than stated.

Confirmed facts:
- include/spark_optimizer/coverage.py:7-13 asserts the module "enumerates every unpaused DAG from the Airflow API" and that there are "Two auth paths, because the sweep runs in two places": laptop via `.claude/scripts/airflow_api.py`, and "the automations container" via `AIRFLOW_TI_API_TOKEN` from "an ExternalSecret".
- Neither shape exists here. `grep -rn AIRFLOW_TI_API_TOKEN` outside spark_optimizer returns nothing; only other hit is tests/test_coverage.py:17 ("the only token that exists in the CronJob"). No ExternalSecret, CronJob, or automations container in the repo. airflow-ti has no `.claude/` directory and no `airflow_api.py` anywhere (`find . -name airflow_api.py` is empty), so the coverage.py:27 default `AIRFLOW_API = os.environ.get("AIRFLOW_API_HELPER", ".claude/scripts/airflow_api.py")` points at a file in a different repo.
- coverage.py:103 does repeat it: "The deployment token if one was injected, else the laptop's astro CLI context."

The actual merged path is a third one the module docstring never mentions: dags/spark_optimizer_daily.py:81 passes `airflow_base="local"`, which sweep.py:83-84 routes to `cov_mod.collect_local(date)` — DagBag parsed in-process, no token, no REST call. `_bearer()` is unreachable in this deployment. So coverage.py:26 ("Absent in any deployed copy, which uses the env token") is affirmatively false for the copy being merged, and collect_local's own docstring at :183-185 contradicts the module header two hundred lines above it.

Severity corrected down to low. It breaks nothing and leaks no secret — `AIRFLOW_TI_API_TOKEN` is a variable name, and the ExternalSecret/CronJob mention only reveals that the library has another home. The hour-burn scenario is also blunted: a real coverage failure surfaces the DagBag exception via sweep.py:87 (`[sweep] coverage skipped: ...`) and coverage.py render's "Could not enumerate DAGs: {error}", so the operator sees a local-parse error string, not a missing-token symptom. It is stale top-of-file documentation plus a dead cross-repo default path, in the same family as the personal-scratchpad paths already stripped in round one. Fix is a docstring rewrite naming collect_local as the deployed path (and dropping the `.claude/scripts` default), not a merge block.

</details>

---

### Test docstrings assert a CronJob deployment shape that does not exist here

`include/spark_optimizer/tests/test_coverage.py:17` · found by the **leakage** reviewer · verifier confidence high

**Failure:** The next engineer reads `test_bearer_prefers_the_injected_token` and concludes this DAG is deployed as a Kubernetes CronJob with a secret-injected Airflow API token, which is wrong in every particular, and preserves the dead auth code because it has tests.

**Fix:** Delete `test_bearer_prefers_the_injected_token` and `test_bearer_names_what_is_missing` along with `_bearer()`. Keep `test_collect_local_classifies_without_a_token` and `test_collect_local_reports_a_broken_bundle_instead_of_raising`, which cover the path that actually runs.

<details><summary>verifier</summary>

Reproduced in full; could not refute. All three quoted strings exist verbatim: test_coverage.py:1 ("the container has no astro CLI, so the injected token has to win"), :13 ("shelled out to the astro CLI despite an injected token"), :17 ("The ExternalSecret value is the only token that exists in the CronJob").

The described deployment does not exist in this repo. Repo-wide grep (excluding .git/.venv) finds "CronJob" at exactly one location: test_coverage.py:17. There are no Kubernetes manifests; this is an Astronomer deployment (.astro/config.yaml, .github/workflows/deploy_prod.yaml). AIRFLOW_TI_API_TOKEN is never set in .astro/, .github/, Dockerfile, airflow_settings.yaml, or docker-compose.override.yml — it appears only in coverage.py and its test.

The branch the two tests protect is dead in production. dags/spark_optimizer_daily.py:86 passes airflow_base="local", which sweep.py:83 routes to cov_mod.collect_local(date). _bearer() is called only from collect() (coverage.py:138) and the module CLI (coverage.py:274), neither of which executes in the deployed task. The laptop fallback .claude/scripts/airflow_api.py is also absent from this checkout, so both _bearer() branches are unreachable in effect here.

It also directly contradicts the DAG's own docstring at dags/spark_optimizer_daily.py:10: "No key and no API token exist anywhere in this DAG."

Severity correction: not a blocker. Nothing breaks at runtime and nothing leaks in the security sense — AIRFLOW_TI_API_TOKEN is a variable name, not a value, and no hostname, credential, or personal path is exposed. This is stale provenance from the library's prior home carried into a repo where it is false, on a code path that never runs. Low. A medium case exists because contradicting the DAG's own key-free security claim could lead a future engineer to provision a token or ExternalSecret this deployment neither needs nor should hold.

</details>

---

### Six new test files sit outside the repo's tests/ tree and never run in CI

`include/spark_optimizer/tests/__init__.py:1` · found by the **leakage** reviewer · verifier confidence high

**Failure:** A change to `ledger.classify()` breaks the chronic/resolved state machine. All 13 ledger tests would catch it; none of them run. The PR goes green, the daily digest starts reporting every standing finding as `new`, and nobody notices because the digest is the only consumer.

**Fix:** Move the package to `tests/spark_optimizer/` (matching `tests/models/`) and add a `pytest tests/spark_optimizer -v` job with `paths: [include/spark_optimizer/**]`, or at minimum extend the existing CI step to `python -m pytest tests/models include/spark_optimizer/tests -v` and add `zstandard` to the `test` dependency-group so it can actually run.

<details><summary>verifier</summary>

Reproduced from source. `grep -rn pytest .github/workflows/` returns exactly one hit, pr_model.yaml:94 `python -m pytest tests/models -v`. No conftest.py outside .venv, and pyproject.toml has no [tool.pytest.ini_options]/testpaths (no pytest.ini/setup.cfg/tox.ini/pre-commit either). So the 6 files under include/spark_optimizer/tests/ are collected by nothing. They are real, runnable tests: `python3 -m pytest include/spark_optimizer/tests/test_ledger.py -q` gives 16 passed, and test_states_walk_new_recurring_chronic (:32) plus test_resolved_only_after_the_grace_window (:110) directly exercise ledger.classify() at include/spark_optimizer/ledger.py:98 and _mark_resolved at :127. The claimed failure scenario is accurate.

Two corrections. (1) test_ledger.py has 16 tests, not 13. (2) The implied fix (move to tests/) would not help: pr_model.yaml is also paths-gated on models/**, utils_model/**, utils_deploy/**, model_*.py and two JSON files, so it does not trigger on this PR at all, and tests/dags/ plus the two top-level test files are equally uncollected on main today. Root cause is a missing repo-wide test job, not this PR's file placement.

Severity lowered to low: the failure is a daily markdown digest relabeling standing findings as `new`. Nothing breaks in production, no data is corrupted, nothing leaks, and the PR regresses no currently-working check on a brand-new self-contained module.

</details>

---

### README documents oncall_daily_optimizer.sh as the laptop entrypoint; the script was not vendored

`include/spark_optimizer/README.md:26` · found by the **leakage** reviewer · verifier confidence high

**Failure:** An engineer wants to reproduce a sweep locally, looks for the named shell script, finds it does not exist, and does not realise that `dags/spark_optimizer_daily.py:59-90` is now the only acquisition path (tempdir, download, run, publish).

**Fix:** Replace the parenthetical with the actual local recipe: `python -m include.spark_optimizer.sweep <dir> --date YYYY-MM-DD` after downloading logs by hand, and drop the shell-script references in sweep.py and fetch.py.

<details><summary>verifier</summary>

Half real, and badly mischaracterized. VERIFIED TRUE: include/spark_optimizer/README.md:26 names `oncall_daily_optimizer.sh`; a repo-wide grep (excluding .git/.venv) finds that string at exactly one place, README:26, with no such file anywhere in the tree and no code invoking it (`git log --all -S oncall_daily_optimizer` returns only this branch's own commit, so it never existed here). sweep.py:3 ("The shell script owns acquisition (GCS + PHS downloads)") is stale in the same way. A dangling documentation reference does exist.

VERIFIED FALSE: the claimed failure scenario. dags/spark_optimizer_daily.py:59-90 is NOT "the only acquisition path." Acquisition is vendored as importable library code: fetch.py:30 `newest_logs`, fetch.py:47 `download`, fetch.py:24 `dest_for`, plus phs.py:26 `list_batches`, phs.py:41 `phs_succeeded`, phs.py:74 `fetch_logs` — and phs.py:93 carries its own `if __name__ == "__main__"`. The DAG at lines 66-74 is a thin caller of those functions, not the implementation. The reviewer's own second citation refutes the point: fetch.py:3-4 reads "The shell entrypoint has done this since the laptop days; this is the same logic in Python so the sweep can run as an Airflow task with nothing but the package on the worker" — that tells the reader the Python module IS the replacement, not that a missing script is required.

An engineer reproducing locally is also not stranded: README's "## Use" block at lines 20-24, six lines ABOVE the offending sentence, documents the two working commands (`python3 -m include.spark_optimizer.optimize <eventlog>` and `python3 -m include.spark_optimizer.crawl <dir_or_glob>`), and the sentence at line 26 itself names the runnable module it drives (`include.spark_optimizer.crawl`). sweep.py:119-147 additionally exposes a full argparse CLI (`main()`), so the post-download half runs standalone too.

SEVERITY: not "leakage" at all — a bare filename in prose is not a credential, key, personal path, or internal secret, so this is not the same class as the scratchpad paths already fixed. Zero runtime effect: nothing imports, executes, or shells out to that name, so the DAG behaves identically. Real impact is two stale doc sentences (README:26, sweep.py:3) that should be reworded to point at fetch.py/phs.py. That is a low-severity documentation nit, not a blocker and not a production or security issue.

</details>

---

### Blocking dependency cited as a bare cross-repo PR number (mntn-devops#4724)

`include/spark_optimizer/README.md:52` · found by the **leakage** reviewer · verifier confidence high

**Failure:** The DAG's PHS half silently returns 0 logs. An operator reads `PHS half skipped` in the task log, chases mntn-devops#4724, and finds it merged, then has to re-diagnose from scratch because the real cause is elsewhere.

**Fix:** Since the grant has landed, delete the three references. If a caveat is still needed, describe the permission (`storage.objectViewer on the prod Dataproc temp bucket`) rather than a PR number in another repo.

<details><summary>verifier</summary>

Substance reproduces, failure scenario does not. The stale bare cross-repo reference exists verbatim at exactly the three cited lines (README.md:52, phs.py:8, phs.py:104; `grep -rn 4724` returns only those three, none under dags/), and per the stated IAM the optimizer SA already holds storage.objectViewer on phs.py:19's PHS_TEMP_BUCKET, so all three are now wrong. That is a real cleanup item — a cross-org ref (mntn-devops vs SteelHouse) that will not even auto-link. But every step of the claimed failure is wrong. (1) phs.py:104 is not printed in production: it sits inside `if __name__ == "__main__"` (line 93) behind an additional `"--fetch" in sys.argv` gate (line 101), while the DAG imports the module at spark_optimizer_daily.py:56 and calls phs.fetch_logs directly at line 74, so the block never executes and the string cannot reach a task log. (2) The DAG's only PHS log line, spark_optimizer_daily.py:76 `logger.warning("PHS half skipped: %s", e)`, interpolates the exception and contains no PR number, so nothing in the log sends an operator to mntn-devops#4724. (3) "PHS half skipped" does not fire on a 403 at all: a denied gsutil cp exits non-zero and phs.py:85 converts that to `files = []` with no raise (pinned by test_fetch_leaves_no_empty_dir_for_an_unreachable_batch), and list_batches swallows non-zero returncode and JSONDecodeError at lines 33-38 — so a 403 produces phs_n=0 with the DAG logging nothing about PHS whatsoever. Line 76 only triggers on TimeoutExpired or a missing gcloud/gsutil binary. The genuine defect in this area is the unconditional error swallow at phs.py:85 and phs.py:33, which is a separate finding and would not be fixed by editing a comment. Severity is a documentation-hygiene nit in a vendored library: it alters no code path, breaks no production behavior, and leaks no secret, so it is low, not a blocker.

</details>

---

### The documented offline test command crashes with TypeError

`include/spark_optimizer/README.md:58` · found by the **leakage** reviewer · verifier confidence high

**Failure:** `python3 -m include.spark_optimizer.tests.test_eventlog` prints nothing, runs the first two tests, then dies with `TypeError: test_optimize_entrypoint_end_to_end() missing 1 required positional argument: 'monkeypatch'`. The engineer concludes the package is broken.

**Fix:** Delete both `__main__` blocks (test_eventlog.py:261, test_optimizations.py:69) and document `pytest include/spark_optimizer/tests` instead, which is how these are meant to run anyway.

<details><summary>verifier</summary>

Could not refute — reproduced verbatim by running the documented command. From the repo root, `python3 -m include.spark_optimizer.tests.test_eventlog` exits with:

  File "/Users/malachi/Developer/work/mntn/airflow-ti/include/spark_optimizer/tests/test_eventlog.py", line 264, in <module>
    test_optimize_entrypoint_end_to_end()
  TypeError: test_optimize_entrypoint_end_to_end() missing 1 required positional argument: 'monkeypatch'

Every detail of the reviewer's scenario checks out. The `__main__` block at include/spark_optimizer/tests/test_eventlog.py:261-268 calls six functions with zero arguments; three take a required `monkeypatch: pytest.MonkeyPatch` positional — line 45 (test_detectors_flag_skew_on_real_run), line 103 (test_optimize_entrypoint_end_to_end), line 249 (test_fleet_crawl_ranks_worst_first). The crash lands on the third call because line 264 orders test_optimize_entrypoint_end_to_end first among the three. It prints nothing beforehand: the only `print` is line 268, after the failing call. The two preceding tests do run and pass (the missing `zstandard` module is irrelevant here — eventlog.py:172 falls back to the `zstd` CLI, which is installed). The other half of the brace-expanded command, `python3 -m include.spark_optimizer.tests.test_optimizations`, succeeds and prints its OK line, since its `__main__` at line 69-74 calls only no-arg tests. So the README line fails halfway through, which is the most confusing possible outcome for someone following it.

Severity correction, downward. The reviewer's own stated consequence — "the engineer concludes the package is broken" — is the whole blast radius, and it is a documentation/DX defect only:
- Nothing in production touches this path. `grep -n "test|pytest|__main__" dags/spark_optimizer_daily.py` returns zero matches; the DAG never invokes the test modules or their `__main__` blocks. No runtime, identity, IAM, or GCS behavior is affected, and nothing leaks.
- The tests themselves are correct and green. `python3 -m pytest include/spark_optimizer/tests/test_eventlog.py -q` → 14 passed in 0.11s. The bug is in the hand-rolled `__main__` shim and the README line advertising it, not in the library or its coverage.
- The fix is one line (document `python3 -m pytest include/spark_optimizer/tests/`, or drop the three fixture-taking calls from the `__main__` block).

Real, cheap, worth fixing before merge so the vendored library's stated validation story actually runs — but not a blocker and not high. Correct severity is low. Filing it under a "leakage" review pass is also a category error; there is no leakage of any kind here.

</details>

---

### Four docstring lines describe a shell+cron+GitHub-runner deployment instead of the Airflow DAG being merged

`include/spark_optimizer/sweep.py:3` · found by the **leakage** reviewer · verifier confidence high

**Failure:** An on-call engineer debugging a missing report reads line 7, goes looking for an `outputs/` directory on the worker, and does not find it because the tempdir was already torn down at line 90. Line 41's justification for publishing to GCS instead of committing is nonsense in the merged shape and will be cited in a future design argument.

**Fix:** Rewrite the docstring around the actual caller: the DAG downloads into a tempdir, calls `run()`, and `publish()` copies the four artifacts to `gs://mntn-data-archive-prod/optimizer/` because the task's tempdir is destroyed on exit.

<details><summary>verifier</summary>

Cannot refute: all four lines exist verbatim at the cited line numbers in include/spark_optimizer/sweep.py and the merged code contradicts them. Line 3 says "The shell script owns acquisition (GCS + PHS downloads)" while dags/spark_optimizer_daily.py:66-74 does the acquisition via fetch.newest_logs/fetch.download/phs.fetch_logs; fetch.py:3 in the same package states the opposite ("this is the same logic in Python so the sweep can run as an Airflow task"). Line 7 "all under the ticket's outputs/" is wrong under every invocation: OUTDIR defaults to "optimizer_out" (sweep.py:27) and the DAG passes outdir=os.path.join(tmp,"out") from a TemporaryDirectory (line 59); "outputs/" appears nowhere else except a test literal (tests/test_ledger.py:138). Line 41's GitHub-runner rationale is inert - the task's only identity is CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT (DAG line 54). Line 120's "daily cron calls this" points at main(), which nothing in the merged shape invokes (the DAG calls sweep_mod.run() at line 83). Severity is low, not blocker/high: this is docstring drift with zero runtime effect, and nothing sensitive is exposed - it leaks a prior deployment shape, not a secret or a path. The reviewer also overstates two of the four: README.md:26 explicitly documents a dual shape ("the spark_optimizer_daily DAG in production, oncall_daily_optimizer.sh on a laptop"), so lines 3 and 120 are accurate for the laptop/CLI path and are unlabeled rather than false; only 7 and 41 are wrong in all shapes. The claimed on-call failure is weak because dags/spark_optimizer_daily.py:94-95 logs every published gs:// destination, so the debugging path surfaces the real location before anyone reads a library docstring. Two stale strings the reviewer missed: eventlog.py:121 ("OOMs the cron") and tests/test_coverage.py:17 ("the only token that exists in the CronJob").

</details>

---

### Streaming rationale cites 'the cron' as the process that would OOM

`include/spark_optimizer/eventlog.py:121` · found by the **leakage** reviewer · verifier confidence high

**Failure:** Someone sizing worker memory for this DAG reads the note, does not connect 'the cron' to the Airflow worker, and does not account for the streaming decompression footprint of 200 logs.

**Fix:** Say 'OOMs the task' or 'OOMs the worker'.

<details><summary>verifier</summary>

The artifact is real but the harm is not; it is a cosmetic doc-hygiene nit, not a memory-sizing hazard.

VERIFIED: include/spark_optimizer/eventlog.py:121 reads verbatim "Streams line-by-line (a 98MB .zstd expands to ~1.8GB; materializing it OOMs the cron)." No cron exists in the merged shape (dags/spark_optimizer_daily.py:36-58 is a @dag on schedule "0 9 * * *" invoking sweep_mod in-process). A second instance sits at include/spark_optimizer/sweep.py:120: "CLI entry point - the daily cron calls this once the downloads are on disk." Same class of residue as the scratchpad paths the author already stripped, so flagging it is legitimate.

REFUTED, the claimed failure scenario: (1) The note's operative content is that the function does NOT materialize the decompressed log; it is a bounded-memory reassurance. A reader sizing the worker draws the identical conclusion whether they picture a cron or a task pod. The stale noun changes no number. (2) "The streaming decompression footprint of 200 logs" is not a thing in this code. include/spark_optimizer/crawl.py:70-80 iterates event logs strictly sequentially, and the SparkRun returned by analyze_eventlog is dropped at the end of each iteration (only findings and app_name are retained on the JobReport). There is no parallelism anywhere in the package: grep for ThreadPool/ProcessPool/concurrent.futures/multiprocessing/max_workers across include/spark_optimizer/ and the DAG returns zero hits. Peak footprint is one file's TextIOWrapper buffer (eventlog.py:157-160), not 200x anything. (3) The DAG itself already names the real resource driver, and it is not parse memory: spark_optimizer_daily.py:29-31 says "Raising this raises the download, which is the task's only real resource cost (~7.9 MB per log)."

So the defect that survives is only that merged production code names a process that does not exist, which mildly misleads a future maintainer about provenance. Fix is a two-word docstring edit in two files. Nothing breaks and nothing leaks, so this is low, not blocker or high.

</details>

---

### Test docstring cites IMP-029, an improvement-backlog ID from outside this repo

`include/spark_optimizer/tests/test_eventlog.py:117` · found by the **leakage** reviewer · verifier confidence high

**Failure:** A reader wanting the history behind the part-ordering bug has a dead reference; the docstring's own sentence already carries the whole fact, so the ID adds only confusion.

**Fix:** Delete the parenthetical.

<details><summary>verifier</summary>

Reproduced from source. `/Users/malachi/Developer/work/mntn/airflow-ti/include/spark_optimizer/tests/test_eventlog.py:117` reads: `"""A v2 rolling dir is parsed across ALL events_* parts, in numeric part order (IMP-029)."""` A repo-wide grep for `IMP-[0-9]+` across airflow-ti returns exactly that one line — nothing defines the namespace, and neither `include/spark_optimizer/README.md` nor `__init__.py` links a backlog.

IMP-029 resolves only in the author's private workspace repo (mdunn-mntn/malachi-workspace), `improvements_backlog.md:62` — "Handle eventlog_v2_* rolling event-log DIRECTORIES... FIXED 2026-08-07". That repo is not readable by SteelHouse/airflow-ti engineers, and IMP- is explicitly a non-Jira namespace (the backlog's own keywords line says "would-be ticket, not on jira"). So the reference is genuinely dead for this repo's audience, and it is the same residue class as the hardcoded personal scratchpad paths the author already removed in round one.

The best refutation available does not hold up. airflow-ti does embed external tracker IDs in docstrings (`dags/machine_learning/segment_quality_scoring_dag.py:1` "TI-956", `dags/models/feature_store_snapshot.py:163` "AUDI-1005", and the vendored `include/spark_optimizer/__init__.py:1` "AUDI-1194"), so a bare ID is not itself off-convention. But those are Jira keys resolvable at mntn.atlassian.net — segment_quality_scoring_dag.py:17 even gives the URL. IMP-029 has no such destination. The distinction the reviewer draws is real.

Severity is overstated by the "leakage" label. Nothing sensitive is disclosed: no credential, path, hostname, or PII — only the existence of an internal backlog row. It sits in a test docstring, is unreachable at runtime, and the sentence already carries the whole fact without the ID. Nothing breaks in production. This is a one-word documentation nit (delete the parenthetical), not a security or correctness issue. Corrected to low.

</details>

---

### A failed ledger upload is reported as a successful run and loses that sweep's state irrecoverably

`include/spark_optimizer/sweep.py:108` · found by the **data** reviewer · verifier confidence high

**Failure:** The three markdown reports upload fine and the ledger upload hits the objectUser condition, a quota error, or a timeout. The task goes green and the digest is published as if the sweep were complete. Tomorrow's run downloads a ledger that is one day stale: streaks are understated by one, `_mark_resolved`'s grace window (ledger.py:132) shifts by a date, and any finding that first appeared on the lost day is reported as `new` a second time.

**Fix:** Have `run` raise when `gcs_prefix` is set and `ledger_path` is not in `published` — the ledger is the only artifact whose loss is not recoverable by re-running. Log `r.stderr` through the `logging` module rather than `print`, so the failure is visible in the Airflow task log.

<details><summary>verifier</summary>

Mechanism reproduces, but the reviewer's trigger list and blast radius are both wrong.

VERIFIED IN SOURCE:
- sweep.py:51-55 — non-zero `gsutil cp` is printed and the loop continues; the dest is simply absent from `landed`.
- sweep.py:108-116 — `publish([backlog, digest_path, coverage, ledger_path], ...)` result is stuffed into the return dict; nothing asserts `ledger_path`'s dest is in it.
- dags/spark_optimizer_daily.py:59-90 — `ledger = os.path.join(tmp, "out", LEDGER_NAME)` lives inside `with tempfile.TemporaryDirectory(...) as tmp:`; the block closes at line 90 and the only local copy is deleted.
- dags/spark_optimizer_daily.py:94-96 — logs only what landed; no check, no raise. No downstream task consumes the XCom.
- include/spark_optimizer/tests/* — zero tests reference `publish`/gsutil, so nothing pins this behaviour.
- digest.py never reports ledger persistence, so the published digest does read as complete.

TWO OF THE THREE NAMED TRIGGERS ARE FALSE:
1. "hits the objectUser condition" — impossible. dest is `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, object name `optimizer/...`, which satisfies the condition. All four files go to REPORT_PREFIX (dag:29), so the condition cannot block the ledger while passing the three markdowns.
2. "or a timeout" — `subprocess.run(..., timeout=300)` at sweep.py:51 is not wrapped; a timeout raises `subprocess.TimeoutExpired`, which propagates out of `publish` (the "never raises" docstring at :39 is wrong) and out of `run`, failing the task RED and firing the DAG's one retry (dag:43). No green run.
Only the plain non-zero-exit path (quota, persistent 5xx, revoked grant) produces green-with-lost-ledger. Real, but narrower than claimed. Also not fully silent: sweep.py:55 prints the failed dest into the task log.

SEVERITY OVERSTATED. The lost state is change-detection metadata for an internal markdown digest, not a production dataset. Concrete drift, all confirmed against ledger.py: `entry.streak = last.streak + 1` (:108) is permanently off by one for pre-existing keys, delaying `chronic` (threshold RESOLVE_SWEEPS=3, :121) by one day; keys first seen on the lost day get one duplicate `new` line; `_mark_resolved`'s `recent = seen_dates[-2:]` (:132) shifts one date, delaying a `resolved` line by a day. Nothing breaks in production, nothing leaks, no downstream consumer, no pipeline stalls. That is low, not blocker — the missing guard is worth adding (raise or at minimum fail the task when `ledger_path`'s dest is absent from `published`), but it does not gate the merge.

</details>

---

### `owner` and `dcu_h` are never populated, so the register's cost columns can never fill in

`include/spark_optimizer/sweep.py:92` · found by the **data** reviewer · verifier confidence high

**Failure:** `render_shipped` (ledger.py:319-320) prints `-` for both `DCU/h before` and `after` on every row forever, so the register that exists to answer "did the fix reduce cost" never answers it. `digest._line` (digest.py:58-60) never renders the DCU annotation, and `_section`'s secondary sort key (digest.py:73) is a constant, so the digest's ranking degrades to impact-only with arbitrary ties. `shipped()`'s entire second pass over the ledger (ledger.py:292-304) is a no-op.

**Fix:** Either wire a DCU-hours source into the sweep (Dataproc batch metadata is already enumerated in phs.list_batches) and pass `dcu=`/`owners=` through, or delete the columns and the annotation rather than shipping a register whose outcome measure is structurally empty.

<details><summary>verifier</summary>

Reproduces exactly from source, but the reviewer overstates the consequence.

Verified: sweep.py:92 is the only non-test caller of `ledger_mod.record` and passes neither `owners` nor `dcu`. `record`'s argparse CLI (sweep.py:120-134) exposes no flag for either, and the laptop driver (/Users/malachi/Developer/work/mntn/workspace/.claude/scripts/oncall_daily_optimizer.sh:87,163) also goes through that CLI. Repo-wide, case-insensitive `dcu` hits only ledger.py, digest.py, and tests/test_ledger.py:44,48,136 — no cost source is vendored at all. So ledger.py:179-180 writes `owner=""` and `dcu_h=None` on every row this DAG will ever produce, and ledger.py:296 `continue`s on every entry, making the second pass at 292-304 a no-op. Confirmed.

Severity corrections (why this is low, not a blocker/high):
1. Nothing breaks or misreports. digest.py:59 guards with `if dcu:` so the annotation is silently omitted; ledger.py:319-320 renders `-`, which honestly means "unknown", not a fabricated 0. No crash, no wrong number, no leak.
2. The register is not part of what the DAG publishes. `shipped()`, `render_shipped()`, `mark_applied()`, and `set_state()` have zero call sites outside ledger.py's own `__main__` block (361-379) and tests — the DAG (dags/spark_optimizer_daily.py:83-90) publishes only backlog, digest, coverage, and the JSONL. And since nothing in the deployed path calls `mark_applied`, no row ever carries `fix_pr`, so `shipped()` returns [] and `render_shipped` prints "No optimizations recorded as shipped yet." The cost columns are unreachable before they are unpopulated. This is an unwired hand-run tool, not a degraded production artifact.
3. The ranking claim is partly wrong: digest.py:72 uses `sorted()`, which is stable, so a constant secondary key leaves ties in entries order (deterministic per sweep), not "arbitrary".
4. `owner` is deader than claimed but harms less: render_shipped's columns (ledger.py:313) include no owner column, so the field is unread everywhere except being copied forward at 148/244/262 and into the row dict at 284. Its emptiness is invisible.

Net: a real dead-parameter / unfinished-feature finding worth a comment on a package vendored into prod, not a merge blocker.

</details>

---

### Nothing anywhere catches the new DAG failing to import — both candidate suites are dead

`tests/dags/test_dag_example.py:81` · found by the **tests** reviewer · verifier confidence high

**Failure:** `dags/spark_optimizer_daily.py:22` imports `include.job_config`, and line 41 calls `TEAM.make_dag_args(...)`. If `JobTeamConfig.TARGETING` is renamed, or `include/spark_optimizer/__init__.py` gains a module-level import of `zstandard` before the image is rebuilt with the new `requirements.txt`, the file raises at parse time. CI is green, the file lands in the shared bundle, and the failure appears only as a red import-error banner in the prod Airflow UI after deploy — discovered by whoever next opens the UI, not by the merge.

**Fix:** Wire `pytest tests/dags` into a PR workflow on `paths: [dags/**, include/**]`, and first repair `test_dag_example.py:81` so it does not error on the existing fleet (e.g. assert `retries` is present and `>= 0`, or drop the retries test and keep only `test_file_imports`). Alternatively add a targeted `tests/dags/test_spark_optimizer_daily.py` that does `DagBag(dag_folder="dags/spark_optimizer_daily.py", include_examples=False)` and asserts `not dag_bag.import_errors`.

<details><summary>verifier</summary>

Could not refute — every factual assertion reproduces from source, and I found one point in the reviewer's favor they missed. (1) The only pytest in CI is .github/workflows/pr_model.yaml:94, scoped to tests/models; stronger still, pr_model.yaml:8-15 paths-filters to models/**, utils_model/**, utils_deploy/**, two JSON files and model_*.py, none of which this PR touches, so that workflow never fires on this PR — only trufflehog-scan.yaml does. deploy_prod.yaml only calls deploy_gcs.yaml (uploads spark/ to GCS), and the Dockerfile has no test step. (2) tests/dags/test_dag_example.py:81 is genuinely unrunnable: include/job_config/job_config.py:124-147 make_default_args returns {**args, **kwargs} and never injects retries, so dags/conversion_signal/conversion_workflow.py:150 (default_args={}) gives None >= 2 → TypeError, while dags/ddp_url_verticals_filtered_backfill.py:21 and dags/domain_vertical_mappings_backfill.py:21 set retries: 0 → AssertionError. Red on main today. (3) .gitignore:12 lists .astro/ and `git ls-files .astro` is empty; the parse gate exists only in the local working copy. (4) tests/test_attribution_bundle_imports.py:15,45 is hard-scoped to dags/attribution (its REPO_ROOT = parents[2] is itself wrong) and is not in CI. The repo documents the gap itself at tests/dags/test_tpa_ipdsc_export.py:3. Severity lowered to low, not blocker/high: nothing breaks now. JobTeamConfig.TARGETING exists (include/job_config/job_team_config.py:151) and the vendored library plus zstandard are imported inside the task body at dags/spark_optimizer_daily.py:56, not at module level, so parse-time surface is only pendulum/airflow/include.job_config. The claimed failure is conditional on a future edit, the gap is pre-existing and applies to every DAG on main rather than being introduced by this PR, and an Airflow 3 import error is isolated to the one file — no prod outage, no credential exposure.

</details>

---

### 7 of 51 tests silently depend on a zstd decoder that is declared nowhere CI can see

`include/spark_optimizer/tests/test_eventlog.py:165` · found by the **tests** reviewer · verifier confidence high

**Failure:** Someone follows up on the previous finding and adds `pytest include/spark_optimizer/tests` to `pr_model.yaml` using the existing `uv export --only-group models --only-group test` install step. The new job fails immediately with `FileNotFoundError: [Errno 2] No such file or directory: 'zstd'` (or `assert 0 >= 1` from `test_fleet_crawl_ranks_worst_first`, whose JobReport carries `error="[Errno 2] No such file or directory: 'zstd'"`), and the whole parser half of the suite is red on a runner image that lacks the binary.

**Fix:** Add `zstandard>=0.18` to the `test` dependency group in `pyproject.toml:29-31` and refresh `uv.lock`, so the same decoder CI uses is the one the image uses. Then rewrite `test_multiframe_zstd_reads_all_frames` (lines 158-171) to build the multi-frame bytes with `zstandard.ZstdCompressor().compress()` twice instead of `subprocess.run(["zstd", ...])`, removing the host-binary dependency entirely.

<details><summary>verifier</summary>

Reproduced exactly, so the defect is real, but the severity is overstated. Verified premises: grep -c zstandard uv.lock returns 0; pyproject.toml:28-31 [dependency-groups] declares only pytest==8.4.* under test and no zstandard under models; requirements.txt:21 has zstandard (Astro image only); packages.txt and Dockerfile contain no zstd binary (Dockerfile installs only google-cloud-cli). test_eventlog.py:165 is literally `frames += subprocess.run(["zstd", "-q", "-c"], input=chunk.encode(), capture_output=True).stdout`, and eventlog.py:162 imports zstandard with a :172 fallback to subprocess.Popen(["zstd", "-dc", part]). The author's Mac has /opt/homebrew/bin/zstd and NO zstandard module, so the suite passes locally only via the CLI fallback, exactly as the reviewer diagnosed. Empirical repro in a clean venv (pytest only, PATH=/usr/bin:/bin): "7 failed, 44 passed" — the 7 are precisely test_parse_real_eventlog_all_surfaces, test_parse_storage_surface_real, test_detectors_flag_skew_on_real_run, test_optimize_entrypoint_end_to_end, test_multiframe_zstd_reads_all_frames, test_corrupt_zstd_raises_not_clean, test_fleet_crawl_ranks_worst_first, with the exact predicted errors (FileNotFoundError: [Errno 2] No such file or directory: 'zstd' and assert 0 >= 1 from a JobReport carrying that error string). 7+44 = the claimed 51. Where the reviewer overstates: the claimed failure is contingent on a future workflow edit that nobody has made. pr_model.yaml triggers only on models/**, utils_model/**, utils_deploy/**, model_*.py, scripts/generate_ipdsc_third_party_audience_builders.py, dags/model_task_config.json, dags/ipdsc_third_party_audience_builders.json — this PR touches dags/spark_optimizer_daily.py, include/**, requirements.txt, so the workflow does not even fire; and its test step is `python -m pytest tests/models -v`, which never collects include/spark_optimizer/tests. No CI job that exists runs these tests. Production is also unaffected: requirements.txt:21 puts zstandard in the Astro image and _zstd_lines tries the library path first, so the missing zstd CLI is never reached. Real present-tense harm is limited to developer experience: include/spark_optimizer/README.md:58 tells readers to run the offline tests, so a fresh clone without Homebrew zstd shows 7 red tests, and the vendored suite's green status is an artifact of one machine's PATH. That is a dependency-declaration/test-hygiene gap, not a prod break or a leak, so low rather than blocker (medium would be defensible only if the team commits to wiring this suite into CI, at which point the fix is adding zstandard to the test dependency group and relocking).

</details>

---

### The README's documented offline test entrypoint crashes with a TypeError

`include/spark_optimizer/tests/test_eventlog.py:264` · found by the **tests** reviewer · verifier confidence high

**Failure:** A reviewer asked to confirm "49 tests pass" runs the README command, gets a TypeError stack trace pointing at the test file itself, and either concludes the library is broken or gives up and approves on trust. Either way the diff merges into a shared prod deployment without independent verification.

**Fix:** Delete the `__main__` blocks at test_eventlog.py:261-268 and test_optimizations.py:69-74 — they duplicate pytest badly and drift. Change `README.md:58` to `python3 -m pytest include/spark_optimizer/tests` and state the dependency (`pip install pytest zstandard`) alongside it.

<details><summary>verifier</summary>

Reproduced verbatim, so it cannot be refuted. README.md:58 documents `python3 -m include.spark_optimizer.tests.test_{eventlog,optimizations}`; running the eventlog half raises `TypeError: test_optimize_entrypoint_end_to_end() missing 1 required positional argument: 'monkeypatch'` at test_eventlog.py:264. The __main__ block (261-268) calls test_optimize_entrypoint_end_to_end (def line 103), test_detectors_flag_skew_on_real_run (line 45), and test_fleet_crawl_ranks_worst_first (line 249) with no args, but all three declare a required `monkeypatch: pytest.MonkeyPatch` parameter; 264 fails first, leaving 265 and 267 as unreached dead calls. The CI premise also checks out: the only pytest run in .github/workflows is pr_model.yaml:94 (`pytest tests/models`), which never touches include/spark_optimizer/tests/. test_optimizations exits 0, so exactly half the documented path is broken as stated. Severity corrected down to low: this is a stale __main__ shim plus a wrong README line, not a library defect. `python3 -m pytest include/spark_optimizer/tests/ -q` passes 51 tests in 0.15s, and dags/spark_optimizer_daily.py contains no reference to the test package, so no production code path or credential is affected. The reviewer's framing that a reviewer would "conclude the library is broken" overstates the impact given the working pytest command; their "49 tests" figure is also off by two.

</details>

---

### The committed .zstd fixtures cannot be reproduced by the committed generator

`include/spark_optimizer/tests/fixtures/gen_eventlog.py:28` · found by the **tests** reviewer · verifier confidence high

**Failure:** Dataproc Serverless moves to a Spark runtime whose event schema changes a field name, the parser starts returning empty stage metrics on real logs, and the fixtures still pass because they are frozen at Spark 4.0.0. A maintainer tries to refresh them by running `gen_eventlog.py` as the docstring instructs, gets an uncompressed Spark 3.5.3 log with no cache fixture at all, and cannot reproduce either committed file — so the fixture stays stale and the tests keep certifying a parser that no longer matches production.

**Fix:** Make the generator reproduce what is committed: add `.config("spark.eventLog.compress", "true")` and `.config("spark.io.compression.codec", "zstd")`, emit both the skew and the cache fixture in one run, and record the exact Spark version used at the top of the file. Replace `eventlog_cache.zstd` with a hand-written ~2 KB JSON file containing only the `SparkListenerBlockUpdated` events the two storage assertions need, dropping 263 KB of binary from the repo and the prod image.

<details><summary>verifier</summary>

Confirmed from source, not speculation. (1) gen_eventlog.py:25-34 sets only spark.eventLog.enabled/.dir; the committed eventlog.zstd's SparkListenerEnvironmentUpdate lists exactly ['spark.eventLog.dir','spark.eventLog.enabled','spark.rdd.compress'] - spark.eventLog.compress is absent and defaults to false, so the generator emits plain JSON. The committed file is nonetheless a 36-frame zstd (zstd -l); recompressing its decompressed bytes with plain `zstd -c` gives 1 frame / 40.1 KiB vs the committed 36 frames / 75.3 KiB, so an undocumented streaming compressor produced it. grep for gen_eventlog across the repo hits only the docstring at test_eventlog.py:4 and the generator's own comment - no compress/split/rename step exists anywhere. (2) The strongest proof: eventlog_cache.zstd carries "App Name":"storagefx", spark.eventLog.dir=file:///tmp/storage-events, and spark.eventLog.logBlockUpdates.enabled=true, while gen_eventlog.py:27 names the app audi1191_eventlog_fixture and never sets logBlockUpdates. eventlog.zstd contains 0 SparkListenerBlockUpdated events; eventlog_cache.zstd contains 33. eventlog.py:106 itself comments that the storage surface "needs spark.eventLog.logBlockUpdates.enabled=true, else zeros" - so test_eventlog.py:41-42 asserts on a surface the committed generator structurally cannot emit. A second, uncommitted script made that fixture. (3) Both fixtures report "Spark Version":"4.0.0" while pyproject.toml:20 pins pyspark==3.5.3, so running the generator in-repo yields a 3.5.3 log. Corroborating: eventlog.zstd still bakes in spark.eventLog.dir=file:///tmp/claude-501/-Users-malachi-.../scratchpad/spark-events, i.e. the earlier "scratchpad paths removed" fix touched the generator but not the binaries it supposedly produces. Reviewer overstated two details: the 340 KB is both files combined (77105 + 263347 bytes), and eventlog_cache.zstd backs three assertions, not two - it is also an input at test_eventlog.py:255 (test_fleet_crawl_ranks_worst_first). The Spark-3.5-vs-4.0 schema-divergence leg is asserted rather than demonstrated (the field names the parser reads are largely stable across those releases), but the claim stands without it. Severity corrected down: nothing breaks at merge and no data leaks - tests pass today and the parser works on real logs. The defect is a false reproduction instruction plus 340 KB of unregenerable binary, i.e. test hygiene and future maintainability. Fix is cheap: correct the docstring, add the missing spark.eventLog.compress / logBlockUpdates configs and a second app to the generator, or state plainly that the fixtures are captured artifacts and record the exact capture command.

</details>

---

## Refuted (40)

Claims that did not survive an independent attempt to reproduce them from source.

| where | claim | why not |
|---|---|---|
| `dags/spark_optimizer_daily.py:27` | Prod-only service account and bucket constants with no env guard, in a DAG bundle that also deploys to dev | The load-bearing premise is factually wrong: `deploy_dev.yaml` does not deploy the `dags/` tree. It calls only `deploy_gcs.yaml` (which uploads exactly one path, `path: 'spark'`, to `mntn-data-archive-dev/ti_resources` — .github/workflows/deploy_gcs.yaml:28-33) and `deploy_model_to_gcs.yaml` (which  |
| `include/spark_optimizer/phs.py:17` | Prod project and prod Dataproc temp bucket hardcoded as module defaults, duplicating an existing env-aware constant | The literals are real (phs.py:17 PROJECT="mntn-prj-prod-00", phs.py:19 PHS_TEMP_BUCKET=...svhwvc6j), but every load-bearing part of the claim fails. 1) phs.py is not the leak site. dags/spark_optimizer_daily.py already pins prod at module scope: line 27 SERVICE_ACCOUNT=spark-optimizer@mntn-prj-prod- |
| `include/spark_optimizer/coverage.py:27` | Auth fallback points at .claude/scripts/airflow_api.py, a path in the author's private workspace that does not exist in this repo | The quoted line is real but the defect is not. Three independent checks refute it. 1. Nothing leaks. coverage.py:27 is a RELATIVE path, ".claude/scripts/airflow_api.py". `.claude/scripts/` is a public, documented Claude Code convention directory and `airflow_api.py` is a generic filename. It disclos |
| `include/spark_optimizer/tests/fixtures/gen_eventlog.py:10` | Test tree and 332KB of binary fixtures ship into the production Airflow image | The packaging facts are right but the named failure scenario is not reproducible, and the "leak" is not one. VERIFIED TRUE: .dockerignore (lines 1-13) excludes models/, utils_deploy/, utils_model, utils_runner, model_run.py, model_upload.py and does not exclude include/ or tests/. gen_eventlog.py:10 |
| `include/spark_optimizer/tests/fixtures/gen_eventlog.py:27` | Fixture generator stamps the wrong ticket into the Spark app name | The literal observations check out but the defect does not. Verified: gen_eventlog.py:27 does say .appName("audi1191_eventlog_fixture"), and `zstd -dc eventlog.zstd / grep '"App Name"'` returns "App Name":"audi1191_eventlog_fixture", which eventlog.py:223 (`run.app_name = e.get("App Name")`) reads b |
| `include/spark_optimizer/tests/fixtures/gen_eventlog.py:16` | Usage comment shows a home-directory path as the example override | The string at include/spark_optimizer/tests/fixtures/gen_eventlog.py:16 exists (`# EVENTLOG_FIXTURE_DIR=~/spark-fixture python3 gen_eventlog.py`) but it is not leakage. `~` is the POSIX shorthand for the invoking user's home and carries no username, UID, hostname, or machine-specific component. What |
| `include/spark_optimizer/README.md:5` | README anchors the package to airflow_debugger/, a sibling directory that does not exist in this repo | Premise is factually true but the defect is not. `airflow_debugger` appears exactly twice in the repo (README.md:5 and the __init__.py:4 module docstring) and the directory is indeed absent here (it lives in the author's separate workspace at /Users/malachi/Developer/work/mntn/workspace/airflow_debu |
| `include/spark_optimizer/README.md:14` | README defers to 'the AUDI-1194 ticket artifacts' for the Databricks acquisition route | The claimed failure ("the knowledge is unrecoverable from this repo") is contradicted by the very sentence the reviewer quoted. include/spark_optimizer/README.md:10-14 states the route inline: the specced path fails because `jobs get-run-output` returns an empty `notebook_output` even on a SUCCEEDED |
| `include/spark_optimizer/README.md:57` | README states a one-off finding from a specific prod crawl date as package documentation | Not a defect, and the claimed failure scenario is contradicted by the code. (1) README.md:56-57 is a single bullet whose subject is detector validation ("Parser + detectors validated on real Spark event logs..."); the crawl result is the evidence clause, not a backlog entry, and it carries its own d |
| `include/spark_optimizer/__init__.py:1` | Package docstring is the pre-vendoring one: wrong package name, dangling sibling, and a file that is not in this repo | Not reproducible as stated, and not "leakage". (1) "Wrong package name" is wrong: include/spark_optimizer/__init__.py:1 is a descriptive summary line ("Airflow/Spark job optimization crawler (AUDI-1194)"), not a package-name declaration, and it cites this PR's own ticket. The package's name is state |
| `include/spark_optimizer/__init__.py:23` | __all__ and the docstring module list drift after vendoring: 5 of 12 modules listed, and they omit everything the DAG drives | The drift exists but the defect does not, and the reviewer's supporting reasoning is factually wrong on two counts. Verified at /Users/malachi/Developer/work/mntn/airflow-ti/include/spark_optimizer/__init__.py:15-29: the docstring Modules block and __all__ both list 5 names (crawl, eventlog, optimiz |
| `include/spark_optimizer/fetch.py:3` | Docstring frames the module as a port of a laptop shell entrypoint | Refuted on the source. include/spark_optimizer/fetch.py:3-4 reads "The shell entrypoint has done this since the laptop days; this is the same logic in Python so the sweep can run as an Airflow task with nothing but the package on the worker." It names no file — the reviewer imported `oncall_daily_op |
| `include/spark_optimizer/crawl.py:4` | Docstring hedges the GCS event-log prefix as future work; that is the merged DAG's only input | Refuted: the hedged capability still does not exist, and the DAG never exercises it. crawl.py's only path expander, _event_logs (include/spark_optimizer/crawl.py:41-64), is pure local filesystem — os.path.isdir + glob.glob. Hand it "gs://mntn-data-archive-prod/spark-events" and isdir is False, glob. |
| `include/spark_optimizer/optimizations.py:134` | Detector comments cite incident IDs (INC-009, INC-005) that exist in no artifact in this repo | Refuted on four grounds. (1) The reviewer's core premise is false: citing on-call incident IDs in code comments is a PRE-EXISTING repo convention, not introduced here. dags/tpa_export/tpa_ipdsc_export.py:51 ("on-call INC-018") and :56 ("on-call INC-012, INC-016, INC-017") are already on origin/main  |
| `include/spark_optimizer/tests/test_optimizations.py:4` | Test docstring identifies the fixture plan by a foreign incident ID | Refuted on three checks. (1) INC-NNN is not "foreign" or undecodable — it is this repo's existing on-call convention, already shipping on origin/main in dags/tpa_export/tpa_ipdsc_export.py:51 ("on-call INC-018") and :56 ("on-call INC-012, INC-016, INC-017"), and used consistently inside this PR at i |
| `include/spark_optimizer/tests/test_ledger.py:94` | A coworker's first name is hardcoded as test data | Refuted on both prongs. (1) No leakage: SteelHouse/airflow-ti is a private corporate repo whose commit history authors include "Ryan Kleck <rkleck@mountain.com>", and whose origin/main ALREADY carries his full name in production code predating this PR — models/machine_learning/segment_quality_scorin |
| `include/spark_optimizer/digest.py:7` | Module docstring encodes a workspace policy date instead of describing the module | Not a defect, and the two facts the claim rests on are both wrong in source. (1) digest.py:110-114 render_plain does not strip the Slack markup "back out" — it converts `<url/label>` into `label (url)` and removes bold asterisks, preserving the link target in the written file; tests/test_ledger.py:1 |
| `requirements.txt:21` | zstandard added only to the image manifest, not to pyproject.toml or uv.lock | Observations are true but the failure does not reproduce, and the fix would not fix it. 1. CI never runs these tests. `.github/workflows/pr_model.yaml:8-15` path-filters to `models/**`, `utils_model/**`, `utils_deploy/**`, `scripts/generate_ipdsc_*`, `dags/model_task_config.json`, `dags/ipdsc_third_ |
| `dags/spark_optimizer_daily.py:54` | Task mutates the process-wide gcloud impersonation env var and never restores it | The line exists (dags/spark_optimizer_daily.py:54 sets os.environ["CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"] with no try/finally), but every claimed consequence fails against the source. (1) Airflow 3 forks a child process per task instance (.venv/lib/python3.11/site-packages/airflow/sdk/execution |
| `include/spark_optimizer/coverage.py:28` | Three modules default their output paths to a relative directory that depends on the worker's cwd | Refuted. The load-bearing scenario is contradicted by the source. (1) `mark_applied` (ledger.py:250-257) reads history first and raises `ValueError("no ledger history for {dag}/{key}; nothing to mark applied")` before `append()` (ledger.py:153-159) is ever reached; `read()` returns [] for a missing  |
| `dags/spark_optimizer_daily.py:36` | No pool, no queue, no resource bound: a multi-GB multi-hour task competes with real pipelines in default_pool at the deployment's busiest cron slot | The code facts check out but the failure chain does not reproduce from source. (1) Volume: the reviewer's own numbers give 1.6 GB (spark_optimizer_daily.py:30-32, LOG_CAP=200 x ~7.9 MB); the PHS half is called "unbounded" while citing the constant that bounds it, phs.py:26 limit=500, further narrowe |
| `include/spark_optimizer/fetch.py:36` | Unbounded recursive GCS listing of the entire spark-events archive, buffered whole in the task's memory, on every run | The code description is accurate but the failure scenario is unreachable, so this is a style/future-proofing nit, not a defect. fetch.py:36 does shell out to `gsutil ls -l gs://mntn-data-archive-prod/spark-events/**` with capture_output=True and no date bound, and spark_optimizer_daily.py:68 does no |
| `dags/spark_optimizer_daily.py:25` | Failures alarm Targeting's production monitoring channel for a job Targeting does not own, and the PagerDuty connection behind that config belongs to Data Platform | The claim's mechanics are half-wrong and its failure scenario is unsupported by the source. (1) The email leg cannot fire: task_fail_email is never wired anywhere. make_default_args (include/job_config/job_config.py:124-146) builds only the Slack and PagerDuty callbacks; the `email` property (job_co |
| `dags/spark_optimizer_daily.py:43` | The task is one non-idempotent monolith, so the configured retry duplicates ledger entries and repeats the full 1.6 GB download | The two load-bearing mechanisms are contradicted by the source. (1) classify at include/spark_optimizer/ledger.py:103 filters prior entries with `if e.get("date") != date`, so a same-date retry's rows are excluded from _history — state and streak are unchanged. Executed: attempt 1 yields ('chronic', |
| `dags/spark_optimizer_daily.py:28` | Bucket, project and service account are hardcoded to prod, so the same DAG running in the dev deployment reads and writes production | The claimed failure chain breaks at three independent points, and its central premise misreads the cited CI files. 1) The deploy premise is wrong. `.github/workflows/deploy_dev.yaml` and `deploy_prod.yaml` do not deploy DAGs or an image. Each only calls `deploy_gcs.yaml` (single step: `upload-cloud- |
| `dags/spark_optimizer_daily.py:37` | DAG metadata drifts from repo convention: no explicit dag_id, no description, no tz on start_date, ticket-key used as a tag | Every premise fails against the source. 1. "no description → DAG list shows an empty column" is factually wrong. The Airflow task-SDK `dag` decorator populates docs from the function docstring: `/Users/malachi/.cache/uv/archive-v0/IJAQJoTxDaK94WFozok4d/airflow/sdk/definitions/dag.py:1407-1408` — `if |
| `dags/spark_optimizer_daily.py:56` | Task hard-depends on include/, which this repo documents as able to lag a DAG-only deploy, and the vendored test fixtures ship into the prod image | The claimed mechanism is not reproducible from source. (1) The reviewer's load-bearing premise is factually wrong: dags/spark_optimizer_daily.py:22 imports `from include.job_config import JobTeamConfig`, and include/job_config/ is a directory INSIDE include/. So the DAG's parse-time dependency on in |
| `dags/spark_optimizer_daily.py:49` | Task requests no pod resources; 0.25 CPU / 0.5Gi cannot hold ~1.6 GB of downloads plus a Spark log parser | The claimed OOM mechanism does not reproduce from source. (a) The load-bearing premise — a 0.25 CPU / 0.5Gi deployment default pod — appears nowhere: no AIRFLOW__CORE__EXECUTOR, no worker-queue or deployment config, .astro/config.yaml holds only the project name, and the Dockerfile sets only cron-in |
| `dags/spark_optimizer_daily.py:54` | gsutil and gcloud do not inherit the deployment's ADC; setting CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT is not sufficient to authenticate | The mechanism half checks out; the decisive premise does not, and the reviewer concedes it is "unproven in this image." Verified in the local SDK (/opt/homebrew/share/google-cloud-sdk): gcs_json_credentials.py:307 applies impersonation only `if _HasImpersonateServiceAccount() and creds`, and :382-38 |
| `include/spark_optimizer/eventlog.py:207` | Full sparkPlanInfo trees and per-task lists accumulate for the whole parse, defeating the streaming the docstring claims | The code observations are literally correct (eventlog.py:256 retains full sparkPlanInfo per execution; :253 caps only plan_text; :346-347 append one int per succeeded task), but the claimed failure is refuted by measurement. I ran parse_eventlog against synthetic logs exceeding the fleet's worst cas |
| `dags/spark_optimizer_daily.py:51` | get_current_context()["ds"] raises KeyError on a manual trigger with no logical date | The mechanism is real but the claimed failure scenario is refuted by source, and the severity is badly overstated. VERIFIED (the half the reviewer got right): `ds` is gated on a truthy logical_date. Confirmed verbatim in the pinned dev SDK at /Users/malachi/Developer/work/mntn/airflow-ti/.venv/lib/p |
| `include/spark_optimizer/fetch.py:41` | Whitespace-splitting gsutil ls output truncates any object name containing a space, and mis-derives its rolling-log parent | The parse observation is literally true (fetch.py:40 splits on whitespace, so a spaced object name yields only the tail fragment), but every consequence the reviewer attaches to it fails against the source. 1. The chimera path is unreachable. Any space anywhere in the URL leaves the `gs://` scheme B |
| `include/spark_optimizer/eventlog.py:172` | zstd CLI fallback is not installed in the image, and zstandard is missing from pyproject/uv.lock | REFUTED. The mechanism is reproducible only under a precondition the deploy path cannot produce, and the claimed consequence is factually wrong. 1) The fallback is unreachable in any deployable image. eventlog.py:158-171 reaches the Popen at :172 ONLY through `except ImportError: pass`. Every other  |
| `dags/spark_optimizer_daily.py:74` | The PHS download is unbounded and lands on shared worker ephemeral disk; overflow evicts the pod and kills sibling tasks | The claim's load-bearing premise ("no cap at all") is contradicted by the source the reviewer themselves cites: phs.py:26 sets limit=500 on `gcloud dataproc batches list`, a hard ceiling on how many batch dirs dags/spark_optimizer_daily.py:74 can ever fetch. phs.py:41-50 then narrows that 500 to a s |
| `dags/spark_optimizer_daily.py:54` | Impersonation is set as unrestored process-global state instead of scoped per gcloud/gsutil invocation | The mechanism is real but the defect is not; the reviewer's own investigation already removed both concrete failure paths and the residual one does not reproduce. Confirmed accurate: dags/spark_optimizer_daily.py:54 sets os.environ["CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"] with no try/finally and |
| `include/spark_optimizer/fetch.py:36` | Full recursive listing of a growing prod bucket prefix every day, buffered in memory, with a fixed 900s timeout | Mechanics are described accurately (fetch.py:36-37 lists gs://mntn-data-archive-prod/spark-events/** with capture_output=True and timeout=900; DAG:68 is outside the only try at DAG:73-77; no TimeoutExpired handler exists in the package), but the claimed failure does not follow. The reviewer's own nu |
| `include/spark_optimizer/coverage.py:110` | Airflow API bearer-token resolution and an env-controlled arbitrary-module import ship into the prod image as dead code | Refuted. The code exists as described but the claimed failure does not follow from the source. 1) Unreachable, and reaching it is a future code change, not a defect here. `_bearer` is called only from `collect` (coverage.py:138), and `collect` only from sweep.py:84 when `airflow_base` is truthy and  |
| `include/spark_optimizer/sweep.py:108` | Publishing the full DAG inventory into the shared data-archive bucket, whose read scope was never considered | Refuted on facts and on premise. 1) The claim's own evidence is wrong. `coverage.render()` (coverage.py:213-256) never emits `owners` or `tags`. `DagCoverage.owners` is populated at coverage.py:146 and :200 and then read by nothing — grep over the cited range 237-254 returns zero hits for `owner` or |
| `requirements.txt:21` | zstandard is unpinned with no declared floor, and an unusable decoder degrades the sweep to a silent all-clear | The load-bearing premise is factually wrong. `read_across_frames` is NOT a recent addition to `ZstdDecompressor.stream_reader()` — python-zstandard's own NEWS.rst records it added in the 0.11.0 era (2019), and I downloaded and grepped the sdists for 0.11.1, 0.13.0, 0.14.1, 0.15.2, 0.17.0, 0.19.0, 0. |
| `.dockerignore:9` | The test package and 340 KB of fixtures ship into the shared production image but cannot run there | REFUTED — the claimed failure cannot occur, and the mechanism is the repo's existing convention. 1. pytest IS in the prod image. I ran the currently-deployed prod image (cmcv0v0ae01bk01ngimis9kjy.registry.astronomer.run/.../cmcvcbd3j03vk01p91ksvm1vd:deploy-2026-08-19T23-41-54): `python -m pytest --v |
