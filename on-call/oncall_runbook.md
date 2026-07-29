---
doc_type: runbook
title: On-Call Runbook — Master
summary: "Read FIRST on any Airflow/pager/pipeline alert. Triage protocol, alert catalog (signature→verdict→protocol), incident log, producer→consumer maps. Every resolution appends back here."
last_verified: 2026-07-28
keywords: [on-call, oncall, on call, incident, pager, pagerduty, alert triage, airflow failure, airflow alert, pipeline failure, dag failure, task failed, sensor timeout, AirflowSensorTimeout, precondition_bombora, ipdsc_monitor, tpa_ipdsc_export, ipdsc, bombora, DS51, optional partner skip, fangorn_inference_pipeline, inference_pipeline, create-dataproc-cluster, dataproc, dataproc saturation, resource contention, champion challenger, 94% cap, vertex pipeline, benign expected, late data, batch-id trap, force_export, prod safety, escalation, runbook, daily_drift_pipeline, feature drift, fangorn_daily_feature_drift_pipeline, reference_date, run_date, parameter not found, input definitions, ValueError, param mismatch, param contract, TiVertexPipelineOperator, PipelineJob, latest bundled version, audience_intent, fangorn_score_monitor, ipdsc_geo, ModelPysparkBatchOperator, dataproc serverless, dataproc batch, batches wait, driver output, AnalysisException, PATH_NOT_FOUND, path does not exist, producer consumer race, PAM, privileged access manager, storage.objects.get, INC-001, INC-002, INC-003, INC-004, enriched_impressions, analytics_curated, bombora skip downstream, ds51 zero, ds51 disappeared, tpa_mntn_id_export, mntn id export, mntn_id_data, tpa export, ttl exceeded, batch cancelled, batch was cancelled, cancelling batch as ttl exceeded, dataproc serverless ttl, ModelPysparkBatchOperator ttl, FetchFailedException, FetchFailed storm, shuffle fetch, shuffle fetch failure, auth bootstrap timeout, doSparkAuth, SettableFuture timeout, maxExecutors, zero ttl headroom, sh-dw-external-tables, INC-005, recomputation spiral, uncached lineage, shuffle spill, memory bytes spilled, disk spill, spark.sql.shuffle.partitions, shuffle partitions too few, spark event log, eventlog profiler, cache mntn_df, persist dataframe, dataproc temp bucket, spark-job-history, zstd event log, gcloud-crc32c gatekeeper, storage api download]
tags: [on-call, airflow, incident-response]
---

# On-Call Runbook — Master

**Read this FIRST on any on-call alert. Append an incident entry after every resolution.**
The more incidents we log, the faster the next one closes. If an alert matches a row in
§2 Known-Alert Catalog, jump straight to its protocol.

- **Entry point:** run **`/oncall`** (or `/oncall <alert-log-file>`) — it triages, matches the catalog,
  and **enforces the write-back** (§3 + §2 + the JSONL log) so nothing leaks. This runbook is what
  `/oncall` reads and writes.
- **Home:** `on-call/`. A **new** raw alert log lands at the top level (named as downloaded) so the
  triage-reminder Stop hook (`find -maxdepth 1`) flags it as un-triaged. **On resolution, file its logs
  under `on-call/incidents/INC-NNN/`** (one folder per incident), renamed
  `<dag-or-task>_<rundate>_try<N>_<outcome>.txt` — this clears the loose-log signal and keeps evidence with
  its incident. Top level stays just the runbook, `incident_log.jsonl`, and the `incidents/` tree.
- **Indexed:** this file carries `doc_type: runbook` front-matter, so `.claude/scripts/build_index.sh`
  folds its `keywords:` into `knowledge/_ROUTING.md` and lists it in `knowledge/runbooks/INDEX.md`.
  Grep `_ROUTING.md` for an alert symptom (`sensor timeout`, `dataproc`, `bombora`) and it points here.
  **After editing §2/§3 keywords, rebuild:** `.claude/scripts/build_index.sh`.
- **Update rule (3 surfaces, every resolution):**
  1. **§3 Incident log** — full incident (the human narrative + diagnosis + decision tree).
  2. **§2 Known-Alert Catalog** — one-line signature row (so the next match is instant).
  3. **`on-call/incident_log.jsonl`** — one machine-readable record (the queryable index; see §5).

  Never delete rows — a "benign, expected" verdict is as valuable as a fix.
- **Prod safety (non-negotiable):** never modify prod DAGs or push to `main` in `airflow-ti` /
  `sqlmesh` to "fix" an alert. Diagnose → clear/re-run or route to the owner. Widening a timeout or
  soft-failing a sensor is a code change owned by the producing team, not an on-call action.

---

## 0. Is this on-call? — classify FIRST, then pick the surface to write to

On-call work and ticket work look similar (both "something's wrong, investigate") but they are
**different workflows with different homes.** Decide before you start — the write-back surface differs.

**It's ON-CALL (→ this runbook + an INC entry) when the trigger is an operational alert:**
- An Airflow/Astronomer task **FAILURE** or **retry-exhausted** email/Slack (`🔴 [prod] Airflow <Team>
  FAILURE [dag/task] at <ts>`), a **PagerDuty** page, a sensor **timeout**, a pipeline that **broke**.
- The job is to **restore/clear/route** and **explain the alert**, not to answer an open question.
- Output = a resolved alert + an incident record that makes the next identical alert instant.

**It's a TICKET (→ `tickets/`, `/frame`, `summary.md`) when the trigger is a question or a change:**
- "Did X move a KPI?", "size/evaluate Y", "build Z", "why does the system do W?" — analysis, design,
  or a deliverable. No pager fired; nobody is waiting on a pipeline.
- The job is to **answer a falsifiable question** and produce a deliverable.

**Decision rule:** _Did an alert/pager fire and is a pipeline currently degraded?_
→ **yes = on-call**, use `/oncall`, write to this runbook.
→ **no = ticket**, use `/frame`, write to `tickets/`.

**Where each artifact goes:**

| Artifact | On-call | Ticket |
|---|---|---|
| Entry-point skill | `/oncall` | `/frame` → work → `/capture` |
| Working record | §3 incident (`INC-NNN`) | `tickets/<key>/summary.md` |
| Fast-match index | §2 catalog row + JSONL | `tickets/INDEX.md` + `_ROUTING.md` keywords |
| Raw evidence | `on-call/<downloaded log>` | `tickets/<key>/outputs/` |
| Durable code fix | **route to owning team** (never hot-patch) | the ticket / a PR |

**The crossover:** an alert that reveals a real, recurring defect (not a one-off) **spawns a ticket for
the durable fix** — but the incident still gets logged here first. Example: INC-001's durable fix
(`soft_fail=True` on optional-partner preconditions) is an `airflow-ti` change → propose it as a ticket,
don't hot-patch. The INC entry records "routed to ticket TI-XXX"; it doesn't do the code change.

---

## 1. General triage protocol (any Airflow alert)

1. **Identify** DAG + task + logical date from the alert (`[prod] Airflow <Team> FAILURE [dag/task] at <ts>`).
2. **Pull the task log**, find what the task is *actually doing* — not that it failed:
   - **Sensor** → the poke target (`Sensor checks existence of : <bucket>, <object>`).
   - **Producer/Spark/BQ** → the output path / query / the real exception (search the log tail for `ERROR`/`Exception`/`Traceback`, skip the boilerplate).
3. **Check empirical state** — did the thing it waited on / was supposed to write actually land?
   `gcloud storage ls -l "gs://<bucket>/<path>/"` (reauth with `gcloud auth login` if you get
   `Reauthentication required`; do it as ONE call — parallel gsutil calls trip the reauth quota).
4. **Classify** (see verdict taxonomy below).
5. **Act** per class. **Log** the incident on all 3 surfaces (§3 + §2 + JSONL).

**Verdict taxonomy**

| Class | Signature | Action |
|---|---|---|
| **Benign / expected** | Alert is a known side-effect of intended behavior (e.g. optional-partner skip). Main pipeline succeeded. | Ack. Reply in thread "expected, <reason>". No re-run. Log it. |
| **Late data** | The awaited object exists *now*, arrived after the sensor's window. | Clear the failed task → it passes immediately. Not an outage. |
| **Transient infra** | A downstream cloud resource failed to provision/transiently errored (e.g. Dataproc cluster create, quota/stockout, 5xx). Config + inputs are fine. | Re-run the task once. If it recurs, check quota/region capacity, then route to the owning team. |
| **Resource contention** | Our OWN concurrent job holds the resource (e.g. a challenger/QA run saturating Dataproc → `create-dataproc-cluster` code 9). Not stockout, not config. | Do NOT blind-re-run (it re-fails while the other job holds it). Confirm no concurrent job, let it FINISH, then re-trigger. Recurs → durable fix (stagger/quota) → `improvements_backlog.md`. |
| **Real upstream failure** | Object genuinely absent AND was required; or producer task threw a real error. | Find + re-run the producer task (mind batch-id traps), or route to the feed/vendor owner. |
| **DAG/logic bug** | Wrong path, bad param, code regression. | Route to the owning team with the evidence. Do NOT hot-patch prod. |

---

## 2. Known-Alert Catalog (signature → verdict → protocol)

Grep the **DAG/task key** to match fast. If your alert's key is here, jump to its protocol.

| DAG / task key | Alert signature | Root cause | Verdict | Protocol |
|---|---|---|---|---|
| `ipdsc_monitor / precondition_<partner>` | GCS sensor **18h timeout** (e.g. `precondition_bombora`, DS51) `AirflowSensorTimeout` | Optional 3P partner didn't deliver source files that day → producer skips it silently → monitor pages on the absent `ipdsc/dt=.../data_source_id=<id>/` partition | **Benign / expected** on partner-skip days (verify source absence first) | INC-001 |
| `fangorn_inference_pipeline_run / inference_pipeline` | `RuntimeError: Job failed with: code: 9 … failed tasks are: [create-dataproc-cluster]` (PagerDuty page, retries exhausted) | Fangorn (or any Fangorn-like) inference pipeline saturates Dataproc at ~94%; ANY concurrent Dataproc job — even in QA / a challenger run — starves `create-dataproc-cluster` → code 9. Resource contention, NOT stockout or config. | **Resource contention** — confirm no other Dataproc job is running, let the concurrent/challenger job FINISH, then manually re-trigger the champion. Blind re-run fails while a job still runs. | INC-002 |
| `fangorn_inference_pipeline_run / daily_drift_pipeline` | `ValueError: The pipeline parameter reference_date is not found in the pipeline job input definitions` (retries exhausted → PagerDuty). **Different task + signature from INC-002 — not resource contention.** | `TiVertexPipelineOperator` ALWAYS injects `reference_date` into the Vertex `parameter_values`, but the drift template declares `run_date` (its KFP source `fangorn_daily_feature_drift_pipeline.py:393` uses `run_date`) → `PipelineJob.__init__` rejects the unknown param before submission. Param-contract mismatch. | **DAG bug** — route to owner (Brian/ML). **PR #1158 (airflow-ti) does NOT fix it** (confirmed: re-run on the fixed bundle re-failed identically); the operator-injected `reference_date` is the failing param. Real fix = rename the KFP pipeline param `run_date`→`reference_date` in **`targeting-infra-ml`** + recompile/redeploy the template. Do NOT blind-re-run until that ships. **RESOLVED 2026-07-28** (Brian redeployed template, green on try 5). | INC-003 |
| `audience_intent / fangorn_score_monitor` | Airflow log = boilerplate `AirflowException: … Dataproc Agent reports job failure`; **batch driver output** = `AnalysisException [PATH_NOT_FOUND]: gs://mntn-data-archive-prod/ipdsc_geo/dt=<run_date>`. PagerDuty, retries exhausted. | Consumer `ModelPysparkBatchOperator` reads `ipdsc_geo/dt=<run_date>`, which lands on D+1 with ~3.5h-variable timing (tpa_export `run_geo`); monitor has only `retries=2×10min` + no cross-DAG sensor → races the producer, pages when it slips past ~07:45Z. | **Late data** (this case) — pull the driver output for the real error (Airflow log is boilerplate), confirm `ipdsc_geo/dt=<run_date>/_SUCCESS` is present, then clear+re-run the monitor. If partition still absent → real upstream failure, re-run tpa_export `run_geo`. **RESOLVED 2026-07-29.** | INC-004 |
| `tpa_mntn_id_export / tpa_mntn_id_export` | Airflow log = boilerplate `AirflowException: Batch job <id> was cancelled`; **batch `stateHistory`** = `Cancelling batch as ttl exceeded` (ran the full `ttl=10800s`=3h); **event log** = the final `.write.json()` shuffle stage recomputed 7-9× (same `json at ...` call site, ~1900GB each), 29TB memory + 14TB disk spill, `shuffle.partitions=1000`, 150 executors 0 removed. All retries exhausted. | **DAG_BUG (Spark perf), verified from the event log.** The `mntn_df` lineage is never cached, so the ~1.9TB `mntn_id` shuffle is recomputed 7-9× per action + FetchFailed resubmit; `shuffle.partitions=1000` → ~1.9GB partitions → 29TB spill → I/O-bound tasks (70-97% fetch-wait, 8-10% CPU) → past 3h. NOT infra (0 executor loss), NOT data volume (inputs identical to last good day), NOT contention. | **dag_bug (perf)** — get the TTL reason from `batches describe` stateHistory; then download+parse the Spark **event log** (`eventlog_profiler.py`) for the real profile (driver output alone is not enough). Fix is owner-side: **cache `mntn_df`** + raise `shuffle.partitions` 1000→~6000 + collapse the 14 crossJoins, then a modest TTL bump. A re-run may pass (spiral is timing-dependent) but does NOT fix it. Do NOT hot-patch. **OBSERVED 2026-07-29, root cause verified; 07-28 export still missing.** | INC-005 |

---

## 3. Incident log

### INC-001 — `ipdsc_monitor` `precondition_bombora` sensor timeout (DS51 Bombora)
**Date:** 2026-07-28 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [ipdsc_monitor/precondition_bombora] at 2026-07-26 17:05 PT` · `AirflowSensorTimeout: run duration 64836s exceeds timeout 64800.0` (18h).

**STATUS: RESOLVED — confirmed benign by owners (no action).** Sean Yang, Brian McAdams (Sr MLE), and
Jordan Piepkow (Staff SWE, author of the skip step) confirmed in #alerts-tpa-pipeline that a missing
Bombora drop "is not an error" — it's skipped by design. Standing practice: **let it slide, drop a note in
the #alerts-tpa-pipeline thread** so the next on-call doesn't re-investigate. Escalate only if Bombora
misses go chronic (feed → vendor, not a re-run).

**Open design question (Jordan, 2026-07-28): remove the sensor, or not?** Recommendation = don't remove;
make it not *page*. Set `soft_fail=_partner.optional` on the registry-driven preconditions in
`ipdsc_monitor` (mirrors `wait_{name}_src` in `tpa_ipdsc_export`): absent partition → SKIPPED not FAILED →
no alert, while drop days stay monitored (removing the sensor would also drop that QA coverage). Optionally
`mode="reschedule"` so it doesn't hold a worker slot for 18h. One-line change, owned by TPA_EXPORT team.

**Provenance (traced 2026-07-28 — the Bombora drop IS the external top of what we own):** No MNTN code
we control fetches Bombora. In airflow-ti nothing writes `partners/bombora/` (only wait/read); our S3→GCS
transfer DAG `storage_transfer.py` does NOT include Bombora; grepping all 8 local MNTN repos found zero
Bombora references outside airflow-ti's read path. The one MNTN transform (`ipdsc_bombora` builder) is
*downstream* of the drop and correctly skips when source is absent. Delivery is automated (good drops land
~20:1x UTC daily, e.g. 07-25 file created 07-26 20:16Z). The only hop I couldn't inspect is whether that
automation is Bombora pushing straight to GCS vs a managed GCP Storage Transfer job in `mntn-prj-prod-00`
(Transfer API disabled on my project) — but owners treat it as a vendor drop, so external.

**Verdict: BENIGN / EXPECTED.** Bombora (DS51) is an `optional: true` partner. It didn't deliver its
source files, so the producer skipped it and no `data_source_id=51` partition was written; the separate
`ipdsc_monitor` DAG doesn't know about the skip and pages on the absent partition. The producer's own
docstring documents this exact alert as expected on partner-skip days. **No action needed** — the main
`tpa_export` pipeline completed fine (exports `{"data_source_id": 51, "cats": []}`).

**The two DAGs (both team TPA_EXPORT):**
- **Producer** `tpa_ipdsc_export` — `dags/tpa_export/tpa_ipdsc_export.py`, schedule `35 2 * * *` (02:35 UTC), severity 0. Writes `gs://mntn-data-archive-prod/ipdsc/dt=<ds>/data_source_id=<id>/`. Bombora build = task **`ipdsc_bombora`** (model `ipdsc_third_party_audience_builder --partner bombora`), gated by source sensor **`wait_bombora_src`** (1h timeout, `mode=reschedule`, `soft_fail=True` because optional → SKIPPED when source absent).
- **Consumer/monitor** `ipdsc_monitor` — `dags/monitoring/ipdsc_monitor.py`, schedule `5 0 * * *` (00:05 UTC), severity 1. Registry-driven loop builds one `precondition_<partner>` `GCSObjectExistenceSensor` per entry in `THIRD_PARTY_AUDIENCE_BUILDERS`, each **18h timeout / 60s poke / `soft_fail=False`** → this is what pages.

**Registry** `dags/ipdsc_third_party_audience_builders.json` — Bombora entry:
`data_source_id: 51`, `optional: true`, `source_date_offset_days: 1`,
`input_glob_template: gs://mntn-data-partners/partners/bombora/segments/{yyyymmdd}/hem_segments_*.csv.gz`.
So `ipdsc/dt=D` sources Bombora files dated **D−1**.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Confirm the missing ipdsc partition + that ONLY the partner is missing (pipeline else healthy)
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/data_source_id=51/"   # -> no objects
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/"                       # -> 16 other DS present
# 2. Root cause = check the PARTNER SOURCE for date D-1 (offset 1)
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260726/"            # -> no objects = Bombora didn't deliver
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260725/"            # -> delivered fine (contrast)
```
**07-27 evidence:** DS51 partition absent; 16 other sources present (2,4,8,13,14,16,17,18,19,35,42,43,46,47,49,63); Bombora source `20260726/` absent (07-25 present, 07-24/07-27 also absent → Bombora feed is intermittent).

**Decision tree for this alert next time:**
1. Source dir for D−1 **absent** → benign optional-partner skip. Ack, reply, done. (this case)
2. Source dir **present** but ipdsc partition absent → the `ipdsc_bombora` builder failed with a real error. Check the `tpa_ipdsc_export` run's `ipdsc_bombora` task log; re-run it (or mark-success to ship without it — export tolerates `cats: []`). **Batch-id trap:** to re-run `tpa_export`/`ipdsc_geo`, clear the paired `create_batch_id*` task WITH its downstream, else it silently reattaches to the old batch. `ipdsc_<partner>` tasks are immune (try_number in batch id).
3. Partition present *now* → it landed late; clear `precondition_bombora` to pass.

**Late-arriving recovery (partner file shows up after export shipped):** trigger a NEW manual
`tpa_ipdsc_export` run with params `{"dt":"<YYYY-MM-DD>","force_export":true}`. Do NOT task-clear the
original run (params can't change on an existing run; `force_export` stays false → no-op).

**⚠ A re-run does NOT clear an absent-source day — verified.** Bombora source ↔ DS51 partition is a
clean 1:1 all month (offset 1: `ipdsc dt=D` needs source `D−1`). Re-running the monitor just restarts an
18h wait for a file that isn't coming; re-running the producer skips again (no source to build). The only
re-run that ever helps is the **late-arrival** path (`force_export:true`) AND only if the source file
actually shows up. Absent-source days do NOT backfill (e.g. 07-24 source still missing 4 days later).

**History proves self-heal (no manual action needed):**

| ipdsc dt | source (D−1) | source present | DS51 partition | note |
|---|---|---|---|---|
| 07-24 | 07-23 | ✓ | ✓ 49 files | |
| 07-25 | 07-24 | ✗ | ✗ skipped | **same failure — self-healed next day, no rerun** |
| 07-26 | 07-25 | ✓ | ✓ 49 files | recovered automatically |
| 07-27 | 07-26 | ✗ | ✗ skipped | INC-001 alert |

DS51 just has no data on skip days (dt=07-19, 07-25, 07-27, …); the next delivery day self-recovers. This
matches the "we leave it failed and it picks up the following day" prior practice — that is correct.

**Reconciliation (2026-07-28, Brian McAdams, Sr MLE said "needs to be re-run"):** for an absent-source day
there is nothing for a re-run to act on (evidence above). Do NOT restart the monitor on a source-absent
day. The one legitimate action is upstream: **the Bombora feed is degrading** — daily through ~07-09, now
every other day, missed 07-24/26/27 (DS51 empty ~half of recent days). If it needs chasing, that's the
Bombora vendor/feed, not a DAG re-run. Only re-run when the D−1 source is actually present (real builder
crash) or arrives late (`force_export:true` manual run).

**If this pages too often:** the durable fix is to make `ipdsc_monitor`'s DS51 precondition tolerate
skips (e.g. `soft_fail=True` on optional partners' preconditions) so it stops paging on expected skips.
That's a `airflow-ti` code change owned by the TPA_EXPORT / AUDI team — propose it, don't hot-patch.
Tracked as **IMP-001** in `improvements_backlog.md`.

**Update 2026-07-29 (07-28 self-heal confirmed for the IPDSC partition).** Re-verified live in GCS: Bombora
source `partners/bombora/segments/20260726/` **empty** → `ipdsc/dt=2026-07-27/data_source_id=51/` **absent**
(07-26 ✓, 07-28 ✓; sources 20260725 ✓ / 20260727 ✓ → **07-28 recovered automatically**). DS51 same-day
partition calendar (delivery began ~07-06): **ABSENT** 07-13/15/17/19/25/27; **PRESENT** 07-06→12, 14, 16,
18, 20→24, 26, 28. All correct/expected for the intermittent Bombora feed.

**Downstream symptom — `enriched_impressions` DS51=0 for 07-27. RESOLVED: 0 IS correct / by-design (benign).**
Jordan Piepkow (Staff SWE, #alerts) flagged `mntn-analytics-prod-01.analytics_curated.enriched_impressions`
DS51 count for `dt=2026-07-27` reading ~110,798 one day and **0** the next. He confirmed 2026-07-29 the
first-pass answer (**0 is correct; it's the DS51 same-day skip**) was right — he just didn't yet know the
mechanism (why the lookback doesn't backfill it). So: a DS51 skip day genuinely produces ~0 DS51 impressions
for that `dt`; the ~110,798 was a **preliminary/incomplete build** of the 07-27 partition that reconciled to
the correct 0 on overnight completion.

**Why 0 despite a ~30-35d IPDSC lookback (the piece Jordan is still chasing — mechanism OPEN, not settled):**
the outcome (0 is correct) is confirmed by the owner; the *reason* is not. **Leading hypothesis (UNCONFIRMED):**
the lookback governs IP *eligibility*, but the effective `data_source_id=51` tag is **same-day-keyed** for this
table, so an absent same-dt partition zeroes it and the lookback doesn't resurrect it. Note this sits in tension
with the documented ~30-35d MES lookback inner-join (`data_knowledge.md` § IPDSC), which is exactly why it's
non-obvious — don't assert the mechanism until tested. **Discriminating test (one query, settles it):** run the
DS51 count over 07-06→28 and overlay the skip-day calendar above — DS51=0 landing on *exactly* the ABSENT days
(07-13/15/17/19/25/27) confirms same-day keying; anything else points elsewhere. Not a bug either way.

**⚠ Process lesson (the real takeaway — a reasoning trap, logged so we don't repeat it):** the original
GCS-evidenced call (0 correct, benign skip) was right. When Jordan raised a smart architectural objection
(*"IPDSC = targetable IPs, 35-day lookback should preserve DS51"*) hedged with *"who knows,"* I treated a
plausible-but-unconfirmed hypothesis as a refutation and **fully flipped** to a "suspected build bug / not
benign" reframe — abandoning a well-evidenced conclusion instead of holding it and running the discriminating
test. Correct move: **acknowledge the objection, keep the evidenced verdict, and settle it with the test** —
not concede. A domain owner's plausible pushback is a hypothesis to check, not an authority to fold to.

---

### INC-002 — `fangorn_inference_pipeline_run` `inference_pipeline` — Dataproc cluster-create failure
**Date:** 2026-07-27 · **Alert:** PagerDuty page, `fangorn_inference_pipeline_run/inference_pipeline`,
run `scheduled__2026-07-26T18:00:00+00:00`, `try_number=2` (final retry, `max_tries=1` → exhausted → paged).
**Error (log tail):**
```
RuntimeError: Job failed with:
code: 9
message: "The DAG failed because some tasks failed. The failed tasks are: [create-dataproc-cluster].;
Job (project_id = mntn-targeting-prj-prod, job_id = 951702149350293504) is failed due to the above error."
```
Vertex AI pipeline `fangorn_inference_dataproc_pipeline` (template
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_inference_dataproc_pipeline.json`,
project `mntn-targeting-prj-prod`, region `us-central1`).

**STATUS: RESOLVED — owner root-caused + fixed (Brian McAdams, 2026-07-28, #alerts-tpa-pipeline).**

**Verdict: RESOURCE CONTENTION (Dataproc saturation) — NOT stockout / quota / config.** The Fangorn
inference pipeline — and any Fangorn-like inference pipeline — caps out MNTN's Dataproc usage at **~94%**.
So if ANY other Dataproc job is running concurrently (even a **QA / challenger** run), `create-dataproc-cluster`
can't get capacity to provision and fails with gRPC `code: 9` (FAILED_PRECONDITION). The blocker is another
job holding the compute — not a regional stockout and not a template regression (the pipeline submitted
cleanly: template resolved, params rendered, run URL emitted). Brian caused this run's failure (a challenger
was running) and fixed it by **letting the challenger finish, then manually re-triggering the champion**.

**Action next time (decision tree):**
1. **Do NOT blind-re-run.** First check whether another Dataproc job is running (a challenger pipeline, a QA
   job, another Fangorn-like inference run) in project `mntn-targeting-prj-prod` / region `us-central1`.
2. **Another job running** → **wait for it to finish**, THEN manually re-trigger the champion
   `inference_pipeline`. Re-running while the other job holds Dataproc just re-fails with code 9.
3. **Nothing else running yet it still fails** → inspect the Dataproc job via the Vertex Run URL from the
   log (`console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-inference-dataproc-pipeline-<ts>`),
   drill into `create-dataproc-cluster` for the real GCP error (now genuine quota/stockout/config is in play).
4. **Recurring collisions** (champion + challenger routinely overlap) → durable fix is scheduling/quota
   (stagger runs, raise the Dataproc ceiling, or a concurrency guard), owned by the Fangorn/ML + infra team
   (template lives in `targeting-infra`, not `airflow-ti`). Spawn a ticket; do NOT hot-patch.

**Durable-fix note:** the ~94% Dataproc ceiling makes champion/challenger collisions a standing risk. If
this pages repeatedly, the durable fix is run-staggering or a higher Dataproc quota rather than
hand-re-triggering each time. Tracked as **IMP-002** in `improvements_backlog.md`.

---

### INC-003 — `fangorn_inference_pipeline_run` `daily_drift_pipeline` — Vertex param `reference_date` not in template
**Date:** 2026-07-28 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [fangorn_inference_pipeline_run/daily_drift_pipeline] at 2026-07-27 11:00 PT`, run `scheduled__2026-07-27T18:00:00+00:00`, `try_number=2` (`max_tries=1` → exhausted → PagerDuty).
**Error (log tail):**
```
ValueError: The pipeline parameter reference_date is not found in the pipeline job input definitions.
  .../google/cloud/aiplatform/utils/pipeline_utils.py, line 241 in _get_vertex_value
  .../include/vertex/operators.py, line 145 in _run_pipeline
```
Vertex pipeline `fangorn_daily_feature_drift_pipeline` (template
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json`,
project `mntn-targeting-prj-prod`, region `us-central1`).

**STATUS: RESOLVED — owner redeployed the template; re-run went green (2026-07-28).** Two-part fix, both
landed: (1) **PR #1158** (airflow-ti) dropped the DAG's redundant `run_date`; (2) **Brian McAdams
redeployed the Vertex template** — renamed the KFP param `run_date`→`reference_date` in `targeting-infra-ml`,
recompiled, redeployed `fangorn_daily_feature_drift_pipeline.json` (GCS object updated 23:31Z; verified it now
declares `reference_date`, no `run_date`). A plain **Clear Task Instance** on the same v6 run then went green
(Try #5, `Pipeline completed with state: 4` = SUCCEEDED). No new DAG version was needed — the template is read
live from GCS at task runtime, so the redeploy applied without a bundle change.

**Key lesson — the DAG fix alone was a NO-OP; the empirical re-run caught it.** The param mismatch is
invisible at DAG-parse, so PR #1158 looked like the fix but the re-run on the fixed bundle re-failed
identically. Only checking the compiled template's declared params (diagnosis command 2) revealed the real
half. **When a Vertex `parameter … not found` fix is proposed, verify the *template* param list changed —
don't trust a DAG-side PR alone.**

**Logs:** `on-call/incidents/INC-003/` (try2/try3 failed = `reference_date` mismatch; try5 succeeded).

**Re-run proof (2026-07-28 23:16Z, attempt 3):** re-ran WITH "Run with latest bundled version" — the bundle
loaded was `2026-07-28T21:55:33Z`, i.e. AFTER PR #1158 merged (21:54:58Z), so the DAG fix WAS active — and
it failed with the **identical** `ValueError: … parameter reference_date … not found`. This proves the
DAG-side change is insufficient (see mechanism below). Do NOT keep re-running; every retry reproduces it
until the drift template is redeployed.

**The actual fix (owner = Brian, `targeting-infra-ml`):** the top-level KFP pipeline
`fangorn_daily_feature_drift_pipeline(...)` declares its date param as **`run_date: str = "2026-07-25"`**
(`vertex/fangorn/pipelines/fangorn_daily_feature_drift_pipeline.py:393`, threaded to
`submit_daily_drift_job`'s `run_date`). The airflow-ti operator ALWAYS injects `reference_date`. Rename the
pipeline param `run_date` → `reference_date`, recompile, and redeploy
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json` — then the
operator-injected `reference_date` is accepted and PR #1158 (dropping the DAG's redundant `run_date`) is
correct/complete. PR #1158 alone is a **no-op** for the failing param. (The drift task was NEW —
`43f11915 "Add daily roll-up"`, 2026-07-27 — so it was broken from inception, never green.)

**Verdict: DAG BUG (param-contract mismatch) — NOT resource contention.** Same DAG as INC-002 but a
different task (`daily_drift_pipeline`, the last task in the chain) and an unrelated cause.
`TiVertexPipelineOperator._run_pipeline` (`include/vertex/operators.py`) ALWAYS builds
`parameter_values = {google_cloud_project, google_cloud_region, bucket_name, branch, reference_date, **additional_params}`,
so it injects `reference_date` for every pipeline it submits. `PipelineJob.__init__` validates each key
against the compiled template's `inputDefinitions` (`_get_vertex_value`); the drift template doesn't
declare `reference_date` → hard ValueError before submission. The task ALSO passed a redundant
`parameter_values={"run_date": run_date}` (same value as `reference_date`) — the "extra passed data"
PR #1158 removes.

**Empirical ground truth (confirmed 2026-07-28):** the deployed drift template declares params
`[branch, bucket_name, google_cloud_project, google_cloud_region, run_date, service_account]` — it uses
**`run_date`, NOT `reference_date`.** It is the odd one out: the DAG's other Fangorn pipelines
(`inference_pipeline` → `fangorn_inference_dataproc_pipeline`, `challenger_inference_pipeline`) also pass
`reference_date` and succeed, so their templates declare `reference_date`. The proper fix therefore
standardizes the drift template on `reference_date` (a **targeting-infra** recompile) AND drops the DAG's
`run_date` workaround (PR #1158, `airflow-ti`).
**⚠ DAG-only bundle update is CONFIRMED insufficient** (attempt 3 on the fixed bundle failed identically —
see STATUS above). The compiled template's param name comes straight from the KFP pipeline source:
the deployed JSON declares `run_date` because `fangorn_daily_feature_drift_pipeline.py:393` declares
`run_date: str`. Until that source is renamed to `reference_date` and the template recompiled/redeployed
(owner-side, `targeting-infra-ml`), the operator-injected `reference_date` is rejected every run.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Which param does the failing task send that the template rejects? (log tail)
#    -> "ValueError: The pipeline parameter <X> is not found in the pipeline job input definitions."  (here X=reference_date)
# 2. Ground truth: what params does the compiled Vertex template actually declare?
gcloud storage cat "gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(sorted(d['root']['inputDefinitions']['parameters'].keys()))"
#    -> ['branch','bucket_name','google_cloud_project','google_cloud_region','run_date','service_account']  (no reference_date)
# 3. What does the operator send? include/vertex/operators.py _run_pipeline always adds "reference_date": reference_date  + **additional_params
```

**Decision tree next time (Vertex `ValueError: parameter <X> not found in input definitions`):**
1. This is a DAG/template param-contract mismatch, NOT infra — do NOT blind-re-run against the same
   bundle+template (it reproduces the exact error).
2. Diff operator-sent params vs template-declared params (commands above): the submitter is sending a key
   the compiled template doesn't declare.
3. Fix is owner-side (align the DAG param, or recompile the template). Route to the Fangorn/ML owner —
   template lives in **targeting-infra**, the DAG in **airflow-ti**. Do NOT hot-patch prod.
4. After the owner's fix is merged AND the bundle + template version propagate (~30 min): clear+re-run WITH
   "Run with latest bundled version" checked, or let the next scheduled run pick it up. Verify green.

**Severity: LOW.** `daily_drift_pipeline` is Fangorn feature-DRIFT monitoring, downstream of
`inference_pipeline`/`challenger_inference_pipeline` (`trigger_rule="all_done"`, last in the chain); it does
not score or serve. Impact = a one-day gap in drift telemetry, no scoring/serving impact.

**Durable-fix note:** the operator silently injects `reference_date` into every pipeline it submits, so any
template whose date-param name drifts from the operator convention fails only at task-exec (runtime), not at
DAG-parse. Tracked as **IMP-003** in `improvements_backlog.md` (standardize Fangorn template param names on
`reference_date`, or validate the operator↔template param contract earlier).

---

### INC-004 — `audience_intent` `fangorn_score_monitor` — Dataproc batch AnalysisException, missing `ipdsc_geo/dt=<run_date>` (LATE DATA)
**Date:** 2026-07-29 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [audience_intent/fangorn_score_monitor] at 2026-07-27 17:08 PT`, run `scheduled__2026-07-28T00:08:00+00:00`, try 3/3 (`max_tries=2` → exhausted → PagerDuty).
**Error — the Airflow task log is boilerplate** (`AirflowException: Batch job … Google Cloud Dataproc Agent reports job failure`); the real Spark traceback is only in the **Dataproc Serverless batch driver output**:
```
File ".../fangorn_score_monitor.py", line 216, in model
    self.spark.read.parquet(self._ipdsc_geo_path(run_date))
pyspark.errors.exceptions.captured.AnalysisException: [PATH_NOT_FOUND] Path does not exist: gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-07-28.
```

**STATUS: RESOLVED — late data; clear+re-run the monitor.** The monitor reads `ipdsc_geo/dt=<run_date>`;
that partition was absent when the batch ran (last retry 2026-07-29 07:43Z) and landed **complete 34 min
later at 08:17:34Z** (18 part-files ~5.2GB + `_SUCCESS`, same shape as 07-27). Action: **Clear Task
Instance on `fangorn_score_monitor`** → it re-reads the now-present partition and passes. Each clear mints a
fresh Dataproc batch_id (timestamped) so there's **no batch-id trap**. No producer re-run and no code change
— the producer completed, just late.

**Confirmed green (2026-07-29):** cleared+re-ran (try 4), batch SUCCEEDED 11:38:11Z, output
`fangorn_score_monitor/dt=2026-07-28/_SUCCESS` written 11:37:15Z. **Runtime ~66 min against a 90-min TTL
(`ttl: 5400s`) — only ~24 min headroom** (reads the 20K-file `prospecting_intent` partition + the ~5.2GB
`ipdsc_geo` join). A heavier-data day could hit the TTL and hard-fail. Tracked as IMP-005.

**Root cause = a producer/consumer RACE, not a bug.** `ipdsc_geo/dt=D` (tpa_export `run_geo`,
`gs://mntn-data-archive-prod/ipdsc_geo/`) lands on D+1 with a **~3.5h-variable arrival**: 07-25→04:56Z,
07-26→06:26Z, 07-27→05:00Z, **07-28→08:17Z (late)**. `audience_intent` (`8 0 * * *`) runs
`fangorn_score_monitor` with only `retries=2 × 10-min` (~30-40 min of slack) and **no cross-DAG sensor** on
`ipdsc_geo`, so when the producer slips past ~07:45Z the monitor exhausts retries and pages. Same
tpa_export/ipdsc chain as INC-001. Monitor code + DAG both unchanged (07-02 / 07-24) — not a regression.

**Upstream cause corroborated (mission control, 2026-07-29):** Zach reported the **DS data flow was behind
for many data sources** last night (self-correcting the next day); Scotty separately reported a data-pipeline
delay that left mission-control system-signals un-updated for the prior day. That systemic DS-flow delay is
why `ipdsc_geo/dt=2026-07-28` (built at the tail of the ipdsc chain) landed ~3h late. So the "why was it
late" is a system-wide DS-flow lag, not an isolated builder bug. (Same-night `aud22` Geo Includes/Excludes
audit: **RULED OUT as related** — Compass confirmed aud22 reads CIL + `geo.network_locations` + audience
config from BigQuery, never `ipdsc_geo`/the DS chain, and is geo_version-pinned. Its 07-28 fires are *real
but boundary-noise* (6 CGs, 11 IPs, 12 imps, $0.12), below aud22's own daily FAIL noise floor. NOT a data
artifact. Separate from INC-004.)

**Compass RCA (2026-07-29) — deeper findings (evidence: Cloud Audit Logs + Spark logs + git):**
- **Late START, not slow compute.** `run_geo` CreateBatch was accepted at **08:07:23Z**; the geo Spark job
  then ran ~10 min (`_SUCCESS` 08:17:34Z). The miss is 100% "run_geo launched 3h late," not slow geo compute.
  The delay is upstream of run_geo's launch (a builder/gate finishing late), not run_geo itself.
- **Chronically flaky producer, NOT a new regression.** `ipdsc_geo` launch crept from a tight ~03:45Z band
  (mid-June) to ~04:45–06:53Z (late July), with historical outliers of **+14h (06-23→24) and +16h (06-28→29)**
  that dwarf 07-28's +3h. **07-28 was the first time this recurring lateness collided with an unguarded
  consumer deadline** (the then-new `fangorn_score_monitor`). Design for multi-hour, even mid-day, delays.
- **Leading (unconfirmed) suspect: `DS9` was added to `tpa_ipdsc_export` the day before** (commits 07-27
  "Add DS 9" / 07-28 "Fix DS number and add DS 9", Alyson) — one more `ipdsc_ds_*` builder upstream of
  `run_geo`. **Open gap:** the specific long-pole builder couldn't be pinned from audit logs (batch-ID naming
  mismatch); to close it, pull the `ipdsc_ds_*` task-instance start/end times from the **Astronomer/Airflow
  metadata DB**, not GCP audit logs.
- **Consumer scope confirmed complete.** Only two `ipdsc_geo` consumers exist: `tpa_mntn_id_export`
  (triggered synchronously downstream inside the producer DAG → no race) and `fangorn_score_monitor` (the one
  that raced). PR #1160's sensor covers the only racing consumer. 18h sensor timeout > worst-ever +16h slip.
- **Separate anomaly to route (not causal here):** run_geo's Spark workers hit `Failed to connect to master …
  Connection refused` + `SIGNAL TERM` at 08:20:17–18Z — AFTER `_SUCCESS` (08:17:34Z), so it reads as normal
  decommission of a finished batch. Flag to whoever owns the Dataproc Serverless subnet/SA config.
- **Producer-side fix recommended (Compass):** the consumer sensor stops the page but doesn't touch the
  producer's recurring tail latency. Add wall-clock alerting (repo pattern `dag_run_duration_watchdog`) on
  `tpa_ipdsc_export` so a 3h+ slip pages **TPA_EXPORT** proactively. Tracked as **IMP-006**.

**Diagnosis path (copy-paste — the Airflow log is NOT enough):**
```bash
# 1. Airflow log only says "Dataproc Agent reports job failure" — get the REAL error from the batch driver output:
gcloud dataproc batches wait '<batch-id>' --region us-central1 --project mntn-prj-prod-00 2>&1 | tail -80
#    (needs storage.objects.get on the mntn-prj-prod-00 dataproc-staging bucket — request PAM 'audi-storage-object-view' if 403)
# 2. The traceback names the missing path. Does it exist NOW, and when did it land?
gcloud storage ls -l "gs://mntn-data-archive-prod/ipdsc_geo/dt=<run_date>/_SUCCESS"
# 3. Present + landed AFTER the task's last try = LATE DATA -> clear+re-run the consumer task.
```

**Decision tree — `ModelPysparkBatchOperator` "Dataproc Agent reports job failure":**
1. The Airflow log is boilerplate — always pull the batch **driver output** (command 1) for the Spark traceback.
2. `AnalysisException [PATH_NOT_FOUND]` → an input partition was missing at run time. Check if it exists now (command 2):
   - **Present, landed AFTER the last try** → **late_data → clear+re-run the task** (this case).
   - **Still absent** → real upstream failure → re-run the producer (`ipdsc_geo` = tpa_export `run_geo`; mind the INC-001 batch-id trap), or route to the feed owner.
3. Other Spark errors (OOM / skew / schema) → the driver output shows it; route to the owning team.

**Severity: LOW.** `fangorn_score_monitor` is a reporting leaf (emails/Slacks Fangorn intent-band counts,
writes a monitoring parquet). Its downstream siblings (`household_score_distribution`, `intent_score_map`,
`trigger_intent_score_household`) all went green in the same run → no scoring/serving impact, just a missed
monitoring email for 07-28.

**Durable fix → IMP-004 → PR #1160 (MERGED 2026-07-29).** Adds a `GCSObjectExistenceSensor`
`wait_for_ipdsc_geo` on `ipdsc_geo/dt={{ ds }}/_SUCCESS` gating `fangorn_score_monitor` (mirrors the DAG's
existing `wait_for_ipdsc_13/19` preconditions; `soft_fail` so a truly-absent day skips instead of paging).
Wired `scoring() >> wait_for_ipdsc_geo >> fangorn_score_monitor` so only the monitor waits on geo, not the
scoring path. Suggestion PR only (not merged) — Ryan owns the DAG.

**Logs:** `on-call/incidents/INC-004/`.

---

### INC-005 — `tpa_mntn_id_export` `tpa_mntn_id_export` — Dataproc batch cancelled at 3h TTL (uncached recomputation + 29TB shuffle spill; verdict corrected below)
**Date:** 2026-07-29 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [tpa_mntn_id_export/tpa_mntn_id_export] at 2026-07-29 01:19:35 PT`, run `manual__2026-07-29T08:19:35+00:00`, try 3/3 (`max_tries=2` → exhausted). Slack (#alerts-tpa-pipeline): Brian McAdams "Looks like it hit TTL", pinged Sean Yang.
**Error — the Airflow log is boilerplate** (`AirflowException: Batch job tpa-mntn-id-20260729-3 was cancelled`); the TTL reason is only in the **batch state history**:
```
stateHistory: PENDING 14:40:09Z → RUNNING 14:41:42Z → CANCELLING "Cancelling batch as ttl exceeded" 17:40:14Z   (ttl=10800s = 3h)
driveroutput tail:  95× FetchFailedException (shuffleId=45, stage 829), 33 Lost task, 58 stage Resubmitted,
                    Caused by TimeoutException: Waited 30000 ms for SettableFuture ... doSparkAuth / AuthClientBootstrap
```

**STATUS: OBSERVED — root cause VERIFIED from the Spark event log (see correction). Fix not yet deployed; 07-28 export still MISSING.** Routed to owner (TPA_EXPORT / Sean Yang + Brian McAdams, already in-thread). Do NOT mark resolved until a re-run produces `mntn_id_data/2026/07/28/`.

**⚠ ROOT CAUSE — verified from the try-3 Spark event log (2026-07-29 deep-dive). This CORRECTS the first-pass transient_infra verdict below.** Downloaded + profiled the full event log (`app-20260729144156809-0952.zstd`, 326K events, 2h56m of the 3h captured; profiler + output in `incidents/INC-005/`). Real verdict: **DAG_BUG (Spark perf). The final `.write.json()` recomputes a ~1.9TB `mntn_id`-keyed shuffle 7-9 times because the `mntn_df` lineage is never cached, and `spark.sql.shuffle.partitions=1000` makes each partition ~1.9GB so it spills to disk.** Not infra, not data volume, not the auth-handshake storm.
- **150 executors up the entire run, 0 removed.** No executor loss. Rules out infra / INC-004's subnet anomaly as the cause.
- **The 7-9 dominant stages ALL share one call site (`json at NativeMethodAccessorImpl.java:0`)**, each ~1000 tasks / ~1900GB shuffle-read + ~1900GB shuffle-write. That is the SAME write recomputed, not distinct work. Per-job stage counts climb 17→19→21→23→25 across the tail jobs (the uncached-lineage recomputation signature). The last write attempts took 15, 19, 34, 34 min; killed mid-write (job 146 INCOMPLETE).
- **Spill, not memory or CPU.** Big stages are 70-97% shuffle fetch-wait, 8-10% CPU, ~0% GC. **App-wide: 17.3TB shuffle write, 29.0TB memory spill, 13.8TB disk spill** for a job whose output is a few GB. peakExecMem 7.9GB/task vs 24GB executors → no OOM, just spill.
- **FetchFailed = 33 tasks** (NOT the 95 I first cited — that was grep-of-stacktrace-lines; and the "re-ran 58×" was resubmit log-lines). FetchFailed is an aggravator, not the root: each one resubmits a ~1900GB map stage, feeding the recompute spiral.
- **Why 78 min some days, >3h others on identical input:** the recompute spiral is kicked off by timing-dependent fetch failures, so the design sits right on the cliff.

**Real fix (owner-side; supersedes "just raise the TTL"):** (1) **cache/persist `mntn_df`** (e.g. `DISK_ONLY`) before the `_apply_audience_filter` loop + write so the ~1.9TB shuffle computes ONCE, not 7-9×. Biggest win. (2) **raise `spark.sql.shuffle.partitions` 1000 → ~6000-8000** (~256MB/partition, kills the multi-TB spill). (3) **collapse the 14 sequential `crossJoin(F.broadcast(...))` in `_apply_audience_filter` into one broadcast intersect** (one allowlist map, one select) so the plan isn't 14 stacked exchanges. THEN a modest TTL bump as margin. A TTL bump ALONE just lets the 29TB-spill spiral run longer. Owner-side model-decorator + code change, do NOT hot-patch. → IMP-007.

**Event-log deep-dive method (reusable):** the batch describe/driver-output only gets you the symptom. For the real per-stage profile, download the Spark event log and parse it:
```bash
# 1. find the event log (zstd, .inprogress if the batch was killed):
gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 --format="value(uuid)"
gcloud storage ls -l "gs://dataproc-temp-us-central1-<PROJNUM>-<suffix>/<uuid>/spark-job-history/"
# 2. if gcloud-crc32c is Gatekeeper-blocked on macOS, download via the storage API instead (click "Done" on the popup, NOT "Move to Trash"):
curl -sS --fail -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://storage.googleapis.com/storage/v1/b/<bucket>/o/<url-encoded-object>?alt=media" -o eventlog.zstd
zstd -d eventlog.zstd -o eventlog.json
# 3. profile it (per-stage runtime/shuffle/spill/skew/FetchFailed/executor churn):
python3 incidents/INC-005/eventlog_profiler.py eventlog.json
```

**[SUPERSEDED first-pass read] Verdict: TRANSIENT_INFRA (Dataproc Serverless shuffle-fetch instability) aggravated by ZERO TTL headroom — NOT data volume, NOT config, NOT concurrent-job contention.** The batch ran the full 3h and Dataproc killed it at the TTL. Root of the slowness = a **shuffle FetchFailed retry storm**: executors could not complete the RPC **auth handshake** to fetch each other's shuffle blocks within its 30s timeout (`doSparkAuth` → `SettableFuture` TimeoutException), so Spark marked map output lost and **re-ran map stages 58×** → runtime blew past 3h. Same-day sibling of INC-004, same `targeting` prod Dataproc Serverless subnet (`mntn-prod-prj-snet-central1`) whose Compass RCA already flagged a "Failed to connect to master / Connection refused" networking anomaly — this is the same class of network/RPC instability, and it hurts far more here because this job does a ~54TB all-to-all shuffle.

**What was ruled out (the discriminators that matter):**
- **Data volume — RULED OUT.** Input partitions ~identical 07-27 (last good) vs 07-28 (failed): `ipdsc_geo` 4.94GB vs 4.93GB; `ipdsc/…ds=4` 11.21GB vs 11.23GB; `ds=19` 1.91GB vs 1.88GB. Runtime **doubled on identical input** (78 min on 07-28 → >180 min ×3 on 07-29).
- **Concurrent-job contention (INC-002 shape) — RULED OUT.** The batch provisioned fine (RUNNING 1.5 min after create); the surrounding window is healthy small-job churn (`site_network_hourly`, `aug_log_rollup`, … all SUCCEEDED in 2-6 min). No big concurrent job hogging Dataproc.
- **OOM / executor loss — RULED OUT.** 0 `OutOfMemory`, 0 `ExecutorLost`/decommission in the driver output. The failures are purely shuffle-fetch RPC-auth timeouts.
- **Code change / "Sean edited it mid-incident" — WEAKENED.** The model's 16:21Z mtime is a **full `ti_resources_v2/main` redeploy** (every model file stamped 16:21:55–58Z = routine CI sync on a main merge), not a targeted edit. All 3 tries ran the pre-16:21 bundle. (The main branch DID get a merge at 16:21Z; whether it touched this model needs the source repo git history — the current GCS copy still hardcodes `timeout=10_800`.)

**The job** (`gs://mntn-data-archive-prod/ti_resources_v2/main/models/tpa_export/tpa_mntn_id_export.py`, `ModelPysparkBatchOperator`, team `targeting`): "TPA export keyed by MNTN (household) id." Reads ~15 IPDSC `ipdsc/dt=D/data_source_id=*` partitions, does a **14-way cascade of FULL OUTER JOINs on `ip`**, joins `ipdsc_geo` + the identity graph (`identity-graph-prod/mntn-graph/household_graph_parquet`, pinned max(asOfDate)→max(GraphVersion)), maps ip→mntn_id, then `groupBy(mntn_id)` collect_list of fat category arrays **+ a `Window.partitionBy(mntn_id)` best-location sort** (the `sort_addToSorter` in the FetchFailed stack), intersects with a BQ campaign-group allowlist (CG 127075, broadcast), writes JSONL to `gs://sh-dw-external-tables-prod/mntn_id_data/YYYY/MM/DD/`. Inherently shuffle-heavy. `timeout=10_800` (3h) and `spark.dynamicAllocation.maxExecutors=150` are **hardcoded in the model decorator** (lines 48-75). Idempotent: `force=false` + `_already_exported()` skips a day whose output exists; `export_date = latest ipdsc dt ≤ run_date`.

**Binding-timeout insight for the owner:** the failing timeout is the **30s auth-bootstrap** (`Waited 30000 ms for SettableFuture` in `doSparkAuth`), NOT `spark.network.timeout` (already 600s). So widening `network.timeout` won't help. The storm-reducing levers are: **lower `maxExecutors`** (150 → e.g. 60-80, fewer all-to-all auth handshakes), raise the RPC/auth + `spark.shuffle.io.maxRetries`/`retryWait`, and **raise the 3h TTL for headroom** (precedent: sibling `fangorn_prospecting_scoring` got an emergency 6h TTL bump, PR #1147, 07-24 — IMP-005). All are owner-side model-decorator changes — do NOT hot-patch.

**Diagnosis run (copy-paste — Airflow log says only "was cancelled"):**
```bash
# 1. WHY cancelled — the state history gives the TTL reason the Airflow log omits:
gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 \
  --format="yaml(state,stateHistory,runtimeInfo.approximateUsage)"     # -> "Cancelling batch as ttl exceeded"
# 2. WHY it ran long — pull the driver output, count the storm:
OUT=$(gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 --format="value(runtimeInfo.outputUri)")
gcloud storage cat "${OUT}.*" | grep -c 'FetchFailed'      # >0 with 0 OOM / 0 ExecutorLost = shuffle-fetch storm
# 3. Rule out data volume — compare input sizes vs the last GOOD day (identical => not data):
for dt in 2026-07-27 2026-07-28; do gcloud storage du -s "gs://mntn-data-archive-prod/ipdsc_geo/dt=${dt}/"; done
# 4. Rule out contention — is a big concurrent batch running the same window?
gcloud dataproc batches list --region us-central1 --project mntn-prj-prod-00 --sort-by=~createTime --limit=60 --format="table(state,createTime,stateTime,labels.job_type)"
# 5. Is the output actually missing?
gcloud storage ls "gs://sh-dw-external-tables-prod/mntn_id_data/2026/07/28/"    # 0 entries = export missing
```

**Decision tree — `ModelPysparkBatchOperator` "Batch job … was cancelled" (Dataproc Serverless):**
1. Get the **state history** (cmd 1). `Cancelling batch as ttl exceeded` = it ran the full TTL, was NOT a code/input error → go to 2. (Any other cancel reason → treat as its own error via the driver output.)
2. Pull the **driver output** (cmd 2). `FetchFailed` storm + 0 OOM + 0 ExecutorLost = **shuffle-fetch/RPC-auth instability** (this case). OOM/skew/AnalysisException instead → route that specific error.
3. Rule out data volume (cmd 3) and contention (cmd 4). Normal input + healthy churn → **transient_infra on a zero-TTL-headroom job.**
4. **Action = one controlled re-run** (env may have recovered; a fresh batch picks up the live bundle) — clear the task in Astronomer, or the owner triggers a new `tpa_ipdsc_export.trigger_mntn_id_export`. Each re-run mints a fresh timestamped batch_id → **no batch-id trap** for this task. Verify `mntn_id_data/<date>/` lands.
5. **Re-storms again** → do NOT keep burning 3h runs (~2,400 DCU-hr/try, ~$100-150/try). Route to owner for the model-decorator fix (raise TTL / lower maxExecutors / raise auth+shuffle-retry timeouts — see binding-timeout insight). Never hot-patch prod.

**Severity: MEDIUM.** Production household-keyed TPA audience export for the default campaign group (CG 127075) → `mntn_id_data/` external-table bucket (activation/serving contract). 07-28's export is missing (last good = dt=07-27, written by the 07-28 17:41Z run). Self-heals on the next successful run; downstream consumer of `mntn_id_data/` not traced (owner can confirm activation impact). Not a monitoring leaf like INC-003/004 — this is serving data — hence MEDIUM not LOW.

**Durable fix → IMP-007.** Raise TTL / cap `maxExecutors` / raise auth+shuffle-retry timeouts on the `tpa_mntn_id_export` model decorator to survive a shuffle-fetch storm on a zero-headroom job; and (cross-ref IMP-006) the same Dataproc Serverless subnet networking anomaly (INC-004 Compass §, run_geo "connection refused to master") should be routed to whoever owns the `dataproc-prod` subnet/SA config.

**Fix PR: [SteelHouse/airflow-ti#1161](https://github.com/SteelHouse/airflow-ti/pull/1161)** (2026-07-29, CI green) — 3 commits: core (persist `mntn_df` DISK_ONLY + `shuffle.partitions` 1000→8000 + TTL 10800→14400), optional (collapse the 14 crossJoins), and the regenerated `dags/model_task_config.json`. Patch + linted PR body in `incidents/INC-005/`. **CI gotcha:** the first push failed the `model-upload-dryryn` job's "check generated model artifacts" step — any `@compute.dataproc_batch` decorator change must be followed by `python model_upload.py --dryrun` and committing the regenerated `dags/model_task_config.json` (the compiled batch config the operator reads at runtime), else `git diff --quiet` on that file fails CI. **Owner routing (Sean Yang, #alerts-tpa-pipeline):** real fix/redevelop routed to **Nivas Nalla** (with Ryan Kleck); Sean asked whether the job is business-critical and, if not, to re-route its alerts off #alerts-tpa-pipeline and let Nivas take it in a sprint. So the PR is a ready stopgap Nivas can take or supersede. A re-run to unblock the missing 07-28 export is still separate (may pass since the spiral is timing-dependent).

**Logs:** `on-call/incidents/INC-005/`.

---

## 4. System reference (producer → consumer maps as we learn them)

**IPDSC / TPA export chain (team TPA_EXPORT, `airflow-ti`)**
```
Bombora vendor drop ──▶ gs://mntn-data-partners/partners/bombora/segments/<D-1>/   (source, optional)
        │  wait_bombora_src (1h, reschedule, soft_fail)  [tpa_ipdsc_export @ 02:35 UTC]
        ▼
ipdsc_bombora builder ──▶ gs://mntn-data-archive-prod/ipdsc/dt=<D>/data_source_id=51/_SUCCESS
        │                                                   ▲
        ▼                                                   │ polls (18h, hard-fail)  [ipdsc_monitor @ 00:05 UTC]
run_geo ──▶ tpa_export ──▶ external table bucket           precondition_bombora  ← ALERTS here
```
- Mandatory data sources (DS4, DS17, …) are never tolerated — a missing mandatory partition
  hard-fails `tpa_export`. Only optional partners (currently just Bombora/DS51) skip silently.
- `ds17` sources ShareThis at `gs://mntn-data-partners/partners/sharethis/segments/date=<D-1>/`.
- Full DS id → vendor map + ipdsc query tips: `knowledge/data_catalog.md` (`bronze.external.ipdsc__v1`, DS-id legend).
- **`run_geo` also writes `gs://mntn-data-archive-prod/ipdsc_geo/dt=D` (lands D+1, ~05:00–08:17Z, variable).**
  Producer = task `run_geo` (Airflow task_id `ipdsc_geo`, a `DataprocCreateBatchOperator`) in
  `tpa_ipdsc_export.py`, TPA_EXPORT team, schedule `35 2 * * *` (02:35 UTC). It runs LAST — after all
  `ipdsc_ds_*` builders + audience-builder tasks (`trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS`) — and joins
  them into the IP→geo table `external.ipdsc_geo__v1` (`ip, geo_version, location_id[], lat, long,
  accuracy_radius`). Its finish time swings ~3.5h with the builder chain's runtime, so its landing time is
  not fixed. Writes `_SUCCESS` last (complete-partition marker).
  Downstream consumer (different team + schedule, no cross-DAG dep): `audience_intent` `fangorn_score_monitor`
  (targeting, `8 0 * * *`) reads `ipdsc_geo/dt=<run_date>`. Pre-INC-004 it had no sensor → **raced the
  producer** and paged (`AnalysisException [PATH_NOT_FOUND]`) when geo slipped past its ~30-40min retry slack.
  PR #1160 added `wait_for_ipdsc_geo` on the `_SUCCESS` marker so the monitor waits instead of racing.
- **`tpa_mntn_id_export` is the OTHER (synchronous, no-race) `ipdsc`/`ipdsc_geo` consumer** — triggered inside
  the producer DAG (`tpa_ipdsc_export.trigger_mntn_id_export`), so it never races. It reads ~15
  `ipdsc/dt=D/data_source_id=*` partitions + `ipdsc_geo/dt=D` + the identity graph, does a 14-way
  full-outer-join on `ip` + a `groupBy(mntn_id)` window (~54TB shuffle), and writes household-keyed JSONL to
  `gs://sh-dw-external-tables-prod/mntn_id_data/YYYY/MM/DD/` for CG 127075 (TPA activation).
  `ModelPysparkBatchOperator`, hardcoded **3h TTL** + `maxExecutors=150`, baseline ~78 min → **zero TTL
  headroom**; a shuffle-fetch storm tips it over the wall (INC-005).

**Fangorn inference chain (ML pipeline, `airflow-ti` → Vertex/Dataproc)**
```
fangorn_inference_pipeline_run  [Astronomer, PythonOperator: inference_pipeline]
        │  submits Vertex AI pipeline
        ▼
fangorn_inference_dataproc_pipeline  (template in gs://targeting-infra-vertex-pipelines-prod/fangorn/)
        │  step: create-dataproc-cluster  ← INC-002 failed HERE (code 9 = Dataproc saturated by a concurrent job)
        ▼
inference on Dataproc ──▶ Fangorn scores  (project mntn-targeting-prj-prod, region us-central1)
```
- Alerts route via PagerDuty (`pagerduty_events` connection), not just Slack.
- **Dataproc ~94% cap:** a Fangorn(-like) inference run saturates Dataproc; a concurrent Dataproc job (even
  QA / a challenger) starves `create-dataproc-cluster` → code 9. Fix = let the other job finish, then
  re-trigger the champion. Never two Fangorn-like inference runs on Dataproc at once. (INC-002)
- The Vertex pipeline template + Dataproc config live in **`targeting-infra`** (not `airflow-ti`); a
  config regression is routed there. `airflow-ti` only *submits* the pipeline.
- **DAG shape** (`dags/machine_learning/fangorn_inference_pipeline_run.py`, team TPA_EXPORT, `0 18 * * *`,
  severity 0, PagerDuty on failure): `wait_for_features >> inference_pipeline >> challenger_inference_pipeline >> daily_drift_pipeline`.
  All three pipeline tasks use `TiVertexPipelineOperator`, which ALWAYS injects `reference_date` into the
  Vertex `parameter_values`. Each submitted template (`fangorn_inference_dataproc_pipeline`,
  `fangorn_challenger_inference_pipeline`, `fangorn_daily_feature_drift_pipeline`) MUST declare
  `reference_date` in its `inputDefinitions` or the task hard-fails at exec with
  `ValueError: … parameter reference_date not found …` (INC-003 — drift template declared `run_date`).
- Fangorn context: see `[[fangorn_tier_assignment]]`, `[[fangorn_two_model_passes]]`, `[[fangorn_detection]]` in memory.

---

## 5. Structured incident log (`on-call/incident_log.jsonl`)

Append-only JSONL, one record per incident — the machine-readable index over §3 (mirrors the perf/request
logs). Lets you answer "how often does `precondition_bombora` page?" or "which DAG pages most?" without
reading the prose. **Write one record every time you add an INC to §3.**

Record shape (one line per incident):
```json
{"inc":"INC-001","date":"2026-07-28","dag":"ipdsc_monitor","task":"precondition_bombora","team":"TPA_EXPORT","signature":"AirflowSensorTimeout 18h optional-partner skip","verdict":"benign_expected","action":"ack_no_rerun","resolved":true,"ticket":null,"ref":"§3 INC-001"}
```
Fields: `inc` · `date` (YYYY-MM-DD) · `dag` · `task` · `team` · `signature` (short) · `verdict`
(`benign_expected|late_data|transient_infra|resource_contention|real_upstream_failure|dag_bug`) · `action`
(`ack_no_rerun|clear_task|rerun|force_export|routed_owner|spawned_ticket`) · `resolved` (bool) ·
`ticket` (TI/AUDI key if a durable fix was spun out, else null) · `pagerduty` (PD incident # if it paged, else null) ·
`ref` (`§3 INC-NNN`). **When the user gives a PagerDuty incident #, record it in `pagerduty` and cite it in the §3 alert line** — it ties our INC-NNN to the PD record.
