"""Deterministic Spark/Airflow failure-signature taxonomy.

Matches an error string against known fingerprints BEFORE any LLM call. Each
signature carries a class, likely root cause, and whether a code-level
(programmatic) fix is even possible — the last field gates whether an automated
PR is attempted downstream. Ordered most-specific first; `classify` returns the
first match.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Signature:
    """One failure fingerprint: regex + class + likely cause + fixability."""

    key: str
    pattern: str  # regex, searched case-insensitively
    sig_class: str
    likely_cause: str
    programmatic_fix: str  # "yes" | "sometimes" | "no"
    remedy: str = ""  # the concrete change to make, not a category
    engine: str = "any"  # "any" | "dataproc" | "databricks"


SIGNATURES: list[Signature] = [
    Signature(
        "table_or_view_already_exists",
        r"TABLE_OR_VIEW_ALREADY_EXISTS|already exists.{0,40}SQLSTATE: 42P07|\b42P07\b",
        "idempotency/orphaned-run",
        "A prior (orphaned or retried) run already created the target table; the re-run collides. "
        "The overwrite/create is not idempotent under retry.",
        "sometimes",
        remedy=(
            "Make the write idempotent (CREATE OR REPLACE, or overwrite mode), or drop the "
            "orphaned table before retrying. Check for a still-running earlier attempt first."
        ),
    ),
    Signature(
        "concurrent_delta_write",
        r"ConcurrentAppendException|ConcurrentModificationException|ConcurrentDeleteReadException",
        "concurrency",
        "Concurrent writes to the same Delta table/partition.",
        "sometimes",
        remedy=(
            "Serialise the two writers, or partition them so they touch different partitions. "
            "Retrying alone re-races and fails the same way."
        ),
    ),
    Signature(
        "executor_oom_yarn",
        r"Container killed by YARN for exceeding memory|physical memory used|exit code 137|"
        r"Exit code is 137|oom[-_ ]?reaper",
        "executor-oom",
        "Executor exceeded memory/overhead (skew or under-provisioned memory).",
        "no",
        remedy=(
            "Raise executor memory or memoryOverhead, or cut the skew feeding the stage. Check "
            "the largest partition's size first: a 10x skewed key is cheaper to salt than to "
            "fund."
        ),
    ),
    Signature(
        "driver_oom",
        r"OutOfMemoryError: Java heap space|OutOfMemoryError: GC overhead",
        "driver-oom",
        "Driver OOM, often after collect()/toPandas() pulling too much to the driver.",
        "sometimes",
        remedy=(
            "Remove the collect()/toPandas() pulling the result to the driver, or raise driver "
            "memory. Writing from the executors avoids the problem entirely."
        ),
    ),
    Signature(
        "shuffle_oom",
        r"SparkOutOfMemoryError|Unable to acquire.{0,20}bytes of memory",
        "shuffle-oom",
        "Shuffle OOM: too few shuffle partitions or skew.",
        "sometimes",
        remedy=(
            "Raise spark.sql.shuffle.partitions, or salt the skewed join key. More executor "
            "memory only postpones it."
        ),
    ),
    Signature(
        "shuffle_fetch_failure",
        r"FetchFailedException|MetadataFetchFailedException|Failed to connect to .{0,40}shuffle",
        "shuffle-fetch-failure",
        "Executor loss/downscale/bad node, or a >2GB shuffle block.",
        "no",
        remedy=(
            "Re-run once; a lost executor is the usual cause. Repeating at the same stage means a "
            "shuffle block over 2GB, so raise the partition count."
        ),
    ),
    Signature(
        "spot_preemption",
        r"PREEMPTIBLE_WITH_FALLBACK|SPOT_INSTANCE_TERMINATION|was preempted|"
        r"spot instance.{0,40}(reclaim|terminat|preempt)|InsufficientInstanceCapacity",
        "infra/spot-preemption",
        "Spot/preemptible instance reclaimed mid-run.",
        "no",
        remedy=(
            "Re-run. If it recurs on the same DAG, move that job's workers off spot or give it a "
            "non-preemptible primary pool."
        ),
    ),
    Signature(
        "gcs_list_timeout",
        r"Error listing gs://|GoogleCloudStorageImpl\.listStorageObjects|"
        r"gs://.{0,300}?SocketTimeoutException: Read timed out|"
        r"SocketTimeoutException: Read timed out.{0,300}?gs://",
        "transient-infra/gcs-listing",
        "Driver-side GCS LIST timed out during input discovery - usually a glob the GCS connector "
        "resolves by flat-listing the entire prefix (O(all history)) before filtering. Retries die "
        "at a ~constant elapsed (same execution point). 'Lost executor ... spark scale down' lines "
        "alongside are benign idle decommissions, NOT the cause. A re-run usually passes (list "
        "latency is variable); durable fix is literal partition paths instead of a glob, or "
        "fs.gs.glob.flat.enable=false. The evidence lives in the staging-bucket driveroutput, "
        "not the Airflow log.",
        "sometimes",
        remedy=(
            "Replace the input glob with literal partition paths, or set "
            "fs.gs.glob.flat.enable=false. A re-run clears the immediate failure."
        ),
    ),
    Signature(
        "executor_lost",
        # benign dynamic-allocation decommissions log 'Lost executor ... spark scale down'
        r"ExecutorLostFailure|Slave lost|Lost executor(?![^\n]*spark scale down)|"
        r"Executor heartbeat timed out",
        "executor-lost",
        "Executor lost: OOM, spot preemption, or an unhealthy node (disk >90%).",
        "no",
        remedy=(
            "Read the executor's own log before acting: OOM, preemption and a full disk all land "
            "here and each needs a different change."
        ),
    ),
    Signature(
        "broadcast_timeout",
        r"Broadcast timeout|Could not execute broadcast|BroadcastTimeout",
        "broadcast-join",
        "Broadcast side too large or slow.",
        "yes",
        remedy=(
            "Raise spark.sql.broadcastTimeout, or set autoBroadcastJoinThreshold=-1 for this join "
            "if the build side has outgrown broadcasting."
        ),
    ),
    Signature(
        "schema_drift",
        r"Failed to merge incompatible data types|INCOMPATIBLE_DATA_TYPE|Schema mismatch",
        "schema-drift",
        "Upstream schema change.",
        "sometimes",
        remedy=(
            "Align the reader's schema with the producer's, or add an explicit cast. Ask the "
            "producing team what changed before widening types."
        ),
    ),
    Signature(
        "dbt_test_failure",
        # 'Completed with N error' is dbt's generic failure summary, not a test marker
        r"Failure in test |Got \d+ results, configured to fail if|\d+ of \d+ FAIL \d+",
        "dbt-test/data-quality",
        "A dbt data-quality test tripped its threshold (the test query returned more failing "
        "rows than allowed, e.g. 'Got N results, configured to fail if >M'). The upstream data "
        "violated an expectation - route to the model owner to fix the source data or adjust the "
        "test bound; not an auto-fixable code crash.",
        "no",
        remedy=(
            "Route to the model owner: either the source data is wrong or the test bound is. Do "
            "not re-run, the test trips again on the same rows."
        ),
    ),
    Signature(
        "invalid_output_path_config",
        r"Invalid GCS bucket name|bucket name must contain only|<bound method|"
        r"IllegalArgumentException.{0,80}(bucket|path|location)",
        "code/config-error",
        "A model produced an invalid output path/bucket - often a Python bug where a method "
        "reference (e.g. write_location) is passed instead of its call result write_location(), "
        "so the bucket becomes the method's repr. A real code fix in the model, not a re-run.",
        "yes",
        remedy=(
            "Fix the path expression in the model. A method reference passed without its call "
            "parentheses is the usual cause; a re-run cannot help."
        ),
    ),
    Signature(
        "path_not_found_late_data",
        r"PATH_NOT_FOUND|Path does not exist|path does not exist.{0,60}gs://|"
        r"AnalysisException.{0,40}(PATH_NOT_FOUND|does not exist)|"
        r"Missing( required)?.{0,30}partition|Missing required.{0,40}at gs://",
        "late-data/missing-partition",
        "A source partition the job reads (e.g. gs://.../dt=<run_date>) has not landed yet.",
        "no",
        remedy=(
            "Check the partition and its _SUCCESS marker. Present, re-run this task. Absent, fix "
            "or re-run the producer; do not widen the sensor to hide it."
        ),
    ),
    Signature(
        "vertex_param_contract",
        r"pipeline parameter .{0,60}not found in the pipeline( job)? input definitions|"
        r"parameter .{0,40}is not found in the pipeline",
        "vertex/param-mismatch",
        "The operator injects a param name the Vertex/KFP template does not declare (e.g. "
        "reference_date vs run_date), so PipelineJob rejects it before submission.",
        "yes",
        remedy=(
            "Rename the KFP pipeline parameter to match what the operator sends, then recompile "
            "and redeploy the template."
        ),
    ),
    Signature(
        "vertex_pipeline_task_failed",
        # Two log shapes: real newlines in the older incident .txt captures, literal two-char
        # \n escapes in the current Airflow-3 logs (the payload survives only inside the
        # slack_messages dict repr, the [error] line itself carries no text).
        r"The DAG failed because some tasks failed\. The failed tasks are: \[[^\]]+\]|"
        r"Job failed with:(\\n|\s)+code: 9",
        "vertex/pipeline-task-failed",
        "A Vertex AI pipeline step failed; the Airflow log carries only the code-9 wrapper, "
        "not the cause. Read the bracketed step name and job_id from the message, then pull "
        "that step's own logs (Vertex console run URL is logged just above the traceback). "
        "The step is usually a Dataproc/custom job whose real error lives one layer down.",
        "no",
        remedy=(
            "Open the named step's own logs at the logged job_id. The change belongs to whatever "
            "that step runs, not to this DAG."
        ),
    ),
    Signature(
        "model_alias_not_found",
        # Only ever printed on failure, and only reachable from the Vertex driver output the
        # router fetches. INC-024: a model re-registration silently dropped the alias.
        r"No version found with alias pattern '([^']+)' for model|"
        r"alias .{0,40}(not found|does not exist).{0,40}model",
        "vertex/model-alias-missing",
        "The inference job resolves its model by alias pattern (e.g. challenger-v*) and the "
        "registry has no version carrying it. Re-registering a model drops the aliases it "
        "replaces, so this fires on every run until the owner re-applies the alias.",
        "no",
        remedy=(
            "Re-apply the alias to the intended model version in the registry. A retry cannot "
            "recreate an alias, so every run fails until someone does."
        ),
    ),
    Signature(
        "analysis_exception",
        r"AnalysisException|TABLE_OR_VIEW_NOT_FOUND|UNRESOLVED_COLUMN|cannot resolve",
        "query/schema-error",
        "Invalid SQL, missing column/table.",
        "yes",
        remedy=(
            "Fix the query: the named table or column does not resolve. Check whether an upstream "
            "rename landed before editing the SQL."
        ),
    ),
    Signature(
        "pod_evicted_404",
        r"pods .{0,40}not found|istio check|pod.{0,20}evict|"
        r"Could not read served logs.{0,40}timed out|\(404\).{0,40}not found",
        "orchestration/pod-evicted",
        "K8s pod evicted or lost mid-run (orchestration-only; the Spark/Databricks job may have "
        "succeeded and written data).",
        "no",
        remedy=(
            "Check whether the underlying job succeeded and wrote its output before re-running. "
            "This is orchestration loss, not job failure."
        ),
    ),
    Signature(
        "ttl_exceeded",
        # DEADLINE_EXCEEDED alone is any gRPC client timeout; require batch/TTL context
        r"cancell(ed|ing).{0,20}(TTL|ttl)|exceeded.{0,20}ttl|wall.?clock.{0,20}(exceed|limit)|"
        r"(\bbatch\b|\bttl\b).{0,60}DEADLINE_EXCEEDED|DEADLINE_EXCEEDED.{0,60}(\bbatch\b|\bttl\b)|"
        r"reached its.{0,10}timeout",
        "ttl/wall-clock",
        "Job cancelled at its TTL / wall-clock limit (often a perf regression).",
        "sometimes",
        remedy=(
            "Compare the runtime against recent green runs. Creeping up, find the regression or "
            "raise the TTL. Flat then stalled, the job is stuck and a longer TTL hides it."
        ),
    ),
    Signature(
        "openai_file_quota",
        r"exceeded your file storage quota|Projects are limited to .{0,10}TB of files|"
        r"file storage quota",
        "vendor-quota/openai",
        "OpenAI project hit its 2.5TB file-storage quota, so the batch-input upload is rejected "
        "(deterministic 400 - retries cannot fix it).",
        "no",
        remedy=(
            "Delete old files in the OpenAI project, or set an expiry on batch inputs. The "
            "rejection is deterministic, so retries cannot clear it."
        ),
    ),
    Signature(
        "cluster_create_stockout",
        r"code:?\s*14\b|\bUNAVAILABLE\b.{0,60}resource|does not have enough resources|"
        r"ZONE_RESOURCE_POOL_EXHAUSTED|resource pool exhausted|out of .{0,20}(capacity|stock)",
        "infra/zonal-stockout",
        "Dataproc/GCE could not get machines in the zone (transient GCP stockout). Usually "
        "self-recovers in ~1-2h; autozone re-picks. Delete any lingering ERROR cluster (it "
        "self-blocks the retry on quota), then re-run. Also hits Databricks-on-GCP cluster "
        "launches, so engine is 'any'.",
        "no",
        remedy=(
            "Delete the lingering ERROR cluster first (it holds quota and blocks the retry), then "
            "re-run in 1-2h. Recurring in one zone means pinning another zone or widening the "
            "machine family."
        ),
    ),
    Signature(
        "quota_exhaustion",
        r"Insufficient .{0,30}quota|QUOTA_EXCEEDED|quota.{0,20}exceeded|"
        r"\bN2_CPUS\b|\bDISKS_TOTAL_GB\b",
        "infra/quota",
        "The request is at/over a regional quota ceiling (often a large cluster near 90%+ of "
        "N2_CPUS/DISKS_TOTAL_GB, or a prior failed cluster's VMs self-blocking the retry). "
        "Raise quota / delete the lingering cluster / shrink the request. Also hits "
        "Databricks-on-GCP cluster launches, so engine is 'any'.",
        "no",
        remedy=(
            "Raise the quota named in the error, or shrink the request. Check first whether "
            "another cluster is holding the headroom: a QA cluster taking the region's N2_CPUS "
            "reads identically (INC-025, AUDI-1217)."
        ),
    ),
    Signature(
        "sensor_timeout",
        r"AirflowSensorTimeout|Sensor has timed out|Snap\. Time is up|"
        r"up_for_reschedule.{0,40}timeout",
        "sensor-timeout",
        "A sensor watched a partition/upstream that was not ready by its deadline. Often benign "
        "(optional 3P partner skipped that day) or the upstream is still running; verify source "
        "presence before treating it as a real failure.",
        "no",
        remedy=(
            "Check whether the awaited object exists. Present, clear the sensor. Absent by design "
            "(partner skipped the day), no-op it. Absent unexpectedly, chase the producer."
        ),
    ),
    Signature(
        "external_task_failed",
        r"ExternalTaskFailedError|ExternalTaskSensor.{0,40}fail|state.{0,10}upstream_failed|"
        r"upstream task.{0,20}(failed|upstream_failed)",
        "upstream-failure",
        "The sensor's external task is in a failed state - this task is a symptom, not the cause. "
        "ExternalTaskFailedError uses the SAME message for a SKIPPED external task (producer "
        "short-circuited on missing source data = benign partner-data gap, INC-011) as for a "
        "truly failed/upstream_failed one (real break, INC-006/007), so resolve the external "
        "task's ACTUAL state first: skipped -> check the producer's source_available_<ds> log for "
        "'No source data', no-op the hour, do not backfill; failed/upstream_failed -> audit the "
        "upstream chain.",
        "no",
        remedy=(
            "Resolve the external task's real state first. Skipped means no-op and do not "
            "backfill; failed means fix that task, because this one is only the symptom."
        ),
    ),
    Signature(
        "batch_id_attach_trap",
        r"Batch with given id already exists|Attaching to the job.{0,60}if it is still running",
        "dag_bug/batch-id-reattach",
        "The batch id is minted once by an upstream task and cached in XCom, so this retry "
        "reattached to the ALREADY-FAILED batch and inherited its error. The error text here is "
        "not a fresh fault.",
        "yes",
        remedy=(
            "Clear the id-minting task WITH downstream so a new batch id is minted. Clearing this "
            "task alone reattaches to the same failed batch."
        ),
    ),
    Signature(
        "impersonation_unavailable",
        r"Unable to acquire impersonated credentials|"
        r"Getting metadata from plugin failed.{0,80}UNAVAILABLE",
        "transient-infra/iam-503",
        "GCP's credential-minting service returned 503 while impersonating the job service "
        "account, so the task died BEFORE submitting anything. No batch exists, nothing to clean "
        "up.",
        "no",
        remedy=(
            "Re-run once; the credential service returned 503 and nothing was submitted. If it "
            "repeats, raise it with the IAM owners rather than the DAG owner."
        ),
    ),
    Signature(
        "slack_notify_failed",
        # Must be the TASK's own exception. The notifier error also appears in the failure
        # callback of any DAG that posts to Slack, where it would steal the real cause.
        r"'exception': SlackApiError|SlackApiError\(.{0,160}(channel_not_found|not_in_channel)",
        "config/slack-channel",
        "The Slack notification call failed: the bot is not in the target channel, or the channel "
        "id is wrong or renamed.",
        "yes",
        remedy=(
            "Invite the app to the channel, or correct the channel id in the DAG config. The "
            "task's own work may well have succeeded."
        ),
    ),
    Signature(
        "task_execution_timeout",
        r"\[error\] task Process timed out|Process timed out",
        "timeout/execution",
        "Airflow killed the task at its execution_timeout. The work itself may be fine but "
        "slow, so read the runtime trend before raising the timeout: a task that crept past "
        "the budget is a capacity problem, one that hangs is not.",
        "sometimes",
        remedy=(
            "Read the runtime trend before changing the budget. Creeping up is a capacity or "
            "data-volume regression; flat then a cliff means it hung, and a longer timeout hides "
            "that."
        ),
    ),
    Signature(
        "dbt_model_runtime_error",
        r"Runtime Error in model|Database Error in model",
        "dbt/model-runtime-error",
        "A dbt model raised at runtime (not a data-quality test). The real exception is in "
        "the Python traceback printed under the Runtime Error line; dbt's own line numbers "
        "are templated and do not match the source file.",
        "sometimes",
        remedy=(
            "Read the Python traceback under the Runtime Error line and fix it in the model's "
            "source. dbt's own line numbers are templated and point at the wrong place."
        ),
    ),
    Signature(
        "dag_not_found_at_startup",
        r"Dag not found during start ?up|DAG '[^']+' not found in serialized_dag table",
        "orchestration/dag-not-loaded",
        "The worker could not load the DAG when the task started, so the task died before running "
        "any of its own code. Usually a deploy or DAG-bundle race: the scheduler queued the task "
        "against a bundle version the worker no longer has.",
        "no",
        remedy=(
            "Re-run once the bundle version settles. Repeating across a deploy means the bundle "
            "did not propagate; check the deploy rather than editing the DAG."
        ),
    ),
    Signature(
        "batch_id_missing",
        r"Starting batch None(-\d+)?\b",
        "dag_bug/no-batch-id",
        "Airflow logged the batch id as literally 'None': the upstream id-minting task returned "
        "nothing, so no batch was ever submitted. The missing id IS the fault.",
        "yes",
        remedy=(
            "Fix the upstream task that returns the batch id, whose XCom came back empty. The "
            "Spark job never ran, so there is nothing wrong with it."
        ),
    ),
    Signature(
        "batch_cancelled",
        r"Batch job \S+ was cancelled",
        "batch-cancelled",
        "The Dataproc batch was CANCELLED, not failed. Either it hit its TTL, or someone cleared "
        "the Airflow task while the batch was still running, which cancels it and can record the "
        "next try as a green run with no output.",
        "no",
        remedy=(
            "Check the batch state history. A TTL cancel is a timeout; a human clear means re-run "
            "and expect no output from the cancelled attempt."
        ),
    ),
    Signature(
        "task_externally_terminated",
        r"Server indicated the task shouldn't be running anymore|Task killed!|"
        r"Task received SIGTERM signal",
        "orchestration/externally-killed",
        "Airflow terminated the process; the task did not fail on its own. Usually a clear, a "
        "DAG-run reset, or the scheduler adopting the instance. This try's log holds no cause: "
        "if the task really is broken, the reason is in an EARLIER try.",
        "no",
        remedy=(
            "Read the earlier try; this one holds no cause. If no earlier try failed, the kill "
            "was a clear or a scheduler adoption and nothing is broken."
        ),
    ),
    Signature(
        "db_credential_rejected",
        r"PSQLException: FATAL: password authentication failed|"
        r"password authentication failed for user|"
        r"Access denied for user '[^']+'@|"
        r"ORA-01017|Login failed for user",
        "auth/database-credential",
        "The database rejected the credential itself, which is not the same as a missing grant: "
        "the password is wrong, rotated, or the secret the job reads is stale.",
        "no",
        remedy=(
            "Compare the secret's last rotation against the last green run, then repoint the job "
            "at the current secret. Re-running with the same credential fails identically."
        ),
    ),
    Signature(
        "auth_error",
        r"AccessDenied|PERMISSION_DENIED|Unauthorized|invalid[_ ]token|token.{0,20}expired|"
        r"(?<![0-9])(401|403)(?![0-9]).{0,30}(Forbidden|Unauthorized|denied)",
        "auth",
        "Expired token or missing IAM/UC grant.",
        "no",
        remedy=(
            "Read the principal and resource out of the error, then grant or refresh. An expired "
            "token re-runs clean; a missing grant never will."
        ),
    ),
    # LAST by design: fires only when nothing specific matched, i.e. the Airflow log is
    # pure wrapper and the cause lives one layer down.
    Signature(
        "downstream_job_no_local_cause",
        # Failure-only wording. 'Waiting for the completion of batch job' is NOT usable here:
        # it appears in every healthy Dataproc log, so it fired on 325 green runs.
        r"Dataproc Agent reports job failure|returned a failure\.\s*\\?n?remote_pod",
        "boilerplate/cause-one-layer-down",
        "The downstream job was submitted and failed, but this Airflow log carries only the "
        "wrapper, no cause.",
        "no",
        remedy=(
            "Pull the downstream job's own log (Dataproc driver output, or the pod log) and "
            "diagnose there. Nothing in the Airflow log is the cause."
        ),
    ),
]


@dataclass(frozen=True)
class Match:
    """A signature hit against an error string."""

    key: str
    sig_class: str
    likely_cause: str
    programmatic_fix: str
    matched_on: str
    remedy: str = ""


def classify(text: str, engine: str = "any") -> Match | None:
    """Return the first matching signature (most-specific first), or None."""
    if not text:
        return None
    for s in SIGNATURES:
        if s.engine != "any" and engine != "any" and s.engine != engine:
            continue
        m = re.search(s.pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            return Match(
                s.key,
                s.sig_class,
                s.likely_cause,
                s.programmatic_fix,
                m.group(0)[:120],
                s.remedy,
            )
    return None
