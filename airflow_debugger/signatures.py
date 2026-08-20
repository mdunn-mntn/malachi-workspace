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
    engine: str = "any"  # "any" | "dataproc" | "databricks"


SIGNATURES: list[Signature] = [
    Signature(
        "table_or_view_already_exists",
        r"TABLE_OR_VIEW_ALREADY_EXISTS|already exists.{0,40}SQLSTATE: 42P07|\b42P07\b",
        "idempotency/orphaned-run",
        "A prior (orphaned or retried) run already created the target table; the re-run collides. "
        "The overwrite/create is not idempotent under retry.",
        "sometimes",
    ),
    Signature(
        "concurrent_delta_write",
        r"ConcurrentAppendException|ConcurrentModificationException|ConcurrentDeleteReadException",
        "concurrency",
        "Concurrent writes to the same Delta table/partition.",
        "sometimes",
    ),
    Signature(
        "executor_oom_yarn",
        r"Container killed by YARN for exceeding memory|physical memory used|exit code 137|"
        r"Exit code is 137|oom[-_ ]?reaper",
        "executor-oom",
        "Executor exceeded memory/overhead (skew or under-provisioned memory).",
        "no",
    ),
    Signature(
        "driver_oom",
        r"OutOfMemoryError: Java heap space|OutOfMemoryError: GC overhead",
        "driver-oom",
        "Driver OOM, often after collect()/toPandas() pulling too much to the driver.",
        "sometimes",
    ),
    Signature(
        "shuffle_oom",
        r"SparkOutOfMemoryError|Unable to acquire.{0,20}bytes of memory",
        "shuffle-oom",
        "Shuffle OOM: too few shuffle partitions or skew.",
        "sometimes",
    ),
    Signature(
        "shuffle_fetch_failure",
        r"FetchFailedException|MetadataFetchFailedException|Failed to connect to .{0,40}shuffle",
        "shuffle-fetch-failure",
        "Executor loss/downscale/bad node, or a >2GB shuffle block.",
        "no",
    ),
    Signature(
        "spot_preemption",
        r"PREEMPTIBLE_WITH_FALLBACK|SPOT_INSTANCE_TERMINATION|was preempted|"
        r"spot instance.{0,40}(reclaim|terminat|preempt)|InsufficientInstanceCapacity",
        "infra/spot-preemption",
        "Spot/preemptible instance reclaimed mid-run.",
        "no",
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
    ),
    Signature(
        "executor_lost",
        # benign dynamic-allocation decommissions log 'Lost executor ... spark scale down'
        r"ExecutorLostFailure|Slave lost|Lost executor(?![^\n]*spark scale down)|"
        r"Executor heartbeat timed out",
        "executor-lost",
        "Executor lost: OOM, spot preemption, or an unhealthy node (disk >90%).",
        "no",
    ),
    Signature(
        "broadcast_timeout",
        r"Broadcast timeout|Could not execute broadcast|BroadcastTimeout",
        "broadcast-join",
        "Broadcast side too large or slow.",
        "yes",
    ),
    Signature(
        "schema_drift",
        r"Failed to merge incompatible data types|INCOMPATIBLE_DATA_TYPE|Schema mismatch",
        "schema-drift",
        "Upstream schema change.",
        "sometimes",
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
    ),
    Signature(
        "path_not_found_late_data",
        r"PATH_NOT_FOUND|Path does not exist|path does not exist.{0,60}gs://|"
        r"AnalysisException.{0,40}(PATH_NOT_FOUND|does not exist)|"
        r"Missing( required)?.{0,30}partition|Missing required.{0,40}at gs://",
        "late-data/missing-partition",
        "A source partition the job reads (e.g. gs://.../dt=<run_date>) has not landed yet. "
        "Usually the upstream producer runs late or FAILED (timing race or a broken producer); "
        "verify the partition + _SUCCESS then re-run the consumer, else fix/re-run the producer.",
        "no",
    ),
    Signature(
        "vertex_param_contract",
        r"pipeline parameter .{0,60}not found in the pipeline( job)? input definitions|"
        r"parameter .{0,40}is not found in the pipeline",
        "vertex/param-mismatch",
        "The operator injects a param name the Vertex/KFP template does not declare "
        "(e.g. reference_date vs run_date), so PipelineJob rejects it before submission. "
        "Fix = rename the KFP pipeline param to match + recompile/redeploy the template.",
        "yes",
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
    ),
    Signature(
        "analysis_exception",
        r"AnalysisException|TABLE_OR_VIEW_NOT_FOUND|UNRESOLVED_COLUMN|cannot resolve",
        "query/schema-error",
        "Invalid SQL, missing column/table.",
        "yes",
    ),
    Signature(
        "pod_evicted_404",
        r"pods .{0,40}not found|istio check|pod.{0,20}evict|"
        r"Could not read served logs.{0,40}timed out|\(404\).{0,40}not found",
        "orchestration/pod-evicted",
        "K8s pod evicted or lost mid-run (orchestration-only; the Spark/Databricks job may have "
        "succeeded and written data).",
        "no",
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
    ),
    Signature(
        "openai_file_quota",
        r"exceeded your file storage quota|Projects are limited to .{0,10}TB of files|"
        r"file storage quota",
        "vendor-quota/openai",
        "OpenAI project hit its 2.5TB file-storage quota, so the batch-input upload is rejected "
        "(deterministic 400 - retries cannot fix it). Purge old OpenAI files / let expiry clear it.",
        "no",
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
    ),
    Signature(
        "external_task_failed",
        r"ExternalTaskFailedError|ExternalTaskSensor.{0,40}fail|state.{0,10}upstream_failed|"
        r"upstream task.{0,20}(failed|upstream_failed)",
        "upstream-failure",
        "The sensor's external task is in a failed state - this task is a symptom, not the cause. "
        "ExternalTaskFailedError uses the SAME message for a SKIPPED external task (producer "
        "short-circuited on missing source data = benign partner-data gap, INC-011) as for a truly "
        "failed/upstream_failed one (real break, INC-006/007), so resolve the external task's ACTUAL "
        "state first: skipped -> check the producer's source_available_<ds> log for 'No source "
        "data', no-op the hour, do not backfill; failed/upstream_failed -> audit the upstream chain. "
        "Never clear-to-retry a skip - the awaited partition will not land.",
        "no",
    ),
    Signature(
        "batch_id_attach_trap",
        r"Batch with given id already exists|Attaching to the job.{0,60}if it is still running",
        "dag_bug/batch-id-reattach",
        "The batch id is minted once by an upstream task and cached in XCom, so this retry "
        "reattached to the ALREADY-FAILED batch and inherited its error. The error text here "
        "is not a fresh fault. Check GCS for _SUCCESS before re-running; to genuinely re-run, "
        "clear the id-minting task WITH downstream so a new id is minted.",
        "yes",
    ),
    Signature(
        "impersonation_unavailable",
        r"Unable to acquire impersonated credentials|"
        r"Getting metadata from plugin failed.{0,80}UNAVAILABLE",
        "transient-infra/iam-503",
        "GCP's credential-minting service returned 503 while impersonating the job service "
        "account, so the task died BEFORE submitting anything. No batch exists, nothing to "
        "clean up. Confirm the log never reaches a batch state, then check whether the DAG "
        "self-heals or has retries before acting.",
        "no",
    ),
    Signature(
        "slack_notify_failed",
        # Must be the TASK's own exception. The notifier error also appears in the failure
        # callback of any DAG that posts to Slack, where it would steal the real cause.
        r"'exception': SlackApiError|SlackApiError\(.{0,160}(channel_not_found|not_in_channel)",
        "config/slack-channel",
        "The Slack notification call failed: the bot is not in the target channel, or the "
        "channel id is wrong or renamed. Fix the channel id in the DAG config or invite the "
        "app to the channel.",
        "yes",
    ),
    Signature(
        "task_execution_timeout",
        r"\[error\] task Process timed out|Process timed out",
        "timeout/execution",
        "Airflow killed the task at its execution_timeout. The work itself may be fine but "
        "slow, so read the runtime trend before raising the timeout: a task that crept past "
        "the budget is a capacity problem, one that hangs is not.",
        "sometimes",
    ),
    Signature(
        "dbt_model_runtime_error",
        r"Runtime Error in model|Database Error in model",
        "dbt/model-runtime-error",
        "A dbt model raised at runtime (not a data-quality test). The real exception is in "
        "the Python traceback printed under the Runtime Error line; dbt's own line numbers "
        "are templated and do not match the source file.",
        "sometimes",
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
    ),
    Signature(
        "auth_error",
        r"AccessDenied|PERMISSION_DENIED|Unauthorized|invalid[_ ]token|token.{0,20}expired|"
        r"(?<![0-9])(401|403)(?![0-9]).{0,30}(Forbidden|Unauthorized|denied)",
        "auth",
        "Expired token or missing IAM/UC grant.",
        "no",
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
        "wrapper, no cause. Pull the job's own output: the Dataproc batch driver log (the "
        "batch id is logged above) or the Kubernetes pod log. Do not read the wrapper text "
        "as the root cause.",
        "no",
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


def classify(text: str, engine: str = "any") -> Match | None:
    """Return the first matching signature (most-specific first), or None."""
    if not text:
        return None
    for s in SIGNATURES:
        if s.engine != "any" and engine != "any" and s.engine != engine:
            continue
        m = re.search(s.pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            return Match(s.key, s.sig_class, s.likely_cause, s.programmatic_fix, m.group(0)[:120])
    return None
