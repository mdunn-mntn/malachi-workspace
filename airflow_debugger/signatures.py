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
        r"OutOfMemoryError: Java heap space|OutOfMemoryError: GC overhead|"
        r"Driver stacktrace.*OutOfMemory",
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
        "executor_lost",
        r"ExecutorLostFailure|Slave lost|Lost executor|Executor heartbeat timed out",
        "executor-lost",
        "Executor lost: OOM, spot preemption, or an unhealthy node (disk >90%).",
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
        r"cancelled.{0,20}(TTL|ttl)|exceeded.{0,20}ttl|wall.?clock.{0,20}(exceed|limit)|"
        r"DEADLINE_EXCEEDED|reached its.{0,10}timeout",
        "ttl/wall-clock",
        "Job cancelled at its TTL / wall-clock limit (often a perf regression).",
        "sometimes",
    ),
    Signature(
        "auth_error",
        r"AccessDenied|PERMISSION_DENIED|Unauthorized|invalid[_ ]token|token.{0,20}expired|"
        r"(?<![0-9])(401|403)(?![0-9]).{0,30}(Forbidden|Unauthorized|denied)",
        "auth",
        "Expired token or missing IAM/UC grant.",
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
