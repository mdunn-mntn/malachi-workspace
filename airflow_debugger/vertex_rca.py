"""Vertex AI pipeline-run failure analyzer (key-free).

A Vertex pipeline failure carries NO cause in the Airflow log: the task prints a
`Pipeline Run URL` and then dies with an empty exception. Matching that line with a
signature is not an option, because every SUCCESSFUL run prints it too. The cause is
five layers down, and every layer is mechanical:

    Airflow log            -> pipeline run id, project, location (the Run URL)
    pipelineJobs GET       -> the FAILED leaf task (not the root DAG node) + its error
    leaf error message     -> the ml_job replica id named in its console log link
    ml_job Cloud Logging   -> the component traceback + the Dataproc job ids it submitted
    dataproc jobs describe -> driver output holding the real exception

Layer 5 is where the answer usually is: INC-024's whole cause was a `ValueError` in the
driver output, seven Airflow-log-free hops from the alert.

Auth is the same pattern as `dataproc_rca`: the gcloud CLI and a short-lived
`print-access-token`, never a stored key. There is no `gcloud ai pipeline-jobs`
subcommand, so the pipelineJob read is a REST GET.
"""

from __future__ import annotations

import contextlib
import json
import re
from dataclasses import dataclass, field

from .dataproc_rca import (
    _access_token,
    _run,
    driveroutput_text,
    error_region,
    logging_messages,
)
from .masks import detect as detect_mask
from .masks import note as mask_note
from .signatures import classify

REGION = "us-central1"
_ML_JOB_LOG_LIMIT = 400  # the component traceback prints line-per-entry; 80 truncates it
_MAX_DATAPROC_JOBS = 3  # the executor retries an index 3x; describing every retry is noise

# The leaf error embeds the replica's log link percent-encoded, in either of two shapes.
_ML_JOB_RE = re.compile(r"ml_job%2Fjob_id%2F(\d{6,})|job_id(?:%3D%22|=\")(\d{6,})")
_DATAPROC_JOB_RE = re.compile(
    r"job (?:index \d+ as |[0-9a-f-]{36} \(index)?.*?([0-9a-f]{8}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)
_CLUSTER_RE = re.compile(r"parallel jobs to cluster (\S+)")

_LAST_JOB_RE = re.compile(r"last job_id: ([0-9a-f-]{36})")


@dataclass
class VertexEvidence:
    """Deterministic evidence bundle for one Vertex AI pipeline run."""

    engine: str = "vertex"
    run_id: str | None = None
    project: str | None = None
    location: str = REGION
    state: str | None = None  # PIPELINE_STATE_FAILED / _SUCCEEDED / _CANCELLED
    pipeline_error: str | None = None  # the root DAG node's error message
    failed_tasks: list = field(default_factory=list)  # leaf component names that FAILED
    ml_job_id: str | None = None
    cluster_name: str | None = None
    dataproc_job_ids: list = field(default_factory=list)
    error_text: str | None = None  # the deepest real error found
    error_layer: str | None = None  # which layer error_text came from
    signature: dict | None = None
    notes: list = field(default_factory=list)


def _api_get(project: str, location: str, run_id: str) -> tuple[dict | None, str | None]:
    """GET one pipelineJob. Returns (job, None) or (None, why)."""
    token, err = _access_token()
    if err:
        return None, f"access token: {err}"
    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/projects/{project}"
        f"/locations/{location}/pipelineJobs/{run_id}"
    )
    stdout, err = _run([
        "curl", "-s", "--max-time", "60", url,
        "-H", f"Authorization: Bearer {token}",
    ])  # fmt: skip
    if err:
        return None, err
    try:
        body = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return None, "pipelineJobs GET: non-json response"
    if "error" in body and "state" not in body:  # an API error, not a job error
        err_obj = body["error"]
        return None, f"HTTP {err_obj.get('code')}: {str(err_obj.get('message'))[:280]}"
    return body, None


def _failed_leaves(job: dict, run_id: str) -> list[dict]:
    """FAILED taskDetails minus the root DAG node, which only restates its children."""
    details = (job.get("jobDetail") or {}).get("taskDetails") or []
    return [
        t
        for t in details
        if t.get("state") == "FAILED" and t.get("taskName") not in (run_id, job.get("displayName"))
    ]


def _ml_job_id(message: str) -> str | None:
    m = _ML_JOB_RE.search(message or "")
    return (m.group(1) or m.group(2)) if m else None


def _dataproc_jobs(messages: str) -> list[str]:
    """Dataproc job uuids the component submitted, newest-relevant first.

    The executor retries a failed index up to 3 times and names the final one in its
    abort line; that is the attempt whose driver output holds the real exception.
    """
    ordered: list[str] = []
    last = _LAST_JOB_RE.search(messages or "")
    if last:
        ordered.append(last.group(1))
    for m in _DATAPROC_JOB_RE.finditer(messages or ""):
        if m.group(1) not in ordered:
            ordered.append(m.group(1))
    return ordered


def _dataproc_error(job_uuid: str, project: str, region: str) -> tuple[str | None, str | None]:
    """Driver-output error text for one nested Dataproc job."""
    stdout, err = _run([
        "gcloud", "dataproc", "jobs", "describe", job_uuid,
        "--region", region, "--project", project, "--format", "json",
    ])  # fmt: skip
    if err:
        return None, f"dataproc describe {job_uuid[:8]}: {err}"
    try:
        d = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return None, f"dataproc describe {job_uuid[:8]}: non-json output"
    uri = d.get("driverOutputResourceUri")
    if not uri:
        return None, f"dataproc job {job_uuid[:8]} has no driverOutputResourceUri"
    text, do_err = driveroutput_text(f"{uri}*")
    if not text:
        return None, f"driver output {job_uuid[:8]}: {do_err or 'empty'}"
    return error_region(text), None


def analyze_pipeline_run(
    run_id: str, project: str, location: str = REGION
) -> VertexEvidence:
    """Deterministic RCA for one Vertex pipeline run. Never raises for a CLI error."""
    ev = VertexEvidence(run_id=run_id, project=project, location=location)
    job, err = _api_get(project, location, run_id)
    if err or job is None:
        ev.notes.append(f"pipelineJobs GET failed: {err}")
        return ev

    ev.state = job.get("state")
    ev.pipeline_error = ((job.get("error") or {}).get("message") or "").strip() or None
    leaves = _failed_leaves(job, run_id)
    ev.failed_tasks = [t.get("taskName") for t in leaves if t.get("taskName")]
    if ev.state == "PIPELINE_STATE_SUCCEEDED":
        return ev  # the pipeline was fine; the Airflow failure is orchestration-only
    if not leaves:
        ev.error_text = ev.pipeline_error
        ev.error_layer = "pipeline"
        ev.notes.append("no FAILED leaf component; the pipeline error is all Vertex recorded")
        ev.signature = _classified(ev.error_text)
        return ev

    leaf_error = (leaves[0].get("error") or {}).get("message") or ""
    ev.error_text, ev.error_layer = leaf_error.strip() or None, "component"
    ev.ml_job_id = _ml_job_id(leaf_error)
    if not ev.ml_job_id:
        ev.notes.append("component error names no ml_job replica; stopping at the component layer")
        ev.signature = _classified(ev.error_text)
        return ev

    msgs, log_err = logging_messages(
        f'resource.type="ml_job" AND resource.labels.job_id="{ev.ml_job_id}"',
        project,
        limit=_ML_JOB_LOG_LIMIT,
    )
    if not msgs:
        ev.notes.append(f"ml_job {ev.ml_job_id} logs unavailable: {log_err or 'no entries'}")
        ev.signature = _classified(ev.error_text)
        return ev

    replica_error = error_region(_messages_text(msgs))
    if replica_error:
        ev.error_text, ev.error_layer = replica_error, "replica"
    cluster = _CLUSTER_RE.search(msgs)
    ev.cluster_name = cluster.group(1) if cluster else None
    ev.dataproc_job_ids = _dataproc_jobs(msgs)[:_MAX_DATAPROC_JOBS]

    for uuid in ev.dataproc_job_ids:
        text, why = _dataproc_error(uuid, project, location)
        if text:
            ev.error_text, ev.error_layer = text, "dataproc-driver"
            break
        if why:
            ev.notes.append(why)

    ev.error_text, ev.error_layer = _past_the_mask(ev, msgs, project)

    ev.signature = _classified(ev.error_text)
    return ev


def _past_the_mask(ev: VertexEvidence, msgs: str, project: str) -> tuple[str | None, str | None]:
    """Follow a masking error to the real one, or say in the notes that we stopped on a mask."""
    blob = f"{ev.error_text or ''}\n{msgs}"
    mask = detect_mask(blob)
    if not mask or mask.key != "dataproc_cleanup_delete_404":
        if mask:
            ev.notes.append(mask_note(mask))
        return ev.error_text, ev.error_layer

    import re as _re

    m = _re.search(mask.pattern, blob, _re.IGNORECASE)
    cluster = m.group(1) if m else None
    ev.cluster_name = ev.cluster_name or cluster
    text, why = _cluster_create_error(cluster, project) if cluster else (None, "no cluster name")
    if text:
        return text, "dataproc-create"
    ev.notes.append(f"{mask_note(mask)} ({why or 'no CreateCluster audit entry'})")
    return ev.error_text, ev.error_layer


def _cluster_create_error(cluster_name: str, project: str) -> tuple[str | None, str | None]:
    """Why CreateCluster refused, from the admin audit log the delete-404 traceback buries."""
    filt = (
        'protoPayload.methodName="google.cloud.dataproc.v1.ClusterController.CreateCluster" '
        f'AND protoPayload.resourceName:"{cluster_name}"'
    )
    text, err = logging_messages(filt, project, limit=5, field="protoPayload.status.message")
    text = (text or "").strip()
    if not text:
        return None, err
    return text, None


def _messages_text(raw: str) -> str:
    """Cloud Logging returns the ml_job payload as a JSON dict per line; unwrap to text.

    Entries arrive newest-first, so reverse them: `error_region` anchors on the LAST
    `Traceback` header, which in chronological order is the real one.
    """
    out = []
    for line in raw.splitlines():
        s = line.strip()
        if s.startswith("{"):
            with contextlib.suppress(json.JSONDecodeError):
                s = json.loads(s).get("message", s)
        if s:
            out.append(s)
    return "\n".join(reversed(out))


def _classified(text: str | None) -> dict | None:
    from dataclasses import asdict

    sig = classify(text or "")
    return asdict(sig) if sig else None


if __name__ == "__main__":
    import sys
    from dataclasses import asdict

    argv = [a for a in sys.argv[1:] if not a.startswith("-")]
    if len(argv) < 2:
        print(
            "usage: python -m airflow_debugger.vertex_rca <pipeline_run_id> <project> [location]"
        )
        raise SystemExit(2)
    loc = argv[2] if len(argv) > 2 else REGION
    print(json.dumps(asdict(analyze_pipeline_run(argv[0], argv[1], loc)), indent=2, default=str))
