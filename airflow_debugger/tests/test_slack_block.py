"""The Slack post's shape is the product, so the shape is what these pin.

Every case is built from a real diagnosis dict, and the invariant under test is that the four
labels appear in the same order regardless of which layer produced the cause — otherwise on-call
has to re-learn the layout per failure, which is the thing this replaces.
"""

from __future__ import annotations

from airflow_debugger import slack_block

_LABELS = ("*What*", "*Where*", "*Why*", "*How*")

_QUOTA = {
    "identity": {
        "dag_id": "fangorn_inference_pipeline_run",
        "task_id": "challenger_inference_pipeline",
    },
    "engine": "vertex",
    "vertex_run_id": "fangorn-challenger-inference-pipeline-20260824225530",
    "vertex_project": "mntn-targeting-prj-prod",
    "vertex_location": "us-central1",
    "root_error": "Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0.",
    "root_signature": {
        "key": "quota_exhaustion",
        "sig_class": "infra/quota",
        "likely_cause": "The request is at/over a regional quota ceiling.",
        "programmatic_fix": "no",
        "remedy": "Raise the quota named in the error, or shrink the request. Check first whether another cluster is holding the headroom: a QA cluster taking the region's N2_CPUS reads identically (INC-025, AUDI-1217).",
    },
}

_MASKED = {
    "identity": {"dag_id": "d", "task_id": "t"},
    "root_error": (
        "google.api_core.exceptions.NotFound: 404 Not found: Cluster "
        "projects/p/regions/us-central1/clusters/fangorn-challenger-a483e22d"
    ),
    "root_signature": {},
}

_UNKNOWN = {
    "identity": {"dag_id": "d", "task_id": "t"},
    "root_error": "something odd",
    "root_signature": {},
}


def _order(text: str) -> list[int]:
    return [text.index(lbl) for lbl in _LABELS]


def test_the_four_labels_appear_in_the_same_order_every_time() -> None:
    """The whole point: one layout, whatever the failure or the evidence source."""
    for diag in (_QUOTA, _MASKED, _UNKNOWN):
        out = slack_block.render(diag, repo_paths={})
        assert all(lbl in out for lbl in _LABELS), out
        assert _order(out) == sorted(_order(out)), out


def test_a_matched_signature_is_labelled_as_evidence() -> None:
    """A deterministic hit says so, so the reader knows it is not a guess."""
    out = slack_block.render(_QUOTA, repo_paths={})
    assert "(matched signature)" in out
    assert "regional quota ceiling" in out
    assert "Raise the quota named in the error" in out


def test_an_llm_cause_is_labelled_unverified_and_never_outranks_a_signature() -> None:
    """A model's guess must never be presentable as a matched signature."""
    text, source = slack_block.why(_UNKNOWN, llm_cause="probably a bad credential")
    assert source == slack_block.WHY_LLM
    assert "LLM, unverified" in slack_block.render(
        _UNKNOWN, llm_cause="probably a bad credential", repo_paths={}
    )

    text, source = slack_block.why(_QUOTA, llm_cause="probably a bad credential")
    assert source == slack_block.WHY_DETERMINISTIC
    assert "quota" in text


def test_a_gap_says_where_the_chain_stopped_instead_of_guessing() -> None:
    """An honest dead end beats a plausible invention."""
    out = slack_block.render(_MASKED, repo_paths={})
    assert "(no cause found)" in out
    assert "masking error" in out
    assert "audit log" in out


def test_an_unknown_failure_admits_it() -> None:
    """No signature, no mask, no LLM: say that, and name it as a taxonomy gap."""
    out = slack_block.render(_UNKNOWN, repo_paths={})
    assert "(no cause found)" in out
    assert "not yet in the taxonomy" in out


_STUB = {
    "identity": {"dag_id": "vertical_classification_api", "task_id": "response_tests"},
    "ti_state": "upstream_failed",
    "no_error_text": True,
    "upstream_failed_tasks": ["ddp_vertical_classification_api"],
}

_POD = {
    "identity": {"dag_id": "databricks_guid_geos", "task_id": "run_databricks_job"},
    "ti_state": "failed",
    "pod_deleted": True,
    "pod_wait_seconds": 120,
    "pod_name": "run-databricks-job-xrc0t925",
}


def test_a_stub_names_its_culprit_in_slack_not_just_in_the_report() -> None:
    """The 39-log case. The report resolved the upstream task; the post said "no cause found"."""
    out = slack_block.render(_STUB, repo_paths={})
    assert "(no cause in this log)" in out
    assert "ddp_vertical_classification_api" in out
    assert "no cause found" not in out
    assert "not yet in the taxonomy" not in out


def test_a_pod_that_never_started_says_so_in_slack() -> None:
    """An empty exception is a startup timeout, and the channel has to carry that, not a shrug."""
    out = slack_block.render(_POD, repo_paths={})
    assert "(no cause in this log)" in out
    assert "run-databricks-job-xrc0t925" in out
    assert "node capacity" in out


def test_a_masking_error_outranks_an_empty_airflow_log() -> None:
    """INC-025's shape. The Airflow log carries no error, the layer below carries a mask. Saying
    "the worker died" here overwrites the real evidence with a confident wrong cause."""
    diag = {
        "identity": {"dag_id": "fangorn_inference_pipeline_run", "task_id": "challenger"},
        "engine": "vertex",
        "ti_state": "failed",
        "no_error_text": True,
        "spark": {
            "error_text": (
                "google.api_core.exceptions.NotFound: 404 Not found: Cluster "
                "projects/p/regions/us-central1/clusters/fangorn-challenger-a483e22d"
            )
        },
    }
    out = slack_block.render(diag, repo_paths={})
    assert "masking error" in out
    assert "audit log" in out
    assert "worker died" not in out


def test_an_upstream_stub_still_speaks_even_with_downstream_text() -> None:
    """A task that never ran cannot own a downstream error, so the guard must not swallow it."""
    diag = dict(_STUB, spark={"error_text": "some batch error from a sibling"})
    out = slack_block.render(diag, repo_paths={})
    assert "(no cause in this log)" in out
    assert "ddp_vertical_classification_api" in out


def test_a_stated_condition_outranks_the_llm() -> None:
    """A condition read off the log is evidence; a model's guess about it is not."""
    text, source = slack_block.why(_STUB, llm_cause="probably a bad credential")
    assert source == slack_block.WHY_STATED
    assert "ddp_vertical_classification_api" in text


def test_a_matched_signature_still_outranks_a_stated_condition() -> None:
    """Precedence is not reordered by the new source."""
    diag = dict(_QUOTA, ti_state="failed", no_error_text=True)
    _, source = slack_block.why(diag)
    assert source == slack_block.WHY_DETERMINISTIC


def test_the_body_stays_inside_slack_s_block_limit() -> None:
    """Slack rejects a section block over 3000 chars, so truncate rather than fail to post."""
    big = dict(_QUOTA)
    big["root_signature"] = dict(_QUOTA["root_signature"], likely_cause="x" * 5000)
    out = slack_block.render(big, repo_paths={})
    assert len(out) <= slack_block.MAX_BLOCK


def test_a_missing_deployment_url_drops_the_link_rather_than_emitting_a_404() -> None:
    """No AIRFLOW_API_BASE means no run link, not a link to nowhere."""
    saved = slack_block._ASTRO_UI
    slack_block._ASTRO_UI = ""
    try:
        assert slack_block._astro_run_url("d", "r") is None
    finally:
        slack_block._ASTRO_UI = saved


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - slack block shape tests passed")
