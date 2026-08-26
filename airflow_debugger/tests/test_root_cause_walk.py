"""The walk exists to end the "diagnose the upstream task" hand-off, so that is what these pin.

Every case runs against a fake Airflow API rather than the live one: the behaviour under test is
which task the walk lands on and what it says when it cannot land anywhere, and neither depends on
the network. The dangerous failure is a confident wrong root, so most of these assert the stop.
"""

from __future__ import annotations

from unittest import mock

from airflow_debugger import root_cause_walk as walker

_STUB = {
    "identity": {
        "dag_id": "vertical_classification_api",
        "task_id": "response_tests",
        "run_id": "scheduled__2026-08-25T05:00:00+00:00",
    },
    "ti_state": "upstream_failed",
    "no_error_text": True,
}

_DEFAULT_GRAPH = {
    "ddp_vertical_classification_api": ["response_tests"],
    "first_to_break": ["also_broke"],
    "also_broke": ["response_tests"],
    "late_consequence": ["response_tests"],
    "odd_task": ["response_tests"],
    "empty_a": ["empty_b"],
    "empty_b": ["response_tests"],
    "response_tests": [],
}

_TIMEOUT_LOG = "[2026-08-25T05:38:00Z] ERROR - [error] task Process timed out"


class _Api:
    """A fake Client. Patching the seam keeps this file identical in the workspace and the bundle,
    which hold two different REST clients behind the same four methods."""

    def __init__(self, tis: dict, logs: dict, graph: dict | None = None) -> None:
        self.tis, self.logs = tis, logs
        self.fetched: list[str] = []
        # Default edge: everything feeds response_tests, which is the shape most cases want.
        self.graph = graph if graph is not None else _DEFAULT_GRAPH

    def task_graph(self, dag_id: str) -> dict:
        """task_id -> downstream task ids."""
        return self.graph

    def tis_in_run(self, dag_id: str, run_id: str) -> list:
        """Every task instance in one run."""
        return self.tis.get((dag_id, run_id), [])

    def failed_try(self, ti: dict) -> dict:
        """The try that actually failed."""
        return ti

    def log_text(self, ti: dict) -> str:
        """One task instance's log."""
        self.fetched.append(ti["task_id"])
        return self.logs.get(ti["task_id"], "")

    def find_run(
        self, dag_id: str, task_id: str, on_date: str | None, ti_state: str | None
    ) -> str | None:
        """The run this task ran in, when only its day is known."""
        return None


def _run(api: _Api, diag: dict | None = None, **kw: object) -> dict:
    with mock.patch.object(walker, "_CLIENT", lambda: api):
        return walker.walk(diag or _STUB, **kw)


def _ti(task_id: str, state: str = "failed", start: str = "2026-08-25T05:10:00Z") -> dict:
    return {
        "dag_id": "vertical_classification_api",
        "dag_run_id": "scheduled__2026-08-25T05:00:00+00:00",
        "task_id": task_id,
        "state": state,
        "start_date": start,
    }


def test_it_reaches_the_task_that_actually_raised() -> None:
    """The 39-log case: the stub's verdict was a pointer, and this follows it to the exception."""
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("ddp_vertical_classification_api"),
                _ti("response_tests", state="upstream_failed"),
            ]
        },
        {"ddp_vertical_classification_api": _TIMEOUT_LOG},
    )
    out = _run(api)
    assert out["root"]["task_id"] == "ddp_vertical_classification_api"
    assert out["root"]["signature"]["key"] == "task_execution_timeout"
    assert out["root"]["signature"]["remedy"]


def test_the_earliest_failure_is_the_root_not_the_last_one_listed() -> None:
    """Later failures are consequences. Picking by list order lands on a symptom."""
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("late_consequence", start="2026-08-25T06:00:00Z"),
                _ti("first_to_break", start="2026-08-25T05:01:00Z"),
            ]
        },
        {"first_to_break": _TIMEOUT_LOG, "late_consequence": _TIMEOUT_LOG},
    )
    out = _run(api)
    assert out["root"]["task_id"] == "first_to_break"
    assert api.fetched == ["first_to_break"]


def test_other_failures_in_the_run_are_reported_not_dropped() -> None:
    """One root plus "3 others failed" is a different incident from one root alone."""
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("first_to_break", start="2026-08-25T05:01:00Z"),
                _ti("also_broke", start="2026-08-25T05:02:00Z"),
            ]
        },
        {"first_to_break": _TIMEOUT_LOG},
    )
    out = _run(api)
    assert out["hops"][0]["siblings"] == ["also_broke"]


def test_a_walk_that_reaches_nothing_says_why() -> None:
    """Silence reads as "there was nothing upstream", which is a different fact."""
    api = _Api({("vertical_classification_api", _STUB["identity"]["run_id"]): []}, {})
    out = _run(api)
    assert out["root"] is None
    assert "no failed task in this run" in out["note"]


def test_it_does_not_walk_a_task_that_carries_its_own_error() -> None:
    """A task with a real exception is already the root; walking past it invents a cause."""
    assert walker.walk({"identity": {"dag_id": "d", "task_id": "t"}, "ti_state": "failed"}) is None


def test_a_log_with_no_signature_still_ends_the_walk_on_its_text() -> None:
    """An unclassified exception is still the root. Only an EMPTY log is a pointer."""
    api = _Api(
        {("vertical_classification_api", _STUB["identity"]["run_id"]): [_ti("odd_task")]},
        {"odd_task": "Traceback: something nobody has a signature for"},
    )
    out = _run(api)
    assert out["root"]["task_id"] == "odd_task"
    assert "nobody has a signature" in out["root"]["error"]


def test_an_empty_upstream_log_stops_and_says_so() -> None:
    """The ancestor set already spans the chain, so the earliest failed ancestor is the deepest.
    When its log is empty there is nothing further upstream, and that is the honest answer."""
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("empty_a", start="2026-08-25T05:01:00Z"),
                _ti("empty_b", start="2026-08-25T05:02:00Z"),
            ]
        },
        {"empty_a": "", "empty_b": ""},
    )
    out = _run(api, max_hops=2)
    assert [h["task_id"] for h in out["hops"]] == ["empty_a"]
    assert out["root"] is None
    assert "no FAILED task is upstream" in out["note"]


def test_a_failure_in_a_parallel_branch_is_never_called_the_root() -> None:
    """The gauntlet blocker. Two independent branches fail; only one feeds this task. Start time
    does not prove an edge, and naming the wrong one sends on-call at a fault that is not theirs."""
    graph = {"branch_a_load": [], "branch_b_transform": ["response_tests"], "response_tests": []}
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("branch_a_load", start="2026-08-25T05:01:00Z"),
                _ti("branch_b_transform", start="2026-08-25T05:20:00Z"),
            ]
        },
        {"branch_a_load": _TIMEOUT_LOG, "branch_b_transform": _TIMEOUT_LOG},
        graph,
    )
    out = _run(api)
    assert out["root"]["task_id"] == "branch_b_transform"
    assert api.fetched == ["branch_b_transform"]


def test_no_dag_structure_means_no_root_cause_claim() -> None:
    """Without the edges the claim cannot be proved, and an unproved root reads identically."""
    api = _Api(
        {("vertical_classification_api", _STUB["identity"]["run_id"]): [_ti("something")]},
        {"something": _TIMEOUT_LOG},
        {},
    )
    out = _run(api)
    assert out["root"] is None
    assert "DAG structure is unavailable" in out["note"]


def test_failures_with_no_edge_to_this_task_are_reported_not_claimed() -> None:
    """A run can fail tasks that have nothing to do with this one. Say so; do not pick one."""
    graph = {"unrelated": [], "response_tests": []}
    api = _Api(
        {("vertical_classification_api", _STUB["identity"]["run_id"]): [_ti("unrelated")]},
        {"unrelated": _TIMEOUT_LOG},
        graph,
    )
    out = _run(api)
    assert out["root"] is None
    assert "no FAILED task is upstream" in out["note"]
    assert "unrelated" in out["note"]


def test_a_grandparent_counts_as_upstream() -> None:
    """The edge is transitive; only a direct-parent check would miss the real root."""
    graph = {"grandparent": ["parent"], "parent": ["response_tests"], "response_tests": []}
    api = _Api(
        {
            ("vertical_classification_api", _STUB["identity"]["run_id"]): [
                _ti("grandparent", start="2026-08-25T05:01:00Z"),
            ]
        },
        {"grandparent": _TIMEOUT_LOG},
        graph,
    )
    assert _run(api)["root"]["task_id"] == "grandparent"


def _sensor(state: str, states: dict) -> dict:
    return {
        "identity": {"dag_id": "consumer", "task_id": "wait_for_producer"},
        "spark": {
            "engine": "external_task",
            "dag_id": "producer",
            "run_id": "scheduled__2026-08-25T05:00:00+00:00",
            "task_ids": list(states),
            "state": state,
            "states": states,
        },
    }


def test_a_sensor_whose_target_succeeded_is_not_walked() -> None:
    """The gauntlet blocker. A green target means the SENSOR is wrong, not the target. Walking
    anyway reads a green log and prints "Fix <task>" under the signature's own "do not backfill"."""
    assert _run(_Api({}, {}), _sensor("success", {"a_ok": "success"})) is None


def test_a_skipped_target_is_not_walked_either() -> None:
    """A by-design skip has its own remedy, and it is the opposite of "fix that task"."""
    assert _run(_Api({}, {}), _sensor("skipped", {"a_ok": "skipped"})) is None


def test_the_failed_target_is_followed_not_the_first_one_poked() -> None:
    """A sensor pokes several tasks. The one to follow is the one holding the failure."""
    api = _Api({}, {"b_failed": _TIMEOUT_LOG, "a_ok": "Marking task as SUCCESS"})
    out = _run(api, _sensor("failed", {"a_ok": "success", "b_failed": "failed"}))
    assert out["root"]["task_id"] == "b_failed"
    assert api.fetched == ["b_failed"]


def test_an_api_failure_is_reported_not_raised() -> None:
    """A walk failure must never take the diagnosis that produced it down with it."""

    class _Dead(_Api):
        def tis_in_run(self, *a: object, **k: object) -> list:
            raise RuntimeError("HTTP 403")

    out = _run(_Dead({("vertical_classification_api", _STUB["identity"]["run_id"]): []}, {}))
    assert out["root"] is None
    assert "403" in out["note"]


def test_no_run_id_and_no_date_stops_rather_than_guessing() -> None:
    """A stub's filename carries its day, not its run. Guessing the run reports another failure."""
    diag = {
        "identity": {"dag_id": "tpa_ipdsc_export", "task_id": "trigger_crm_match_rate"},
        "ti_state": "upstream_failed",
        "no_error_text": True,
    }
    out = _run(_Api({}, {}), diag)
    assert out["root"] is None
    assert "could not identify the run" in out["note"]


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - upstream walk tests passed")
