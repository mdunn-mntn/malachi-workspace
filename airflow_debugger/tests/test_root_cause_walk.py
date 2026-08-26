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

_TIMEOUT_LOG = "[2026-08-25T05:38:00Z] ERROR - [error] task Process timed out"


class _Api:
    """Minimal stand-in for the on-call puller: only what the walk actually calls."""

    def __init__(self, tis: dict, logs: dict) -> None:
        self.tis, self.logs = tis, logs
        self.fetched: list[str] = []

    def resolve_bearer(self, explicit: str | None = None) -> str:
        return "tok"

    def list_task_instances_in_run(self, base: str, token: str, dag_id: str, run_id: str) -> list:
        return self.tis.get((dag_id, run_id), [])

    def expand_tries(self, base: str, token: str, ti: dict) -> list:
        return [ti]

    def fetch_log(self, base: str, token: str, ti: dict) -> str:
        self.fetched.append(ti["task_id"])
        return self.logs.get(ti["task_id"], "")

    def day_window(self, date_str: str) -> tuple:
        return f"{date_str}T00:00:00Z", f"{date_str}T23:59:59Z"

    def list_runs_for_day(self, base: str, token: str, dag_id: str, start: str, end: str) -> list:
        return []


def _run(api: _Api, diag: dict | None = None, **kw: object) -> dict:
    with (
        mock.patch.object(walker, "_api", lambda: api),
        mock.patch.object(walker, "_resolve_base", lambda: "https://x/api/v2"),
    ):
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
    assert "1 other task(s) failed" in walker.chain_text(out)


def test_a_walk_that_reaches_nothing_says_why() -> None:
    """Silence reads as "there was nothing upstream", which is a different fact."""
    api = _Api({("vertical_classification_api", _STUB["identity"]["run_id"]): []}, {})
    out = _run(api)
    assert out["root"] is None
    assert "no failed task in this run" in out["note"]
    assert "stopped" in walker.chain_text(out)


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


def test_an_empty_upstream_log_keeps_walking() -> None:
    """A stub pointing at a stub is the case one hop was built to miss."""
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
    assert [h["task_id"] for h in out["hops"]] == ["empty_a", "empty_b"]
    assert out["root"] is None
    assert "hop limit" in out["note"]


def test_an_api_failure_is_reported_not_raised() -> None:
    """A walk failure must never take the diagnosis that produced it down with it."""

    class _Dead(_Api):
        def list_task_instances_in_run(self, *a: object, **k: object) -> list:
            raise RuntimeError("HTTP 403")

    out = _run(_Dead({}, {}))
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
