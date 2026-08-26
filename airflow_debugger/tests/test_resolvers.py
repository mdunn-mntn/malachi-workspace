"""A resolver either settles the fork or says nothing. These pin both halves.

Every fixture is text the corpus actually produced, because a resolver's whole job is reading real
error shapes and the hand-written version of an error is the one that never appears. The stop
cases matter as much as the hits: a guessed branch reads exactly like a settled one, and the
reader has no way to tell them apart.
"""

from __future__ import annotations

from airflow_debugger import resolvers

_TIMEOUT_DIAG = {
    "identity": {
        "dag_id": "vertical_classification_api",
        "task_id": "ddp_vertical_classification_api",
    },
    "root_signature": {"key": "task_execution_timeout"},
}


class _History:
    """A fake client returning one task's run durations, in seconds."""

    def __init__(self, ok: list, bad: list, timeout: float | None = 3600) -> None:
        self.rows = [{"state": "success", "duration": d} for d in ok]
        self.rows += [{"state": "failed", "duration": d, "dag_run_id": "r"} for d in bad]
        self.timeout = timeout

    def task_timeout(self, dag_id: str, task_id: str) -> float | None:
        """The task's declared execution_timeout."""
        return self.timeout

    def task_history(self, dag_id: str, task_id: str, limit: int = 100) -> list:
        """Recent instances of one task."""
        return self.rows


def test_a_timeout_that_crept_up_is_called_capacity_not_a_hang() -> None:
    """Green runs already near the budget mean the work grew into it."""
    client = _History(ok=[3300, 3200, 3250, 3100, 3000, 2900], bad=[3600])
    res = resolvers.resolve(_TIMEOUT_DIAG, "execution_timeout 3600 seconds", client)
    assert "outgrew its time limit" in res.verdict
    assert any("raise execution_timeout" in s for s in res.solutions)
    assert any("holds for about" in s for s in res.solutions)
    assert any("input row count" in s for s in res.solutions)
    assert "successful runs took" in res.evidence


def test_a_timeout_whose_green_runs_are_fast_is_called_a_hang() -> None:
    """The opposite branch, and the one where raising the timeout is the wrong move."""
    client = _History(ok=[600, 620, 590, 610, 580, 600], bad=[3600])
    res = resolvers.resolve(_TIMEOUT_DIAG, "execution_timeout 3600 seconds", client)
    assert "hung" in res.verdict
    assert any("Do not raise the time limit" in s for s in res.solutions)
    assert any("re-run it once" in s for s in res.solutions)


def test_the_new_limit_says_how_long_it_holds() -> None:
    """ "Raise it" without a horizon is the same paging again next month. The growth rate is
    known, so the answer carries how many runs the new limit survives."""
    client = _History(ok=[2050, 2040, 2020, 2000, 1980, 1950, 1930, 1920], bad=[2700], timeout=2700)
    res = resolvers.resolve(_TIMEOUT_DIAG, "execution_timeout 2700 seconds", client)
    horizon = next(s for s in res.solutions if "holds for" in s)
    assert "more runs" in horizon


def test_no_internal_vocabulary_reaches_the_reader() -> None:
    """A solution the reader has to decode is not a solution. Ban the shorthand outright."""
    for client in (
        _History(ok=[3300, 3200, 3250, 3100, 3000, 2900], bad=[3600]),
        _History(ok=[600, 620, 590, 610, 580, 600], bad=[3600]),
    ):
        res = resolvers.resolve(_TIMEOUT_DIAG, "execution_timeout 3600 seconds", client)
        blob = " ".join([res.verdict, res.evidence, *res.solutions]).lower()
        for banned in ("green median", "green runs", "cut the work", "the budget"):
            assert banned not in blob, f"{banned!r} leaked into the answer"


def test_too_little_history_settles_nothing() -> None:
    """Two green runs cannot establish a trend, and a guessed trend is worse than none."""
    assert resolvers.resolve(_TIMEOUT_DIAG, "", _History(ok=[600, 620], bad=[3600])) is None


def test_one_stale_failure_cannot_flip_the_verdict() -> None:
    """The gauntlet blocker. The limit was inferred from the longest past failure, so a single
    row from an older config read as today's budget and told on-call the opposite of the fix."""
    client = _History(ok=[1700, 1650, 1600, 1750, 1680, 1620], bad=[7200, 2700], timeout=2700)
    res = resolvers.resolve(_TIMEOUT_DIAG, "[error] task Process timed out", client)
    assert "outgrew its time limit" in res.verdict, res.verdict
    assert "45m" in res.evidence and "120m" not in res.evidence, res.evidence


def test_no_declared_limit_settles_nothing() -> None:
    """Without the real limit every number is a guess, and a guessed verdict reads as measured."""
    client = _History(ok=[1700, 1650, 1600, 1750], bad=[7200], timeout=None)
    assert resolvers.resolve(_TIMEOUT_DIAG, "[error] task Process timed out", client) is None


def test_a_missing_table_is_named_in_full() -> None:
    """`prod` is not the answer; `prod.ml.ddp_url_verticals` is. The chain has to survive."""
    diag = {"root_signature": {"key": "analysis_exception"}}
    text = (
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `prod`.`ml`.`ddp_url_verticals` cannot be "
        "found. Verify the spelling and correctness of the schema and catalog."
    )
    res = resolvers.resolve(diag, text)
    assert "prod.ml.ddp_url_verticals" in res.verdict
    assert "`prod`," not in res.verdict


def test_a_denial_quotes_the_service_rather_than_guessing_a_permission() -> None:
    """GA4 explains its own refusal; inventing an IAM permission name would be wrong."""
    diag = {"root_signature": {"key": "auth_error"}}
    text = (
        'status = StatusCode.PERMISSION_DENIED\n\tdetails = "User does not have sufficient '
        'permissions for this property. To learn more about Property ID, see the docs."'
    )
    res = resolvers.resolve(diag, text)
    assert "sufficient permissions for this property" in res.verdict
    assert "a retry cannot clear a refusal" in " ".join(res.solutions)


def test_an_expired_token_is_not_reported_as_a_missing_grant() -> None:
    """They need opposite actions: one re-runs clean, the other never will."""
    diag = {"root_signature": {"key": "auth_error"}}
    res = resolvers.resolve(diag, "401 Unauthorized: invalid_token, the token expired at noon")
    assert res is None or "expired" in res.verdict


def test_a_quota_error_reports_the_shortfall_in_numbers() -> None:
    """INC-025. The exact gap is what tells you whether something else is holding the headroom."""
    diag = {"root_signature": {"key": "quota_exhaustion"}}
    res = resolvers.resolve(
        diag, "Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0."
    )
    assert "4672" in res.verdict and "328" in res.verdict
    assert "4344" in res.verdict
    assert any("AUDI-1217" in s for s in res.solutions)


def test_a_stockout_names_the_zone_so_the_retry_moves() -> None:
    """A stockout is a zone fact; a retry into the same zone fails the same way."""
    diag = {"root_signature": {"key": "cluster_create_stockout"}}
    text = "code: 14 ... zones/us-central1-a does not have enough resources available"
    res = resolvers.resolve(diag, text)
    assert "us-central1-a" in res.verdict
    assert any("ERROR" in s for s in res.solutions)
    assert any(s.startswith("Now:") for s in res.solutions)


def test_a_dbt_failure_reports_the_exception_not_dbt_s_summary() -> None:
    """The line that matters is the deepest one, under dbt's own wrapper."""
    diag = {"root_signature": {"key": "dbt_model_runtime_error"}}
    text = (
        "Runtime Error in model ddp_url_verticals\n"
        "Traceback (most recent call last):\n"
        "ValueError: Too many signals to process 156056261 for period 2026-08-07"
    )
    res = resolvers.resolve(diag, text)
    assert "ValueError" in res.verdict
    assert "156056261" in res.verdict


def test_late_data_names_the_path_and_both_branches() -> None:
    """Landed late and never landed need opposite actions, so the post must carry both."""
    diag = {"root_signature": {"key": "path_not_found_late_data"}}
    text = "PATH_NOT_FOUND: gs://mntn-data-archive-prod/feature_store/dt=2026-08-07/_SUCCESS"
    res = resolvers.resolve(diag, text)
    assert "gs://mntn-data-archive-prod/feature_store/dt=2026-08-07" in res.verdict
    assert any("re-running this task is the whole fix" in s for s in res.solutions)
    assert any("producer" in s for s in res.solutions)


def test_every_resolver_opens_with_an_action_and_offers_alternatives() -> None:
    """One shape for every answer: do this now, then this, and here is the case where it differs.
    A single unranked instruction leaves the reader deciding what to try first."""
    cases = [
        (
            "analysis_exception",
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `a`.`b` cannot be found",
        ),
        ("auth_error", 'details = "the caller lacks access to this property and cannot read it"'),
        ("quota_exhaustion", "Insufficient 'N2_CPUS' quota. Requested 128, available 40"),
        ("cluster_create_stockout", "code: 14 zones/us-central1-a does not have enough resources"),
        ("path_not_found_late_data", "PATH_NOT_FOUND: gs://bucket/dt=2026-08-07/_SUCCESS"),
        ("dbt_model_runtime_error", "Runtime Error in model m\nValueError: bad input"),
        ("db_credential_rejected", 'password authentication failed for user "svc_bot"'),
    ]
    for key, text in cases:
        res = resolvers.resolve({"root_signature": {"key": key}}, text)
        assert res, key
        assert len(res.solutions) >= 3, f"{key}: only {len(res.solutions)} option(s)"
        assert res.solutions[0].startswith("Now:"), f"{key}: first step is not an action"
        assert res.solutions[1].startswith(("Then", "If")), f"{key}: no second step"


_PREAMBLE = "\n".join(
    f"2026-08-24T03:0{i}:00Z [info] airflow.task Impersonating service account "
    "airflow-ti-prod@mntn-prj-prod-00.iam.gserviceaccount.com using bigquery.jobs.list to poll "
    "gs://mntn-airflow-artifacts-prod/configs/feature_store/daily.yaml"
    for i in range(9)
)


def test_the_preamble_never_outranks_the_exception() -> None:
    """The gauntlet blocker. Resolvers take the FIRST regex match, and an Airflow log opens with
    thousands of INFO lines. Scanning the whole file names the wrong service account, and someone
    files a production IAM change against it."""
    log = _PREAMBLE + (
        "\n2026-08-24T03:10:00Z [error] google.api_core.exceptions.Forbidden: 403 Access Denied: "
        "User does not have bigquery.tables.updateData permission for principal "
        "ddp-exporter@mntn-prj-prod-00.iam.gserviceaccount.com"
    )
    res = resolvers.resolve({"root_signature": {"key": "auth_error"}}, log)
    assert "ddp-exporter@" in res.verdict, res.verdict
    assert "airflow-ti-prod@" not in res.verdict
    assert "bigquery.tables.updateData" in res.verdict


def test_a_config_path_in_the_preamble_is_not_the_missing_partition() -> None:
    """Same shape for late data: the yaml always exists, so naming it sends on-call to re-run
    a task that fails identically while the real partition is never mentioned."""
    log = _PREAMBLE + (
        "\n2026-08-24T03:10:00Z [error] Path does not exist: "
        "gs://mntn-data-archive-prod/feature_store/dt=2026-08-24/_SUCCESS"
    )
    res = resolvers.resolve({"root_signature": {"key": "path_not_found_late_data"}}, log)
    assert "feature_store/dt=2026-08-24" in res.verdict
    assert "daily.yaml" not in res.verdict


def test_only_the_failure_region_is_read_when_two_regions_match() -> None:
    """A retried task logs its earlier failure too. Both regions match the same signature, so only
    the window anchored on the classifier's own hit can pick the one this diagnosis is about."""
    decoy = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `old`.`gone` cannot be found.\n"
    real = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `prod`.`ml`.`live` cannot be found."
    log = decoy + ("2026-08-24T03:00:00Z [info] airflow.task heartbeat\n" * 900) + real
    diag = {"root_signature": {"key": "analysis_exception", "matched_on": "view `prod`"}}
    res = resolvers.resolve(diag, log)
    assert "prod.ml.live" in res.verdict, res.verdict
    assert "old.gone" not in res.verdict


def test_a_signature_with_no_resolver_settles_nothing() -> None:
    """Most signatures have no fork to settle; they must fall through to their own remedy."""
    assert (
        resolvers.resolve({"root_signature": {"key": "spot_preemption"}}, "was preempted") is None
    )


def test_a_resolver_that_raises_never_takes_the_diagnosis_down() -> None:
    """A resolver is an enrichment. It must never turn a working diagnosis into an exception."""

    class _Boom:
        def task_history(self, *a: object, **k: object) -> list:
            raise RuntimeError("HTTP 500")

    assert resolvers.resolve(_TIMEOUT_DIAG, "", _Boom()) is None


def test_the_rendered_solutions_are_numbered_in_order() -> None:
    """They are ranked, so they are numbered; an unordered list reads as interchangeable."""
    res = resolvers.Resolution("v", "e", ["first", "second"])
    why, how = resolvers.as_lines(res)
    assert why == "v (e)"
    assert how == "1. first 2. second"


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - resolver tests passed")
