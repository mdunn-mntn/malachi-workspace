"""Offline tests for the in-callback first-look extraction (no Airflow, no network).

Proves the Phase-3 callback contract: an Airflow Context -> ParsedFailure with the
right engine + Airflow-log signature, computed with zero network.
"""

from __future__ import annotations

from airflow_debugger.context_parse import is_final_attempt, parse_context


class _TI:
    """Stand-in for an Airflow TaskInstance (attribute access, like the real one)."""

    def __init__(self, **kw: object) -> None:
        self.__dict__.update(kw)


class _DagRun:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id


# Operator stand-ins: only the CLASS NAME matters for engine routing.
class DbxDbtOperator:  # noqa: D101
    pass


class ModelPysparkBatchOperator:  # noqa: D101
    pass


class ExternalTaskSensor:  # noqa: D101
    pass


def _ctx(operator: object, exc: str, **ti_kw: object) -> dict:
    base: dict = {
        "dag_id": "keyword_ddp_reporting",
        "task_id": "write_targeted_signal_ds_19",
        "try_number": 2,
        "max_tries": 1,
        "map_index": -1,
        "log_url": "http://af/log",
    }
    base.update(ti_kw)
    ti = _TI(**base)
    return {
        "task_instance": ti,
        "dag_run": _DagRun("scheduled__2026-08-03"),
        "task": operator,
        "exception": exc,
    }


def test_databricks_engine_and_signature() -> None:
    """A DbxDbtOperator pod-evict failure -> databricks engine + pod_evicted_404."""
    ctx = _ctx(DbxDbtOperator(), "ApiException: (404) ... pods 'x' not found during istio check")
    p = parse_context(ctx)
    assert p.engine == "databricks"
    assert p.dag_id == "keyword_ddp_reporting"
    assert p.run_id == "scheduled__2026-08-03"
    assert p.airflow_signature and p.airflow_signature["key"] == "pod_evicted_404"


def test_dataproc_engine() -> None:
    """A ModelPysparkBatchOperator TTL failure -> dataproc engine + ttl_exceeded."""
    ctx = _ctx(ModelPysparkBatchOperator(), "Batch was cancelled as ttl exceeded")
    p = parse_context(ctx)
    assert p.engine == "dataproc"
    assert p.airflow_signature and p.airflow_signature["key"] == "ttl_exceeded"


def test_sensor_routes_to_other_but_still_classifies() -> None:
    """A sensor failure has no Spark engine, but the Airflow-log signature still fires."""
    ctx = _ctx(ExternalTaskSensor(), "airflow.exceptions.AirflowSensorTimeout: Snap. Time is up.")
    p = parse_context(ctx)
    assert p.engine == "other"
    assert p.airflow_signature and p.airflow_signature["key"] == "sensor_timeout"


def test_final_attempt_gate() -> None:
    """is_final_attempt is True on the last try, False when retries remain."""
    assert is_final_attempt(_ctx(ExternalTaskSensor(), "x", try_number=2, max_tries=1)) is True
    assert is_final_attempt(_ctx(ExternalTaskSensor(), "x", try_number=1, max_tries=1)) is False


def test_no_exception_is_safe() -> None:
    """Missing exception text yields no signature, no crash."""
    ctx = _ctx(ModelPysparkBatchOperator(), "")
    p = parse_context(ctx)
    assert p.airflow_signature is None
    assert p.engine == "dataproc"


if __name__ == "__main__":
    test_databricks_engine_and_signature()
    test_dataproc_engine()
    test_sensor_routes_to_other_but_still_classifies()
    test_final_attempt_gate()
    test_no_exception_is_safe()
    print("OK - context_parse first-look tests passed")
