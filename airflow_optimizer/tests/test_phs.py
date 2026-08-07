"""PHS enumeration: the PHS-attached SUCCEEDED filter and per-uuid log-path derivation."""

from __future__ import annotations

from airflow_optimizer.phs import log_uri, phs_succeeded

_PHS = {"sparkHistoryServerConfig": {"dataprocCluster": "projects/x/regions/y/clusters/phs"}}


def _batch(state: str = "SUCCEEDED", phs: bool = True, uuid: str = "u-1") -> dict:
    return {
        "name": f"projects/p/locations/r/batches/b-{uuid}",
        "state": state,
        "uuid": uuid,
        "environmentConfig": {"peripheralsConfig": _PHS if phs else {}},
    }


def test_phs_succeeded_filters_state_phs_and_uuid() -> None:
    """Only SUCCEEDED + PHS-attached + uuid-bearing batches survive the filter."""
    batches = [
        _batch(),
        _batch(state="FAILED"),
        _batch(phs=False, uuid="u-2"),
        {"state": "SUCCEEDED", "environmentConfig": {"peripheralsConfig": _PHS}},  # no uuid
    ]
    kept = phs_succeeded(batches)
    assert [b["uuid"] for b in kept] == ["u-1"]


def test_log_uri_is_per_uuid_spark_job_history() -> None:
    """The log path is gs://<temp-bucket>/<uuid>/spark-job-history."""
    uri = log_uri(_batch(uuid="5696be37-x"), bucket="tb")
    assert uri == "gs://tb/5696be37-x/spark-job-history"
