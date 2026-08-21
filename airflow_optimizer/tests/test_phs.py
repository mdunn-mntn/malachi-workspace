"""PHS enumeration: the PHS-attached SUCCEEDED filter and per-uuid log-path derivation."""

from __future__ import annotations

import glob
import os
from collections.abc import Callable
from pathlib import Path

import pytest

from airflow_optimizer import phs
from airflow_optimizer.crawl import _event_logs
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


def _fake_download(layout: dict) -> Callable[..., object]:
    """subprocess.run stand-in that materialises `layout` (relpath -> content) in the dest dir."""
    calls: list[list[str]] = []

    class _Result:
        returncode = 0

    def run(cmd: list[str], **_kw: object) -> _Result:
        calls.append(cmd)
        dest = cmd[-1]
        for rel, body in layout.items():
            path = os.path.join(dest, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write(body)
        return _Result()

    run.calls = calls  # type: ignore[attr-defined]
    return run


def test_fetch_copies_recursively(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without -r a rolling eventlog_v2_* dir is skipped and the batch reads empty."""
    run = _fake_download({"app-1.zstd": "x"})
    monkeypatch.setattr(phs.subprocess, "run", run)

    phs.fetch_logs([_batch()], str(tmp_path))

    assert "-r" in run.calls[0]  # type: ignore[attr-defined]


def test_fetch_strips_top_markers_so_the_uuid_dir_is_not_one_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A top-level appstatus_* beside two logs would merge them into one chimera job."""
    monkeypatch.setattr(phs.subprocess, "run", _fake_download({
        "appstatus_app-1": "",
        "app-1.zstd": "x",
        "eventlog_v2_batch-u-1/appstatus_app-2": "",
        "eventlog_v2_batch-u-1/events_1_app-2": "y",
    }))

    got = phs.fetch_logs([_batch()], str(tmp_path))

    local = got[0]
    assert not glob.glob(os.path.join(local, "appstatus_*"))
    # the rolling dir keeps its own marker - that is what makes it one log
    assert glob.glob(os.path.join(local, "eventlog_v2_*", "appstatus_*"))
    assert sorted(os.path.basename(p) for p in _event_logs([local])) == [
        "app-1.zstd", "eventlog_v2_batch-u-1",
    ]


def test_fetch_leaves_no_empty_dir_for_an_unreachable_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A 403 (grant pending) must not litter dest with empty dirs the crawl then scans."""
    class _Denied:
        returncode = 1

    monkeypatch.setattr(phs.subprocess, "run", lambda *a, **k: _Denied())

    assert phs.fetch_logs([_batch()], str(tmp_path)) == []
    assert list(tmp_path.iterdir()) == []


def test_rolling_parts_keep_their_batch_dir(tmp_path: Path) -> None:
    """Flattened, every events_* part reads as one merged job and standalone logs are lost."""
    from airflow_optimizer import fetch

    root = str(tmp_path)
    assert fetch.dest_for(root, "gs://b/p/app-1.zstd") == root
    assert fetch.dest_for(root, "gs://b/p/eventlog_v2_batch-x/events_1_batch-x.zstd") == \
        str(tmp_path / "eventlog_v2_batch-x")


def test_newest_logs_takes_the_tail_and_drops_inprogress(monkeypatch) -> None:  # noqa: ANN001
    """Budget spent on an .inprogress log is budget not spent on a readable one."""
    from airflow_optimizer import fetch

    listing = (
        "  100  2026-08-19T01:00:00Z  gs://b/p/old.zstd\n"
        "  100  2026-08-21T01:00:00Z  gs://b/p/new.zstd\n"
        "  100  2026-08-21T02:00:00Z  gs://b/p/newest.zstd.inprogress\n"
    )
    monkeypatch.setattr(fetch.subprocess, "run",
                        lambda *a, **k: type("R", (), {"stdout": listing, "returncode": 0})())
    assert fetch.newest_logs("gs://b/p", 2) == ["gs://b/p/old.zstd", "gs://b/p/new.zstd"]
    assert fetch.newest_logs("gs://b/p", 1) == ["gs://b/p/new.zstd"]
