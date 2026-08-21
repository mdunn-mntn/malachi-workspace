"""Coverage-pass auth: the container has no astro CLI, so the injected token has to win."""

from __future__ import annotations

from typing import Any, NoReturn

import pytest

from airflow_optimizer import coverage


def _explode(*_a: Any, **_k: Any) -> NoReturn:
    raise AssertionError("shelled out to the astro CLI despite an injected token")


def test_bearer_prefers_the_injected_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ExternalSecret value is the only token that exists in the CronJob."""
    monkeypatch.setenv("AIRFLOW_TI_API_TOKEN", "  tok-123  ")
    monkeypatch.setattr(coverage.subprocess, "run", _explode)
    assert coverage._bearer() == "tok-123"


def test_bearer_names_what_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A blank env var must not fall through to a stack trace about a missing file."""
    monkeypatch.setenv("AIRFLOW_TI_API_TOKEN", "   ")
    monkeypatch.setattr(coverage.os.path, "exists", lambda _p: False)
    with pytest.raises(RuntimeError, match="AIRFLOW_TI_API_TOKEN"):
        coverage._bearer()
