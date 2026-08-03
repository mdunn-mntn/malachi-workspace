"""In-callback first-look extraction from an Airflow task Context.

This is the key-free, network-free tier of the Phase-3 in-DAG auto-fire callback
(design: tickets/audi_1191.../artifacts/audi_1191_indag_callback_design.md). It
turns an Airflow `on_failure_callback` Context into the same `ParsedFailure` the
log parser produces - identity, operator->engine, and the Airflow-log signature
from the exception text - WITHOUT any Airflow import or cloud call, so the prod
callback stays a fast no-op-safe closure and this stays offline-testable.

Engine correlation (batch_id / dbx run_id) is deliberately NOT done here; that
needs the full log body / cloud APIs and belongs in the off-worker deep tier.
"""

from __future__ import annotations

from typing import Any

from .parse import _DATABRICKS_OPS, _DATAPROC_OPS, ParsedFailure
from .signatures import classify


def _attr(obj: Any, name: str) -> Any:
    """Read `name` from an object (attribute) or a mapping (key); None if absent."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _engine_for(operator: str | None) -> str:
    """Map an operator class name to a Spark engine (mirrors parse.parse_log)."""
    if not operator:
        return "unknown"
    if any(o in operator for o in _DATABRICKS_OPS):
        return "databricks"
    if any(o in operator for o in _DATAPROC_OPS):
        return "dataproc"
    # Any other concrete operator/sensor class is a non-Spark task; we still classify
    # its exception, just without engine correlation.
    return "other"


def parse_context(ctx: dict) -> ParsedFailure:
    """Build a first-look ParsedFailure from an Airflow task Context.

    Accepts the real Airflow Context or any Context-shaped mapping (so it is
    testable with plain objects). `ctx["task"]` is the operator instance - its
    class name drives engine routing.
    """
    ti = ctx.get("task_instance") or ctx.get("ti")
    dag_run = ctx.get("dag_run")
    task = ctx.get("task")

    operator = type(task).__name__ if task is not None else None
    exc = ctx.get("exception")
    exc_text = str(exc) if exc else ""

    try_number = _attr(ti, "try_number")
    p = ParsedFailure(
        dag_id=_attr(ti, "dag_id"),
        task_id=_attr(ti, "task_id"),
        run_id=_attr(dag_run, "run_id") or _attr(ti, "run_id"),
        try_number=int(try_number) if try_number is not None else None,
        operator=operator,
        engine=_engine_for(operator),
    )
    asig = classify(exc_text)
    p.airflow_signature = _asdict_match(asig) if asig else None
    if _attr(ti, "log_url"):
        p.notes.append(f"log_url={_attr(ti, 'log_url')}")
    return p


def _asdict_match(m: Any) -> dict:
    """Match -> dict (kept local so this module has no dataclasses import cost)."""
    return {
        "key": m.key,
        "sig_class": m.sig_class,
        "likely_cause": m.likely_cause,
        "programmatic_fix": m.programmatic_fix,
        "matched_on": m.matched_on,
    }


def is_final_attempt(ctx: dict) -> bool:
    """True only on the last retry, so a retried-then-succeeded task doesn't fire."""
    ti = ctx.get("task_instance") or ctx.get("ti")
    tn = _attr(ti, "try_number")
    mt = _attr(ti, "max_tries")
    if tn is None:
        return True
    return int(tn) > int(mt or 0)
