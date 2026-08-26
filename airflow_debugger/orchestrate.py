"""Top-level orchestrator: Airflow log -> deterministic diagnosis -> report.

Deterministic-first: the signature classifier + structural detection settle
known failures with zero LLM cost. The incident matcher always runs to attach
"similar past incidents". Only when the classifier finds nothing (no root
signature) does it call the LLM synthesizer over the evidence bundle.
"""

from __future__ import annotations

import logging

from .incident_match import match as match_incidents
from .parse import diagnose, parse_log_file
from .report import build_report, build_troubleshooting

logger = logging.getLogger(__name__)

_LOG_TAIL_CHARS = 4000  # raw log tail given to the LLM when signatures found nothing


_RESOLVER_MAX_CHARS = 4_000_000


def investigate(
    log_path: str,
    use_llm: bool = True,
    profile_perf: bool = True,
    run_id: str | None = None,
) -> dict:
    """Run the full chain on one failed-task log; return report + provenance.

    The caller passes run_id when it has one. An upstream_failed stub carries no identity in its
    body and the daily sweep writes logs to a flat directory, so without it the walk has nothing
    to look up and the whole upstream half goes dark in production.
    """
    parsed = parse_log_file(log_path)
    if run_id and not parsed.run_id:
        parsed.run_id = run_id
    diag = diagnose(parsed)
    try:
        from .root_cause_walk import walk

        walked = walk(diag, on_date=parsed.log_date)
    except Exception:
        logger.warning("upstream walk failed", exc_info=True)
        walked = None
    if walked:
        diag["upstream_walk"] = walked
    try:
        from dataclasses import asdict

        from .resolvers import resolve
        from .root_cause_walk import Client

        with open(log_path, encoding="utf-8", errors="replace") as f:
            # The whole log, not the tail: the line that settles a fork can sit 400 KB from the end.
            whole = f.read()[:_RESOLVER_MAX_CHARS]
        client = Client()
        res = resolve(diag, whole, client)
        if not res:
            # The walked root carries its own signature, and it is the one the reader acts on.
            root = (walked or {}).get("root") or {}
            if root.get("signature"):
                res = resolve(
                    {
                        "identity": {"dag_id": root["dag_id"], "task_id": root["task_id"]},
                        "root_signature": root["signature"],
                    },
                    whole,
                    client,
                )
    except Exception:
        # Silence here hid a NameError that killed every resolver in production.
        logger.warning("resolver failed", exc_info=True)
        res = None
    if res:
        diag["resolution"] = asdict(res)
    if profile_perf:  # perf-shaped failures only (IMP-032); never on other classes
        try:
            from .perf_profile import profile

            perf = profile(diag)
        except Exception:
            perf = None
        if perf:
            diag["perf_profile"] = perf
    ident = diag.get("identity", {})
    root = diag.get("root_signature") or {}
    spark = diag.get("spark") or {}
    # dataproc bundles carry error_text/state_message, not root_error
    root_error = diag.get("root_error") or spark.get("error_text") or spark.get("state_message")
    query = " ".join(
        filter(
            None,
            [
                root.get("sig_class"),
                root_error,
                (diag.get("airflow_signature") or {}).get("sig_class"),
            ],
        )
    )
    try:
        matches = match_incidents(ident.get("dag_id"), ident.get("task_id"), query)
    except Exception:  # a matcher crash degrades to no matches, never kills the diagnosis
        matches = []
    report = build_report(diag)

    llm_report = llm_note = None
    if not root and use_llm:  # deterministic classifier found nothing -> synthesize
        from .synth import synthesize

        with open(log_path, encoding="utf-8", errors="replace") as f:
            log_tail = f.read()[-_LOG_TAIL_CHARS:]
        llm_report, llm_note = synthesize(
            {
                "identity": ident,
                "engine": diag.get("engine"),
                "airflow_signature": diag.get("airflow_signature"),
                "spark": diag.get("spark"),
                "spark_outcome": diag.get("spark_outcome"),
                "root_error": root_error,
                "parse_notes": parsed.notes,
                "log_tail": log_tail,
            },
            matches,
        )
        if llm_report:
            report = llm_report
    if llm_note:
        diag["llm_note"] = llm_note

    return {
        "report": report,
        "troubleshooting": build_troubleshooting(diag, matches),
        "confidence": "high" if root else ("llm" if llm_report else "low"),
        "diagnosis": diag,
        "similar_incidents": matches,
        "llm_used": bool(llm_report),
    }


if __name__ == "__main__":
    import sys

    argv = sys.argv[1:]
    paths = [a for a in argv if not a.startswith("-")]  # flag-order robust
    if not paths:
        print(
            "usage: python -m airflow_debugger.orchestrate <airflow_log_file>"
            " [--no-llm] [--troubleshoot]"
        )
        raise SystemExit(2)
    res = investigate(paths[0], use_llm="--no-llm" not in argv)
    if "--troubleshoot" in argv:
        print(res["troubleshooting"])
    else:
        print(res["report"])
        print("---")
        print(f"confidence: {res['confidence']} | llm_used: {res['llm_used']}")
        if res["similar_incidents"]:
            print(
                "similar: "
                + ", ".join(f"{m['inc']}({m['score']})" for m in res["similar_incidents"])
            )
