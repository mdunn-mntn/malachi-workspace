"""Top-level orchestrator: Airflow log -> deterministic diagnosis -> report.

Deterministic-first: the signature classifier + structural detection settle
known failures with zero LLM cost. The incident matcher always runs to attach
"similar past incidents". Only when the classifier finds nothing (no root
signature) does it call the LLM synthesizer over the evidence bundle.
"""

from __future__ import annotations

from .incident_match import match as match_incidents
from .parse import diagnose, parse_log_file
from .report import build_report


def investigate(log_path: str, use_llm: bool = True) -> dict:
    """Run the full chain on one failed-task log; return report + provenance."""
    diag = diagnose(parse_log_file(log_path))
    ident = diag.get("identity", {})
    root = diag.get("root_signature") or {}
    query = " ".join(
        filter(
            None,
            [
                root.get("sig_class"),
                diag.get("root_error"),
                (diag.get("airflow_signature") or {}).get("sig_class"),
            ],
        )
    )
    matches = match_incidents(ident.get("dag_id"), ident.get("task_id"), query)
    report = build_report(diag)

    llm_report = None
    if not root and use_llm:  # deterministic classifier found nothing -> synthesize
        from .synth import synthesize

        llm_report = synthesize(
            {
                "identity": ident,
                "engine": diag.get("engine"),
                "airflow_signature": diag.get("airflow_signature"),
                "spark": diag.get("spark"),
                "spark_outcome": diag.get("spark_outcome"),
            },
            matches,
        )
        if llm_report:
            report = llm_report

    return {
        "report": report,
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
        print("usage: python -m airflow_debugger.orchestrate <airflow_log_file> [--no-llm]")
        raise SystemExit(2)
    res = investigate(paths[0], use_llm="--no-llm" not in argv)
    print(res["report"])
    print("---")
    print(f"confidence: {res['confidence']} | llm_used: {res['llm_used']}")
    if res["similar_incidents"]:
        print(
            "similar: " + ", ".join(f"{m['inc']}({m['score']})" for m in res["similar_incidents"])
        )
