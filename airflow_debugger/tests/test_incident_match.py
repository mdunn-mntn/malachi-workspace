"""Offline test for the local incident matcher (reads the git corpus, no network).

Run: python3 -m airflow_debugger.tests.test_incident_match  (or via pytest).
"""

from __future__ import annotations

from airflow_debugger.incident_match import match


def test_inc009_surfaces_itself() -> None:
    """An INC-009-shaped query ranks INC-009 first (dag+task boost + overlap)."""
    hits = match(
        "keyword_ddp_reporting",
        "write_targeted_signal_ds_19",
        "orchestration pod evicted 404 targeted_signal",
    )
    assert hits, "expected at least one match"
    assert hits[0]["inc"] == "INC-009"
    assert hits[0]["score"] > 0.5  # exact dag+task boost dominates


def test_empty_query_returns_empty() -> None:
    """No query tokens yields no matches."""
    assert match(None, None, "") == []


def test_scores_sorted_descending() -> None:
    """Results are ranked by score, highest first."""
    hits = match("tpa_mntn_id_export", "tpa_mntn_id_export", "cancelled ttl shuffle spill")
    scores = [h["score"] for h in hits]
    assert scores == sorted(scores, reverse=True)


if __name__ == "__main__":
    test_inc009_surfaces_itself()
    test_empty_query_returns_empty()
    test_scores_sorted_descending()
    print("OK - incident matcher tests passed")
