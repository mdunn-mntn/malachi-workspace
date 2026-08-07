"""Offline test for the local incident matcher (reads the git corpus, no network).

Run: python3 -m airflow_debugger.tests.test_incident_match  (or via pytest).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from airflow_debugger import incident_match
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


def test_malformed_corpus_line_is_skipped() -> None:
    """A truncated JSONL append (interrupted write-back) must not crash match()."""
    good = (
        '{"inc": "INC-010", "dag": "tpa_ipdsc_export", "task": "wait_ds17_src",'
        ' "signature": "GCS existence sensor hard-timeout", "verdict": "real_upstream_failure"}'
    )
    truncated = '{"inc": "INC-013", "dag": "foo", "signa'
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write(good + "\n" + truncated + "\n")
        tmp = Path(f.name)
    orig = incident_match._CORPUS
    incident_match._CORPUS = tmp
    try:
        hits = match("tpa_ipdsc_export", "wait_ds17_src", "sensor timeout")
    finally:
        incident_match._CORPUS = orig
        tmp.unlink()
    assert hits and hits[0]["inc"] == "INC-010"


def test_text_only_query_matches_long_signature() -> None:
    """No dag/task identity: full query-token overlap must still clear min_score.

    Real shape from the 2026-08-05 wait_ds17_src failed log, whose pipeline query
    is 'sensor-timeout sensor-timeout'; the old Jaccard denominator (|doc|~51)
    scored it 0.039 and returned [].
    """
    hits = match(None, None, "sensor-timeout sensor-timeout")
    assert hits, "text-only query with full token overlap returned no matches"
    assert "INC-010" in {h["inc"] for h in hits}


def test_dag_boost_does_not_crowd_out_similar_incident() -> None:
    """Same-dag records with zero text overlap must not fill top_k over the real match."""
    hits = match(
        "keyword_ddp_reporting",
        "export_spark_job",
        "Dataproc batch cancelled at TTL shuffle spill",
    )
    assert hits and hits[0]["inc"] == "INC-005"  # the actual TTL/shuffle-spill incident
    assert "INC-009" not in {h["inc"] for h in hits}  # pod eviction, dag boost alone


def test_inc012_query_tops_inc012() -> None:
    """An INC-012-shaped query ranks INC-012 first, with and without identity."""
    text = "Error listing gs augmentor_log SocketTimeoutException Read timed out"
    hits = match("materialize_mntn_select", "materialize", text)
    assert hits and hits[0]["inc"] == "INC-012"
    hits = match(None, None, text)
    assert hits and hits[0]["inc"] == "INC-012"


if __name__ == "__main__":
    test_inc009_surfaces_itself()
    test_empty_query_returns_empty()
    test_scores_sorted_descending()
    test_malformed_corpus_line_is_skipped()
    test_text_only_query_matches_long_signature()
    test_dag_boost_does_not_crowd_out_similar_incident()
    test_inc012_query_tops_inc012()
    print("OK - incident matcher tests passed")
