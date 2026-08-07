"""Offline tests for the troubleshooting package (IMP-030).

Run: python3 -m airflow_debugger.tests.test_report  (or via pytest).
"""

from __future__ import annotations

from airflow_debugger.report import build_troubleshooting, code_links

_REPO = {
    "dsid30_augmentor_log_processing.py": "spark/fpa/dsid30_augmentor_log_processing.py",
    "materialize_mntn_select.py": "spark/data_source/materialize_mntn_select.py",
    "dataproc.py": "utils_runner/dataproc.py",
}

# INC-013-shaped diagnosis: dataproc traceback + gcs_list_timeout signature.
_DIAG = {
    "identity": {
        "dag_id": "fpa_site_visit_batch_serverless",
        "task_id": "dsid30_augmentor_log_processing",
    },
    "engine": "dataproc",
    "batch_id": "fpa-dsid30-20260807-20260807t130000-7149",
    "root_error": (
        'Traceback (most recent call last):  File "/var/dataproc/tmp/srvls-batch-292f6a06/'
        'dsid30_augmentor_log_processing.py", line 30, in <module>    '
        'df = spark.read.option("basePath", AUGMENTOR_LOG_BASE).parquet(input_path) '
        "java.io.IOException: Error listing gs://mntn-data-archive-prod/augmentor_log/region= "
        "SocketTimeoutException: Read timed out"
    ),
    "root_signature": {
        "key": "gcs_list_timeout",
        "sig_class": "transient-infra/gcs-listing",
        "likely_cause": "Driver-side GCS listing of a huge prefix timed out during input discovery.",
        "programmatic_fix": "sometimes",
        "matched_on": "Error listing gs://",
    },
}

_MATCHES = [
    {
        "inc": "INC-013",
        "verdict": "transient_infra",
        "score": 0.847,
        "signature": "GCS list timeout: augmentor_log full-prefix glob + basePath root stat",
        "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1179",
        "fix_files": [
            "spark/fpa/dsid30_augmentor_log_processing.py",
            "spark/auction_log_augmentor_process_gcs.py",
        ],
    },
    {
        "inc": "INC-008",
        "verdict": "transient_infra",
        "score": 0.082,
        "fix_pr": None,
        "fix_files": None,
    },
]


def test_code_links_maps_traceback_to_repo() -> None:
    """The failing script's traceback frame becomes a repo link with its line number."""
    links = code_links(_DIAG, repo_paths=_REPO)
    assert links == [
        "https://github.com/SteelHouse/airflow-ti/blob/main/spark/fpa/dsid30_augmentor_log_processing.py#L30"
    ]


def test_code_links_skips_framework_frames() -> None:
    """site-packages / /usr frames never produce links, even when basenames collide."""
    diag = {
        "root_error": (
            'File "/usr/local/lib/python3.12/site-packages/airflow/providers/google/cloud/'
            'operators/dataproc.py", line 2458, in execute'
        )
    }
    assert code_links(diag, repo_paths=_REPO) == []


def test_troubleshooting_carries_known_fix_pr() -> None:
    """A high-score match with fix_pr yields the ready-to-go PR + fix-file links."""
    out = build_troubleshooting(_DIAG, _MATCHES, repo_paths=_REPO)
    assert "Problem" in out and "Solution" in out and "Code" in out
    assert "Known fix: https://github.com/SteelHouse/airflow-ti/pull/1179 (INC-013" in out
    assert "spark/fpa/dsid30_augmentor_log_processing.py#L30" in out
    assert "auction_log_augmentor_process_gcs.py (fixed by INC-013)" in out
    assert (
        out.count("dsid30_augmentor_log_processing.py#L30") == 1
    )  # fix_files dedupes vs traceback link


def test_low_score_fix_pr_not_claimed() -> None:
    """A weak match must not present its PR as the known fix."""
    weak = [
        {
            "inc": "INC-012",
            "score": 0.2,
            "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1177",
        }
    ]
    out = build_troubleshooting(_DIAG, weak, repo_paths=_REPO)
    assert "Known fix" not in out
    assert "Code fix possible" in out  # falls back to the signature's fix flag


def test_unclassified_diag_still_produces_package() -> None:
    """No signature, no matches: sections still render without crashing."""
    out = build_troubleshooting({"identity": {"dag_id": "d", "task_id": "t"}}, [], repo_paths={})
    assert "Problem" in out and "Solution" in out
    assert "No known fix on record" in out


if __name__ == "__main__":
    test_code_links_maps_traceback_to_repo()
    test_code_links_skips_framework_frames()
    test_troubleshooting_carries_known_fix_pr()
    test_low_score_fix_pr_not_claimed()
    test_unclassified_diag_still_produces_package()
    print("OK - troubleshooting package tests passed")
