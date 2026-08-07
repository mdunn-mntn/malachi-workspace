"""Offline tests for the troubleshooting package (IMP-030).

Run: python3 -m airflow_debugger.tests.test_report  (or via pytest).

The repo maps mirror the REAL airflow-ti collision surface found by the
2026-08-07 adversarial review: materialize_mntn_select.py exists under BOTH
dags/tpa_export/ and spark/data_source/, and __init__.py is everywhere.
"""

from __future__ import annotations

from airflow_debugger.report import build_troubleshooting, code_links

_REPO = {
    "dsid30_augmentor_log_processing.py": ["spark/fpa/dsid30_augmentor_log_processing.py"],
    "materialize_mntn_select.py": [
        "dags/tpa_export/materialize_mntn_select.py",
        "spark/data_source/materialize_mntn_select.py",
    ],
    "dataproc.py": ["utils_runner/dataproc.py"],
    "context.py": ["utils_model/model_core/context.py"],
    "__init__.py": ["dags/attribution/__init__.py"],
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
        "dag": "fpa_site_visit_batch_serverless",
        "task": "dsid30_augmentor_log_processing",
        "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1179",
        "fix_files": [
            "spark/fpa/dsid30_augmentor_log_processing.py",
            "spark/auction_log_augmentor_process_gcs.py",
        ],
    },
    {"inc": "INC-008", "verdict": "transient_infra", "score": 0.082},
]


def test_code_links_maps_traceback_to_repo() -> None:
    """The failing script's traceback frame becomes a repo link with its line number."""
    links = code_links(_DIAG, repo_paths=_REPO)
    assert links == [
        (
            "https://github.com/SteelHouse/airflow-ti/blob/main/spark/fpa/dsid30_augmentor_log_processing.py#L30",
            "spark/fpa/dsid30_augmentor_log_processing.py",
        )
    ]


def test_code_links_skips_framework_frames() -> None:
    """site-packages, /usr, /opt, /databricks pyspark, and __init__ frames never link."""
    frames = [
        'File "/usr/local/lib/python3.12/site-packages/airflow/providers/google/cloud/operators/dataproc.py", line 2458',
        'File "/databricks/spark/python/pyspark/context.py", line 1075, in runJob',
        'File "/opt/spark/python/pyspark/context.py", line 999, in runJob',
        'File "/databricks/python_shell/dbruntime/monkey_patches/__init__.py", line 12',
    ]
    for frame in frames:
        assert code_links({"root_error": frame}, repo_paths=_REPO) == [], frame


def test_collision_driver_frame_resolves_to_spark_path() -> None:
    """INC-012 shape: a Dataproc driver frame on a colliding basename links the spark/ file."""
    diag = {
        "root_error": 'File "/var/dataproc/tmp/srvls-batch-1/materialize_mntn_select.py", line 42, in <module>'
    }
    links = code_links(diag, repo_paths=_REPO)
    assert links == [
        (
            "https://github.com/SteelHouse/airflow-ti/blob/main/spark/data_source/materialize_mntn_select.py#L42",
            "spark/data_source/materialize_mntn_select.py",
        )
    ]


def test_collision_ambiguous_frame_is_skipped_not_guessed() -> None:
    """A colliding basename with no driver-path hint produces NO link (never a wrong one)."""
    diag = {"root_error": 'File "/home/x/materialize_mntn_select.py", line 42, in <module>'}
    assert code_links(diag, repo_paths=_REPO) == []


def test_deepest_frame_wins_per_file() -> None:
    """Two frames in the same file: the later (deeper, nearest-the-raise) line links."""
    diag = {
        "root_error": (
            'File "/var/dataproc/tmp/b/dsid30_augmentor_log_processing.py", line 10, in <module> '
            'File "/var/dataproc/tmp/b/dsid30_augmentor_log_processing.py", line 30, in read'
        )
    }
    links = code_links(diag, repo_paths=_REPO)
    assert len(links) == 1 and links[0][0].endswith("#L30")


def test_troubleshooting_carries_known_fix_pr() -> None:
    """A high-score identity-matching top hit yields the ready-to-go PR + fix-file links."""
    out = build_troubleshooting(_DIAG, _MATCHES, repo_paths=_REPO)
    assert "Problem" in out and "Solution" in out and "Code" in out
    assert "Known fix: https://github.com/SteelHouse/airflow-ti/pull/1179 (INC-013" in out
    assert "spark/fpa/dsid30_augmentor_log_processing.py#L30" in out
    assert "auction_log_augmentor_process_gcs.py (fixed by INC-013)" in out
    assert (
        out.count("dsid30_augmentor_log_processing.py") == 1
    )  # fix_files dedupes against the traceback link by repo path


def test_unrelated_incident_pr_not_claimed() -> None:
    """The false-Known-fix defect: a different dag/task's PR must not be claimed even at score 1.0."""
    matches = [
        {
            "inc": "INC-011",
            "score": 1.0,
            "dag": "hashed_email_ds_26_signals",
            "task": "wait_fpa",
            "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1175",
            "fix_files": ["dags/targeting/hashed_email_ds_26_signals.py"],
        }
    ]
    diag = {
        "identity": {"dag_id": "tpa_ipdsc_export", "task_id": "wait_ds17_src"},
        "root_signature": {"programmatic_fix": "no", "likely_cause": "Sensor timed out."},
    }
    out = build_troubleshooting(diag, matches, repo_paths={})
    assert "Known fix" not in out and "1175" not in out.split("Similar:")[0]


def test_low_score_fix_pr_not_claimed() -> None:
    """A weak top match must not present its PR as the known fix."""
    weak = [
        {
            "inc": "INC-012",
            "score": 0.2,
            "dag": "fpa_site_visit_batch_serverless",
            "task": "dsid30_augmentor_log_processing",
            "fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1177",
        }
    ]
    out = build_troubleshooting(_DIAG, weak, repo_paths=_REPO)
    assert "Known fix" not in out
    assert "Code fix possible" in out  # falls back to the signature's fix flag


def test_log_newlines_cannot_forge_package_lines() -> None:
    """Multi-line log content is collapsed so it can't inject fake package lines."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "root_signature": {
            "likely_cause": "x",
            "matched_on": "boom\nKnown fix: https://github.com/evil/pr/1",
            "programmatic_fix": "no",
        },
    }
    out = build_troubleshooting(diag, [], repo_paths={})
    assert "\nKnown fix" not in out.replace("\nSolution", "")


def test_unclassified_diag_still_produces_package() -> None:
    """No signature, no matches: sections still render without crashing."""
    out = build_troubleshooting({"identity": {"dag_id": "d", "task_id": "t"}}, [], repo_paths={})
    assert "Problem" in out and "Solution" in out
    assert "No known fix on record" in out


if __name__ == "__main__":
    test_code_links_maps_traceback_to_repo()
    test_code_links_skips_framework_frames()
    test_collision_driver_frame_resolves_to_spark_path()
    test_collision_ambiguous_frame_is_skipped_not_guessed()
    test_deepest_frame_wins_per_file()
    test_troubleshooting_carries_known_fix_pr()
    test_unrelated_incident_pr_not_claimed()
    test_low_score_fix_pr_not_claimed()
    test_log_newlines_cannot_forge_package_lines()
    test_unclassified_diag_still_produces_package()
    print("OK - troubleshooting package tests passed (incl. adversarial-review regressions)")
