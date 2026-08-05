"""Optimization-detector tests, validated on the REAL targeted_signal plan (2026-07-31).

Fixture = the physical-plan text + Optimizer Statistics block from the Databricks Spark UI
for write_targeted_signal (INC-009's job). Proves the detectors fire the low-hanging wins.
"""

from __future__ import annotations

from airflow_optimizer.optimizations import analyze_plan, parse_plan_text

# Trimmed to the load-bearing lines from the Spark SQL detail screenshots.
PLAN = """
(1): Scan parquet prod.mntn_matched.product_categorization | 3.29 h (0 ms, 4.7 s, 57.9 s) | 13,500,313,667
(36): SortMergeJoin Inner | - | 554,260,244
(39): ShuffleQueryStage | Statistics(sizeInBytes=182.2 GiB, rowCount=2.04E9, isRuntime=true) | -
(41): RunningWindowFunction | - | -
(44): Execute InsertIntoHadoopFsRelationCommand | Arguments: gs://mntn-data-archive-prod/signals/targeted_signal, false, Parquet

== Physical Plan ==
+- * SortMergeJoin Inner (36)
   +- ShuffleQueryStage (39), Statistics(sizeInBytes=182.2 GiB, rowCount=2.04E9, ColumnStat: N/A, isRuntime=true)
   +- TableCacheQueryStage (16), Statistics(sizeInBytes=166.8 GiB, rowCount=1.38E9, isRuntime=true)

== Optimizer Statistics (table names per statistics state) ==
  missing = product_categorization
  partial =
  full   =
Corrective actions: ANALYZE TABLE <table-name> COMPUTE STATISTICS FOR ALL COLUMNS
"""


def test_parse_extracts_core_facts() -> None:
    """The parser pulls the scan rowcount, missing-stats table, join, shuffle, write target."""
    p = parse_plan_text(PLAN)
    assert any(s.table.endswith("product_categorization") and s.rows == 13_500_313_667 for s in p.scans)
    assert "product_categorization" in p.missing_stats
    assert "SortMergeJoin" in p.join_types
    assert any(b > 180 * 1024**3 for b in p.shuffle_bytes)
    assert p.write_target == "gs://mntn-data-archive-prod/signals/targeted_signal"
    assert p.has_running_window


def test_missing_statistics_is_top_finding() -> None:
    """The optimizer's own `missing =` advisory becomes the highest-impact finding."""
    findings = analyze_plan(PLAN)
    keys = [f.key for f in findings]
    assert "missing_statistics" in keys
    top = findings[0]
    assert top.impact == "high"
    ms = next(f for f in findings if f.key == "missing_statistics")
    assert "product_categorization" in ms.title
    assert "13,500,313,667" in ms.title  # the scanned rowcount is surfaced
    assert "ANALYZE TABLE" in ms.fix


def test_wide_shuffle_and_window_detected() -> None:
    """The 182 GiB shuffle and the window-over-sort both surface with concrete fixes."""
    keys = [f.key for f in analyze_plan(PLAN)]
    assert "shuffle_partition_sizing" in keys
    assert "window_full_sort" in keys


def test_clean_plan_yields_nothing() -> None:
    """A small, fully-analyzed plan produces no findings."""
    clean = "== Optimizer Statistics ==\n  missing =\n  partial =\n  full = a, b\n+- BroadcastHashJoin"
    assert analyze_plan(clean) == []


if __name__ == "__main__":
    test_parse_extracts_core_facts()
    test_missing_statistics_is_top_finding()
    test_wide_shuffle_and_window_detected()
    test_clean_plan_yields_nothing()
    print("OK - optimization detectors validated on the real targeted_signal plan")
