"""Demo: the analyzer's recommendations for the REAL targeted_signal job (INC-009's Databricks
run, 2026-07-31), built from the Spark UI screenshot values. Illustrates the three output types
(code / infra / failure) on a real production problem. Reads no live data - values are transcribed
from the screenshots; the same output is produced automatically once the event log is enabled.
"""

import sys

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from airflow_debugger.eventlog import ExecutorInfo, SparkRun, StageMetrics  # noqa: E402
from airflow_debugger.optimizations import analyze_plan, analyze_run  # noqa: E402

# --- from the SQL/DataFrame plan (screenshot) ---
PLAN = """
(1): Scan parquet prod.mntn_matched.product_categorization | 3.29 h | 13,500,313,667
(36): SortMergeJoin Inner | 554,260,244
ShuffleQueryStage (39), Statistics(sizeInBytes=182.2 GiB, rowCount=2.04E9, isRuntime=true)
(44) Execute InsertIntoHadoopFsRelationCommand gs://mntn-data-archive-prod/signals/targeted_signal Parquet
== Optimizer Statistics ==
  missing = product_categorization
"""

# --- from Stages / Executors / Environment (screenshots) ---
run = SparkRun(spark_props={"spark.databricks.clusterUsageTags.clusterAvailability":
                            "PREEMPTIBLE_WITH_FALLBACK_GCP"})
run.stages = [
    StageMetrics(stage_id=10, num_tasks=1718, shuffle_write_bytes=768 * 1024**3,
                 input_bytes=700 * 1024**3, run_time_ms=int(24 * 60 * 1000)),
    StageMetrics(stage_id=14, num_tasks=853, fetch_failed=168, shuffle_read_bytes=631 * 1024**3,
                 shuffle_write_bytes=72 * 1024**3, run_time_ms=int(40 * 60 * 1000),
                 failure_reason="MetadataFetchFailedException: Missing an output location for shuffle 3"),
]
# 7 executors, spot-reclaimed, 168 task failures (executor 2 alone = 113)
run.executors = [ExecutorInfo(exec_id="2", removed_reason="spot instance preemption / lost",
                              failed_tasks=113)]
run.executors += [ExecutorInfo(exec_id=str(i), removed_reason="spot instance preemption",
                               failed_tasks=8) for i in range(6)]

print("=== targeted_signal recommendations (real job) ===\n")
allf = analyze_plan(PLAN) + analyze_run(run)
rank = {"high": 0, "medium": 1, "low": 2}
for f in sorted(allf, key=lambda x: (rank[x.impact], x.rec_type)):
    print(f"[{f.rec_type.upper():7}] [{f.impact:6}] {f.title}")
    print(f"           WHY: {f.evidence}")
    print(f"           FIX: {f.fix}\n")
