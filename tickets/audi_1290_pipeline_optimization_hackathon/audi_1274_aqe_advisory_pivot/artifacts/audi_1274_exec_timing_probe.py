"""Print registered executors and cores at each stage submission from one Spark event log.

Usage: python3 audi_1274_exec_timing_probe.py app-<id>.zstd
"""

import json
import subprocess
import sys

TRACKED_KEYS = (
    "spark.default.parallelism",
    "spark.executor.cores",
    "spark.dynamicAllocation.initialExecutors",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
)


def main(path: str) -> None:
    """Print tracked env keys, then one row per submitted stage."""
    proc = subprocess.Popen(["zstd", "-dc", path], stdout=subprocess.PIPE)
    executors = 0
    cores_per_executor = 0
    app_start = None
    rows = []
    for line in proc.stdout:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = event.get("Event")
        if name == "SparkListenerApplicationStart":
            app_start = event["Timestamp"]
        elif name == "SparkListenerEnvironmentUpdate":
            props = event["Spark Properties"]
            cores_per_executor = int(props.get("spark.executor.cores", 1))
            for key in TRACKED_KEYS:
                print(f"{key} = {props.get(key, '<unset>')}")
        elif name == "SparkListenerExecutorAdded":
            executors += 1
        elif name == "SparkListenerStageSubmitted":
            info = event["Stage Info"]
            submitted = info.get("Submission Time")
            elapsed = round((submitted - app_start) / 1000, 1) if submitted and app_start else None
            rows.append((info["Stage ID"], info["Number of Tasks"], elapsed, executors, executors * cores_per_executor))
    print("stage_id tasks elapsed_s executors_registered cores_registered")
    for row in rows:
        print(*row)


if __name__ == "__main__":
    main(sys.argv[1])
