"""Read one parquet file under a given spark.sql.files.maxPartitionBytes and print rows per input partition."""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path, caps = sys.argv[1], sys.argv[2:]
for cap in caps:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("audi_1273_split_probe")
        .config("spark.sql.files.maxPartitionBytes", cap)
        .config("spark.sql.files.openCostInBytes", "4194304")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    df = spark.read.parquet(path)
    rows = (
        df.groupBy(F.spark_partition_id().alias("partition"))
        .count()
        .orderBy("partition")
        .collect()
    )
    per_partition = {r["partition"]: r["count"] for r in rows}
    print(
        f"cap={cap} ({int(cap) // 1048576} MiB) input_partitions={df.rdd.getNumPartitions()} "
        f"partitions_with_rows={len(per_partition)} rows_per_partition={per_partition} "
        f"total_rows={sum(per_partition.values())}"
    )
    spark.stop()
