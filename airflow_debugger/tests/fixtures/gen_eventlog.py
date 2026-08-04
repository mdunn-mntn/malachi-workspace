"""Generate a REAL Spark event log exercising shuffle/skew/join/window/cache/spill.

Produces a genuine event log we can iterate the parser against, so 'do we grab every
valuable field' is tested on real data, not asserted on paper.
"""
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812  (canonical pyspark alias)
from pyspark.sql.window import Window

EVT = "/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad/spark-events"
OUT = "/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/3c4f6695-7891-4554-8d46-623110bfd018/scratchpad/out"
shutil.rmtree(EVT, ignore_errors=True)
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(EVT, exist_ok=True)

spark = (
    SparkSession.builder.master("local[2]")
    .appName("audi1191_eventlog_fixture")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{EVT}")
    .config("spark.sql.shuffle.partitions", "8")  # few partitions -> big -> spill-ish + skew visible
    .config("spark.sql.adaptive.enabled", "false")  # keep the plan stable/legible
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

# Skewed fact: 80% of rows share key=0 -> one shuffle partition dominates (skew signal).
fact = spark.range(0, 600_000).select(
    F.when(F.rand(1) < 0.8, F.lit(0)).otherwise((F.rand(2) * 500).cast("int")).alias("k"),
    (F.rand(3) * 1000).cast("int").alias("v"),
    F.col("id"),
)
dim = spark.range(0, 500).select(F.col("id").alias("k"), (F.col("id") % 7).alias("bucket"))

cached = fact.cache()  # storage tab signal
cached.count()

agg = cached.groupBy("k").agg(F.sum("v").alias("sv"), F.count("*").alias("n"))  # shuffle + skew
joined = agg.join(dim, "k")  # SortMergeJoin
w = Window.partitionBy("bucket").orderBy(F.col("sv").desc())
ranked = joined.withColumn("rnk", F.row_number().over(w))  # window -> full sort
ranked.write.mode("overwrite").parquet(f"file://{OUT}")

# a second action reusing the cached df (recompute if cache were absent)
cached.groupBy("k").count().count()

spark.stop()
print("EVENTLOG_DIR", EVT)
print("files:", os.listdir(EVT))
