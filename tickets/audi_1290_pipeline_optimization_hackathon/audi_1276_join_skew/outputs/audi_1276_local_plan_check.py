#!/usr/bin/env python3
"""Local Spark 3.5.3 check of the AUDI-1276 edits: the broadcast hints move the shuffle off the join key and the split monitor SQL returns the same rows as the original.

Usage: JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
       <worktree>/.venv/bin/python audi_1276_local_plan_check.py <worktree>
The stats-less JDBC relation is emulated with autoBroadcastJoinThreshold=-1 at plan time and a
10 MB adaptive threshold at runtime, which is the decision path the prod event logs show.
Writes audi_1276_local_plan_check.txt next to this file.
"""
import ast
import os
import random
import re
import subprocess
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

MONITOR = "models/monitoring/ipdsc_42_monitor.py"
EXCHANGE_RE = re.compile(r"Exchange hashpartitioning\(([^)]*)\)")
ID_SUFFIX_RE = re.compile(r"#\d+L?")
HOT_ADVERTISER = 36206
ADVERTISERS = [HOT_ADVERTISER, 1, 2, 3, 4, 5, 6, 7, 8]


def sql_constants(source: str) -> dict:
    consts = {}
    for node in ast.parse(source).body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            consts[node.targets[0].id] = node.value.value
    return consts


def plan(df, mode: str = "simple") -> str:
    return ID_SUFFIX_RE.sub("", df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode))


def shuffle_keys(plan_text: str) -> list:
    return [m.group(1) for m in EXCHANGE_RE.finditer(plan_text)]


def join_strategies(plan_text: str) -> list:
    return sorted(set(re.findall(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin)", plan_text)))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[4]")
        .appName("audi_1276_local_plan_check")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .getOrCreate()
    )


def synthetic_log(spark, with_guid: bool):
    random.seed(7)
    rows = []
    for i in range(20000):
        advertiser = HOT_ADVERTISER if random.random() < 0.3 else random.choice(ADVERTISERS[1:] + [99])
        ip = f"10.0.{i % 250}.{(i * 7) % 250}"
        if with_guid:
            rows.append((ip, advertiser, 1_700_000_000 + i, f"g{i % 500}" if i % 10 else None, f"o{i % 300}" if i % 7 else None, float(i % 50)))
        else:
            rows.append((ip, advertiser, 1_700_000_000 + i))
    schema = "ip string, advertiser_id bigint, time bigint" + (", guid string, order_id string, order_amt double" if with_guid else "")
    return spark.createDataFrame(rows, schema)


def small_sides(spark):
    valid = spark.createDataFrame([(a,) for a in ADVERTISERS], "advertiser_id int").dropDuplicates(["advertiser_id"])
    verticals = spark.createDataFrame([(a, 100 + a % 5) for a in ADVERTISERS], "advertiser_id int, vertical_id int").dropDuplicates(["advertiser_id", "vertical_id"])
    return valid, verticals


def guid_log_shape(spark, hint: bool):
    valid, verticals = small_sides(spark)
    side = F.broadcast if hint else (lambda d: d)
    joined = (
        synthetic_log(spark, with_guid=False)
        .filter(F.col("ip").isNotNull() & (F.trim(F.col("ip")) != ""))
        .join(side(valid), "advertiser_id", "inner")
    )
    rollup = (
        joined.withColumn("dt", F.lit("2026-09-02"))
        .groupBy("ip", "advertiser_id", "dt")
        .agg(F.count("*").alias("visits"), F.min("time").alias("first_visit_time"), F.max("time").alias("last_visit_time"))
    )
    return (
        rollup.join(side(verticals), on="advertiser_id", how="left")
        .select("dt", "ip", "advertiser_id", "vertical_id", "visits", "first_visit_time", "last_visit_time")
        .repartition(8, "ip")
    )


def conv_log_shape(spark, hint: bool):
    valid, verticals = small_sides(spark)
    side = F.broadcast if hint else (lambda d: d)
    raw = (
        synthetic_log(spark, with_guid=True)
        .filter(F.col("ip").isNotNull() & (F.trim(F.col("ip")) != ""))
        .join(side(valid), "advertiser_id", "inner")
    )
    has_identity = raw.filter(F.col("guid").isNotNull() & F.col("order_id").isNotNull())
    no_identity = raw.filter(F.col("guid").isNull() | F.col("order_id").isNull())
    w = Window.partitionBy("advertiser_id", "guid", "order_id", "order_amt").orderBy("time")
    deduped = has_identity.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    rollup = (
        deduped.unionByName(no_identity, allowMissingColumns=True)
        .withColumn("dt", F.lit("2026-09-02"))
        .groupBy("ip", "advertiser_id", "dt")
        .agg(F.count("*").alias("conversions"), F.min("time").alias("first_conv_time"), F.max("time").alias("last_conv_time"))
    )
    return (
        rollup.join(side(verticals), on="advertiser_id", how="left")
        .select("dt", "ip", "advertiser_id", "vertical_id", "conversions", "first_conv_time", "last_conv_time")
        .repartition(8, "ip")
    )


def run_shape(name: str, build, out) -> list:
    lines = [f"\n#### {name}"]
    results = {}
    for hint in (False, True):
        df = build(hint)
        initial = plan(df)
        rows = sorted(tuple(r) for r in df.collect())
        final = plan(df)
        results[hint] = rows
        label = "with F.broadcast" if hint else "as on main"
        lines.append(f"[{label}] rows={len(rows)} initial_plan_shuffles={shuffle_keys(initial)}")
        lines.append(f"[{label}] final_plan_joins={join_strategies(final)} final_plan_shuffles={shuffle_keys(final)}")
        out.append(f"\n===== {name} {label}: initial plan =====\n{initial}\n===== {name} {label}: final plan =====\n{final}")
    lines.append(f"same output rows with and without the hint: {results[False] == results[True]}")
    return lines


def monitor_inputs(spark):
    schema = T.StructType([
        T.StructField("ip", T.StringType(), True),
        T.StructField("data_source_category_ids", T.ArrayType(T.LongType()), True),
    ])
    today = spark.createDataFrame(
        [("1.1.1.1", [1, 2, 9]), ("1.1.1.2", [1, 2]), ("1.1.1.3", [2, 3]), ("1.1.1.1", [1]), ("1.1.1.4", [3, 3]), ("1.1.1.5", []), ("1.1.1.6", None)],
        schema,
    )
    yesterday = spark.createDataFrame(
        [("2.2.2.1", [2, 4, 9]), ("2.2.2.2", [3]), ("2.2.2.3", [3, 4]), ("2.2.2.4", [2]), ("2.2.2.1", [4])],
        schema,
    )
    empty = spark.createDataFrame([], schema)
    deals = spark.createDataFrame(
        [(1, "D1", "Deal One", "MNTN_SELECT"), (2, "D2", "Deal Two", "EXCLUSIVE"), (3, "D3", "Deal Three", "MNTN_SELECT"), (4, "D4", "Deal Four", "EXCLUSIVE"), (5, "D5", "Deal Five", "MNTN_SELECT")],
        "data_source_category_id int, deal_id string, deal_name string, deal_type string",
    )
    return today, yesterday, empty, deals


def run_monitor(spark, worktree: str, out: list) -> list:
    main_src = subprocess.check_output(["git", "-C", worktree, "show", f"HEAD:{MONITOR}"], text=True)
    branch_src = open(os.path.join(worktree, MONITOR)).read()
    old_sql = sql_constants(main_src)["IPDSC42_COMPARE_SQL"]
    new = sql_constants(branch_src)
    today, yesterday, empty, deals = monitor_inputs(spark)
    deals.createOrReplaceTempView("deal_df")
    lines = ["\n#### ipdsc_42_monitor"]
    for case, (t, y) in {"today+yesterday": (today, yesterday), "empty today": (empty, yesterday), "empty yesterday": (today, empty)}.items():
        t.createOrReplaceTempView("today_df")
        y.createOrReplaceTempView("yesterday_df")
        spark.catalog.clearCache()
        old_df = spark.sql(old_sql)
        old_initial = plan(old_df)
        old_rows = [tuple(r) for r in old_df.collect()]
        old_final = plan(old_df)
        comparison = spark.sql(new["IPDSC42_BY_CATEGORY_SQL"]).cache()
        cmp_initial = plan(comparison)
        n_cat = comparison.count()
        cmp_final = plan(comparison)
        comparison.createOrReplaceTempView("comparison")
        new_df = spark.sql(new["IPDSC42_COMPARE_SQL"])
        new_df.cache()
        new_rows = [tuple(r) for r in new_df.collect()]
        new_final = plan(new_df)
        lines.append(f"[{case}] main: rows={len(old_rows)} initial_shuffles={shuffle_keys(old_initial)} final_joins={join_strategies(old_final)}")
        lines.append(f"[{case}] branch by_category: categories={n_cat} initial_shuffles={shuffle_keys(cmp_initial)} final_joins={join_strategies(cmp_final)}")
        lines.append(f"[{case}] branch compare: rows={len(new_rows)} cached_scans_of_comparison={new_final.count('InMemoryTableScan')} shuffles={shuffle_keys(new_final)}")
        lines.append(f"[{case}] same row multiset={sorted(old_rows) == sorted(new_rows)} TOTAL first in both={old_rows[0][0] == 'TOTAL' and new_rows[0][0] == 'TOTAL'}")
        lines.append(f"[{case}] rows={sorted(new_rows)}")
        out.append(f"\n===== monitor {case} main: final plan =====\n{old_final}\n===== monitor {case} branch by_category: final plan =====\n{cmp_final}\n===== monitor {case} branch compare: final plan =====\n{new_final}")
    return lines


def main() -> None:
    worktree = sys.argv[1]
    spark = build_spark()
    plans = []
    summary = [f"spark={spark.version} java={spark._jvm.System.getProperty('java.version')}"]
    summary += run_shape("guid_log_ip_advertiser_id shape", lambda hint: guid_log_shape(spark, hint), plans)
    summary += run_shape("conv_log_ip_advertiser_id shape", lambda hint: conv_log_shape(spark, hint), plans)
    summary += run_monitor(spark, worktree, plans)
    text = "\n".join(summary) + "\n" + "\n".join(plans) + "\n"
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "audi_1276_local_plan_check.txt"), "w") as f:
        f.write(text)
    print("\n".join(summary))
    spark.stop()


if __name__ == "__main__":
    main()
