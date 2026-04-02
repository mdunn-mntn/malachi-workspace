# Databricks notebook source
"""
Databricks: Fast-food vertical (129) advertiser traffic vs PMP overlap — exploratory (NBA vs HGTV).

1) advertiser_ids from GCS parquet: household-scoring advertiser_verticals snapshot (vertical_id = 129)
2) guid_log: distinct IPs with events for those advertisers in a 5-hour partition window
3) augmentor_log: distinct IPs in the same window whose `pmp` array contains NBA or HGTV deal ids
4) Metrics: same-window overlap with fast-food IPs; compare NBA cohort vs HGTV cohort

**Interpretation:** Same-IP overlap in a fixed time window supports a *hypothesis* for testing
(e.g. modeling, holdout tests). It does not prove causation ("NBA causes fast food orders") —
confounders include geography, time-of-day, and household composition. Use for directional
evidence or to power a stricter test design.

Set WINDOW_* variables to match "last night" (UTC vs PT matters for dt/hh — align with how
guid_log is partitioned).
"""

from pyspark.sql import functions as F

# --- "Last night" = adjust these to your run (partition dt/hh in GCS) ---
# Example crossing midnight UTC: 2026-03-31 22–23h + 2026-04-01 00–02h
GUID_LOG_PATHS = [
    "gs://mntn-data-archive-prod/guid_log/dt=2026-03-31/hh=22",
    "gs://mntn-data-archive-prod/guid_log/dt=2026-03-31/hh=23",
    "gs://mntn-data-archive-prod/guid_log/dt=2026-04-01/hh=00",
    "gs://mntn-data-archive-prod/guid_log/dt=2026-04-01/hh=01",
    "gs://mntn-data-archive-prod/guid_log/dt=2026-04-01/hh=02",
]

REGIONS = ("west", "east")
DT_HH = (
    ("2026-03-31", "22"),
    ("2026-03-31", "23"),
    ("2026-04-01", "00"),
    ("2026-04-01", "01"),
    ("2026-04-01", "02"),
)

AUGMENTOR_PATHS = [
    f"gs://mntn-data-archive-prod/augmentor_log/region={region}/dt={dt}/hh={hh}"
    for region in REGIONS
    for dt, hh in DT_HH
]

# PMP deal ids treated as "NBA / live game" exposure in augmentor (tune list with your team)
NBA_PMP_DEALS = [
    "54ac349e-48ed-4502-932a-ac8f64257fa5",
    "IXMSADSPRPNBAOTTUSpPr29IAC",
    "a88892e4-8ba9-4760-8c9a-e1f557284dac",
]

# HGTV / DTV-style PMP exposure (Beeswax + internal ids) — compare overlap vs NBA
HGTV_PMP_DEALS = [
    "DIR-BEE-00096",
    "SCR-MNT-00006",
    "Beeswax-MNTN-Multiple-DTV-HGTV-OTT-UFR",
    "eb549542-ae55-40d7-8ca7-334a5d56c020",
]

VERTICAL_ID_FAST_FOOD = 129

# Snapshot date for advertiser ↔ vertical mapping (align with scoring pipeline; change folder as needed)
ADVERTISER_VERTICALS_PATH = (
    "gs://household-scoring-prod/output/data_aggregation/advertiser_verticals/2026/03/31"
)

# --- 1) Fast-food advertisers (vertical 129) from parquet ---
_av = spark.read.parquet(ADVERTISER_VERTICALS_PATH)
fast_food_advertisers = (
    _av.filter(F.col("vertical_id") == VERTICAL_ID_FAST_FOOD)
    .select("advertiser_id")
    .distinct()
)
# If vertical_id is string in the file: .filter(F.col("vertical_id").cast("int") == VERTICAL_ID_FAST_FOOD)

# --- 2) guid_log: IPs that visited any of those advertisers in the window ---
guid_log = spark.read.parquet(*GUID_LOG_PATHS)
g = guid_log.withColumn("ip", F.trim(F.col("ip").cast("string")))

ff_visits = (
    g.join(fast_food_advertisers, "advertiser_id", "inner")
    .where(F.col("ip").isNotNull() & (F.col("ip") != ""))
)

ff_ips = ff_visits.select("ip").distinct()

# --- 3) augmentor: IPs with ≥1 NBA or HGTV PMP in the same window ---
pmp_nba = F.array(*[F.lit(s) for s in NBA_PMP_DEALS])
pmp_hgtv = F.array(*[F.lit(s) for s in HGTV_PMP_DEALS])
augmentor = spark.read.parquet(*AUGMENTOR_PATHS)
a = augmentor.withColumn("ip", F.trim(F.col("ip").cast("string")))

nba_ips = (
    a.filter(
        F.col("pmp").isNotNull()
        & (F.size(F.array_intersect(F.col("pmp"), pmp_nba)) > 0)
    )
    .select("ip")
    .where(F.col("ip").isNotNull() & (F.col("ip") != ""))
    .distinct()
)

hgtv_ips = (
    a.filter(
        F.col("pmp").isNotNull()
        & (F.size(F.array_intersect(F.col("pmp"), pmp_hgtv)) > 0)
    )
    .select("ip")
    .where(F.col("ip").isNotNull() & (F.col("ip") != ""))
    .distinct()
)

# --- 4) Overlap: fast-food IPs that also have a PMP augmentor row (same window) ---
n_ff = ff_ips.count()
n_nba = nba_ips.count()
n_hgtv = hgtv_ips.count()
n_both_nba = ff_ips.join(F.broadcast(nba_ips), "ip", "inner").count()
n_both_hgtv = ff_ips.join(F.broadcast(hgtv_ips), "ip", "inner").count()

pct_ff_with_nba_pmp = (100.0 * n_both_nba / n_ff) if n_ff else 0.0
pct_nba_also_ff = (100.0 * n_both_nba / n_nba) if n_nba else 0.0
pct_ff_with_hgtv_pmp = (100.0 * n_both_hgtv / n_ff) if n_ff else 0.0
pct_hgtv_also_ff = (100.0 * n_both_hgtv / n_hgtv) if n_hgtv else 0.0

# Legacy single-row NBA table (same column names as before)
summary_nba_only = spark.createDataFrame(
    [
        (
            n_ff,
            n_nba,
            n_both_nba,
            round(pct_ff_with_nba_pmp, 4),
            round(pct_nba_also_ff, 4),
        )
    ],
    [
        "ff_distinct_ips_guid",
        "nba_pmp_distinct_ips_augmentor",
        "overlap_ips",
        "pct_ff_ips_also_nba_pmp",
        "pct_nba_pmp_ips_also_ff",
    ],
)

# Side-by-side: NBA vs HGTV (same metrics)
comparison = spark.createDataFrame(
    [
        (
            "nba_pmp",
            n_ff,
            n_nba,
            n_both_nba,
            round(pct_ff_with_nba_pmp, 4),
            round(pct_nba_also_ff, 4),
        ),
        (
            "hgtv_pmp",
            n_ff,
            n_hgtv,
            n_both_hgtv,
            round(pct_ff_with_hgtv_pmp, 4),
            round(pct_hgtv_also_ff, 4),
        ),
    ],
    [
        "pmp_cohort",
        "ff_distinct_ips_guid",
        "pmp_distinct_ips_augmentor",
        "overlap_ips_ff_and_pmp",
        "pct_ff_ips_also_this_pmp",
        "pct_pmp_ips_also_ff",
    ],
)

# Which cohort has higher share of PMP IPs that also hit FF? (directional; not statistical test)
winner_pct_pmp_also_ff = (
    "nba_pmp"
    if pct_nba_also_ff > pct_hgtv_also_ff
    else ("hgtv_pmp" if pct_hgtv_also_ff > pct_nba_also_ff else "tie")
)
winner_pct_ff_also_pmp = (
    "nba_pmp"
    if pct_ff_with_nba_pmp > pct_ff_with_hgtv_pmp
    else ("hgtv_pmp" if pct_ff_with_hgtv_pmp > pct_ff_with_nba_pmp else "tie")
)

comparison_note = spark.createDataFrame(
    [
        (
            winner_pct_pmp_also_ff,
            winner_pct_ff_also_pmp,
            "Higher pct_pmp_ips_also_ff = larger share of that PMP cohort IPs also in FF guid window",
            "Higher pct_ff_ips_also_this_pmp = larger share of FF IPs also exposed to that PMP cohort",
        )
    ],
    [
        "higher_pct_pmp_ips_also_ff",
        "higher_pct_ff_ips_also_pmp",
        "note_pct_pmp_also_ff",
        "note_pct_ff_also_pmp",
    ],
)

display(comparison)
display(comparison_note)
display(summary_nba_only)

# --- Optional: row-level overlap with matched_pmp_ids for QA ---
# overlap_detail = (
#     ff_visits.select("ip", "advertiser_id").distinct()
#     .join(
#         a.filter(F.size(F.array_intersect(F.col("pmp"), pmp_nba)) > 0)
#          .select("ip", F.array_intersect(F.col("pmp"), pmp_nba).alias("nba_pmp_hits")),
#         "ip",
#         "inner",
#     )
# )
# display(overlap_detail.limit(200))

# If join returns 0 rows: printSchema on _av; fix vertical_id / advertiser_id types to match guid_log.
