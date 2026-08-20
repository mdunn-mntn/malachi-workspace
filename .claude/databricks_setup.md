# Databricks setup — TI-837 cluster

**Status:** Smoke tests passing as of 2026-04-29 (Malachi).

**Cluster URL:** https://1262887251702944.4.gcp.databricks.com/compute/clusters/5428-215533-4jodkdfs?o=1262887251702944
**Workspace URL:** `https://1262887251702944.4.gcp.databricks.com`
**Cluster ID:** `5428-215533-4jodkdfs`
**Org ID:** `1262887251702944`
**Owner:** Malachi (TI-837)
**Provisioned by:** Victor Savitskiy (2026-04-28)

## Resolved cluster config (as of 2026-04-29)

| Field | Value |
|---|---|
| spark_version | `17.3.x-scala2.13` (DBR 17.3 LTS, Apache Spark 4.0.0) |
| driver_node_type_id | `c3d-standard-4` (4 vCPU / 16 GB RAM, AMD Genoa) |
| node_type_id | `c3d-standard-8` (8 vCPU / 32 GB RAM, AMD Genoa) |
| autoscale | min/max set per Malachi's edits — bumped from initial 1-2 to higher ceiling |
| autotermination_minutes | 60 (increased from initial 10 to keep cluster alive during long ops) |
| runtime_engine | `STANDARD` (Photon optional upgrade) |
| data_security_mode | `SINGLE_USER` |

## Required tags

Per Victor — finance tracks costs by these. Current tags (as of 2026-04-29):
`project=malachi-ad-hoc, Squad=MLSQUAD, Env=DEV` — Malachi has flagged these are intentional for now; can be flipped to canonical `project=TI-837, squad=ML, env=Dev` later if finance attribution requires it.

## Local environment

**Python.** DBR 17.3 → `databricks-connect==17.3.*` requires **Python 3.12**.
The system Python on this laptop is 3.11. We use a **uv-managed venv** at
`~/.databricks-py312` so the 3.12 dependency is isolated.

```bash
# One-time:
uv venv --python 3.12 ~/.databricks-py312
VIRTUAL_ENV=~/.databricks-py312 uv pip install "databricks-connect==17.3.*"
```

Verify:
```bash
~/.databricks-py312/bin/python -c "from databricks.connect import DatabricksSession; print('ok')"
~/.databricks-py312/bin/python --version    # → Python 3.12.13
```

Pulled in transitively: `databricks-sdk==0.105.0`, `pyarrow`, `grpcio`, `pandas`, `numpy`.
The system-wide `pyspark==4.0.0` does NOT enter this venv (uv venvs are isolated); no conflict.

**Databricks CLI (Go).** Installed via brew tap:
```bash
brew tap databricks/tap
brew install databricks
databricks --version    # → 0.298.0 as of 2026-04-29
```

## Auth setup

The TI-837 PAT lives in the macOS keychain under service name `databricks-ti837`. It is **dead**
as of 2026-08-20 (`databricks tokens list` returns empty) and the keychain entry is stale; the
smoke test below cannot authenticate with it. Prefer the OAuth profile. To clear the entry:
`security delete-generic-password -s databricks-ti837 -a "$USER"`.

**Store / rotate** (run from any terminal — token is read with `-w` so it doesn't echo):
```bash
security add-generic-password -s "databricks-ti837" -a "$USER" -w "<PAT>" -U
```

**Read** (used by scripts):
```bash
DATABRICKS_TOKEN=$(security find-generic-password -s "databricks-ti837" -w)
```

**`~/.databrickscfg` no longer holds a PAT.** The `[DEFAULT]` stanza that wrote one was
removed 2026-08-20 (the token had already been revoked server-side: `databricks tokens list`
returns empty). Do not recreate it. A long-lived token in a dotfile is the pattern MNTN
decommissioned with the Slack bot on 2026-06-10.

Use the U2M OAuth profile instead, and pass it explicitly on every call:
```bash
databricks auth login --profile malachi@mountain.com   # renews when the refresh token expires
databricks current-user me -p malachi@mountain.com
```

The profile carries no secret on disk; the CLI keeps its own token cache. `databricks auth
profiles` shows which profiles are valid.

## Smoke test

Canonical script: [.claude/scripts/databricks_smoke.py](.claude/scripts/databricks_smoke.py)

```bash
~/.databricks-py312/bin/python .claude/scripts/databricks_smoke.py
```

Validates two paths:
- **A. BQ Spark connector** — reads `dw-main-bronze.integrationprod.campaigns`,
  filters `deleted=FALSE AND is_test=FALSE`, returns count.
  Expected: ~437k. Wall time: ~7s on the small interactive cluster.
- **B. GCS Parquet read** — reads `gs://mntn-data-archive-prod/augmentor_log/region=east/dt=YYYY-MM-DD/`,
  prints schema, returns `limit(10).count()`.
  Expected: 10. Wall time: ~100s **on first read** of a partition (Spark walks the directory listing); subsequent reads on the same path are sub-second.

If the cluster is TERMINATED, start it first:
```bash
databricks clusters start --cluster-id 5428-215533-4jodkdfs
# wait ~5 min for RUNNING
```

## Spark BQ Connector — silver-table read pattern (Victor + Dustin, 2026-04-30)

The MNTN silver tables (`logdata.cost_impression_log`, `clickpass_log`, `guid_log`) have type-conversion issues with the Spark BQ connector at the schema-resolution step:

- `cost_impression_log` has `recency_elapsed_time` (BQ `INTERVAL`) — the connector's `SchemaConverters` cannot convert INTERVAL to a Spark type, blocks all reads even when projecting other columns.
- `media_spend / data_spend / platform_spend` are wide `BIGNUMERIC(76)` — Spark's max DECIMAL precision is 38; needs `bigNumericDefaultPrecision=38`.

**Canonical workaround (Victor Savitskiy 2026-04-30, confirmed by Dustin Niehoff):** use `query` mode with materialization to `dw-main-bronze.external` (Terragrunt-managed bronze layer dataset, sanctioned for this pattern). Push the SELECT down to BigQuery so the Spark connector only ever sees the projected result schema.

```python
# All three project options must be set — Victor flagged this explicitly.
# Missing any one can cause silent failures or wrong-quota billing.
df = (
    spark.read.format("bigquery")
    .option("parentProject", "dw-main-bronze")
    .option("billingProject", "dw-main-bronze")
    .option("project",        "dw-main-bronze")
    .option("viewsEnabled", "true")
    .option("materializationDataset", "external")  # dw-main-bronze.external — sanctioned
    .option("bigNumericDefaultPrecision", "38")
    .option("bigNumericDefaultScale", "9")
    .load("""
        SELECT
          CAST(advertiser_id AS INT64) AS advertiser_id,
          ip,
          campaign_id
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE DATE(time) BETWEEN '2026-04-22' AND '2026-04-28'
          AND advertiser_id IN (...)
          AND ip IS NOT NULL AND ip != '0.0.0.0'
    """)
)
```

**Why all three project options matter (Victor):** the connector resolves auth, billing, and storage differently depending on which ones are set. Set all three to `dw-main-bronze` even when reading from `dw-main-silver` tables — the table reference inside the SQL itself tells BQ where the source data is. The three options are about WHERE the connector charges, materializes, and authenticates.

**Materialization happens in `dw-main-bronze.external`** — a Terragrunt-managed bronze-layer scratch dataset. Verified write access (2026-04-30). Don't materialize into other production datasets.

**Keeping INTERVAL columns** (rare — we usually project them away): Victor's example shows the cast pattern:

```python
.load("""
  SELECT * EXCEPT(recency_elapsed_time),
         CURRENT_DATE AS anchor_date,
         CURRENT_DATE + recency_elapsed_time AS recency_elapsed_time_tmp
  FROM `dw-main-silver.logdata.cost_impression_log`
""")
.withColumn("recency_elapsed_time", F.expr("recency_elapsed_time_tmp - anchor_date"))
.drop("recency_elapsed_time_tmp", "anchor_date")
```

**Alternative (Dustin):** instead of BQ-side materialization, use `temporaryGcsBucket=dataproc-temp-us-central1-754673906299-me0b3bsh` to materialize to GCS and read from there. Both approaches are valid; `materializationDataset=external` is simpler in our pipeline.

**The owners of these silver tables (Dustin):** "the datatypes of the source tables are up to the data team that own them, not us, but I do know at the very least that the INTERVAL type is necessary for their pipelines." Long-term changes to the silver schemas are out of scope for us.

## GCS read patterns (critical — partition layout is NOT what the v5 SQL implies)

**Augmentor partition layout** (verified 2026-04-29):

```
gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/
```

NOT `year=YYYY/month=MM/day=DD/` (the original setup-doc smoke test used the wrong filter and hung the cluster — fixed). For a complete daily scan you must read both regions; with the parent path Spark loads both:

```python
# Single region, single day — explicit, no partition pruning ambiguity
df = spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/region=east/dt=2026-04-23/")

# Both regions, range of days (cross-window validation pattern)
df = (spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/")
      .filter("dt BETWEEN '2026-04-20' AND '2026-04-26'"))
```

**Archive history.** Earliest GCS partition is ~`2026-03-30`. Despite "no TTL" framing, the archive is ~30 days deep; very-backward windows (>30d ago) won't find data.

**Other tables.** `guid_log` is also archived in GCS — same `gs://mntn-data-archive-prod/guid_log/` root. `prospecting_intent_v1` archives at `gs://household-scoring-prod/output/scoring/prospecting_intent/`. Both expected to follow Hive partitioning; verify before relying.

## Compute strategy

Two clusters, two purposes:

1. **Interactive cluster (this one, `5428-215533-4jodkdfs`).** Dev, smoke
   tests, schema poking, small pulls. Keep it small. Auto-terminates when idle.

2. **Job clusters.** Spin a fresh one inside each Job's workflow definition for
   each real run (the v5 port, Phase 2a, cross-window validation). Bigger
   config (e.g., `c3d-highmem-16` workers, autoscale 4-16, Photon ON).
   **Job compute is 3× cheaper** than interactive cluster per Victor —
   substantial savings on a 5-hour run. Cluster lives only for the duration
   of the run, then tears down.

Suggested job-cluster spec for the v5 port:
- driver: `c3d-standard-8`
- worker: `c3d-highmem-16` (more memory per worker for augmentor shuffle)
- autoscale: 4-16
- runtime_engine: `PHOTON`
- preemptible workers: OFF (we haven't reasoned about retry tolerance yet)
- tags: same as interactive cluster

## Editing the cluster

Three options. Pick the one that matches the change.

**UI.** Cluster page → Edit. Restart needed for most changes. Easiest for one-offs.

**CLI.**
```bash
databricks clusters edit --json '{
  "cluster_id": "5428-215533-4jodkdfs",
  "cluster_name": "...",
  "spark_version": "17.3.x-scala2.13",
  "node_type_id": "c3d-standard-8",
  "driver_node_type_id": "c3d-standard-4",
  "autoscale": {"min_workers": 1, "max_workers": 4},
  "autotermination_minutes": 60,
  "runtime_engine": "STANDARD",
  "data_security_mode": "SINGLE_USER",
  "single_user_name": "malachi@mountain.com"
}'
```
**`clusters edit` is a replace, not a patch.** Pass the full spec or you'll lose fields.

**SDK** (preferred when scripting):
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import AutoScale, RuntimeEngine, DataSecurityMode

w = WorkspaceClient()  # reads ~/.databrickscfg
w.clusters.edit(
    cluster_id="5428-215533-4jodkdfs",
    cluster_name="Malachi - TI-837",
    spark_version="17.3.x-scala2.13",
    node_type_id="c3d-standard-8",
    driver_node_type_id="c3d-standard-4",
    autoscale=AutoScale(min_workers=1, max_workers=4),
    autotermination_minutes=60,
    runtime_engine=RuntimeEngine.PHOTON,
    data_security_mode=DataSecurityMode.SINGLE_USER,
    single_user_name="malachi@mountain.com",
)
```

## Lessons from the 2026-04-29 setup session

1. **DBR 17.x is gated behind Python 3.12.** `databricks-connect>=16.2`
   declares `Requires-Python ==3.12.*`. Resolved via uv venv with managed
   3.12 — avoids touching system Python.
2. **System pyspark conflicts with databricks-connect.** Both ship the
   `pyspark` namespace. The uv venv sidesteps this completely; no need to
   uninstall the global one.
3. **GCS partition layout was wrong in the original setup template.**
   Augmentor uses `region={east,west}/dt=YYYY-MM-DD/`, not `year/month/day`.
   First Test B run hung the cluster for 10 min (until INACTIVITY
   auto-terminate fired) because the wrong filter forced Spark to scan
   every partition. Caught the bug, updated the canonical pattern.
4. **First-time GCS reads are slow** (~100s for a single partition's
   `limit(10).count()`). Spark walks the GCS listing on cold reads;
   subsequent reads in the same session are sub-second. Don't take
   first-call wall time as the steady-state figure.
5. **`augmentor_log` has no `advertiser_id` column at row level.** Per-bid-
   request log; advertiser linkage goes through `mntn_segments` →
   `audience_segments.expression`. Documented in
   `knowledge/data_catalog.md`.

## Pointers

- Canonical SQL pattern: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql`
- v5 methodology defense: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_methodology_defense.md`
- Victor's walkthrough notes: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/meetings/ti_837_03_victor_meeting_actions.md`
- Catalog entry (augmentor): `knowledge/data_catalog.md` § `bronze.raw.augmentor_log`
