# Databricks setup — TI-837 cluster

**Cluster URL:** https://1262887251702944.4.gcp.databricks.com/compute/clusters/5428-215533-4jodkdfs?o=1262887251702944
**Workspace URL:** `https://1262887251702944.4.gcp.databricks.com`
**Cluster ID:** `5428-215533-4jodkdfs`
**Org ID:** `1262887251702944`
**Owner / requested by:** Malachi (TI-837)
**Provisioned by:** Victor Savitskiy (2026-04-28)

## Required tags on every job/cluster

Per Victor — finance tracks costs by these:
- `project = TI-837` (or current ticket)
- `squad = ML` (universal label, even non-ML squads use it)
- `env = Dev`

## Auth setup (local laptop)

API token is in the user's keychain (Malachi mentioned having one in earlier
conversation). Should be set as env var:
```bash
export DATABRICKS_HOST=https://1262887251702944.4.gcp.databricks.com
export DATABRICKS_TOKEN=<user's PAT>
export DATABRICKS_CLUSTER_ID=5428-215533-4jodkdfs
```
Or via `~/.databrickscfg`:
```
[DEFAULT]
host = https://1262887251702944.4.gcp.databricks.com
token = <user's PAT>
cluster_id = 5428-215533-4jodkdfs
```

## Connection options

### Option 1: Databricks Connect (recommended for interactive analytical work)

Runs Spark code locally; computation happens on the cluster. Limitation:
not all Spark APIs supported. Fine for our use case (DataFrame reads,
filters, aggregations, simple joins).

```bash
pip install --user databricks-connect==<cluster-runtime-version>
```

Match the runtime version to the cluster (check in Databricks UI →
Compute → cluster → Configuration). Typical: `15.4.0` for runtime 15.x.

Then in Python:
```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(
    host=os.environ['DATABRICKS_HOST'],
    cluster_id=os.environ['DATABRICKS_CLUSTER_ID'],
    token=os.environ['DATABRICKS_TOKEN']
).getOrCreate()
```

### Option 2: `databricks-sql-connector` (SQL-only, requires SQL Warehouse — not this cluster)

Different product. Use for SQL Warehouses, not classic compute clusters.
Skip unless we get a SQL Warehouse path from Victor.

### Option 3: Submit jobs via Databricks Jobs API

Fully decoupled — submit a notebook, fetch result via REST. More setup
overhead but most production-like. See `airflow_ti` pattern.

## Cluster modify if needed

Victor: "you should be able to modify the compute if you need to adjust
nodes/settings or extra libraries"

Useful upgrades for our heaviest scans:
- More worker nodes (autoscale max higher) for the augmentor scan
- Photon-enabled runtime if available
- Adjust `spark.executor.memory` for the row-heavy Parquet scans

For long runs, switch to **Job compute** instead (3× cheaper than
interactive cluster — see `knowledge/data_knowledge.md`).

## Quickstart smoke test (after auth setup)

```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(
    host=..., cluster_id=..., token=...
).getOrCreate()

# Smoke: count rows in a tiny BQ dim table via the connector
df = (spark.read.format("bigquery")
    .option("parentProject", "dw-main-bronze")
    .option("billingProject", "dw-main-bronze")
    .option("project", "dw-main-bronze")
    .load("dw-main-bronze.integrationprod.campaigns")
    .filter("deleted = FALSE AND is_test = FALSE")
)
print(df.count())   # should be ~507k
```

```python
# Smoke: scan one day of augmentor from GCS with explicit partition filter
df = (spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/")
    .filter("year = '2026' AND month = '04' AND day = '23'")
    .filter("ip IS NOT NULL AND ip != '0.0.0.0'")
    .select("ip")
    .distinct()
)
print(df.count())  # baseline against BQ scan time
```
