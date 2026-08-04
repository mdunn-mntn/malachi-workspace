# Turning Spark event logging ON — steps (from the Ryan meeting, 2026-08-04)

Event logging is **off right now** (Ryan turned it off after the Nov-2025 test). This is how to turn it
back on so the optimizer gets live fuel, per Ryan's walk-through.

**STATUS: PR OPEN — [SteelHouse/airflow-ti#1169](https://github.com/SteelHouse/airflow-ti/pull/1169)**
(Dataproc eventLog + PHS removal + the 2 extras, one PR; awaiting Ryan review/merge, 2026-08-04). The
Databricks half (below) and the TTL are still separate/pending.

## Dataproc (GCP) — the main path

**Key constraint (Ryan):** you CANNOT run `spark.eventLog.enabled=true` AND the persistent history
cluster on the same batch — Dataproc throws an error. Pick one. Both produce readable `.zstd` event logs
(the PHS is really just aggregating the same logs + serving a Spark UI — "the route you're going down is
your own AI Spark UI").

**Recommended: `spark.eventLog.enabled` → `spark-events`** (lighter; my crawler already reads that prefix).
**How PR #1169 actually did it** (better than per-model: there are 72 `@compute.dataproc_batch` models):
1. **Central injection in the batch operator** — `ModelPysparkBatchOperator.execute` (`include/models/operators.py`)
   already injects `MNTN_RUNTIME_ENV` per submit; added the 4 eventLog props there so all 72 decorator models
   inherit them. **Env-aware dir** `gs://mntn-data-archive-{prod|dev}/spark-events` (dev logs stay out of the prod
   crawl). **Kill switch** Variable `SPARK_EVENT_LOG_ENABLED=false` disables fleet-wide with no revert.
2. **ipdsc/tpa raw path** (`ipdsc_emr_cluster.py`, not decorator-based) — added the same 4 props to `get_config`
   (env-threaded) AND **removed the `peripherals_config.spark_history_server_config` (PHS)**, the only live PHS
   attachment (audience_intent already commented). That is the mutual-exclusion resolution.
3. GCS write perm already set (Ryan). **Not in the PR (follow-ups):** audience_intent raw batch, the 3
   `dataproc_workflow` templates, Databricks (below).

**Alternative Ryan floated:** instead of writing+reading event logs, point the tool at the History Server
URL and pull what it needs from there. (More infra; the eventLog route is simpler for the optimizer.)

## Databricks — more work (Ryan: "you've got some work ahead")

Databricks job clusters don't persist event logs; the Databricks user likely can't write to the GCS
folder either. Ryan's path (skip waiting on DevOps):
1. Enable the setting in the cluster config (`cluster_log_conf` / event-log delivery) and **just run it,
   see what error pops out.**
2. If it needs a GCS-write permission, **have Cursor build a PR against the mountain-devops repo** for
   that permission and send it to **Christina** to approve ("DevOps is the new DBAs — come with a PR, she
   approves, it self-deploys"). Don't file a blocking DevOps ticket first.
3. If GCS delivery is blocked, fall back to giving the tool the Databricks History Server URL to read from.

## The 2 extras (Ryan) — DRAFTED, see `audi_1191_basemodel_observe_patch.md`
- Both methods **already exist** in `utils_model/spark_job_monitor.py` (`log_execution_plan`,
  `log_script_content`). The gap was that nothing calls them unless a model wires a monitor by hand.
- **`log_execution_plan(df)`** — the explain plan "tells you a lot" (Optimized/Analyzed + missing-stats
  advisory the raw event log lacks). I already pull the physical plan from the event log; this adds the rest.
- **`log_script_content(__file__)`** — **for version tracking**: once someone applies the tool's
  recommendation and re-runs, they need to know which script version produced which events/recs.
- **Patch (draft):** invoke the existing monitor **once** from the shared `df_write` path via a guarded
  `BaseModel._observe_output(df)` helper + one line per concrete `df_write`. Zero model-file edits, cannot
  fail a write, `MNTN_SPARK_OBSERVE=0` off-switch. Bundle into the step-#1 eventLog PR. Full diff in
  `audi_1191_basemodel_observe_patch.md`.

## Housekeeping
- **TTL:** `Delete age 30` on `spark-events/` — Ryan approved. I lack `storage.buckets.update`, so Ryan/an
  admin applies it (exact rule staged: `scratchpad/lifecycle_proposed.json`, preserves all 7 existing rules).
- **Delete old logs:** Ryan ok'd deleting everything in `spark-events` incl. `.inprogress` (also note he
  started nesting some in subfolders). Keep a couple for testing (already have 13 local copies).

## Future (Ryan)
- **Scala Spark** — the identity team uses Scala Spark. Event logs are engine-agnostic, so the parser
  should already work on Scala Spark logs; verify when convenient. PySpark first (what most teams use).
- **Adoption stays low-key** — no big ticket; finish + test, then share it with the team ("hey, I've got
  this cool thing"). Broader use later = MCP tool / API-key automation so anyone can run it (Ryan's
  "base camp" idea).
