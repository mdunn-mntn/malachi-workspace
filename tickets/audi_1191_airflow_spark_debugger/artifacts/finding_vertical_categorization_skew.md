# Optimization finding: `Update Vertical Categorization` chronic Spark skew

**What:** the AUDI-1191 optimizer crawled real production Spark event logs
(`gs://mntn-data-archive-prod/spark-events`) and flagged the `Update Vertical Categorization` model as
the #1 optimization target.

**The problem:** **Stage 0 is severely data-skewed on every run** — max task time is **up to 242x the
median** (and 10-20x on the other runs). One partition holds nearly all the data, so one task runs while
the rest sit idle, stretching wall-clock. The job also shows GC pressure (executors memory-starved).

**The fix (code):**
- Salt the skewed group/join key, or enable AQE skew join: `spark.sql.adaptive.enabled=true` +
  `spark.sql.adaptive.skewJoin.enabled=true`. A plain repartition will NOT fix a value-skewed key.
- Secondary: raise executor memory (or cut per-task data with more partitions) for the GC pressure.

**Impact:** skew this severe means most of the cluster is idle for most of the stage — a direct
wall-clock + cost win once the hot key is spread.

**Owner:** vertical-categorization model owner (Sean Yang / DDP). Backlog: IMP-024.

*(Found autonomously by the tool from production event logs — the first real optimization it surfaced.)*
