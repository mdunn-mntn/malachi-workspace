# Victor Savitskiy + Dustin Niehoff — Slack on Spark BQ-Connector silver-table read pattern

**Date:** 2026-04-30 (morning, response to my 2026-04-29 query about scratch BQ dataset for Spark)
**Context:** TI-837 Phase 0 Spark port hit two BQ-connector schema-resolver walls (`INTERVAL` column + wide `BIGNUMERIC(76)`) on `dw-main-silver.logdata.cost_impression_log`. Asked both Victor (compute/Databricks) and Dustin (BQ/dplat) for the canonical workaround.

## Victor (compute/Databricks angle)

> can you share code part where you read the tables?
>
> Workaround I know: viewsEnabled=true + query mode + materializationDataset — push SELECT down to BQ so the connector only sees the result schema. But that needs a writable BQ dataset for materialization.yes it should work. in code I've provided those settings set already:
>
> ```python
> (spark.read
>     .format("bigquery")
>     .option("parentProject", "dw-main-bronze")  # it is important to set all 3 project related properties!!
>     .option("billingProject", "dw-main-bronze")
>     .option("project", "dw-main-bronze")
>     .option("materializationDataset", "external")
>     .option("viewsEnabled", "true")
>     .load("""table_or_query_here""")
> )
> ```
>
> so temporary results will be saved into dw-main-bronze.external
>
> just instead of table, you need to specify query:
> `.load("""select col_1, cast(col_2 as int) from some_table_name""")`
>
> [later]
>
> yeah, interval is tricky, but following should work:
>
> ```python
> spark.read
>     .format("bigquery")
>     .option("viewsEnabled", "true")
>     .option("parentProject", "dw-main-bronze")  # it is important to set all 3 project related properties!!
>     .option("billingProject", "dw-main-bronze")
>     .option("project", "dw-main-bronze")
>     .option("materializationDataset", "external")
>     .option("bigNumericDefaultPrecision", "38")
>     .option("bigNumericDefaultScale", "9")
>     .load("""
>         select * except(recency_elapsed_time), current_date as anchor_date, current_date + recency_elapsed_time as recency_elapsed_time_tmp from dw-main-silver.logdata.cost_impression_log
>         where recency_elapsed_time is not null  limit 5
>           """)
>     .withColumn("recency_elapsed_time", F.expr("recency_elapsed_time_tmp - anchor_date "))
>     .drop("recency_elapsed_time_tmp","anchor_date")
> ```
>
> this could be workaround for interval type incompatibility. with subsequent substraction within spark
>
> [followup]
>
> I think you've mentioned it, but could you please expand on use case of implementing it in spark vs bigquery?

**Three takeaways:**
1. `dw-main-bronze.external` is the sanctioned materialization dataset (Terragrunt-managed, exists, write access verified).
2. **All three project-related options must be set.** Setting only `parentProject` + `billingProject` is insufficient — `project` is also required. Missing any of the three can cause silent failures or wrong-quota billing.
3. INTERVAL workaround: cast it to a date diff in the BQ-side query, drop the original column, reconstruct in Spark via subtraction. (For our use case we just project it away — we don't need `recency_elapsed_time`.)

**Open question Victor asked:** "could you please expand on use case of implementing it in spark vs bigquery?" — i.e., why Spark when BQ works. Answer to share: BQ's 6-hour query timeout killed two of our v5 30-adv 7-day xwin attempts (4-segment variant). Spark on Databricks would let us split the work across more parallel slots without that hard wall, AND lets us read augmentor + guid from GCS directly (no BQ scan billing on those scans). The lift methodology was always BQ-first, Spark-second; the Spark port unblocks Phase 2a (conversions, 30-day window) which the BQ augmentor 10-day TTL prevents.

## Dustin (BQ / dplat angle)

> The workaround is correct, and you're correct that you need to set an option. Dataproc has generated the bucket you want to use already that should work with databricks:
>
> `.option("temporaryGcsBucket", "dataproc-temp-us-central1-754673906299-me0b3bsh")`
>
> Then you can write BQ queries that will read spark-friendly datasets into that bucket that you can then operate on. The datatypes of the source tables are up to the data team that own them, not us, but I do know at the very least that the INTERVAL type is necessary for their pipelines. If you need the interval type, it's a parseable string that you can convert. We actually had to do that to dump from coredw into GCS, and then from GCS into BQ (though the latter has a pretty good function for us)

**Two takeaways:**
1. Alternative: `temporaryGcsBucket=dataproc-temp-us-central1-754673906299-me0b3bsh`. Materializes results to GCS instead of BQ. Both approaches valid — Victor's BQ-side materialization is simpler in our pipeline.
2. INTERVAL is here to stay (data team needs it for their pipelines). Long-term schema changes to silver are out of scope.

## Resolution

Both responses validate the same approach: `viewsEnabled=true` + `query` mode + materialization. Differences are in WHERE the materialization lives — BQ dataset (Victor) vs GCS bucket (Dustin). We use Victor's pattern (`materializationDataset=external` in `dw-main-bronze`) because it's lighter-weight in our pipeline (no GCS hop).

**Documented in:**
- `.claude/databricks_setup.md` — full canonical pattern with code
- `knowledge/data_knowledge.md` — "Spark BQ connector — silver-table type quirks" section
- `tickets/.../artifacts/spark_lift_3adv_1day.py` — script updated to use the Victor pattern

**To-do:**
- Reply to Victor's "use case" question. Draft answer above.
