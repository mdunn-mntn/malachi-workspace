# MM-44: IPDSC Household Discrepancy Investigation

**Ticket:** MM-44 / BER-1993 / PS-7345
**Investigator:** Malachi (Attribution & Identity)
**Date:** 2026-02-26
**Status:** RESOLVED — Three root causes confirmed. Primary driver for cgid 107428: retargeting campaigns are not limited to the user-defined audience, but enrichment validates against it anyway.

---

## 1. Problem Statement

Households reached (HH) counts differ between two reporting paths:

- **All Facts** (`summarydata.all_facts`) — counts all valid impressions from `cost_impression_log`. No audience/category matching required.
- **Segment Reporting** (`summarydata.category_facts`) — counts only impressions that survive an inner join with IPDSC (IP → Data Source Category). Impressions that fail the match are excluded.

**Example discrepancy (cgid 107428, 2026-02-12):**

| Metric | Value |
|--------|-------|
| Advertiser | 32771 |
| Campaign Group | 107428 |
| Data Source IDs | [4, 14] |
| Category IDs | [1, 9119, 19324] |
| All Facts HH | 17,589 |
| Segment HH | 5,944 |
| **Delta** | **11,645 (66.2% drop)** |

This is not a small HLL rounding error — it's a systemic pipeline gap affecting **2,302 campaign groups**.

---

## 2. How the Two Reporting Paths Work

### All Facts Path (no IPDSC)
```
cost_impression_log → impression_facts → all_facts → Reporting
```
- Filters: `unlinked = false`, `ad_served_id IS NOT NULL`
- HH identifier: IP when `channel_id = 8` or `objective_id IN (5,6)`, else GUID
- **No IPDSC join, no block list, no lookback constraints**

### Category Facts Path (requires IPDSC)
```
cost_impression_log → Impression Enrichment (IPDSC inner join) → enriched_impressions → category_facts → Reporting
```
- Same source filters as All Facts
- **Then joins against IPDSC** — this is where impressions get dropped

The `category_facts` job reads from `enriched_impressions`; it does not perform the IPDSC join itself — that happens in **Impression Enrichment** (`impression_enrichment.py`).

### Aylwin's Proof: Divergence Happens at Stage 2

Aylwin demonstrated this with advertiser 34611 (cgid 93373, 2026-02-12):

**Stage 1 — Before IPDSC (matches all_facts):**
```python
summary = impressions_with_categories
    .filter(col("hour").between(lit("2026-02-12 00:00:00"), lit("2026-02-12 23:59:59")))
    .groupBy(to_date("hour").alias("date"), "advertiser_id", "campaign_group_id")
    .agg(
        count_distinct(get_ip_or_guid()).alias("hh"),
        collect_set("data_source_id").alias("dsid"),
        flatten(collect_set("data_source_category_id")).alias("dscid_candidates")
    )

# Result:
# CG 93373: hh=289,466, dsid=[1, 19, 13, 35, 14]
# CG 56957: hh=88,804,  dsid=[2, 14]
```

**Stage 2 — After IPDSC inner join (lower than all_facts):**
```python
# Result:
# CG 93373: hh=282,924, dsid=[19, 1, 13, 35]  ← DS 14 blocked, 6,542 HH dropped
# CG 56957: DISAPPEARED entirely              ← DS [2, 14] both blocked
```

---

## 3. How Impression Enrichment Works (from `impression_enrichment.py`)

### IPDSC Loading
IPDSC is loaded with a **block list** that excludes data sources 2, 14, and 42:

```python
# impression_enrichment.py — IPDSC never loads blocked dsids
ipdsc = (
    spark.read.parquet(config.pipeline_params["ipdsc_path"])
    .filter(
        is_within_lookback(col("dt"), end_date, total_ipdsc_lookback)
        & col("data_source_category_ids").isNotNull()
        & ~col("data_source_id").isin(config.pipeline_params["dsid_block_list"])  # [2, 14, 42]
    )
)
```

### The Inner Join (where HH gets dropped)
```python
# impression_enrichment.py — inner join drops impressions without IPDSC match
enriched_impressions = (
    impressions_with_categories.join(
        all_ip_categories, on=["ip", "data_source_id"], how="inner"  # drops non-matches
    )
    .withColumn(
        "data_source_category_id",
        array_intersect("data_source_category_id", "data_source_category_ids"),
    )
    .filter(
        col("data_source_category_id").isNotNull()
        & (size(col("data_source_category_id")) > 0)  # drops empty intersection
    )
    .filter(
        is_oracle_data_source()
        | (col("ipdsc_dt").between(
            to_date(col("time")) - expr("INTERVAL 35 DAYS"),
            col("time"),
        ))  # 35-day lookback for non-Oracle
    )
)
```

Key mechanisms that cause HH drop:
1. **Block list** — DS 2, 14, 42 are excluded from IPDSC load entirely
2. **Inner join** — impressions without an IPDSC match on `[ip, data_source_id]` are dropped
3. **Empty intersection** — even if an IP is in IPDSC for that DS, the specific category IDs must overlap
4. **35-day lookback** — IPDSC record must be within 35 days of the impression

---

## 4. The Specific Case: cgid 107428

### KEY FINDING: This Is a Retargeting Campaign

Zach confirmed (2026-02-26) that **cgid 107428 is a retargeting campaign**. Retargeting campaigns are **not limited to the user-defined audience** — they serve to IPs based on prior engagement with the advertiser, regardless of the interest segments defined in `audience.audiences`.

> **Zach:** "that is a retargeting campaign. retargeting is not limited to the user defined audience"

This is the critical insight that resolves the investigation. The two audience expressions represent different things:
- `audience.audiences` (DS 4) = the user-defined **prospecting** audience (interest categories)
- `audience.audience_segments` (DS 2/14/16) = the **retargeting** expression (behavioral/engagement-based)

For retargeting, the expressions are NOT expected to resolve to the same IPs. A retargeted IP may never have been in the DS 4 interest segment — it was served to because of prior engagement, not because of interest category membership.

### ADDITIONAL FINDING: This Is a Legacy Campaign in a Disallowed State

Zach further clarified (2026-02-26) that a retargeting campaign targeting only DS 4 is **no longer an allowable state**. DS 4-only audiences should only ever be applied to prospecting campaigns. This means cgid 107428 is a **legacy campaign** — it was created before this constraint was enforced and persists in a configuration that the system would no longer permit for new campaigns.

> **Zach:** "since this is a retargeting campaign that is only targeting ds4, that is technically not an allowable state anymore"
> **Zach:** "this must be a legacy campaign"
> **Zach:** "ds 4 only audiences should only ever be applied to prospecting campaigns"

This adds a fourth dimension to the root cause: **the campaign itself is misconfigured by modern standards.** The system now prevents this state for new campaigns, but legacy campaigns like cgid 107428 were never migrated or flagged. Any remaining legacy retargeting campaigns with DS 4-only audience expressions will exhibit the same discrepancy.

### Two Audience Expressions

This campaign has **two different audience expressions** from two different tables. Per Jordan Piepkow: `audience.audiences` is like a template; `audience.audience_segments` is what membership DB actually uses for targeting.

**Only `audience.audience_segments` is used for targeting.** For retargeting campaigns, this expression serves to IPs based on prior engagement, not the user-defined interest audience.

**From `audience.audiences` (NOT used for targeting):**
```json
{
  "interest": {
    "include": [{"or": [{"data_source_id": 4, "cats": [9119, 19324]}]}],
    "exclude": [{"or": [{"data_source_id": 4, "cats": [16603]}]}]
  }
}
```

**From `audience.audience_segments` (USED for targeting):**
```json
{
  "categories": {
    "where": {
      "op": "and",
      "value": [
        {"op": "any", "value": {"data_source_id": 2, "category_ids": [171180]}},
        {"op": "any", "value": {"data_source_id": 14, "category_ids": [1]}},
        {"op": "not", "value": {"op": "any", "value": {"data_source_id": 16, "category_ids": [3993, 823101]}}}
      ]
    }
  }
}
```

**Summary of actual targeting expression:**

| Role | Data Source | Categories | Description |
|------|-----------|------------|-------------|
| Include (AND) | DS 2 (OPM) | 171180 | Real-time segment |
| Include (AND) | DS 14 | 1 | IPDSC-based |
| Exclude | DS 16 (MNTN events) | 3993, 823101 | Real-time |

The DS 4 expression (cats 9119, 19324) from `audience.audiences` was **never used to target this IP**. This is expected for retargeting — the IP was served to because of prior engagement, not because it matched interest categories 9119/19324.

### Queries Used to Retrieve Expressions
```sql
-- audience.audiences (template — NOT used for targeting)
SELECT *
FROM audience.audiences
WHERE advertiser_id = 32771
AND audience_id = 55994;

-- audience.audience_segments (actual targeting expression)
SELECT *
FROM audience.audience_segments
WHERE campaign_id = 525820
AND expression_type_id = 2;
```

### CIL Confirmation (impression exists)
```sql
SELECT *
FROM logdata.cost_impression_log cil
JOIN public.campaigns c
  ON c.campaign_id = cil.campaign_id
WHERE cil.time >= '2026-02-12'
  AND cil.time <  '2026-02-13'
  AND c.campaign_group_id = 107428
  AND ip = '8.41.17.75';

-- Result: 1 row
-- advertiser_id: 32771, campaign_id: 525820, audience_id: 55994
-- time: 2026-02-12 11:01:22
```

---

## 5. IPDSC Investigation Results

### What Exists in IPDSC for This IP

Checked IP `8.41.17.75` across a 31-day lookback (2026-01-22 to 2026-02-21) for all data sources in the audience expression.

**PySpark query used:**
```python
from datetime import datetime, timedelta
import pyspark.sql.functions as F

end_date = datetime(2026, 2, 21).date()
IP = '8.41.17.75'
LOOKBACK_DAYS = 31
IPDSC_BASE = "gs://mntn-data-archive-prod/ipdsc"

INCLUDES = {
    4: [9119, 19324],
    2: [171180],
    14: [1],
}
EXCLUDES = {
    4: [16603],
    16: [3993, 823101],
}

ALL_DS_IDS = set(INCLUDES.keys()) | set(EXCLUDES.keys())

ipdsc_dfs = []
for i in range(LOOKBACK_DAYS):
    d = end_date - timedelta(days=i)
    for ds_id in ALL_DS_IDS:
        path = f"{IPDSC_BASE}/dt={d.isoformat()}/data_source_id={ds_id}"
        try:
            day_df = (
                spark.read.parquet(path)
                .filter(F.col("ip") == IP)
                .withColumn("date", F.lit(d.isoformat()).cast("date"))
                .withColumn("data_source_id", F.lit(ds_id))
            )
            ipdsc_dfs.append(day_df)
        except Exception as e:
            print(f"DS {ds_id}: partition not found - {e}")

if not ipdsc_dfs:
    print("No data found.")
else:
    all_ipdsc = ipdsc_dfs[0]
    for df in ipdsc_dfs[1:]:
        all_ipdsc = all_ipdsc.unionByName(df, allowMissingColumns=True)

    for ds_id, cats in INCLUDES.items():
        cat_array = F.array([F.lit(c) for c in cats])
        col_name = f"incl_ds{ds_id}_{'_'.join(str(c) for c in cats)}"
        all_ipdsc = all_ipdsc.withColumn(
            col_name,
            F.when(
                (F.col("data_source_id") == ds_id) &
                F.arrays_overlap(F.col("data_source_category_ids"), cat_array),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

    for ds_id, cats in EXCLUDES.items():
        cat_array = F.array([F.lit(c) for c in cats])
        col_name = f"excl_ds{ds_id}_{'_'.join(str(c) for c in cats)}"
        all_ipdsc = all_ipdsc.withColumn(
            col_name,
            F.when(
                (F.col("data_source_id") == ds_id) &
                F.arrays_overlap(F.col("data_source_category_ids"), cat_array),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

    display(all_ipdsc.orderBy("date", "data_source_id"))
```

### Results

| Data Source | Categories Checked | In IPDSC? | Notes |
|-------------|-------------------|-----------|-------|
| DS 14, cat 1 | Include | Present every day | `[1, 150]` |
| DS 4, cats 9119/19324 | Include | Never present | IP has hundreds of DS 4 categories, but NOT 9119 or 19324 |
| DS 2, cat 171180 | Include | Partition does not exist | `PATH_NOT_FOUND` for `data_source_id=2` |
| DS 16, cats 3993/823101 | Exclude | Partition does not exist | `PATH_NOT_FOUND` for `data_source_id=16` |
| DS 4, cat 16603 | Exclude | Not present | Expected — this is an exclude |

**Key findings:**
- **DS 2 and DS 16 have NO partitions in IPDSC at all.** They are real-time data sources.
- **DS 4 IS in IPDSC** for this IP with hundreds of categories — but NOT the specific cats (9119, 19324) required by the `audience.audiences` expression.
- **DS 14 is in IPDSC** and matches — but DS 14 is on the enrichment block list `[2, 14, 42]`, so it gets filtered out before the join.

### Extended Verification: 38-Day Window (per Zach's Feedback)

Zach noted that targeting works on 30 days of data, not a single day. We re-ran the check across a 38-day window (30 days before + 7 days after the impression date) for DS 4 cats 9119/19324:

```python
from datetime import datetime, timedelta
import pyspark.sql.functions as F

impression_date = datetime(2026, 2, 12).date()
LOOKBACK_DAYS = 30
FORWARD_DAYS = 7
IP = '8.41.17.75'
IPDSC_BASE = "gs://mntn-data-archive-prod/ipdsc"
DS_ID = 4
TARGET_CATS = [9119, 19324]

start_date = impression_date - timedelta(days=LOOKBACK_DAYS)
end_date = impression_date + timedelta(days=FORWARD_DAYS)
# Window: 2026-01-13 to 2026-02-19

ipdsc_dfs = []
for i in range((end_date - start_date).days + 1):
    d = start_date + timedelta(days=i)
    path = f"{IPDSC_BASE}/dt={d.isoformat()}/data_source_id={DS_ID}"
    try:
        day_df = (
            spark.read.parquet(path)
            .filter(F.col("ip") == IP)
            .withColumn("dt", F.lit(d.isoformat()).cast("date"))
            .withColumn("data_source_id", F.lit(DS_ID))
        )
        ipdsc_dfs.append(day_df)
    except Exception as e:
        print(f"dt={d}: partition not found - {e}")

if not ipdsc_dfs:
    print("No data found.")
else:
    all_ipdsc = ipdsc_dfs[0]
    for df in ipdsc_dfs[1:]:
        all_ipdsc = all_ipdsc.unionByName(df, allowMissingColumns=True)

    cat_array = F.array([F.lit(c) for c in TARGET_CATS])
    result = all_ipdsc.withColumn(
        "has_9119_19324",
        F.arrays_overlap(F.col("data_source_category_ids"), cat_array)
    ).withColumn(
        "num_categories",
        F.size(F.col("data_source_category_ids"))
    ).select("dt", "ip", "data_source_id", "num_categories", "has_9119_19324")

    display(result.orderBy("dt"))
```

**Result: `has_9119_19324 = false` on all 38 days.** The IP has 180-271 DS 4 categories every day, but cats 9119 and 19324 never appear in any of them.

| Date Range | DS 4 Categories | Has 9119 or 19324? |
|------------|-----------------|-------------------|
| 2026-01-13 to 2026-01-24 | 235-271 per day | false (all days) |
| 2026-01-25 to 2026-01-30 | 180-182 per day | false (all days) |
| 2026-01-31 to 2026-02-19 | 239-269 per day | false (all days) |

### Sanity Check: Query Logic Verified

To confirm the search logic works correctly, we searched for a category we could see in the raw output (cat 4528):

```python
ipdsc.select(
    F.col("ip"),
    F.size(F.col("data_source_category_ids")).alias("num_cats"),
    F.arrays_overlap(F.col("data_source_category_ids"), F.array(F.lit(4528))).alias("has_4528_SHOULD_BE_TRUE"),
    F.arrays_overlap(F.col("data_source_category_ids"), F.array(F.lit(9119), F.lit(19324))).alias("has_9119_19324_SHOULD_BE_FALSE"),
    F.array_contains(F.col("data_source_category_ids"), 9119).alias("has_9119"),
    F.array_contains(F.col("data_source_category_ids"), 19324).alias("has_19324"),
).show(truncate=False)
```

| ip | num_cats | has_4528 | has_9119_19324 | has_9119 | has_19324 |
|----|----------|----------|---------------|----------|-----------|
| 8.41.17.75 | 242 | **true** | **false** | false | false |

The search correctly finds categories that exist (4528 = true) and correctly reports that 9119 and 19324 do not exist (false both individually and via `arrays_overlap`). The query logic is verified.

---

## 6. What Zach Schoenberger Clarified

From Slack (`#data-platform` and `#midmarket-households-reached-discrepancy`):

> "DS 2 are OPM segments. they are based on a different expression applied to the string pageview and conversion data"

> "DS 16 is a similar thing: its based on MNTN events. they are logged. they could be put in IPDSC but again theres been no reason to yet"

> "DS 2/16 are datasets that are generated real time. not through IPDSC exporting"

> "also DS 1, 18, and 35 are not real time. they are ipdsc based."

> "they [TI] should start adding ds 16 and ds 2 into IPDSC (not export though) for this to be viable"

So:
- **DS 2 (OPM)** — real-time, never in IPDSC. TI needs to add it.
- **DS 16 (MNTN events)** — real-time, never in IPDSC. TI needs to add it.
- **DS 1, 18, 35** — ARE in IPDSC (discrepancies there need separate investigation)
- **IPDSC is owned by TI** (Alyson, Victor)

### Zach's Position on Audience Tables

Zach maintains that reporting should use `audience.audiences` (the user-defined template, DS 4) rather than `audience.audience_segments` (the generated segment). His rationale: DS 14 is on the block list as an exclusion because "you're already going from IPs served backwards to generate data on them" — DS 14 is targeting infrastructure, not a user-facing reportable segment.

For **prospecting** campaigns, DS 4 and DS 2/14/16 should resolve to the same IPs. For **retargeting** campaigns (like cgid 107428), they do NOT — retargeting serves to IPs based on prior engagement, which is a broader pool than the user-defined interest audience.

### Retargeting vs Prospecting: Why the Expressions Diverge

Zach confirmed the critical distinction (2026-02-26):

> "that is a retargeting campaign. retargeting is not limited to the user defined audience"

This means for cgid 107428 specifically, the mismatch between `audience.audiences` (DS 4 cats 9119/19324) and `audience.audience_segments` (DS 2/14/16) is **expected behavior**. The IP `8.41.17.75` was legitimately served to via retargeting, but it was never a member of the DS 4 interest segment. The enrichment pipeline doesn't distinguish between prospecting and retargeting — it validates ALL impressions against the same DS 4 categories from `campaign_segment_history`, causing retargeted IPs to fail the IPDSC join.

### Ray's Question: DS 4 in IPDSC (RESOLVED)

Ray raised a key historical point:

> "I'm not seeing an explicit answer for whether DS4 should be in IPDSC. I remember way back when it was still in the DW, we had a special inclusion of DS4 data for the export as it was not in IPDSC."

**ANSWERED:** DS 4 IS natively in IPDSC. A global BQ query found **327,295 IPs** with cats 9119/19324 in IPDSC on 2026-02-12. DS 4 is populated and authoritative. The issue is not that DS 4 is missing from IPDSC — it's that retargeted IPs are not required to be in the DS 4 interest segment.

---

## 7. Root Cause #1: Enrichment Validates Retargeting Impressions Against Prospecting Audience (CONFIRMED)

### Discovery

Aylwin confirmed the enrichment pipeline uses `summarydata.v_campaign_group_segment_history` to map campaign groups to data source categories. This view is built from `audience.campaign_segment_history`:

```sql
-- View definition: summarydata.v_campaign_group_segment_history
SELECT c.campaign_group_id,
    h.audience_id,
    h.start_time,
    COALESCE(h.end_time, '9999-12-31 00:00:00'::timestamp without time zone) AS end_time,
    h.data_source_id,
    array_agg(h.data_source_category_id ORDER BY h.data_source_category_id) AS data_source_category_id,
    json_agg(json_build_object('data_source_category_id', h.data_source_category_id, 'and_seq', h.and_seq, 'or_seq', h.or_seq)) AS category_info
   FROM audience.campaign_segment_history h
     JOIN campaigns c USING (campaign_id)
  GROUP BY c.campaign_group_id, h.audience_id, h.start_time,
           (COALESCE(h.end_time, '9999-12-31 00:00:00'::timestamp without time zone)),
           h.data_source_id;
```

### Raw Data from `audience.campaign_segment_history`

```sql
SELECT campaign_id, audience_id, start_time, end_time, data_source_id,
       data_source_category_id, and_seq, or_seq
FROM audience.campaign_segment_history
WHERE campaign_id IN (
    SELECT campaign_id FROM public.campaigns WHERE campaign_group_id = 107428
)
ORDER BY data_source_id, data_source_category_id;
```

| campaign_id | audience_id | data_source_id | data_source_category_id | and_seq | or_seq | Source |
|-------------|-------------|---------------|------------------------|---------|--------|--------|
| 525816 | 55994 | 4 | 9119 | 1 | 1 | `audience.audiences` (template) |
| 525816 | 55994 | 4 | 19324 | 1 | 1 | `audience.audiences` (template) |
| 525816 | 55994 | 14 | 1 | 2 | 1 | `audience.audience_segments` (actual) |

The history table is **blending both audience tables**:
- **DS 4 cats 9119/19324** come from `audience.audiences` (the template — never used for targeting)
- **DS 14 cat 1** comes from `audience.audience_segments` (the actual targeting expression)
- **DS 2 cat 171180 is completely absent** (real-time, never written to history)
- **DS 16 cats 3993/823101 are completely absent** (real-time, never written to history)

### The Contamination Chain

```
audience.audiences (template, DS 4)  ─┐
                                      ├→ campaign_segment_history (DS 4 + DS 14)
audience.audience_segments (DS 14)  ──┘         │
                                                ▼
                                   v_campaign_group_segment_history
                                                │
                                                ▼
                                   impression_enrichment.py (Stage 1)
                                                │
                                                ▼
                                   IPDSC inner join on [ip, data_source_id]
                                                │
                                   DS 14 → blocked by [2, 14, 42]
                                   DS 4  → cats 9119/19324 not in IPDSC for IP
                                                │
                                                ▼
                                       IMPRESSION DROPPED
```

### Blast Radius: 2,302 Campaign Groups

```sql
-- Campaign groups where campaign_segment_history contains DS IDs
-- not in the actual targeting expression (audience.audience_segments)
SELECT count(DISTINCT c.campaign_group_id) AS affected_cgs
FROM audience.campaign_segment_history h
JOIN public.campaigns c USING (campaign_id)
WHERE h.data_source_id NOT IN (
    SELECT DISTINCT data_source_id
    FROM audience.audience_segments s
    WHERE s.campaign_id = h.campaign_id
    AND s.expression_type_id = 2
);

-- Result: 2,302 campaign groups
```

**2,302 campaign groups** have data source IDs in `campaign_segment_history` that don't exist in their actual targeting expressions. This is systemic, not an edge case — and likely explains Ray's observation of 50% miss rates.

### What Needs to Be Fixed

The core issue is that `campaign_segment_history` blends both `audience.audiences` (prospecting template) and `audience.audience_segments` (actual targeting) into one table, and the enrichment pipeline has no way to distinguish between them. For retargeting campaigns, this means:

1. The enrichment pipeline tries to validate retargeted IPs against DS 4 interest categories they were never required to match
2. The only DS that could match (DS 14) is blocked
3. DS 2/16 from the actual targeting expression are missing entirely

**Possible fixes:**
- The enrichment pipeline needs to be aware of campaign type (prospecting vs retargeting) and use the appropriate validation
- OR `campaign_segment_history` writer needs to source exclusively from `audience.audience_segments`, not `audience.audiences`
- OR the enrichment join logic needs to change (e.g., left join instead of inner join for retargeting campaigns)

The writer is application-level code (no DB triggers). **Owner needs to be identified by BER team.**

---

## 8. Root Cause #2: IPDSC Missing Real-Time Data Sources (CONFIRMED)

DS 2 (OPM) and DS 16 (MNTN events) are real-time data sources that have **zero partitions in IPDSC**. Any campaign that targets via these data sources will have those impressions dropped at the enrichment inner join because there is nothing in IPDSC to match against.

### What Needs to Be Fixed

TI (Alyson, Victor) needs to add DS 2 and DS 16 to IPDSC. Per Zach: "not export though" — these are real-time data sources that can't go through the existing IPDSC export pipeline. TI needs to build a separate mechanism.

### Related: DS 14 Block List

DS 14 is on the enrichment block list `[2, 14, 42]` but **exists in IPDSC** with valid records (IP `8.41.17.75` has DS 14 records with `[1, 150]` every day). DS 14 appears in actual targeting expressions. If DS 14 is a legitimate IPDSC-based data source, blocking it causes valid impressions to be dropped.

**Open question:** Why is DS 14 on the block list? Should it be removed?

---

## 9. Why the Enrichment Pipeline Fails for cgid 107428

The enrichment pipeline gets `data_source_ids: [4, 14]` from `campaign_segment_history` (via `v_campaign_group_segment_history`) at Stage 1. This is a blend of both audience expressions.

Then at the IPDSC join:
1. **DS 14 is blocked** (block list `[2, 14, 42]`) → cat 1 can't be matched
2. **DS 4 cats 9119/19324 are not in IPDSC** for this IP → inner join fails
3. **DS 2 and DS 16 don't exist in IPDSC at all** → never even in the history table

Result: the impression gets dropped from `enriched_impressions` and never makes it to `category_facts`.

**This is expected for retargeting:** The IP was correctly targeted via `audience.audience_segments` (DS 2/14/16) as a retargeted visitor. It was never required to be in the DS 4 interest segment (cats 9119/19324) because retargeting is not limited to the user-defined audience. But the enrichment pipeline doesn't know this campaign is retargeting — it validates against DS 4 categories from `audience.audiences` regardless of campaign type.

### The Fundamental Gap

The enrichment pipeline treats all campaigns identically: validate impression IPs against IPDSC using the DS/category mappings from `campaign_segment_history`. But:
- **Prospecting campaigns:** IPs should match the user-defined audience (DS 4) — IPDSC validation is appropriate
- **Retargeting campaigns:** IPs are served based on prior engagement, not interest categories — IPDSC validation against DS 4 is inappropriate and will systematically fail for IPs that were retargeted but never in the interest segment

---

## 10. Broader Impact

### Ray's Pushback on Scale

Ray noted in Slack:
> "MES should only account for 3% of our traffic. we're seeing 50% misses sometimes. that can't all be MES"

> "It's a reporting-side gap where IPDSC doesn't contain the data sources needed to reconstruct full audience membership for HH counting. Reporting doesn't own IPDSC."

With 2,302 campaign groups affected by template contamination alone, the 50% miss rate is expected.

### Aylwin's Broader Data

Aylwin captured IPs across multiple data sources (DS 1, 4, 18, 35) where some delivered IPs exist in IPDSC and others don't. For DS 1, 18, 35 (which ARE IPDSC-based per Zach), there may be separate issues:
- IPDSC freshness / lookback limitations
- IPs that were in IPDSC at bid time but aged out
- IP mutation between bid and the IPDSC snapshot date

These need separate investigation.

### What My Audit Table Addresses (and Doesn't)

**The Stage 3 VV IP lineage audit table** (from my ongoing work on TI-684) traces IP mutations through the ad-serving pipeline:

```sql
-- Simplified trace: clickpass_log → event_log (bid_ip)
SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    cp.campaign_id,
    cp.time AS cp_time,
    el.bid_ip,
    el.vast_playback_ip,
    cp.ip AS redirect_ip,
    v.ip AS visit_ip,
    v.impression_ip,
    (el.bid_ip = el.vast_playback_ip) AS bid_eq_vast,
    (el.vast_playback_ip = cp.ip) AS vast_eq_redirect,
    (cp.ip = v.ip) AS redirect_eq_visit,
    (el.bid_ip = el.vast_playback_ip AND el.vast_playback_ip != cp.ip) AS mutated_at_redirect,
    cp.is_new AS cp_is_new,
    v.is_new AS vv_is_new,
    (cp.is_new = v.is_new) AS ntb_agree,
    cp.is_cross_device,
    (el.ad_served_id IS NOT NULL) AS el_matched,
    (v.ad_served_id IS NOT NULL) AS vv_matched,
    DATE(cp.time) AS trace_date
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN el_dedup el
    ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
LEFT JOIN `dw-main-silver.summarydata.ui_visits` v
    ON CAST(v.ad_served_id AS STRING) = cp.ad_served_id
    AND v.from_verified_impression = true
```

**This audit table helps with:** IP mutation between bid and visit — proving that the bid IP was legitimately targeted even if the visit IP changed. This is the proof layer Zach referenced.

**This audit table does NOT help with:** The IPDSC enrichment gap. The root cause here is that certain data sources (DS 2, 16) don't exist in IPDSC, and the enrichment pipeline is using the wrong audience expression (DS 4 from `audience.audiences` instead of the actual targeting expression from `audience.audience_segments`).

---

## 11. Action Items

| Priority | Issue | Action | Owner | Status |
|----------|-------|--------|-------|--------|
| **P0** | Enrichment doesn't distinguish prospecting vs retargeting | Enrichment pipeline validates ALL impressions against DS 4 from `audience.audiences`. For retargeting campaigns, this is wrong — retargeted IPs are not required to be in the interest segment. Pipeline needs campaign-type awareness or `campaign_segment_history` needs to be fixed. | BER team | Root cause identified |
| **P0** | IPDSC missing DS 2 and DS 16 | Add DS 2 (OPM) and DS 16 (MNTN events) to IPDSC via a non-export mechanism | TI (Alyson, Victor) | Identified by Zach, needs formal request |
| **P1** | `campaign_segment_history` blends both audience tables | The writer process blends `audience.audiences` (template/DS 4) and `audience.audience_segments` (actual/DS 14) into one table. Identify and fix the writer. | BER team (writer owner TBD) | Identified, not yet assigned |
| **P1** | DS 14 on block list | DS 14 is intentionally blocked per Zach (targeting infrastructure, not reportable). However, for retargeting campaigns, it's the only IPDSC-matchable DS. | BER / Zach | Answered (intentional), consequence understood |
| ~~P0~~ | ~~DS 4 in IPDSC — is it native?~~ | **ANSWERED:** DS 4 IS natively in IPDSC (327,295 IPs have cats 9119/19324 globally). The issue is that retargeted IPs are not required to have these categories. | Zach / TI | **Resolved** |
| **P1** | Legacy retargeting campaigns with DS 4-only audiences | cgid 107428 is in a disallowed state (retargeting + DS 4-only audience). This was allowed historically but is no longer permitted for new campaigns. Identify and remediate remaining legacy campaigns. | BER team | Identified, scope TBD |
| **P2** | IPDSC freshness for DS 1, 18, 35 | Sample missing IPs from Aylwin's data; check if IPDSC records aged out of the 35-day lookback | Malachi / Aylwin | Pending |
| **P3** | Re-measure miss rate | After P0/P1 fixes, re-measure overall HH discrepancy to see if residual matches expected ~3% MES rate | All | Blocked on P0/P1 |

---

## 12. Summary of Confirmed Facts

### Resolution (2026-02-26)

**cgid 107428 is a retargeting campaign.** Retargeting is not limited to the user-defined audience. The IP `8.41.17.75` was correctly served via retargeting (DS 2/14/16) but was never in the DS 4 interest segment (cats 9119/19324) — and it was never required to be. The enrichment pipeline doesn't distinguish prospecting from retargeting and validates all impressions against DS 4 categories from `audience.audiences`, causing retargeted IPs to fail the IPDSC inner join.

### Three Root Causes + Legacy Configuration Issue

1. **Enrichment is campaign-type blind (PRIMARY for cgid 107428):** The enrichment pipeline validates all impressions against DS 4 interest categories regardless of whether the campaign is prospecting or retargeting. For retargeting campaigns, this systematically drops impressions that were legitimately served.

2. **`campaign_segment_history` blends both audience tables:** DS 4 from `audience.audiences` (template) and DS 14 from `audience.audience_segments` (actual targeting) are mixed together. DS 2/16 from the actual targeting expression are completely absent. 2,302 campaign groups affected.

3. **IPDSC missing real-time data sources:** DS 2 (OPM) and DS 16 (MNTN events) have zero partitions in IPDSC. TI (Alyson, Victor) needs to add them.

4. **Legacy campaign in disallowed state:** cgid 107428 is a retargeting campaign with a DS 4-only audience expression — a configuration that is no longer permitted for new campaigns. DS 4-only audiences should only ever be applied to prospecting campaigns. This campaign predates that constraint and was never migrated or flagged.

### All Confirmed Facts

1. All Facts counts all valid impressions. Segment reporting counts only those surviving an IPDSC inner join.
2. The IPDSC inner join in `impression_enrichment.py` is the sole source of the HH drop.
3. DS 2 (OPM) and DS 16 (MNTN events) are real-time — they have **no partitions in IPDSC at all**.
4. DS 14 is blocked from the IPDSC load (`dsid_block_list = [2, 14, 42]`).
5. DS 1, 18, 35 ARE in IPDSC (per Zach) — discrepancies there need separate investigation.
6. **RESOLVED:** cgid 107428 is a **retargeting campaign** (confirmed by Zach). Retargeting is not limited to the user-defined audience. The actual targeting used `audience.audience_segments` (DS 2, 14, 16), which serves based on prior engagement, NOT the interest categories in `audience.audiences` (DS 4).
7. **CONFIRMED:** The enrichment pipeline sources DS/category mappings from `summarydata.v_campaign_group_segment_history`, which reads from `audience.campaign_segment_history`.
8. **CONFIRMED:** `audience.campaign_segment_history` contains a **blend** of DS 4 (from `audience.audiences` template) and DS 14 (from `audience.audience_segments` actual targeting). DS 2 and DS 16 are completely absent.
9. **CONFIRMED:** 2,302 campaign groups have data source IDs in `campaign_segment_history` that don't exist in their actual targeting expressions — this is systemic.
10. The IP `8.41.17.75` is in IPDSC under DS 4 with hundreds of categories but not cats 9119/19324. **This is expected for a retargeted IP** — it was served based on prior engagement, not interest category membership.
11. Extending the IPDSC lookback will not fix this — for retargeting campaigns, the issue is fundamental, not temporal.
12. IPDSC is owned by TI (Alyson, Victor). Reporting does not own IPDSC.
13. The `campaign_segment_history` writer is application-level code (no DB triggers). Owner needs to be identified by BER.
14. **VERIFIED:** DS 4 cats 9119/19324 do not exist in IPDSC for IP `8.41.17.75` across a 38-day window (2026-01-13 to 2026-02-19). The IP has 180-271 other DS 4 categories each day, but never 9119/19324. Query logic sanity-checked with known-present category 4528.
15. Zach confirmed DS 14 is intentionally on the block list — targeting infrastructure, not a reportable segment.
16. **ANSWERED (Ray's DS 4 question):** DS 4 IS natively in IPDSC. 327,295 IPs have cats 9119/19324 globally on 2026-02-12. The issue is not that DS 4 is missing — it's that retargeted IPs are not required to have these categories.
17. **CONFIRMED (No IP Mutation):** Full IP lineage trace across `event_log`, `clickpass_log`, and `ui_visits`: `bid_ip = vast_playback_ip = 8.41.17.75` across all 6 VAST events. No clickpass, no ui_visits record. Pure CTV impression — no downstream mutation possible.
18. **CONCLUSION:** This IP was correctly retargeted and served via DS 2/14/16. It was never in the DS 4 interest segment because retargeting doesn't require it. The enrichment pipeline doesn't know this and validated against DS 4 anyway, causing the drop.
19. **LEGACY STATE:** cgid 107428 is a retargeting campaign with a DS 4-only audience — a configuration that is no longer allowable. DS 4-only audiences should only be applied to prospecting campaigns. This is a legacy campaign that predates the constraint. Remaining legacy campaigns in this state need to be identified and remediated.

### Global DS 4 Category Check

```sql
-- Are cats 9119/19324 populated in IPDSC at all? (BQ)
SELECT COUNT(*) AS total_ips_with_cats
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE dt = '2026-02-12' AND data_source_id = 4
  AND EXISTS (SELECT 1 FROM UNNEST(data_source_category_ids.list) AS category
              WHERE category.element IN (9119, 19324))
LIMIT 10;

-- Result: 327,295 IPs have cats 9119/19324 globally
```

This answers Ray's question: DS 4 IS natively in IPDSC and is populated. The problem isn't that DS 4 is missing — it's that the enrichment pipeline uses DS 4 categories from `audience.audiences` to validate an IP that was actually targeted via DS 2/14/16 from `audience.audience_segments`.

### IP Lineage Trace — Full Pipeline (No Mutation)

```sql
-- Full IP lineage trace across all pipeline stages (BQ)
SELECT
    el.ad_served_id,
    el.bid_ip,
    el.ip AS vast_playback_ip,
    cp.ip AS redirect_ip,
    v.ip AS visit_ip,
    v.impression_ip,
    (el.bid_ip = el.ip) AS bid_eq_vast,
    (el.ip = cp.ip) AS vast_eq_redirect,
    (cp.ip = v.ip) AS redirect_eq_visit,
    el.event_type_raw
FROM `dw-main-silver.logdata.event_log` el
LEFT JOIN `dw-main-silver.logdata.clickpass_log` cp
    ON cp.ad_served_id = el.ad_served_id
    AND DATE(cp.time) BETWEEN '2026-02-10' AND '2026-02-14'
LEFT JOIN `dw-main-silver.summarydata.ui_visits` v
    ON CAST(v.ad_served_id AS STRING) = el.ad_served_id
    AND v.from_verified_impression = true
    AND DATE(v.time) BETWEEN '2026-02-10' AND '2026-02-28'
WHERE el.ad_served_id = 'a49496ae-ddc0-41b5-b117-e732516fc3f4'
    AND DATE(el.time) BETWEEN '2026-02-10' AND '2026-02-14'
LIMIT 10;

-- Result: 6 rows (vast_impression, vast_start, vast_firstQuartile,
--         vast_midpoint, vast_thirdQuartile, vast_complete)
-- bid_ip = vast_playback_ip = 8.41.17.75 on ALL rows (bid_eq_vast = true)
-- redirect_ip = null (no clickpass record — CTV, no clickthrough)
-- visit_ip = null, impression_ip = null (no ui_visits record — no site visit)
-- Pure CTV impression: IP lifecycle ends at VAST complete, no downstream mutation possible
```
