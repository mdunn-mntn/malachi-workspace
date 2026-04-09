# Slack Knowledge Review Queue

Items below need human review before being added to knowledge docs.
Reasons for review: contradictions with existing docs, low confidence, ambiguous categorization.

---

## 2026-04-08-full

### [data_catalog] from #production-ops
**Reason:** Potential contradiction with existing documentation
**Confidence:** high

### `archives.advertiser_archives` (CoreDB)

Tracks all database-level changes to advertiser records, including the `is_test` flag and other account-level fields.

**Key columns:**
- `version` — incrementing version number per change
- `update_time` — timestamp of the change
- `user_id` — the internal MNTN user who made the change (populated by the UI layer; may be inaccurate if the change was made directly via database connection, since shared DB credentials are used)
- Various advertiser fields including `is_test`

**Join:** `JOIN users u USING (user_id)` to get `email_addr` of the actor.

**Gotcha:** If a change is made directly to the database (not through the UI), `user_id` will not be reliably set — it will retain the prior value. Only UI-originated changes have accurate `user_id` attribution. Analogous tables exist for other entities (e.g., `archives.campaign_groups_archives`).

### [data_knowledge] from #data-platform
**Reason:** Potential contradiction with existing documentation
**Confidence:** high

### BigQuery Write Patterns — Bulk Loading vs. Direct INSERT

BigQuery is optimized for bulk loading rather than row-by-row `INSERT` statements. Direct `INSERT` queries have a daily quota and are discouraged by Google's quota structure.

**Preferred ingestion patterns:**
- Write Parquet files to GCS, then load to BigQuery (standard pattern for services ingesting from external sources)
- Use the BigQuery Python client's load job method (e.g., `load_table_from_dataframe`), which uses load jobs rather than streaming inserts
- Spark jobs writing directly to BigQuery are also an established pattern

**Load job limits:**
- 1,500 load jobs per table per day
- 100,000 load jobs per project per day
- Max job size: 15 TB per load job
- Jobs fail if runtime exceeds 6 hours

**Anti-pattern:** Do not insert data into an OLTP database (e.g., CoreDB) and then dump it to BigQuery. Write directly to GCS/BigQuery from the service.

### [data_knowledge] from #tgt-infrastructure-squad
**Reason:** Potential contradiction with existing documentation
**Confidence:** high

### Fangorn Continuous Scoring — IVR Evaluation Methodology

IVR rates for Fangorn are not evaluated at the individual keyword level (there is no 1:1 keyword-to-visit association). Instead, evaluation is done at the **IP level using a unified score**:

1. Each IP is assigned a unified score based on its keyword rankings from BUK.
2. IPs are binned by unified score (e.g., IPs scoring ≥ 0.9).
3. Visits and impressions are summed across all IPs in a score bin to calculate IVR for that bin.

This approach means a visit is not attributed to a single keyword — it's attributed to the IP's overall intent score. The result shows how IPs with high unified intent scores perform relative to lower-scored IPs.

**Implication:** Adding 100% of keywords to High Intent is suboptimal because individual keywords have vastly different IVR rates and don't all perform at the average HI level. Continuous scoring addresses this by ordering keywords more precisely by intent signal strength.

### [data_knowledge] from #targeting-squad
**Reason:** Potential contradiction with existing documentation
**Confidence:** medium

### Audience Size Expansion — Keyword Recommendation Complexity

Recommending a specific keyword addition to achieve a target IP count increase (e.g., "add 50,000 IPs") is not straightforward due to IP intersection:

- The marginal IP gain from adding a keyword cannot be estimated without a full evaluation against MemDB, because IP sets across keywords overlap.
- Evaluating how much a category grows an audience in real time is considered a hard blocker for any productized recommendation.
- DAR (Dynamic Audience Recommendations) produces a ranked list of keywords; marginal IP gain could theoretically be computed sequentially from rank 1 to rank N, but this functionality does not exist today.
- IPDSC could be used as an approximation, but would be slow.

**Current workaround:** The existing MemDB hash mechanism (used for holdout bucketing) could potentially be reused for approximate audience sizing, but no automated recommendation tooling exists.

### [data_knowledge] from #production-ops
**Reason:** Potential contradiction with existing documentation
**Confidence:** high

### `core.advertiser_default_values` — Campaign Budget Floor/Ceiling Overrides

The `core.advertiser_default_values` table allows per-advertiser overrides of campaign model settings, including budget floor and ceiling by stage (e.g., stage 15, 23, 25, 42, 43) and campaign status.

**Key behavior:**
- Values are stored as a JSONB column `campaign_values` with keys like `budget_floor`, `budget_ceiling`, and `campaign_status_id`
- Stage 23 and 43 with `campaign_status_id: 7` disable those stages (e.g., for advertisers not tracking Verified Visits)
- Settings flow downstream and affect active campaigns when updated

**Known issue (April 2026):** Many advertisers created before the budget minimum feature was introduced are missing records in this table. When an advertiser has CART disabled, the default values record may not have been created, causing budget floor/ceiling to not apply correctly. 

**Workaround:** Toggle CART ON then OFF in the Advertiser Info page in Command Center — this creates the missing record, fixes existing budget allocations on live campaigns, and syncs everything downstream. A migration to back-populate all affected advertisers is tracked in PRO-497.
