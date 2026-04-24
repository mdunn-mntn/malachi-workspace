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

## 2026-04-16

### [mntn_business] from Mike Dolt in #targeting-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**DMA Targeting — Nielsen Licensing Requirement:** DMAs (Designated Market Areas) are a Nielsen construct and require a paid license to use for targeting. They are not open-source or freely available. Meta was observed switching to Comscore markets as an alternative. MNTN alternatives under consideration include MSA (Metropolitan Statistical Area) or CBSA (Core-Based Statistical Area) data, which do not carry the same licensing requirement.

### [mntn_business] from Ryan Kleck in #tgt-infrastructure-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**MNTN Express — Vertical Targeting Approach (Current vs. Planned):** MNTN Express advertisers are not currently treated differently from a targeting perspective. The current approach scrapes the advertiser's site to determine their vertical. The planned improvement is to use the vertical the customer selects in the UI directly, with a waterfall fallback: use the site-scraped vertical first; if unavailable, use the UI-selected vertical.

### [data_catalog] from Sean Yang in #data-platform
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**`public.ip_info` — CoreDW vs. BQ Row Count Discrepancy:** A minor row count discrepancy exists between the CoreDW and BQ versions of `public.ip_info`:
- CoreDW: 9,144,472 rows
- BQ (`dw-main-silver.public.ip_info`): 9,144,484 rows

The BQ version has slightly more rows. This is likely an artifact of replication timing. Confirm with the data engineering team before treating either as authoritative.

### [mntn_business] from Jack Barbey in #identity_core
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Identity Graph RFD — In Progress (April 2026):** Jack Barbey (identity engineering manager) drafted an Identity Graph RFD and is building internal consensus before sharing externally with downstream teams. The document is hosted in Confluence: https://mntn.atlassian.net/wiki/x/JoDz0w. Review was solicited from the identity_core team.

## 2026-04-17

### [data_knowledge] from Tucker Saland in #identity_core_dev
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**SQLMesh Resolve Flag — Under-the-Hood Behavior**

The `resolve SQLMesh` flag works by finding the underlying physical table that a SQLMesh view references and loading that table directly, bypassing the view layer. This is relevant when debugging view-level vs. table-level query behavior in the identity/targeting pipelines.

## 2026-04-21

### [mntn_business] from malachi in #tgt-infrastructure-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**BUK (Bottoms-Up Keywords) Naming Discussion:** Malachi proposed renaming BUK for marketability. Candidates included: Site Intent, Behavior Keywords, Onsite Intent, Journey Keywords, Intent Trails. Mike Dolt favored "Behavior Keywords". Alex Bloore opposed any "keywords" framing entirely — it confused him when he joined (implying TV search), and he believed the direction was toward "Attributes". Alex requested PMM engagement on the naming decision before finalizing. No final name selected as of 2026-04-20.

### [mntn_business] from Jack Barbey in #identity_core
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Identity Graph — Recency Metric (Not Yet Built):** As of April 2026, the MNTN identity graph does not include a recency metric (i.e., how recently data about a user/household was last received). Jack Barbey noted that recency could be sourced from intermediate tables — the most practical approach would be attaching a "last seen" date to each ID, from which household-level recency could be derived. The `asOfDate` and lookback window of the graph provide a rough date range (approximately a 30-day rolling window). This was flagged as a potential future enhancement, possibly tied to geo confidence scoring.

## 2026-04-22

### [experimentation] from Alex Knorr in #targeting-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Playbook: Extracting Value from Historical Experiments**

Alex Knorr developed a playbook for leveraging historical experiment data to evaluate new signals (e.g., analyzing BUK signals against the Fangorn experiment results). The playbook is documented in Confluence. This approach allows the team to extract signal value from already-run experiments without requiring new test cycles, and has been identified as a reusable methodology worth socializing across the team.

### [mntn_business] from Matt Brorby in #tgt-infrastructure-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Fangorn — Timeline Risk: Continuous Scoring Coupling**

As of late April 2026, there are no blocking obstacles to the Fangorn rollout timeline. The primary identified risk is whether continuous scoring and the Fangorn model rollout should be launched simultaneously — decoupling them introduces complexity. The beta advertiser list is awaiting final feedback before being finalized.

## 2026-04-24

### [strategic] from Alexander Jerneck in #identity_core_dev
**Reason:** Medium confidence — needs verification
**Confidence:** medium

### Identity Graph — DS47 Treatment Revenue Impact Estimate

Rolling out the DS47 treatment (ticket ID-198) is estimated to increase revenue by approximately **$120K per month** based on daily result analysis. This estimate is described as rough. The finding supports prioritization of the DS47 rollout within the identity/graph team's roadmap.

### [mntn_business] from malachi in #targeting-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

### Mountain Matched Audience Terminology — Proposed Clarification (2026-04)

There is recognized ambiguity in how "Mountain Matched" audiences are defined for measurement purposes. The following distinctions have been proposed by the targeting squad:

- **Mountain Matched (broad):** Any audience containing keywords (DS13/DS19 or equivalent). Includes variations such as Max Reach, Peak Performance, Mid Intent, High Intent.
- **Recommended audience:** The exact audience configuration generated by MNTN's recommendation system. If an advertiser modifies the recommended audience (e.g., adds/removes keywords, applies geo restrictions, layers additional DS clauses), it is no longer strictly the "recommended" audience.
- **Measurement challenge:** It is currently difficult to cleanly separate "MM audiences" from "everything else" because advertiser-customized MM audiences are mixed in with recommended ones. There is no reliable technical mechanism to detect whether an advertiser has modified keyword counts within a DS (the DS ID is the same regardless of keyword changes).

**Open question / action item:** Determine whether a technical signal can be added to distinguish recommended vs. customized MM audiences at query time, to enable clean measurement of MNTN-driven recommendation effects vs. advertiser-driven changes. A formal terminology document is being drafted by Malachi.

### [data_knowledge] from Alexander Jerneck in #identity_core_dev
**Reason:** Medium confidence — needs verification
**Confidence:** medium

### Identity Graph — Sources Field Added to Final Graph Output

A `Sources` field is being added to the final identity graph output (PR: SteelHouse/idg#113). This addition surfaces the source provenance of graph edges/nodes in the final output, which supports auditability and downstream filtering (e.g., for privacy-scoped use cases such as the Spotify pixel isolation requirement).
