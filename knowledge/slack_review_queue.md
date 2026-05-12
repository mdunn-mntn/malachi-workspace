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

## 2026-04-30

### [mntn_business] from Matt Brorby in #dev_fangorn-model_ex
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Fangorn Model — Launch Status (Late April 2026)

**Fangorn** is a targeting/scoring model (or pipeline component) that was in active rollout as of late April 2026. The initial launch targeted 3 advertisers. A NASA-style go/no-go launch checklist was created in Confluence to guide the rollout. Coordination involved the PEX (Product Experimentation), PER (Performance/Experimentation Runtime?), and data platform teams. A launch playbook was being documented for future rollouts. The model involves continuous scoring, which required coordination with Forrest/PER to enable.

### [data_knowledge] from ray in #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## NTB (New-to-Brand) Enforcement in Audience Expressions

**NTB Reporting** has two components:
1. A **reporting-only** feature for the incrementality dashboard.
2. A **backend enforcement** component: NTB enforcement in audience expressions, which operates primarily via site visitor/converter exclusions. This was a gradual rollout throughout 2025, with the majority of rollout occurring near end of year.

The backend enforcement is the component with meaningful behavioral impact on targeting — it affects which audiences are eligible by excluding known site visitors/converters from certain audience expressions.

### [data_knowledge] from Alex Knorr in #tgt-infrastructure-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## conversion_signal GCS Archive vs. conversion_log

The GCS path `gs://mntn-data-archive-prod/signals/conversion_signal` is used for ad hoc work but is **not** the primary source for model training. BUK uses `conversion_log` in the feature store (via `gs://mntn-data-archive-prod/conversion_log`). The feature store source is managed via Airflow (`airflow-ti` repo, `models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py`). The Spotify pixel_isolation filter is expected to be applied in the feature store pipeline for these sources.

## 2026-05-01

### [mntn_business] from Mick Mathis in #sales
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Competitive Intelligence: CTV Market Moves (Late April 2026)

Key competitive developments relevant to MNTN sales conversations:

1. **Pinterest + tvScientific:** Pinterest (600M+ MAUs) launched CTV advertising via tvScientific, entering performance CTV. Differentiation angle: MNTN's deterministic measurement and direct-to-publisher inventory vs. intent signals alone.
2. **The Trade Desk + DramaBox:** TTD added short-form drama content to CTV inventory. Brand safety risk is elevated. MNTN differentiates on premium, curated, brand-safe supply.
3. **Amazon Ads:** Launched AI Video Generator and DAX (audio) integration targeting SMB with low-cost, fast creative. MNTN counters with measurement depth, premium inventory, and outcome guarantees.

Source: Mick Mathis's "The CTV Download" mini-series, published in Crayon.

### [mntn_business] from Weiang Li in #identity_core
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Audience Acuity — Behavioral Data (S3)

Audience Acuity is providing two data assets: (1) graph/identity data already merged into main, and (2) S3 behavioral data not yet integrated. As of 2026-04-30, no team (including Targeting) has reviewed the behavioral data. It is currently only available in S3; a GCS importer may be needed if Audience Acuity cannot deliver directly to GCS. The TI squad has been tasked with a spike to evaluate this data for potential use in predictive modeling and targeting.

### [data_knowledge] from Alexander Jerneck in #identity_core_dev
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Identity Graph — PagerDuty Alert Strategy

Proposed alerting architecture for the identity graph build pipeline:
1. Keep the graph build job running overnight (current behavior) so results are ready by start of business.
2. Add a separate lightweight Databricks job that reads the latest graph, performs a basic sanity check, and fails if a new graph is not populated. Schedule this check job during business hours.
3. Configure PagerDuty to alert on the check job (high urgency) rather than on the overnight graph generation job.

PagerDuty supports "support hours" settings that can suppress or downgrade alerts during off-hours — permission changes required to configure this.

### [experimentation] from malachi in #dev-incremental-lift
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Incrementality Test — Minimum Budget for Statistical Power

Malachi (TI-884) is analyzing the minimum advertiser spend required to detect incremental lift with sufficient statistical power. Key findings (preliminary, as of 2026-04-30):

- The minimum detectable budget varies significantly between **visits** and **conversions** as the target metric — results must be reported separately for each.
- The budget threshold is high: **very few currently active MNTN advertisers reach the spend level required to detect lift** under typical conditions.
- Exception: a new advertiser with a strong product and high true lift (e.g., 10%+) may achieve power at lower spend levels.
- Implication: incrementality testing as a product may be a positioning challenge for most of the customer base, not just a measurement challenge.

This analysis was requested by Al Beretta (SVP/VP) to determine budget thresholds before building out incrementality reporting.

### [mntn_business] from Jenien Lim in #ask-incremental-lift-tests
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Lendio — Incrementality Lift Test Request

Lendio (a current MNTN advertiser at $80K/month) has requested an incrementality lift test on their CTV campaigns. Key context: MNTN is not currently included in Lendio's Marketing Mix Model (MMM), which is blocking budget increases. Lendio is open to a lift test as a path to unlocking additional budget. Test design TBD; internal strategy alignment with Edgar von Trotha is the next step before customer call. Budget recommendation for the test is pending from MNTN's side.

### [mntn_business] from Johnny in #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## HHST Issues — Fangorn Scope

As of late April 2026, the performance churn investigation identified three open HHST (Household Scoring/Targeting) action items on the PAC (Performance & Automation Controls) side:
1. Support HHST in Stage 2 campaigns.
2. Resolve HHST moving too fast or too slow based on various campaign group (CG) conditions.
3. Resolve the "HHST=0 trap" (campaigns getting stuck at zero household scoring).

Items 2 and 3 are expected to be addressed by the Fangorn project, pending verification and a confirmed GA plan. Resolution ownership is with Trixy.

## 2026-05-07

### [data_catalog] from scotty in #data-platform
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## sqlmesh model — require_partition_filter removed

The `require_partition_filter` constraint was removed from at least one SQLMesh model (commit `32ef660d57c575f90476984e502afc312e817f3e` in the `sqlmesh` repo) due to a failing plan. If this constraint needs to be re-enabled on the model, a `WHERE` clause must be added to the final SQL portion of the model before the partition filter requirement can be enforced again.

### [mntn_business] from Mick Mathis in #sales
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Competitive Intelligence — Crayon Usage (April 2026)

April 2026 was the highest-usage month for Crayon (competitive battlecard platform) since adoption began, and correlated with the highest number of battlecard-assisted closed-won deals recorded. Sales reps are encouraged to use Crayon battlecards for deal prep, live calls, and follow-ups. The Crayon homepage for MNTN is at https://app.crayon.co/intel/mntn/home/.

### [mntn_business] from Frankie in #sales
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## CTV Market Context — Key 2026 Stats (Third-Party)

The following third-party data points are useful for sales positioning and internal market context:
- **70% of advertisers plan to increase streaming TV spend in 2026** (MNTN research), with CTV leading all channels in planned spend growth — ahead of social, search, and online video.
- **Local cable TV ad spending fell ~20% in 2025**, while local CTV is on track to hit **$3.6B in 2026** (eMarketer). Strong validation for conversations with multi-location or regional advertisers still holding linear budgets.
- **Wellness advertisers** saw YoY **+31% visit rates** and **+19% ROAS** heading into summer 2025 (MNTN data).
- **FIFA World Cup** (June 11) and **NBA Finals** (June 3) audience spikes are key seasonal triggers for getting campaigns in market before Q3 2026.

### [strategic] from Brian McAdams in #tgt-infrastructure-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Proxima — Upcoming Partner Meeting

Kale forwarded an email thread regarding a meeting with **Proxima**. The meeting is scheduled for the following Wednesday (relative to early May 2026). Brian McAdams is coordinating, with at least two other team members included via calendar invite.

## 2026-05-09

### [strategic] from malachi in #targeting-squad
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Incrementality Ascent Team — Ghost Bid Log Schema Planning**

The incrementality first-ascent team is planning the schema for the ghost bid log (the counterfactual bid record used for lift measurement). A standing design requirement has been identified: the ghost bid log schema should include all fields present in `cost_impression_log` (score, campaign, campaign type such as Select, etc.) so that analysts can connect these dimensions without additional joins. A parallel pipeline for ghost bids is also under consideration.

### [mntn_business] from malachi in #fangorn_launch_day
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Fangorn — Early Post-Launch KPI Signal**

In the 3 days following Tier 1 release of Fangorn, preliminary indicators show positive effects on IVR, CVR, and CPA. These are very early signals and not yet statistically conclusive. Per campaign ramp-up research (TI-780), steady-state spend and impression frequency requires ~3–4 weeks after a campaign change, and conclusive KPI impact results require ~2–4 weeks beyond that depending on advertiser cohort size. Full analysis is deferred until the lead analyst (Malachi) returns from a 2-week absence.

### [mntn_business] from Alexander Jerneck in #identity_core
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**CRM Experiment — Results and Phased Rollout Plan**

The CRM experiment (identity/targeting) is nearing completion with positive results. The planned next step is a phased rollout: starting with ~10 advertisers, expanding to ~100, and continuing to scale while monitoring performance throughout. The rollout approach is expected to follow the same methodology used by the targeting team for prior phased launches. Audience expressions are under consideration as one rollout mechanism; other approaches are also being evaluated.

### [data_knowledge] from Victor Savitskiy in #data-platform
**Reason:** Medium confidence — needs verification
**Confidence:** medium

**Predactiv DDP Data Feed — Outage Noted May 2026**

The Predactiv data partner pipeline stopped flowing as of the partition `gs://mntn-data-partners/partners/predactiv/dt=2026050804/`. This was the latest available partition as of the report date. The issue was escalated to the data platform team. Predactiv data arrives via GCS partitioned by `dt` (date+hour format: `YYYYMMDDHR`).

## 2026-05-12

### [mntn_business] from Benny in #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## Fangorn Rollout — HHST Threshold Logic Update

The latest release of Fangorn includes updates to the HHST (Household Scoring Threshold) logic, intended to address gaps in prior HHST behavior. A known side effect of the Fangorn release is that it may alter Intent Level distributions (audience sizes) for customers. There is concern internally (Benny, Q1 2026 Performance Churn Investigation) that prior HHST behavior may have been a contributor to observed performance declines, particularly those surfacing in PEX tickets. Quantification of HHST-level shifts YTD is in progress.

### [mntn_business] from Alexander Jerneck in #identity_core_dev
**Reason:** Medium confidence — needs verification
**Confidence:** medium

## NTB (New-to-Brand) Experiment — Identity Team Rollout Epic

An epic (ID-283) has been created for the NTB Experiment rollout. The experiment has already slipped approximately 5 weeks from its original target. Realistic goal is end of Q2 2026, though this depends on cross-team dependencies — the Identity team may need to get onto other teams' backlogs first. Work is intended to begin as soon as possible given current priorities.
