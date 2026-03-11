# 2026 Performance Evidence Log

Format: [date] [ticket] — [what was done] [metric or outcome if known]
This file is gitignored. Never commit. Update after every meaningful ticket or project.

---

## Manager Goals (2026)

**Kale:** Revenue growth, revenue retention, cost reduction — tie analysis to dollar impact.
**Alyson:** Knowledge sharing across teams. Active goal: causal impact analysis for next experiment
→ present at Engineering All-Hands.

---

## Q1 2026

### Completed tickets

[2026-02] TI-644 Root Insurance — Investigated 92% CRM audience match miss rate for Root Insurance
($10M+ CTV campaign). Traced full CRM pipeline (HEM → ipdsc__v1 → bidder). Confirmed 23.3M
include HEMs, ~18.2M resolved IPs (~15M net after exclusions). Identified dead ends in tmul_daily
(14-day TTL, no DS4) and audience_upload_ips (empty for email uploads). Documented complete table
schemas for ipdsc__v1, audience_upload_hashed_emails, audience_uploads. Produced CSV exports for
stakeholder (Kale) deliverable.

[2026-02/03] TI-650 Stage 3 VV Audit — Led full end-to-end audit of the IP mutation problem in
MNTN's verified visit pipeline. Built BQ silver-based trace covering 3.25M VVs, matched Greenplum
within 0.12pp (validated BQ as GP replacement). Identified 100% of mutation occurs at VAST→redirect
boundary. Quantified mutation range 1.2–33.4% across 15 advertisers. Confirmed 4,006 phantom NTB
events/day for reference advertiser. Fixed A4b dedup bug. Designed production audit table
(audit.vv_ip_lineage). Completed Zach Schoenberger review, incorporated 13 docx corrections.

Second review round (2026-03-05/06): Discovered prior VV impression for Zach's specific S3 VV
was a display impression (impression_log, not event_log). Corrected incorrect claim about NULL cause
(verified empirically — UUID a4074373 had zero event_log rows but IS in impression_log). Extended
production model to cover display inventory via il_all CTE (impression_log fallback, COALESCE pattern
applied to all 3 impression lookups). Added cost analysis: daily $17 → $29 (+impression_log ~1.9TB).
Wrote Part 16 of pipeline_explained.md: comprehensive MES trace permutation reference covering all
9 CTV/display combinations, S1→S2→S3 IP follow-through example, NULL cause table.

Speed: Resolved Zach's NULL independently before escalating — traced root cause to display
impression type via empirical BQ query, not guesswork.

Craft: Extended 7-join model to 10-join with display fallback, covering 100% of inventory types.
Part 16 permutation guide is a permanent reference for the team.

Adaptability: Pivoted immediately when display impression was confirmed — re-architected CTEs same
session, updated cost docs, implementation plan, and column reference.

Third round (2026-03-06): Redesigned S1 resolution from `cp_ft_ad_served_id`-based lookup (40% NULL,
unfixable) to full 4-branch CASE chain traversal via `prior_vv_pool` self-JOINs. Extends to 13 LEFT
JOINs (added s1_pv, s2_pv, s1_lt, s1_lt_d, s2_lt, s2_lt_d). Handles all 10 permutations including
S3→S3→S3→S1. Covers ~99%+ of rows (vs 60% before). Confirmed all branches fire via clickpass-only
proxy query across 10 permutations (44K-211K rows each). Incorporated Zach's all-stage design (no
stage filter in cp_dedup) and Ray's TTL context from architecture diagram (90-day lookback confirmed
sized for ~83-day S1→S3 chains). Updated all 5 artifacts to v4 design standard.

Speed: Independently identified the `cp_ft_ad_served_id` NULL problem as architecturally unfixable
and designed the chain traversal solution without escalation. Added `pv_stage <= vv_stage` semantic
constraint and third-level `s2_pv` JOIN for 3-hop chains in same session.

Craft: 4-branch CASE with monotonic stage constraint (`s1_pv.pv_stage <= pv.pv_stage`) is the
correct generalization — covers all realistic permutations with clean termination conditions.
All 5 ticket artifacts updated to v4 design in one session. Permutation matrix and validation
output documented as permanent reference.

Adaptability: Pivoted from checking `cp_ft_ad_served_id` to chain traversal when confirmed
the system limitation is permanent. Incorporated Zach's "all stages" and Ray's TTL context
from stakeholder Slack without prompting.

Negative case analysis (2026-03-10): Zach directive: "work with the negative case — find the ones
where you can't go the full length." Independently identified that retargeting campaigns exist at
every funnel_level (objective_id=4 at S1/S2/S3) — previous "~20% unresolved" was inflated by
retargeting VVs that lack S1 impressions by design. After scoping to prospecting-only CTV:
- S2 resolution: 98.56% (15,880/16,112) via 5 tiers
- Discovered and implemented household_graph tier using graph_ips_aa_100pct_ip (46 additional VVs)
- Primary VV unresolved: 0.34% (54/16,112) — effectively zero
- Remaining 232 are LiveRamp identity graph entries on CGNAT IPs — can explain all of them
- Documented objective_id reference, VVS attribution model logic, campaign naming patterns

Speed: Independently discovered the retargeting scoping issue by empirically checking campaign
data — eliminated a false 20% ceiling without escalation. Found household_graph as a resolution
tier nobody had suggested.

Craft: 5-tier resolution cascade (bid_ip → guid_vv → guid_imp → redirect → household_graph)
achieves 98.56% CTV S2 resolution. Attribution model analysis (178/232 are competing VVs) shows
primary VV resolution is 99.66%. VVS Business Logic PDF integrated into analysis.

Adaptability: Pivoted entire analysis when Zach clarified retargeting isn't relevant. Re-ran all
queries with correct scope in same session. Updated 5 artifacts + data_knowledge.md + queries.

[2026-02/03] MM-44 IPDSC HH Discrepancy — Investigated 66.2% household drop (17,589 → 5,944 HHs)
across 2,302 campaign groups. Identified 3 root causes: MES inner join block list [2,14,42], DS
type contamination in campaign_segment_history, 35-day lookback behavior. Documented IPDSC pipeline
architecture in full.

[2026-03] TI-684 Missing IPs from IPDSC — In progress. Investigation into IPs absent from
ipdsc__v1. Prior work from TI-644 established schema and query patterns now documented in catalog.

### Infrastructure / knowledge work

[2026-03-03] Workspace audit — Completed comprehensive audit of all 22 ticket folders. Enforced
lowercase_underscore naming convention across entire workspace (git mv for all tracked files).
Extracted and documented ~500 lines of new knowledge across data_catalog.md (Phase 3: ipdsc__v1,
bronze.tpa tables, Greenplum tables reference, audit table) and data_knowledge.md (RTC, IPDSC/MES
pipeline, Stage 3 VV, Jaguar/DS13, ecommerce classifier, NTB clarification, tmul_daily unnest
patterns, email analysis, audience system architecture, coreDW deprecation). Fixed all Drive file
references in ticket summaries.

---

## Q4 2025 (carried forward — estimate from ticket dates)

[~2025] TI-501 Jaguar KPI — Deep analysis of Jaguar/DS13 Audience Intent Scoring impact on
campaign KPIs. Causal impact methodology. IP-level aggregation queries against ui_conversions
and ui_visits.

[~2025] TI-541 IP Scoring Pipeline — Documented full DS13 pipeline architecture end-to-end.
Produced pipeline overview, walkthrough, and unscored IP investigation documents. Architecture
diagrams (Audience Intent Graph, DS13 Data Pipeline, Biddable Inventory Funnel).

[~2025] TI-542 Max Reach Causal Impact — Bayesian causal impact analysis of Max Reach feature.

[~2025] TI-253 TPA Monitor — Investigated missing TPA domains, confirmed tpa.membership_updates_logs
and DDP URL verticals.

[~2025] TI-254 Low NTB Percentage — Investigated NTB misclassification. Identified cross-device as
primary driver. Documented is_new = client-side pixel (not SQL-auditable).

[~2025] TI-270 Pre-Post GA Jaguar Release — Pre/post analysis of Jaguar GA launch impact on
campaign performance (IVR, CPV, conversion rate).

[~2025] TI-310 NTB Investigations — NTB deep-dive. Produced canonical NTB documentation
("New-to-Brand (NTB) Documentation.gdoc").

[~2025] TI-221 Pre-Post Vertical Classification — Pre/post impact analysis of vertical
classification change. 3 queries (vertical-level, daily trend, campaign-level). Metrics across
multiple campaign groups.

[~2025] TI-033 Vertical Classification Changes — Domain vertical classification changes and
downstream impact analysis.

[~2025] TI-391 Audience Intent Scoring Changes — Pre/post analysis of Audience Intent Scoring
model update.

[~2025] TI-390 MMv3 Performance — MMv3 (Membership Model v3) performance investigation.

[~2025] TI-200 Whitelist/Blocklist — Ecommerce domain whitelist/blocklist maintenance using
classifier thresholds from TGT-4016.

[~2025] TGT-4016 Ecomm Classifier Thresholds — Threshold analysis for ecommerce domain classifier.
P90 ≈ 0.9181 (whitelist), P10 ≈ 0.0002 (blocklist). 251M website visits analyzed.

[~2025] TI-502 IP Scoring Reference — Documented score types and model_params field structure.

[~2025] DM-3118/3188 RTC Monitor — Built RTC vs non-RTC impression monitoring query. Delivered
comparison data export.

[~2025] TI-34 Identity Sync Freshness — Established freshness monitoring for IP identity graph
and blocklist pipeline.
