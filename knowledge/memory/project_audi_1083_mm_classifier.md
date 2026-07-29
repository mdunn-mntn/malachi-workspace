---
name: project_audi_1083_mm_classifier
description: "AUDI-1083 MM classifier — DONE/LIVE 2026-07-24: dw-main-silver.audience.mm_campaign_classifier (+_by_group), daily SQLMesh model; mm_class mmv1/2/3+Fangorn, tiers_reachable, restriction_level, is_flagship. Ticket closed (resolution Done)."
metadata: 
  node_type: memory
  type: project
  originSessionId: 604faaf9-ab5f-4b71-bb07-1a88aa0b430e
doc_type: memory
keywords: [audi-1083, mm classifier, mm_campaign_classifier, sqlmesh, mm_class mmv1 mmv2 mmv3, is_flagship, DS19 DS13 DS46, restriction_level, targeting-infrastructure, funnel_level 1]
domain: [audience-scoring, project]
lifecycle: active
last_verified: 2026-07-27
---
**AUDI-1083 = a durable campaign-grain view classifying MNTN Matched configs**, so analyses can
LEFT JOIN on campaign_id (or campaign_group_id) and filter real/flagship MM vs MM-in-name.

**Design (settled):** two orthogonal axes + raw components, NOT one fabricated "MM-ness %".
- **Axis A (config):** `mm_class` = the 6-cell TI-1037 taxonomy, using the TEAM's MM-version names
  (Alyson, implemented 2026-07-23): DS19-only=**mmv2**, DS19+DS13=**mmv3**, DS13-only=**mmv1** (created
  <~2024-09-01) **else mmv3**; `mm_flagship_fangorn`/`fangorn_vertical_only`/`non_mm` keep structural
  names. `tiers_reachable` now from raw DS flags (mmv3 spans 2 configs); new col `campaign_created`.
- **Axis B (restriction):** `restriction_level` (none/geo/audience/geo+audience) + geo/3P/1P/gate flags.
- **Rollups:** `is_unmodified_mm` (any MM, national, no AND-narrow — gate NOT required) and
  `is_flagship` (that AND mm_class='mm_flagship_fangorn' = DS19+DS46).

**Scope decisions (all verified this session, see [[reference_mm_component_taxonomy]] + data_knowledge):**
- **funnel_level = 1 ONLY** (only Stage 1 carries the DS audience expr; 2/3/4 = 100% non_mm). Stage 2/3
  classify via campaign_group_id → group rollup (100% of LIVE Stage 2/3 resolve; NULLs are dormant).
- **campaign_status_id = 3 (Live)** = active filter. NB status 8/9 (Deleted/Legacy Archived) NOT caught
  by the `deleted` boolean.
- MM = DS13/19/46 (broad, canonical); is_flagship = DS19+DS46.
- **Tier rule (AUTHORITATIVE, Ryan Kleck audience_intent DAG, see [[reference_audience_intent_scoring_dag]]):
  HI 10K = in Vertical (DS13) AND in Keywords (DS19), BOTH.** DS19-only still reaches HI because the vertical
  is the advertiser's (always in the score); keywords need the DS19 leaf. "Independent axes" framing was wrong.

**Headline by SPEND (active Stage 1, 45d, $39.5M):** MM 72.4% / unmodified 34.6% / flagship 6.9%.
**By CAMPAIGN COUNT (active Stage 1, 14,475):** MM 43.5% (MM spends bigger); config = non-MM 56.5% /
keyword-only 24.8% / both 14.3% / vertical-only 4.3%. Within MM: 80% modified, 20% unmodified; flagship 3%
(191). All-campaign restriction: 27.3% national / 72.7% narrowed (geo 59.9%). Query `queries/audi_1083_distribution.sql`.

**Status (2026-07-22):** view SQL built + validated end-to-end (`tickets/audi_1083_mm_classifying_view/`).
Spec published to Confluence (TAR): https://mntn.atlassian.net/wiki/spaces/TAR/pages/3712811252 (v9:
plain-voice, rule matches Ryan's prospecting model, Distribution section by campaign count, no spend-% editorializing). Jira comment posted, ticket in team review (left In Progress). Feedback gathered (Alyson naming → mmv1/2/3 adopted). **MATERIALIZED as SQLMesh model 2026-07-23:**
repo `SteelHouse/sqlmesh` (cloned `~/Developer/work/mntn/sqlmesh`), branch `audi-1083-mm-classifier`
(committed LOCAL, not pushed). `models/dw-main-silver/audience/mm_campaign_classifier.sql` (FULL, @daily,
grain campaign_id) + `mm_campaign_classifier_by_group.sql` (group rollup). JS UDFs = pre-statements
(precedent: `summarydata/conversion_signal_impressions.sql`). sqlglot parse-clean; SELECT BQ-validated.
**owner = `targeting-infrastructure`.** **PLAN RAN + VALIDATED IN DEV 2026-07-24:** venv + `pip install -r requirements.txt`,
`sqlmesh info` (warehouse+state OK), `sqlmesh plan dev_malachi --no-prompts --auto-apply` backfilled both models into
`audience__dev_malachi` (14.5k + 13.6k rows); dev-table distribution matches. **Gotcha (now in data_knowledge):** dev plan
crashes in the repo prod-access guard when ADC quota project (`mntn-coredw-prod`) has Cloud Identity API disabled → fix
`gcloud auth application-default set-quota-project dw-main-bronze`. **PR OPEN + CI GREEN 2026-07-24: https://github.com/SteelHouse/sqlmesh/pull/1245** (branch `audi-1083-mm-classifier`,
gh mdunn-mntn). CI needed 2 fixes: (1) `sqlmesh format` the models (pushed eecf7e8); (2) re-plan after format (reformat
invalidated the plan snapshot → verify-impact failed) + `gh run rerun --failed`. All checks pass. **MERGED + LIVE IN PROD 2026-07-24: `dw-main-silver.audience.mm_campaign_classifier` (14,512 rows)
+ `..._by_group` queryable now, refresh daily; prod matches dev. DONE-WHEN MET.** Follow-up: confirm exact mmv3 cutoff
date w/ AP (2024-09-01 approx); owner alerts → #monitor-test. Gotcha in data_knowledge: format BEFORE plan. Benign Requirements diff in plan (google-auth/protobuf
pins) — watch CI verify-impact. geo_reach_pct NULL (v2). (NB I changed the user's ADC quota project to dw-main-bronze.)**
SQLMesh conventions documented in `knowledge/data_knowledge.md` §"SQLMesh Repo & Model Conventions".

**Framing locked 2026-07-24 (via /frame, see [[reference_ticket_framing_gate]]):** §0 added to summary.md;
`framing_state: locked`. **Done-when bar = view LIVE IN PROD (user decision), not spec/authored-on-branch** —
so despite the near-done `result` line, AUDI-1083 is genuinely still OPEN until the SQLMesh model is merged
+ running daily and classifications match a hand-checked set. Goal serves 4 consumers: reusable analyst
infra, MM-vs-3P/Fangorn measurement, MM-adoption/exec reporting, incrementality (BER-2250) cohorting.
(User left the `result` line as-is for now.)

**DAILY REFRESH CONFIRMED RUNNING (2026-07-27):** "did the SQLMesh job work?" — yes. Physical
`sqlmesh__audience.audience__mm_campaign_classifier__…` re-materialized 07/27 00:42 PT, 14,516 rows (up from
07/24's 14,512 → picking up new campaigns, not frozen). Live mm_class dist matches baseline (MM 43.5%, flagship
10.8%). NB the clean-name views read 0 rows in `audience.__TABLES__` (virtual-layer views) = false "failed" signal;
freshness is on the physical `sqlmesh__audience.__TABLES__` (see [[reference_sqlmesh_repo]] GOTCHA 3 + data_knowledge).

Related: [[reference_mm_component_taxonomy]], [[reference_fangorn_tier_assignment]],
[[feedback_plain_voice_internal_docs]], [[reference_mm_vs_3p_scorecard]], [[reference_sqlmesh_repo]].
