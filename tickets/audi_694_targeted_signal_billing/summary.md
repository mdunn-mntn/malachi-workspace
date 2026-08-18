---
doc_type: ticket
title: "AUDI-694: Update targeted_signal to Handle Billing"
status: in_progress
date: 2026-08-17
summary: "DDP vendor crediting as CRM inclusions migrate DS4 -> DS63; review of the unmerged DS63 crediting PR"
result: "DS47 has zero impressions, so the ticket as written is a no-op; all exposure is DS63. Two opposing defects measured: the divisor choice moves deepsync 4.7x (259x under preemption), and 39.3% of in-scope DS63 impressions get no crediting row at all. PR bae-sql-utility#24 cannot compile and is not the current design."
question: "What must change in DDP vendor crediting as CRM inclusion audiences migrate DS4 -> DS63, and what is it worth?"
framing_state: locked
---

# AUDI-694: Update targeted_signal to Handle Billing

**Jira:** https://mntn.atlassian.net/browse/AUDI-694
**Status:** in_progress
**Date Started:** 2026-08-17
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** As CRM inclusion audiences migrate DS4 -> DS63, which sources should sit in the divisor that splits a credited impression, and what does each candidate rule pay the affected vendors versus the DS4 leg it replaces?
- **Goal (why / the decision):** Pick the divisor rule for the graph crediting leg before DS63 GA, **routed to Andy Everson as a contractual decision (Jack Barbey, 2026-08-18)** rather than the engineering-consistency call originally framed. Jack: "This is mostly a contractual question, so we want to make sure we are doing the right thing." AUDI-694 therefore supplies the priced options and the evidence; it does not pick the rule. NOTE: AUDI-1144 already queues an Andy conversation on free-log credit for the wider metered roster, and AUDI-1113's $768,916/yr preemption case is the same question at DS4/MM scale. One thread, not two. No DS63 credit has ever been billed, so the first bill sets the precedent for every future graph vendor credit including MNTN ID. Ties to the vendor-cost workstream (AUDI-1089 -> AUDI-1111 -> AUDI-1113, $768,916/yr preemption already BAE-confirmed) and gates ID-407.
- **Objective (done-when):** The three divisor rules priced on real DS63 output, a written recommendation, and a recorded decision from Wei/Jack/AUDI. Closes when the decision exists in writing and the arithmetic defects are either fixed in the merge candidate or logged with an owner. Ships no code: implementation routes to AUDI-1145.
- **Approach (how):** Price the rules off `dw-main-gold.reporting.ddp_crm_graph_cpm` (real DS63 output, already built) rather than rebuilding from raw. Confirm the billable roster and its dedup pairs in `integrationprod.direct_data_partners`. Use `enriched_impressions` (PAM grant 5d0f053c) for the scope volume, the DS4/DS63 dual-run split, the DS47 negative, and the zero-cpm-filler counterfactual against `ddp_all_matches_cpm_202607`. Assumptions to resolve first: which SQL is actually the merge candidate (PR #24 cannot compile and is not the 2026-08-13 build); whether DS47 ever reaches `category_info`; whether the winner-split grain changes the rule ratios.
- **Decision owner (changed 2026-08-18):** Andy Everson, via a thread Jack is opening. Wei takes the PR/SQL repoint (ask 1), which unblocks ID-407 independently.
- **What would change the answer:** No result stops the work, because the rule must be settled before the first DS63 bill. The deadline is DS63 GA, not a volume threshold. The recommendation itself flips if the cross-provider winner split (`impression_cnt = 1/N` over `ad_served_id`) compresses the 4.7x gap between billable-only and MM-parity to something immaterial, or if the free logs turn out not to be genuine substitutes for deepsync on these impressions, which would undercut applying the AUDI-1113 preemption rule to the graph path.

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

## 4. Investigation & Findings

### 4.0 Phase 1 kill-check (2026-08-17) — the PR that adds DS63 crediting cannot execute

`SteelHouse/bae-sql-utility` PR #24 ("ID-407 adding ds63 into crediting pipeline", opened 2026-07-29
by wei-mntn, Blocked awaiting BAE review) references `ats.translation_date` and `gts.translation_date`
in five join/filter positions plus a pre-check query.

Neither column exists. `INFORMATION_SCHEMA.COLUMNS` on `dw-main-silver.identity`:

| table | columns |
|---|---|
| `auction_translation_signal` | translation_id, data_source_category_id, data_source_id, household_id, graph_version, output_id, output_id_type, graph_data_sources, **translation_timestamp** |
| `graph_translation_signal` | translation_id, data_source_category_id, data_source_id, targeted_id, targeted_id_type, household_id, graph_version, graph_data_sources, **translation_timestamp** |

The column is `translation_timestamp` (TIMESTAMP) on both. PR #24 would fail at compile with
`Unrecognized name: translation_date`.

**Refined 2026-08-17 (see 4.7): `translation_date` never existed.** The pre-refactor dev tables from
2026-08-02/03 (`identity__graph_translation_signal__2646043435__dev`, `__1231710114__dev`) also carry
`translation_timestamp`, so this is not bit-rot from the ID-421 model refactor of 2026-08-05. But the
gold output tables `ddp_graph_matches*` **do exist and are populated** (built 2026-08-10), and a newer
iteration exists (2026-08-13). So an adapted version has been run locally; the SQL sitting in the
GitHub PR is not the SQL that was executed, and it is not the current design either.

Consequence for the ticket: **the artifact under BAE review is stale.** Reviewing PR #24 as written
reviews the wrong thing. The near-term output is a design review against the 2026-08-13 iteration,
plus the divisor decision in 4.7.

### 4.1 The umbrella views read only CRM — the crediting logs from our own DAGs go nowhere

Both signal views are `SELECT *` over a single sqlmesh physical table, with no UNION ALL branches:

```
auction_translation_signal -> sqlmesh__identity.identity__auction_translation_signal__3546476855
graph_translation_signal   -> sqlmesh__identity.identity__graph_translation_signal__745022687
```

View description, verbatim: *"Currently sourced only from CRM (auction_translation_crm,
data_source_id 63); add UNION ALL branches here as new sources are onboarded so downstream consumers
can read one combined table."*

Meanwhile `airflow-ti` PRs #1200 (Feature Store / Fangorn DS46) and #1201 (Bombora) — both open,
updated 2026-08-17 — call `mntn_graph.log_translation(...)`, which writes to
`gs://mntn-data-archive-{env}/identity_resources/{graph,auction}_logs/<name>/dt=...`. Nothing reads
those paths. **Merging #1200/#1201 as-is produces crediting logs no consumer sees.** The union
branches are the missing link and they are not in any open PR.

### 4.2 Physical scale — these are full daily snapshots, not deltas

`INFORMATION_SCHEMA.PARTITIONS` on the sqlmesh physical tables (partitioned `DATE(translation_timestamp)`,
`partition_expiration_days=60`):

| date | auction rows | auction GB | graph rows | graph GB |
|---|---|---|---|---|
| 2026-08-16 | 2,194,949,131 | 335.3 | 2,374,836,610 | 438.8 |
| 2026-08-14 | 2,188,332,524 | 334.3 | 2,367,518,880 | 437.4 |
| 2026-08-03 | 2,168,749,689 | 331.5 | 2,345,752,455 | 433.5 |

Row counts are near-identical day over day and creep upward, so each partition is a **full snapshot of
the current population**, not that day's translation events. Two consequences for PR #24:

1. **Semantics.** Its `translation_date BETWEEN cil.dt - 30 AND cil.dt` lookback unions ~30 copies of
   the same population. The `array_agg(distinct ...)` keeps the vendor list correct, but the intended
   meaning is almost certainly "the snapshot as of the impression date", not a 30-day union.
2. **Cost.** Leg 1 scans ~2.19B rows/day x 30 days; leg 2 joins that against ~2.37B rows/day x 30.
   At ~335 + ~437 GB/day this is a multi-terabyte join inside a monthly billing script whose dates are
   hand-edited. Not viable as written.

### 4.3 Free logs are dropped before the divisor — confirmed against the live registry

`direct_data_partners` where `is_current AND external_reporting_required` (the exact predicate
PR #24's `ddp_tpa_direct_data_partners_graph` uses) is 10 rows:

| report_dsid | credit_dsid | dsid | name | type | billing_type | fixed_cpm |
|---|---|---|---|---|---|---|
| 17 | 17 | 17 | ShareThis | interests | fixed_cpm | 0.95 |
| 24 | 24 | 24 | Justuno | mntn_matched | fixed_cpm | 0.50 |
| 28 | 28 | 28 | 33Across | mntn_matched | fixed_cpm | 0.50 |
| 29 | 29 | 29 | deepsync | crm | fixed_cpm | 0.50 |
| 33 | 33 | 33 | Sovrn | mntn_matched | fixed_cpm | 0.50 |
| 35 | 35 | 35 | LiveRamp IP | interests | variable_cpm | NULL |
| 35 | 35 | 11 | LiveRamp | interests | variable_cpm | NULL |
| 36 | 36 | 36 | Cybba | mntn_matched | fixed_cpm | 0.50 |
| 40 | **28** | 40 | 33Across API | mntn_matched | fixed_cpm | 0.50 |
| 51 | 51 | 51 | Bombora | interests | variable_cpm | NULL |

Billable-row count for the internal/free sources is **0** for every one of them:
23 guid_log, 30 MNTN augmentor_log, 58 Audience Acuity, 22 Experian, 46 ML Audience Intent Scoring
Model (Fangorn), 14 MNTN Global Data. Also 0 for 4 CRM, 47 CRM Identity Graph Generated, 63 CRM
Inclusions themselves.

So PR #24's inner join to that table removes free logs **entirely, before** `graph_dsid_count` is
computed. The MM leg does the opposite: it `left join`s, so guid_log/augmentor_log survive as
`mm_dsid` rows and sit inside `mm_dsid_count`, diluting every paid vendor's share.

**The DS63 leg is therefore strictly more generous to paid vendors than the MM leg it mirrors** — the
opposite direction from AUDI-1113, where full free-log preemption is costed at $768,916/yr
(`tickets/audi_1111_vendor_quality/summary.md` §5b, BAE-confirmed on BAE-4923). The graph era would
inherit the defect we are actively trying to remove, before the leg ships.

Experian (22) is `crm` but `external_reporting_required = false` and `flat_fee`, so it is correctly
excluded from per-impression credit despite PR #24's comment naming it as a partner dsid.

### 4.4 Two arithmetic defects in the DS63 usage split, both live in the registry today

**(a) 33Across is paid twice.** DS40 (33Across API) carries `primary_data_source_id = 28`, so its
`credit_data_source_id` is 28 — the dedup that makes DS28 + DS40 one credit. PR #24 selects
`credit_data_source_id` into `ddp_tpa_direct_data_partners_graph` and then **never uses it**:
`graph_dsid_count` is `count(distinct u.graph_dsid)` over `report_data_source_id`, where 28 and 40 are
distinct. 33Across takes two divisor slots and two usage rows instead of one. This is exactly the
`mm_dsid_count` vs `ARRAY_LENGTH(mm_dsids_winner)` error corrected in BAE-4923 (the two differ on
34.2% of MM rows), reintroduced structurally. The MM path guards it with
`ddp_tpa_direct_data_partners_mm_credit` + `credit_divisor`; PR #24 has no analog.

**(b) LiveRamp fans out.** `report_dsid = 35` has **two** rows (dsid 35 LiveRamp IP, dsid 11 LiveRamp).
`ddp_tpa_direct_data_partners_graph` does not dedup, so every join on
`g.report_data_source_id = src` matches twice. In `ddp_usage_report_ds63` the join
`g on g.report_data_source_id = a.graph_dsid` duplicates each row before `sum(a.impression_cnt)`,
double-counting LiveRamp's impressions.

**(c) Variable-CPM vendors are credited nothing while still diluting.** LiveRamp (35/11) and Bombora
(51) are `variable_cpm` with `fixed_cpm` NULL. PR #24 prices usage as
`(ceil(sum(impression_cnt))/1000) * coalesce(g.fixed_cpm,0)` = **0**, yet they still occupy a slot in
`graph_dsid_count` and shrink every paid vendor's share. The MM leg does not have this problem because
its roster is fixed-CPM only; the graph roster is not. Bombora is arriving in both translation tables
via ID-421 and airflow-ti #1201, so this lands as soon as those ship.

### 4.5 The CRM vendor that actually gets paid is deepsync, and its leg changes underneath it

DS29 (deepsync) is the only `crm`-type billable partner ($0.50 fixed CPM), credited today through
`ddp_usage_report_ds29` off the DS4 leg, which joins `external.targeted_signal` on ip + category with
`ts.data_source_id = 4`. Under the graph, deepsync's contribution surfaces in `graph_data_sources` and
would be credited through the DS63 leg instead — different divisor rule, different CPM aggregation
(`min` on the DS4 leg vs `max` in PR #24), and no `$0` filler row. A vendor bill discontinuity at the
rollout boundary, on a self-reported meter.

### 4.6 Still open (blocked or not yet measured)

- **Dollar sizing of the DS63 hole is blocked.** `mntn-analytics-prod-01.analytics_curated.enriched_impressions`
  was Access Denied as of 2026-07-20; needs a PAM temp-access request. Every impression-side question
  (is DS63 in the billing scope, how much volume migrated, dual-run double-count) waits on it.
- Distinct `graph_version` per day, and the joinable fraction of (auction, graph) day-pairs under
  PR #24's `graph_version` equality join across independent 30-day windows.
- Whether `household_id` is stable across graph rebuilds (`crm_audience_inclusion.sql` is a FULL model
  rebuilt against the latest version, which hints it is not).
- Whether DS47 ever produces a `category_info` entry in `enriched_impressions`. Evidence so far says
  no: `dsanalysis:project-ds47-rollout/rollout_plan.md` states DS47 replaces DS4 for **exclusion**
  only, DS4 is retained for inclusions, and the config column is `crm_exclusion_data_source`. An
  excluded household is never served, so it generates no impression to credit. If that holds, the
  ticket's literal premise has no billing impact and the exposure is entirely DS63.

### 4.7 MEASURED — the divisor choice moves deepsync's bill by 4.7x, and preemption by 259x

The `ddp_crm_graph_cpm` table in `dw-main-gold.reporting` (built 2026-08-13, covering
dt 2026-08-06..2026-08-12) is a **newer iteration than PR #24**: it carries `leg1_graph_dsids` and
`leg2_graph_dsids` as separate arrays alongside the combined `graph_dsids`, i.e. it preserves the
per-touchpoint split the design doc calls for and PR #24 flattens away. A sibling
`ddp_crm_graph_matches_cpm` adds `type` and `auction_signal_timestamp`. Neither shape is in any open PR.

Critically, its `graph_dsids` **retain the free and non-billable sources** — it does not apply the
`external_reporting_required` filter that PR #24 applies before the divisor. So the real data lets the
divisor question be priced directly.

**Who actually enables a DS63 impression** (214,251 impressions, 7 days):

| dsid | name | impressions | share | billing status |
|---|---|---|---|---|
| 30 | MNTN augmentor_log | 211,370 | 98.7% | free, internal |
| 22 | Experian | 208,723 | 97.4% | flat_fee, `external_reporting_required = false` |
| 29 | deepsync | 207,031 | 96.6% | **$0.50 fixed_cpm — the only per-impression-billable source** |
| 58 | Audience Acuity | 205,681 | 96.0% | absent from `direct_data_partners` entirely |
| 23 | guid_log | 175,981 | 82.1% | free, internal |

Average 4.708 sources per impression (leg 1 auction 4.456, leg 2 graph translation 1.684).

**The pricing consequence.** Deepsync is present on 209,076 of 216,409 match rows, and **99.6% of those
(208,269) also carry a free log**. Its credited impression-share under the three candidate rules:

| rule | deepsync shares | usage @ $0.50 CPM | vs PR #24 |
|---|---|---|---|
| **A. PR #24** — divisor counts billable partners only | 209,076.0 | $104.54 / week | 1.0x |
| **B. MM parity** — all graph sources sit in the divisor | 44,626.8 | $22.31 / week | **0.21x (4.7x less)** |
| **C. Full preemption** — free log present, paid vendor gets 0 | 807.0 | $0.40 / week | **0.004x (259x less)** |

Under rule A, deepsync is the *sole* billable source on essentially every DS63 impression, so it takes
100% of each one. That is not a rounding difference from the MM leg, it is a different pricing regime
arriving silently with a data-source migration.

**Scale anchor.** Deepsync bills through the legacy DS4 path today (`ddp_usage_report_ds29`):
Jan-Jul 2026 = **$22,379.79**, roughly a $38K/yr run rate (2026-04 peaked at $8,886.14).
`usage_reporting_data` has **no DS63 rows** — graph crediting has never been billed. Today's DS63
footprint is 4 audience uploads and ~214K impressions/week, so the absolute dollars are small; the
**multiplier is the finding**, and DS63 is targeted GA this quarter as the replacement for DS4.

**Caveat on grain.** These figures are at the match grain, before the cross-provider winner split
(`ddp_winners_imp.impression_cnt = 1/N` over `ad_served_id`). All three columns are upper bounds and
the ratios between them are the durable result, not the absolute dollars.

### 4.8 Leg 2 is working in the 2026-08-13 build, and it is not redundant

Measured on `ddp_crm_graph_cpm` (216,409 rows):

| check | result |
|---|---|
| rows with empty `leg1_graph_dsids` | 0 |
| rows with empty `leg2_graph_dsids` | 0 (0.0%) |
| rows where leg 2 contributes a vendor leg 1 did **not** already carry | **49,016 (22.7%)** |

Two consequences.

**The `graph_version` join-loss concern does not reproduce in this build.** Leg 2 is populated on every
row, so the equality join is not silently discarding the window here. That was a source-reading concern
about PR #24, which ranges `ats` and `gts` independently over 30 days; the 2026-08-13 iteration carries
`auction_signal_timestamp` on `ddp_crm_graph_matches_cpm`, so it appears to have reworked the time
handling. **Re-test against whatever SQL is actually proposed for merge** rather than treating this as
cleared for PR #24.

**Per-touchpoint vs per-vendor crediting is worth 22.7% of impressions.** On roughly a fifth of DS63
impressions, the segment-translation touchpoint credits a vendor the auction touchpoint does not. PR #24
flattens both legs into one `array_agg(distinct ...)`, so that vendor is credited once regardless; the
2026-08-13 build keeps the legs separable. The design doc's worked example ("all four of these vendors
should be recorded and credited") implies per-touchpoint. This is a policy decision with a measured
price tag, not a code-style choice, and it is the right question for the ID/AUDI meeting.

### 4.9 MEASURED (PAM grant 5d0f053c, 2026-08-17) — DS47 is empirically dead, and 39.3% of billable DS63 impressions are silently uncredited

`enriched_impressions` turned out to be an **EXTERNAL BigLake table** over
`gs://mntn-analytics-curated/...`, not a native table, which is why dry-run reports 0 bytes and
`INFORMATION_SCHEMA.PARTITIONS` is empty. Real cost is ~2.4 GB/day with the `dt` hive filter.

**DS47 never reaches the meter.** Daily counts of `data_source_id = 47` in `enriched_impressions`:

| dt | DS4 | DS47 | DS63 |
|---|---|---|---|
| 2026-08-01 .. 08-05 | 1.35M – 1.71M/day | **0** | 0 |
| 2026-08-06 | 1,603,786 | **0** | 28,733 |
| 2026-08-07 | 1,515,611 | **0** | 112,703 |
| 2026-08-08 | 1,590,101 | **0** | 80,049 |
| 2026-08-09 | 1,439,998 | **0** | 79,743 |
| 2026-08-10 | 1,423,562 | **0** | 68,858 |
| 2026-08-11 | 1,380,714 | **0** | 165 |
| 2026-08-12 | 1,495,760 | **0** | 6,476 |
| 2026-08-13 .. 08-16 | 1.38M – 1.60M/day | **0** | 11,097 – 13,997 |

Zero on every day checked, and zero on the 2026-07-15 spot check (where the full data_source_id mix
was 19, 35, 46, 17, 13, 1, 4, 18). **This closes the ticket as literally written:** DS47 is
exclusion-only, an excluded household is never served, and it therefore produces no impression to
credit. Migrating DS4 to DS47 has no DDP billing impact. The entire exposure is DS63.

**DS63 went live 2026-08-06** and is running ~11-14K impressions/day after an initial spike, against
DS4's steady ~1.4-1.6M/day. So DS63 is currently under 1% of DS4 volume.

**The missing zero-cpm filler, measured.** For 2026-08-06..08-12:

| | impressions |
|---|---|
| DS63 total | 376,727 |
| DS63 inside the billing scope (`channel_id=8, funnel_level=1, objective_id=1`) | 352,830 (93.7%) |
| DS63 rows in `ddp_crm_graph_cpm` (the built crediting output) | 214,251 |
| **in scope but with no crediting row at all** | **138,579 (39.3%)** |

Two in five billable DS63 impressions never enter the crediting leg. Under the DS4 leg those
impressions are deliberately inserted at cpm 0 (script L299-322) so they still compete in the OR
groups, with the inline rationale *"to ensure we are not over crediting to TPA/MM Matches for CRM
matches that don't have a valid targeted signal record in the lookback window."* The graph leg has no
equivalent, so those 138,579 impressions hand their full share to whatever TPA/MM vendors are on them.

This is the second money finding and it runs **opposite** to the divisor finding in 4.7: 4.7 says the
graph leg over-credits deepsync on the impressions it does cover, 4.9 says it under-credits the CRM
side entirely on 39.3% of impressions and over-credits TPA/MM there instead. Both need netting before
any single dollar figure is quoted.

**Caveat:** the 214,251 baseline is the crediting table as built on 2026-08-13, which may have run
over a narrower window than the full 08-06..08-12 span. Re-measure against whatever SQL becomes the
merge candidate before quoting 39.3% externally.

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
