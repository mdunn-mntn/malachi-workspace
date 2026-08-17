---
doc_type: ticket
title: "AUDI-694: Update targeted_signal to Handle Billing"
status: backlog
date: 2026-08-17
summary: "DDP vendor crediting as CRM inclusions migrate DS4 -> DS63; review of the unmerged DS63 crediting PR"
result: "Phase 1 done: PR bae-sql-utility#24 cannot execute (references translation_date; the signals expose translation_timestamp). Free-log and 33Across-dedup defects confirmed against the live registry. Dollar sizing blocked on enriched_impressions access."
question: "What must change in DDP vendor crediting as CRM inclusion audiences migrate DS4 -> DS63, and what is it worth?"
framing_state: draft
---

# AUDI-694: Update targeted_signal to Handle Billing

**Jira:** https://mntn.atlassian.net/browse/AUDI-694
**Status:** backlog
**Date Started:** 2026-08-17
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

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
