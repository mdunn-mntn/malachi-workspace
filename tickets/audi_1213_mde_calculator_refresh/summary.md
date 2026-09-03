---
doc_type: ticket
title: "AUDI-1213: mde calculator refresh"
status: done
date: 2026-09-03
summary: "Refresh the MDE calculator onto advertiser-facing spend and a corrected arm-split, add the 365-day lapsed cohort, host on Mode"
result: "Weekly Mode report over 4,387 advertisers; spend basis and three arm-split defects fixed; VR_STACK 0.595 refuted and removed"
question: "For the 4,409 advertisers delivering or lapsed within 365 days, what 8-week test budget and MDE does an incrementality test require on advertiser-facing spend with the corrected holdout arm-split, and how far are those from what the shipped calculator returns?"
framing_state: locked
---

# AUDI-1213: mde calculator refresh

**Jira:** https://mntn.atlassian.net/browse/AUDI-1213
**Status:** backlog
**Date Started:** 2026-08-20
**Assignee:** Malachi

---
## 0. Framing  (locked 2026-08-20)
- **Question (the unknown):** For each of the 4,409 advertisers delivering or lapsed within 365 days, what 8-week test budget and MDE does an incrementality test require when computed on advertiser-facing spend with the corrected holdout arm-split, and how far do those numbers sit from what the shipped calculator returns today?
- **Goal (why / the decision):** Al Beretta decides which revenue-churned accounts are worth a lift test in a win-back pitch, and screens them himself instead of requesting a pull each time. Sits under BER-2250 Incrementality Overhaul, Kale's stated #1 priority (`knowledge/strategic_north_star.md`), so Tier 2 bordering Tier 1. Second decision it unblocks: retiring a shipped tool that currently returns budgets roughly 3x low.
- **Objective (done-when):** A Mode-hosted calculator covering all 4,409 advertisers with per-advertiser prefill, closing when all three hold: (a) required spend and MDE match `ti_884_mde_calculator.py` to <=0.001 pp on identical inputs, (b) both cohorts load, lapsed rows carrying a `lastActive` label and a final-active-month budget, (c) the data pull runs from a committed script on a schedule rather than by hand.
- **Approach (how):** Repoint the pull to `tickets/incr_75_eligible_advertisers/queries/incr_75_advertiser_metrics.sql` (advertiser-facing spend, `distinct_ips_56d`, `is_b2b`), which also replaces the deleted `agg__daily_sum_by_campaign`. Cohort-rewrite the two AUDI-1204 single-advertiser templates to resolve per-advertiser last-active windows under a 365-day recency cut, the widest cut silver serves without truncating either window (`outputs/audi_1213_history_floor_options.md`). Fix `computeMDE` and `spendRequired` in one pass so the unserved holdout stops being charged for impressions (`n_treated = n_total * (1 - h)`); fixing either alone breaks the budget-to-MDE inverse. Carry all 4,409 rows, rendering a stability warning under 100 visiting or 50 converting IPs rather than cutting thin advertisers. Anchor lapsed budgets on the final active month. Re-measure the `lift__ghost_bid_results` SE ratio and commit the SQL before touching `VR_STACK`. Port to Mode per `knowledge/memory/reference_mode_dashboard_porting.md` (SME Nick Martin) once Al's seat is confirmed.
- **What would change the answer:** (1) Al has no Mode seat and cannot get one, which reverts delivery to a filtered gist and reopens the former-customer exposure question. (2) The re-measured ghost-bid SE shows real variance reduction, in which case `VR_STACK` stays 0.595 and post-stack stays the headline. (3) Most of the 2,546 lapsed advertisers come back under 100 visiting IPs, in which case the lapsed half is not screenable and the ticket collapses to a delivering-cohort refresh.

**Decisions taken at framing (2026-08-20):** Mode as the delivery surface, seat confirmation first · carry all 4,409 with warnings rather than cutting to the AUDI-1204 measurability gate · re-measure the SE before changing `VR_STACK` · lapsed budgets anchor on the exit run-rate, not the six-month median.

**Jira:** [AUDI-1213](https://mntn.atlassian.net/browse/AUDI-1213) · Task · 8 pts · requested by Al Beretta (Slack 2026-08-20)

**Scoping doc (read first):** `tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill/artifacts/ti_1019_refresh_scope.md` — 18 verified deltas, re-run requirements, delivery-option comparison, open questions.

**Why this exists:** Al asked for the TI-1019 MDE calculator again. It cannot be re-run as-is: its source table `agg__daily_sum_by_campaign` was deleted 2026-08-19, its CPM is on `media_cost` against advertiser-facing spend everywhere else (3.105x median gap), and required spend charges the unserved holdout for impressions (1.1111x). Data is frozen at 2026-06-04.

**Settled scope:** delivering plus lapsed within 365 days = **4,409 advertisers** (1,863 delivering, 2,546 lapsed). No spend floor. The 365-day cut is where silver stops being lossy: every one of the 4,409 keeps a full 56-day rate window and a full 12-month budget window, earliest day needed 2024-08-20 against a 2024-01-01 floor (`outputs/audi_1213_history_floor_options.md`). Dropped: 1,823 lapsed over a year, and 1,410 pre-2024 names visible only in `all_facts` at 2.68 TB per run. The 2,546 former customers still carry revenue, which is why delivery moves off the unauthenticated gist.

## 1. Introduction
The shipped tool is `ti_xxx_mde_calculator_prefill.html` (TI-1019, 2026-06-04), a single-file MDE power calculator with 879 currently-delivering advertisers embedded as static JSON, published on a secret githack gist. Al Beretta asked for it again on 2026-08-20 and, separately on 2026-08-12 (AUDI-1204), asked to screen revenue-churned advertisers it cannot see.

Full delta analysis, re-run requirements and delivery-option comparison: `tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill/artifacts/ti_1019_refresh_scope.md` (18 verified items).

## 2. The Problem
Four defects, ranked by how far each moves the number a user reads:

1. **CPM is on `media_cost`** while the tool's own CLEAR defaults, INCR-75 and AUDI-1204 all price on advertiser-facing spend (media + data + platform). Median gap 3.105x across 670 overlapping advertisers. Required spend is linear in CPM, so picking an advertiser and clicking CLEAR returns budgets on two incompatible definitions.
2. **The generating query is dead.** `agg__daily_sum_by_campaign` was deleted 2026-08-19; the script aborts at its `DECLARE` before any CTE runs.
3. **Required spend charges the unserved holdout for impressions**, running 1.1111x high against `ti_884_mde_calculator.py`, and displayed MDE 1.054x high.
4. **Lapsed advertisers are absent**, which is the population Al actually asked about.

Data is also 77 days stale, and the budget-basis fields are pinned to a window that ended 2026-04-30.

Original template prompts:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

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

- 2026-08-24: Nick Scialli (eng) is implementing the UI MDE view and asked for the formula behind the gist calculator. Verified numerically: gist JS `mdeBinomial` matches `ti_884_mde_calculator.py` exactly (0.206719 both at nT=90k, nC=10k, p=0.02), but `spendRequired` returns 1.1111x high ($769,190 vs $692,271, holdout charged for impressions) and `computeMDE` displays MDE 1.0541x high, reproducing defect 3 above. Sent Nick the ti_884 Python as source of truth with a warning not to port the JS spend conversion. UI implementation may change who the AUDI-1213 refresh serves.

**2026-08-25 scope shrink:** Nick Scialli confirmed the in-product MDE view covers delivering advertisers only, so this ticket is now the lapsed-cohort build alone (2,546 advertisers, 365d): spend-basis + arm-split fixes and the INCR-75 repoint stay, the delivering half and its Mode port are dropped (the UI owns them). Jira description rewritten to match.

**2026-09-03 — the in-product tab shipped WITH the arm-split defect (Edgar von Trotha question).** Edgar asked whether the MDE calculator is the same logic/data behind the new ghost-bid incrementality testing tab. Answer: same formula family, separate implementation, different data. Settled the AUDI-1213 §8 open question by reading the live code: `SteelHouse/gary-ql@cbae0e94 src/utils/mde/computeMde.ts` derives `totalIps` from spend then splits it `nTreated = totalIps*(1-h)` / `nControl = totalIps*h`, so the never-served holdout is charged for impressions. Against the `ti_884_mde_calculator.py` `spend_required` convention that is MDE **1/sqrt(1-h) = 1.0541x high** at h=0.10 (numeric check at a 1M-IP pool, p=0.02: 6.5370% vs 6.2016%, ratio 1.054093). The warning sent 2026-08-24 was not applied, or was applied only to `spendRequired` and not the forward path. Also confirmed live: `DEFAULT_VAR_REDUCTION = 1`, alpha/power hardcoded with no user control, `DEFAULT_CPM`/`DEFAULT_IMPRESSIONS_PER_IP` deleted 2026-08-25 (RX-7420 series) in favor of live per-advertiser ChAPI values, and the baseline moved off the FPA conversion rate / `graph.usersreached` onto `Graph.IPUserSiteVisitorRate` / `Graph.IPUserConversionRate`. Full comparison in `knowledge/experimentation.md` under "Premier-UI status update (2026-09-03)". Follow-up owed to eng: the 1.0541x forward-path correction.

**AUDI-1323 filed 2026-09-03** for the arm-split fix: https://mntn.atlassian.net/browse/AUDI-1323 (Spike, 0 SP, sprint 8303, Relates To AUDI-1213, writeup attached). Owner routing to Nick Scialli is Malachi's to send. Note the sign correction against the 2026-08-24 entry above: the gist `computeMDE` and the tab both read MDE 1.0541x **high**, i.e. pessimistic, so the fix makes tests look easier to power, not harder. The `spendRequired` 1.1111x-high figure is unchanged and independent.

## Delivering-cohort refresh shipped 2026-09-03 (reverses the 2026-08-25 scope shrink)

**Trigger:** Edgar von Trotha, Slack 2026-09-03. The in-product Testing tab forces you to pick an
already-live campaign, so it cannot answer "what budget would this test need?" He is fielding more
customers asking for lift-test budget recommendations and needs what-if budget exploration, which
only the standalone does. That breaks the 2026-08-25 premise that the UI owns delivering advertisers.
The delivering half is back in scope; the lapsed half (2,546) is still open.

**Data.** Re-ran `tickets/incr_75_eligible_advertisers/queries/incr_75_advertiser_metrics.sql`
(471.7 GB dry-run estimate, on the us-central1 reservation) to
`outputs/audi_1213_prefill_metrics.csv`: **1,859 delivering advertisers**, trailing 30d ending
2026-09-03. This is the AUDI-1213 spend-basis fix landing: CPM is now advertiser-facing
(media + data + platform), not `media_cost`, and the 12-month pattern comes from
`summarydata.sum_by_advertiser_by_day` rather than the deleted `agg__daily_sum_by_campaign`.
Universe is wider than TI-1019's 879 because the >$1k spend floor is gone.

**Cohort defaults moved** (CLEAR button, and the no-advertiser initial state):

| | TI-1019 (2026-06-04) | AUDI-1213 (2026-09-03) |
|---|---:|---:|
| CPM | $24.84 | $26.62 |
| imps/IP | 3.5 | 3.43 |
| IVR | 2.15% | 2.639% |
| CVR | 0.054% | 0.086% |

Medians are over a different universe (1,859 vs 879, no spend floor), so this is composition as much
as drift. WGU CPM $4.607 to $9.222 is the spend-basis change, not a rate move.

**Three code defects fixed in the same pass:**

1. `computeMDE` charged the unserved holdout for impressions. Now
   `nTreated = spend-derived pool`, `nControl = nTreated * h/(1-h)`. Was 1.0541x high at h=0.10.
2. `spendRequired` had the mirror defect (`nTotal * impsPerIp * cpm`, now
   `nTotal * (1-h) * impsPerIp * cpm`). Was 1.1111x high. Fixing either alone breaks the
   budget-to-MDE inverse, which is why both moved together.
3. **New defect found 2026-09-03:** `setOutcome` never read `S.advertiser`, so toggling IVR to CVR
   with an advertiser loaded silently dropped that advertiser's rate and substituted the cohort
   default. It also never wrote `S.currentOutcome`, so `clearAdvertiser`'s
   `setOutcome(S.currentOutcome)` always restored IVR. Both fixed; cohort defaults now read from a
   generated `window.COHORT` instead of three hardcoded literals.

**Verified numerically** (`node` against `ti_884_mde_calculator.py`, 3 cases spanning h=0.10/0.20 and
p=0.0058/0.107/0.1183): `mdeRel` agrees to <1e-11, and `spendRequired(computeMDE(budget)) == budget`
to <1e-9. The round-trip is the check that catches fixing only one side.

**Files:** `artifacts/audi_1213_build_prefill.py` (CSV to JSON + cohort medians),
`artifacts/audi_1213_patch_calculator.py` (rebuilds the HTML from the TI-1019 file; every anchor
asserts uniqueness so a silent no-op edit fails loudly), `artifacts/audi_1213_mde_calculator.html`,
`outputs/audi_1213_prefill_compact.json`.

**Published** to the existing gist under the original filename so every previously shared link stays
valid: https://gist.githack.com/mdunn-mntn/2d362849df017fa243eef03bb61cdfbb/raw/ti_xxx_mde_calculator_prefill.html
Live copy byte-identical to the local build. Exposure widened from 879 to 1,859 named delivering
advertisers now carrying advertiser-facing spend; still no lapsed/churned accounts, so the former-customer
question the framing raised is untouched.

**Still open:** the lapsed 2,546 cohort, the Mode port, and the `VR_STACK` 0.595 re-measurement.

**Sanity flag:** ElevenLabs (51660) reads IVR 0.58% and CPM $31.80 against 3.07% / $8.58 in June. The
account paused a $770K campaign group on 2026-08-20 (AUDI-1215), so the trailing-30d window spans that
change. Not reconciled here.

## AUDI-1324 folded back in, 2026-09-03

Filed AUDI-1324 in the afternoon to split the Mode port out of this ticket, then closed it as a
duplicate the same day on the user's call. The split was wrong: it treated the Mode report as a
separate deliverable when it is the calculator itself, which left two tickets describing one tool.
AUDI-1213 is now the whole thing, retitled "One weekly-refreshed Mode MDE calculator for every
advertiser".

Build artifacts stay under `tickets/audi_1324_mde_calculator_mode_dashboard/` because the committed
scripts and the deployed Mode report reference those paths; that folder's `summary.md` carries a
pointer here.

**Shipped:** the delivering cohort (1,859 advertisers) on advertiser-facing spend, all three
calculator defects fixed, live in Mode report `9a5afa55ca99` and on the original gist link.

**Open:** the 2,546 lapsed advertisers, the weekly schedule and publish/share (both Mode UI actions
the user owns), and the `VR_STACK` 0.595 re-measurement.

## Mode delivery closed out, 2026-09-03

Everything reachable from the API is done. Report `9a5afa55ca99` ("MDE Calculator") carries the
query, the layout and a successful run of 1,859 advertisers, and the layout is byte-identical to
`tickets/audi_1324_mde_calculator_mode_dashboard/artifacts/audi_1324_index.html`.

**Two Mode defects found and fixed after first deploy, both from the fragment conversion:**

1. **Nothing rendered and no control responded.** The first push was a whole HTML document. Mode
   injects the layout into its own page, so the doctype/html/head/body wrappers meant none of the
   scripts executed. Fix: emit a fragment (CDN tags, scoped style, markup under `div#mde`, scripts),
   and call the boot directly rather than waiting on `DOMContentLoaded`, which has already fired by
   the time Mode injects.
2. **The CSS reset never applied.** The scoper mapped the bare universal selector to `#mde` itself,
   so `* { box-sizing: border-box; margin: 0; padding: 0 }` became `#mde { ... }` and nothing inside
   inherited the reset. Fix: `*` scopes to `#mde *`; duplicate selectors inside one rule collapse.

Also hardened: the launcher now waits for `window.Chart` as well as `window.datasets` (booting on
datasets alone could beat the Chart.js CDN, and `initChart` throwing killed the whole render), and
`initChart()` is wrapped so a chart failure no longer stops the numbers.

**Code cleanups from the verification sweep:** the `spendRequired` header comment still described
the pre-fix behaviour ("totalSpend covers full N_total reach") and is corrected; the
`_origSetOutcome` monkey-patch wrapper was removed, since its advertiser branch duplicated the fixed
base function line for line and would silently shadow any future edit to it.

**Regression harness:** `jsdom` reproduction of Mode's injection (innerHTML then re-created script
tags) under both Chart.js load orders, plus a round-trip parity grid over h in {0.05, 0.10, 0.20,
0.50} and p in {0.0006, 0.0058, 0.02, 0.107, 0.30}. Max relative error on
`spendRequired(computeMDE(budget)) == budget` is 5.82e-16 in both builds.

**Cannot be done from the API, both need the Mode UI:**
- The weekly schedule. `POST /reports/<token>/schedules` rejects every cron format tried
  (`0 6 * * 1`, 6-field, `@weekly`, named day) with "Cronline must be formatted as a cron string"
  on a member API key. Treat schedule creation as UI-only.
- Publishing and sharing. `published_at` is null, `public` false, `run_privately` true, and there is
  no `/publish` endpoint (404), so nobody outside the owning account can open the report yet.

**Still open on this ticket:** the 2,546 lapsed advertisers and the `VR_STACK` 0.595
re-measurement. The lapsed build needs a decision first: rate windows anchored per advertiser on
their own last-active day mean scanning `cost_impression_log` across 365 days rather than 30, and
the 30-day scan alone dry-runs at 471.7 GB. That cost lands on every scheduled run, so the cohort
cut is a cost decision, not just a query change.

## Lapsed cohort added, 2026-09-03 (365-day lookback)

Decision: full 365 days, confirmed by Malachi and Edgar von Trotha. Edgar's reason is the one that
settles it, and it is not a cost argument: every LiftLab test he builds uses at least a 365-day
lookback to account for seasonality, so a shorter window would put our screening on a different
footing from the tests it feeds. Malachi's version: some major clients run for one or two months a
year.

**Cohort sizing that informed the cut** (`sum_by_advertiser_by_day`, 0.097 GB):

| Bucket | Advertisers | 365d spend | Over $100k |
|---|---:|---:|---:|
| delivering (0-30d) | 1,868 | $385.6M | 516 |
| lapsed 31-90d | 568 | $36.7M | 74 |
| lapsed 91-180d | 701 | $37.1M | 96 |
| lapsed 181-365d | 1,288 | $38.2M | 88 |

The 181-365d bucket is the one a shorter cut would have thrown away, and it is the largest by
advertiser count with 88 six-figure accounts in it. Dry-run scan by cut: 90d 0.72 TB · 180d 1.47 TB
· 365d 3.12 TB, against 0.47 TB for the delivering-only query.

**Correction to a number quoted in Slack:** the 365-day version was described as "roughly 12x the
data". It is not. Actual billed 2,908 GB against 472 GB, so **6.2x**, and 53s wall / 47,852 slot-sec
on the reservation. The 12x came from scaling 30 days to 365 linearly, which ignores that
`ui_conversions` and `sum_by_advertiser_by_day` are small and that the delivering query also carried
a separate 56-day reach scan.

**Query shape.** `queries/audi_1324_advertiser_prefill.sql` now resolves each advertiser's
`last_active_day` from `sum_by_advertiser_by_day`, then windows that advertiser's rates on their own
last 30 delivering days rather than on a fixed calendar window. A lapsed advertiser is therefore
measured on how it actually performed while running, not on its silence. `is_delivering`,
`last_active_day` and `days_since_active` are carried through to the UI. The monthly-spend pattern
looks back 730 days but is capped at each advertiser's `last_active_day`, so a lapsed budget anchors
on their exit run-rate.

**Cohort medians now come from delivering advertisers only** (1,857 of 4,387), so the CLEAR button
still describes a live account rather than being dragged by dormant ones: CPM 26.60 · imps/IP 3.58 ·
IVR 2.561% · CVR 0.082%. These differ slightly from the delivering-only run earlier the same day
(26.62 / 3.43 / 2.639% / 0.086%) because each advertiser's window is now their own last 30
delivering days instead of the trailing calendar 30.

**UI.** The picker appends `· LAPSED` to the name and shows `last active <date>` in place of
`/30d`; the loaded pane reads `LAPSED 201d (last active 2026-02-14)`. Verified in the jsdom harness
with one delivering and one lapsed advertiser: labels render, cohort medians resolve to the
delivering advertiser's values, no console errors.

**Live:** Mode run `e2073005c2f1` returned 4,387 rows (1,857 delivering, 2,530 lapsed), 180s
end to end. Largest lapsed accounts: Absher Land Service $633k (180d), LifeMD $535k (221d),
UnitedHealthcare $322k (231d). Gist republished from the same build.

## VR_STACK 0.595 re-measured and refuted, 2026-09-03

Four independent measurements against `dw-main-gold.reporting.lift__ghost_bid_results`, each
adversarially re-run by a second agent. Full write-up in `knowledge/experimentation.md` under
"VR_STACK 0.595 is REFUTED end to end".

**Headline: the defensible multiplier is 1.0, not 0.595 and not 0.794.** Over 421 clean campaign
groups the empirical multiplier (`z * se_reported` over the naive binomial MDE) has median 1.0030
and SD 0.0073, and **zero campaigns land at or below 0.595 or 0.794** under any of five gate
variants. 0.595 sits 56 SDs below the observed minimum.

**Why, mechanically:** `se_reported / sqrt(p_t(1-p_t)/n_t + p_h(1-p_h)/n_h)` = 1.000000000 at both
min and max across all 3,273 rows. The ghost-bid pipeline's SE *is* the plain two-proportion
binomial SE. Nothing in the lineage applies CUPED, ghost-ad conditioning or stratification, so there
is no variance reduction for the calculator to inherit.

**Per factor:** ghost-ad 0.75 is a LATE rescaling that *divides* SE (inflating it 1.33x) and is
applied here as a multiplier, the wrong sign; its exposure rate also measures 0.493 mean, not 0.75.
Stratified 0.85 buys ~1.00 on a rare-binary outcome and was authored for a different design
(advertiser-randomized rollout, continuous outcome). CUPED 0.934 roughly survives on its own terms
(measured rho 0.3299 over 18.4M units gives 0.944) but is moot, since the pipeline does not apply
CUPED.

**Method caveat worth keeping.** The first cut of this measurement compared the pipeline's `se`
against a binomial SE rebuilt from the same four stored integers, which is an arithmetic identity
with zero degrees of freedom: it returns 1.0000 by construction and returns it just as readily on
the `bid_count` strata that are documented as contaminated. The verifier caught it. The end-to-end
result above is not that artifact, because its numerator is the stored `se` column and its
denominator is the calculator's own formula on independently pulled arm sizes, but the near-miss is
the reason the SE-identity line is reported as a mechanism finding rather than as the measurement.

**Consequence for anything already circulated:** every post-stack MDE this tool has published is
understated by 1/0.595 = 1.68x, and every post-stack budget by 2.82x.

**Not yet actioned.** `VR_STACK` is still 0.595 in both builds and the FULL STACK toggle still ships.
Changing it moves numbers people have already been quoted, so it needs Malachi's call. Options: set
`VR_STACK` to 1.0 and keep the toggle as a no-op placeholder, or remove the toggle and the
post-stack hero entirely and show RAW only.

**Toggle removed 2026-09-03** (Malachi's call, after the refutation above). Gone from both builds:
the VARIANCE REDUCTION control and `setVR`, the POST-STACK MDE hero and its CI line, the dashed
post-stack chart series and its CI band, the second position marker, the legend entry, and the
`VR_STACK` constant with `S.vrMode`. `spendRequired` and `computeMDE` are now called with
`var_reduction = 1.0` at every call site. The footer keeps only the source citation; a first
draft explained the absence there and Malachi cut it, since a chart footnote is the wrong place to
argue a methodology point. Round-trip parity unchanged at 5.82e-16; jsdom harness clean.

Anyone holding a post-stack figure from before today is holding a number 1.68x too small.
