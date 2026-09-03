---
doc_type: ticket
title: "AUDI-1213: mde calculator refresh"
status: backlog
date: 2026-08-20
summary: "Refresh the MDE calculator onto advertiser-facing spend and a corrected arm-split, add the 365-day lapsed cohort, host on Mode"
result: "not started"
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
