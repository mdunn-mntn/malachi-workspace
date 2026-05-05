# TI-892: Review Owner (AID 46020) geo-holdout test — input to BER-2250 / TI-886

**Jira:** https://mntn.atlassian.net/browse/TI-892
**Status:** In Progress (Phase 0 setup complete; blocked on Lauren/Edgar materials)
**Date Started:** 2026-05-05
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

The Owner geo-holdout test (advertiser AID 46020) was set up by Lauren while Malachi was on vacation, using ChatGPT to select holdout geos. The original Jira framing tied this review to TI-885 (mid-intent experiment design); after the 2026-05-04 BER-2250 review, TI-885 was closed in the pivot to bidder-level ghost bidding. **Reusable methodology now lands in TI-886 (uplift model implementation) and the BER-2250 lessons doc.**

Edgar von Trotha's role on BER-2250 is general — six lessons distilled from 55+ third-party tests, already captured in `tickets/ber_2250_incrementality_overhaul/artifacts/lessons_from_past_incrementality_tests.md`. This ticket harvests Owner-test-specific design choices that those generalized lessons may not cover.

## 2. The Problem

We need a structured methodology review of the Owner geo-holdout test so that:
- Reusable design elements (geo selection, holdout sizing, variance reduction, success metric) feed TI-886 model design
- New lessons not already in `lessons_from_past_incrementality_tests.md` get captured
- Anything broadly reusable lands in `knowledge/experimentation.md`

**Current blocker:** zero artifacts exist for the Owner test in this workspace. AID 46020 surfaces only as a candidate row in TI-884's power-analysis SQL — no design doc, no setup notes, no interim numbers.

## 3. Plan of Action

### Phase 0 — Setup (this commit)
1. ✅ Create ticket folder structure
2. ✅ summary.md with Introduction/Problem from Jira + draft request prompt
3. Add Todoist task + subtasks
4. Commit + push

### Phase 1 — Source materials (BLOCKED on Lauren / Edgar)
5. User sends the request prompt below to Lauren + Edgar
6. On reply, save raw materials to `artifacts/owner_geo_holdout_raw.md`
7. Write one-page extract in this summary's Investigation section

### Phase 2 — Cross-check
8. Compare design vs the 6 lessons in `lessons_from_past_incrementality_tests.md`
9. Cross-reference `knowledge/experimentation.md`
10. Flag confirms / extends / contradicts

### Phase 3 — Apply
11. Identify reusable elements for TI-886 (T-learner / bidder-level model)
12. Update `lessons_from_past_incrementality_tests.md` with Owner-specific findings
13. Promote to `knowledge/experimentation.md` if broadly reusable
14. Comment on TI-886 if any Owner-test element should change its design

### Phase 4 — Document & close
15. Final summary with cross-check + recommendations
16. Jira comment on TI-892 (curl + REST v2 wiki markup) with file links
17. Self-review entry in `self_review/self_review_2.md` — Craft section
18. Transition TI-892 to Done

## 4. Investigation & Findings

_Pending Phase 1 materials._

## 5. Solution

_Pending._

## 6. Questions Answered

_Pending._

## 7. Data Documentation Updates

_Pending Phase 3 — anticipated updates to `lessons_from_past_incrementality_tests.md` and possibly `knowledge/experimentation.md`._

## 8. Open Items / Follow-ups

- **BLOCKER:** Need Lauren's setup doc (geos, rationale, holdout size, pre-period, success metric, variance reduction, sample size) and current numbers. Also any lessons-learned from Edgar.
- After review, decide whether the BER-2250 epic summary's "TI-892 (Edgar geo-holdout)" line warrants a corrective note (currently leaving as-is per user direction — dated section, doesn't affect current work).

---

## Appendix A — Request prompt (send to Lauren + Edgar)

> **Subject:** Quick ask — Owner (AID 46020) geo-holdout test methodology, for BER-2250 review (TI-892)
>
> Hi Lauren / Edgar,
>
> I'm doing a methodology review of the Owner geo-holdout test (AID 46020) as input to BER-2250 — the goal is to extract reusable design elements for our active incrementality workstream (TI-886, bidder-level ghost bidding model). Could you share whatever's easiest from the list below? Bullet replies fine, no need for a polished writeup.
>
> **Design**
> - Which geos held out, and what was the selection rationale? (If ChatGPT picked them, the prompt + output is great context.)
> - Holdout size — % of impressions / spend / population, and why that size?
> - Pre-period length and dates
> - Treatment definition — what changed for treated geos vs held-out geos?
> - Primary success metric (visit rate? conversions? IVR? revenue?) and how it's measured
> - Variance reduction approach if any (CUPED, geo-pair matching, synthetic control, etc.)
> - Sample size / power assumptions (MDE, expected effect size)
>
> **Status & numbers**
> - Launched / running / complete? Start + end dates
> - Any interim or final numbers — treated vs holdout, with the test stat or confidence interval if you have it
>
> **Lessons**
> - Anything you'd do differently next time?
> - Any gotchas (data quality, attribution, geo-leakage, etc.) we should flag for TI-886?
>
> Output goes into `tickets/ber_2250_incrementality_overhaul/ti_892_owner_geo_holdout_review/` and a Jira summary on TI-892. Will credit you both. Thanks!
