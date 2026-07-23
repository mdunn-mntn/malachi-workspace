---
doc_type: ticket
title: "TI-1039: LiftLab Incrementality Test Design Review (6 customers)"
status: in_progress
date: 2026-06-17
summary: "Expert review of 6 LiftLab incrementality test designs before customer offers firm up."
result: "in progress — review framework/scorecard built; awaiting 6 designs + 6/23 review"
keywords: [ti-1039, liftlab, incrementality, test design review, edgar von trotha, mde, lewis-rao, power analysis, geo holdout, ber-2250]
---

## TL;DR

**Q:** Read TI-1039 LiftLab design review summary; produce TL;DR card + delta_facts.

**A:** TI-1039 is a Tier-1 expert review of 6 LiftLab incrementality test designs for prospective customers, requested by Edgar von Trotha before offers firm up (review meeting Tue 2026-06-23). Status: in progress. The deliverable built so far is a test-design review framework + per-design scorecard (10 review levers) at artifacts/ti_1039_design_review_framework.md. The 6 actual designs had not yet been received, so no design has been pre-scored. Three things to land at the meeting: (1) power/MDE first (refuse MDE > ~15%; below 5M impressions directional-only, no point estimate without a +-~50pp interval per Lewis-Rao), (2) audience strategy is the biggest controllable swing (high-intent/retargeting underperform, broad prospecting wins), (3) protect the customer relationship (>=6-wk test + 2-wk post, exclude first ~4 weeks of ramp per TI-780, no early reads, frame nulls as retest inputs). LiftLab's methodology is treated as defensible (TI-856) - the review is about design parameters, not trust. No new queries or schema work; outputs/ and queries/ folders are empty.

**How:** No queries run. Grounding pulled from existing workspace knowledge: iROAS playbook / Lewis-Rao power section, Edgar's 6 lessons from his 50-test review, the customer tracker (55 tests, LiftLab 9), TI-856 methodology map, TI-780 ramp window. Deliverable is a written framework + scorecard, not an analysis output.

**Learned:**
- TI-1039 = review of 6 LiftLab incrementality designs pre-offer; review framework/scorecard built, 6 designs not yet received or pre-scored as of the summary.

**Reuse when:**
- Reviewing a third-party vendor lift-test design
- LiftLab beta customer studies
- Power/MDE and audience-strategy checks before an incrementality test

# TI-1039: Review LiftLab incrementality test designs for 6 prospective customers

**Jira:** https://mntn.atlassian.net/browse/TI-1039 (relates to BER-2250, TI-855)
**Status:** In Progress
**Date Started:** 2026-06-17
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction
Edgar von Trotha (3P-attribution liaison, owns the LiftLab beta customer pipeline) has identified **6 customers** to offer a LiftLab incrementality study. LiftLab's web tool produced a recommended design per customer; Edgar tightened those with "more conservative boundaries." He wants a TI/expert review of the **test designs** before the offers firm up. Caveat from Edgar: designs may still change once LiftLab receives complete performance data — nothing is locked.

Review meeting: **Tue 2026-06-23.**

This is Tier-1 work: incrementality via approved third-party vendors is Kale's #1 priority, and "run 5 experiments with external vendors" is the Q2 OKR. Builds on the now-closed TI-855 epic (TI-856 LiftLab methodology map, TI-884 power analysis, TI-835 observational lift, TI-883 primer).

## 2. The Problem
Make sure each of the 6 designs can **detect the effect it claims to** and that the inputs MNTN controls don't pre-doom the result. The failure mode to prevent: a clean-looking design that returns "no detectable lift" because it was underpowered or aimed at a high-intent audience — which the customer reads as "MNTN doesn't work."

Constraints/realities going in:
- **Power:** CTV incrementality is severely underpowered (Lewis-Rao). Many real tests land <1% lift.
- **LiftLab bias:** paid by the advertiser → conservative measurement by construction.
- **Audience is the swing factor** and it's ours to set (TI-835 + Edgar's 50-test review).
- We don't yet have the actual 6 designs — need them from Edgar to review specifics.

## 3. Plan of Action
1. ✅ Pull existing context: experimentation.md (LiftLab + power), iROAS playbook, Edgar's 6 lessons, customer tracker; confirm TI-855/856/857 status (all Done/Released).
2. ✅ Build a **test-design review framework + per-design scorecard** → `artifacts/ti_1039_design_review_framework.md`.
3. ⬜ Request the 6 LiftLab tool outputs + Edgar's tweaks ahead of Tuesday (draft note in framework doc).
4. ⬜ Pre-score the 6 designs against the scorecard once received; flag any RED levers.
5. ⬜ Review live Tue 6/23; capture decisions + design changes in `meetings/`.
6. ✅ Filed Jira ticket TI-1039 (standalone, relates-to BER-2250 + TI-855); ⬜ update self-review after the meeting.

## 4. Investigation & Findings
Grounding pulled from existing workspace knowledge (no new queries yet):

- **Power (iROAS playbook / Lewis-Rao):** typical 10M-impression CTV campaign sits at break-even MDE; <5M impressions → directional only. **Refuse MDE > 15%.** Below 5M imps, no point estimate without ±~50pp interval.
- **Edgar's 6 lessons (50-test review):** (1) good design ≠ good efficiency; (2) **audience strategy drives more than test structure** — high-intent/retargeting underperform, broad prospecting wins; (3) **exposure density > total spend**; (4) impact often outside the primary KPI; (5) short/reactive tests churn customers; (6) weak results still valuable as retest inputs.
- **Tracker:** 9 prior LiftLab tests of 55; norm = 6-wk min + 2-wk post; ~50% holdout common on geo, 33% on 3-cell; completed lifts mostly <1%.
- **Method:** LiftLab = geo-based lift (randomized geo holdout + synthetic control, and/or switchback/time tests). Treated as defensible (TI-856) — review is about design *parameters*, not trust.
- **Campaign maturity:** exclude first ~4 weeks (TI-780 — only ~89% of steady-state IVR by wk 4).

10 review levers (full detail + questions + fillable scorecard) → `artifacts/ti_1039_design_review_framework.md`.

## 5. Solution
Deliverable = the review framework + scorecard (above). Three things to land Tuesday: **(1) power/MDE first**, **(2) audience strategy is the biggest controllable swing**, **(3) protect the customer relationship** (no early reads, retest framing).

## 6. Questions Answered
- **Q:** Is this a new workstream or covered by an existing ticket? **A:** New — TI-855 (vendor epic) is Released; TI-856/857/884 all Done. No open ticket for the 6-design review.
- **Q:** Do we re-litigate LiftLab's methodology? **A:** No — TI-856 mapped it; geo holdout / switchback are defensible. Review the design parameters.

## 7. Data Documentation Updates
None yet (no new schema/data). If the meeting surfaces reusable design-review patterns, add a "Reviewing a vendor lift-test design" subsection to `knowledge/experimentation.md`.

## 8. Open Items / Follow-ups
- ⬜ **Get the 6 designs from Edgar** before Tuesday (note drafted in framework doc).
- ⬜ Pre-score the 6 once received.
- ✅ Filed TI-1039 and renamed folder/files to the real number.
- ⬜ Capture meeting outcomes in `meetings/`.
