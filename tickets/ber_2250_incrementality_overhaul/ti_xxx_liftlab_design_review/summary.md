# TI-xxx: Review LiftLab incrementality test designs for 6 prospective customers

**Jira:** (not yet ticketed — see Open Items)
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
2. ✅ Build a **test-design review framework + per-design scorecard** → `artifacts/ti_xxx_design_review_framework.md`.
3. ⬜ Request the 6 LiftLab tool outputs + Edgar's tweaks ahead of Tuesday (draft note in framework doc).
4. ⬜ Pre-score the 6 designs against the scorecard once received; flag any RED levers.
5. ⬜ Review live Tue 6/23; capture decisions + design changes in `meetings/`.
6. ⬜ File Jira ticket (under BER-2250 / new, since TI-855 is Released); update self-review.

## 4. Investigation & Findings
Grounding pulled from existing workspace knowledge (no new queries yet):

- **Power (iROAS playbook / Lewis-Rao):** typical 10M-impression CTV campaign sits at break-even MDE; <5M impressions → directional only. **Refuse MDE > 15%.** Below 5M imps, no point estimate without ±~50pp interval.
- **Edgar's 6 lessons (50-test review):** (1) good design ≠ good efficiency; (2) **audience strategy drives more than test structure** — high-intent/retargeting underperform, broad prospecting wins; (3) **exposure density > total spend**; (4) impact often outside the primary KPI; (5) short/reactive tests churn customers; (6) weak results still valuable as retest inputs.
- **Tracker:** 9 prior LiftLab tests of 55; norm = 6-wk min + 2-wk post; ~50% holdout common on geo, 33% on 3-cell; completed lifts mostly <1%.
- **Method:** LiftLab = geo-based lift (randomized geo holdout + synthetic control, and/or switchback/time tests). Treated as defensible (TI-856) — review is about design *parameters*, not trust.
- **Campaign maturity:** exclude first ~4 weeks (TI-780 — only ~89% of steady-state IVR by wk 4).

10 review levers (full detail + questions + fillable scorecard) → `artifacts/ti_xxx_design_review_framework.md`.

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
- ⬜ File the Jira ticket (pending user confirmation on whether to formalize) and rename `ti_xxx_*` to the real number.
- ⬜ Capture meeting outcomes in `meetings/`.
