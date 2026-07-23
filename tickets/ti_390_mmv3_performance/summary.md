---
doc_type: ticket
title: "TI-390: Investigate MMv3 Performance"
status: done
date: 2026-03-03
summary: "Verify MMv3 audience-membership model change improved targeting without regressions"
result: "Investigation complete; findings documented in Drive gdoc and gsheet"
keywords: ["mmv3", "membership model v3", "audience targeting", "ivr", "ti-390"]
---

## TL;DR

**Q:** Did MMv3 (Membership Model v3) improve audience targeting performance without regressions after rollout?

**A:** Investigation complete, but findings live only in the Drive gdoc/gsheet; the local summary is a stub with no metrics recorded.

**How:** Planned pre/post-MMv3 cohort comparison on IVR, conversion rate, and audience-quality metrics; actual results and numbers exist only in the Drive gdoc and gsheet, not in the local summary.

**Tables:** none

**Learned:**
- TI-390 findings are Drive-only (TI-390 MMv3 Performance Investigation gdoc + gsheet); no local files and no metrics in summary.md.
- Summary describes MMv3 as a model update to the IP audience membership system that changed how IPs are scored and assigned to audience segments.

**Reuse when:** MMv3 performance investigation; did MMv3 improve targeting; Membership Model v3 pre/post cohort comparison

# TI-390: Investigate MMv3 Performance

**Jira:** https://mntn.atlassian.net/browse/TI-390
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Investigation into the performance of MMv3 (Membership Model v3), a model update to the IP audience membership system. MMv3 changed how IPs are scored and assigned to audience segments.

---

## 2. The Problem

After MMv3 rollout, needed to verify that performance was as expected — that targeted IPs were converting at higher rates and that the model change didn't introduce regressions.

---

## 3. Plan of Action

1. Define pre-MMv3 vs. post-MMv3 cohorts
2. Pull performance metrics for each cohort
3. Compare IVR, conversion rate, and audience quality metrics
4. Report findings

---

## 4. Investigation & Findings

**All files on Drive** (no local files):
- `TI-390 MMv3 Performance Investigation.gdoc` — written investigation findings
- `TI-390 MMv3 Performance Investigation.gsheet` — data and charts

---

## 5. Solution

Performance investigation complete. Findings documented in Drive.

---

## 6. Questions Answered

- **Q:** Did MMv3 improve audience targeting performance?
  **A:** See Drive gdoc for findings.

---

## 7. Data Documentation Updates

None documented locally — review Drive gdoc for any schema learnings.

---

## 8. Open Items / Follow-ups

- None known.

---

## Drive Files

📁 `Tickets/TI-390 Investigate MMv3 Performance/`
- `TI-390 MMv3 Performance Investigation.gdoc` — investigation writeup
- `TI-390 MMv3 Performance Investigation.gsheet` — data
