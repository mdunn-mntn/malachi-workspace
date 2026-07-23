---
doc_type: ticket
title: "TI-254: Investigate Low NTB Percentage"
status: done
date: 2026-03-03
summary: "Investigation into why New-to-Brand (NTB) percentages were lower than expected."
result: "Answered via TI-650: cross-device events (61% IP mutation) drive NTB misclassification."
keywords: [NTB, New-to-Brand, is_new, clickpass_log, ui_visits, cross-device, IP mutation, TI-650, TI-310]
---

## TL;DR

**Q:** Why are NTB (New-to-Brand) percentages lower than expected across certain campaigns/advertisers?

**A:** Only partially answered, and via sibling ticket TI-650. NTB = visitor had not been to the advertiser's site before (based on GUID/cookie match). No local analysis files remain for TI-254 itself (only an .idea/ folder); the investigation was likely conducted in Greenplum or notebooks that weren't saved locally. The documented answer: cross-device events (61.2% IP mutation rate) drive NTB misclassification, per TI-650, which also quantified a 42% disagreement rate between clickpass_log.is_new and ui_visits.is_new. TI-310 (NTB Investigations) is the continuation of this work.

**How:** No queries or outputs remain locally. Planned steps were: baseline NTB rates across campaigns, compare clickpass_log.is_new vs ui_visits.is_new, investigate cross-device events as a confounder, and determine pipeline vs business-logic cause. These are Plan steps, not documented as executed within TI-254. The quantified findings cited come from TI-650.

**Tables:** clickpass_log, ui_visits

**Learned:**
- TI-254 has no local analysis files; findings live in TI-650 and continuation ticket TI-310.
- The summary's answer (61.2% IP mutation from cross-device, 42% is_new disagreement) is sourced from TI-650, hedged as 'partially answered', not original TI-254 work.

**Reuse when:**
- Someone asks why NTB rates look low or about NTB misclassification
- Question relates clickpass_log.is_new to ui_visits.is_new
- Tracing the TI-254 / TI-310 / TI-650 NTB investigation lineage


# TI-254: Investigate Low NTB Percentage

**Jira:** https://mntn.atlassian.net/browse/TI-254
**Status:** Complete (investigation concluded; no files remaining locally)
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Investigation into why NTB (New-to-Brand) percentages were lower than expected across certain campaigns or advertisers. NTB = visitor had not been to the advertiser's site before (based on GUID/cookie match).

---

## 2. The Problem

NTB rates appeared lower than expected. Possible causes: data pipeline issue, definition mismatch, cross-device inflation of returning visitors, or a legitimate business change in audience composition.

---

## 3. Plan of Action

1. Baseline NTB rates across campaigns
2. Compare `clickpass_log.is_new` vs. `ui_visits.is_new`
3. Investigate cross-device events as a confounding factor
4. Determine if pipeline or business-logic issue

---

## 4. Investigation & Findings

No local analysis files remain (`.idea/` folder only). Investigation likely conducted in Greenplum or notebooks that weren't saved locally.

This investigation contributed to the broader NTB work continued in TI-310 and TI-650.

**See also:** TI-650 summary for NTB disagreement findings (42% disagreement rate between `clickpass_log.is_new` and `ui_visits.is_new`).

---

## 5. Solution

TBD — findings not documented locally. See TI-310 (NTB Investigations) for continued work.

---

## 6. Questions Answered

- **Q:** Why is NTB% lower than expected?
  **A:** Partially answered in TI-650 — cross-device events (61.2% IP mutation rate) drive NTB misclassification.

---

## 7. Data Documentation Updates

None documented.

---

## 8. Open Items / Follow-ups

- TI-310 (NTB Investigations) is the continuation of this work.
- TI-650 quantified the NTB disagreement between pipeline stages.

---

## Drive Files

📁 `Tickets/TI-254 Investigate Low NTB Percentages/`
- (Empty on Drive)
