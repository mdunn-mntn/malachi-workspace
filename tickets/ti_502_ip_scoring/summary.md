---
doc_type: ticket
title: "TI-502: IP Scoring — How We Use Scores"
status: done
date: 2026-03-03
summary: "Document how MNTN uses IP scores across the bidding and targeting pipeline"
result: "Reference doc created; scores live in cost_impression_log.model_params + membership DB"
keywords: [ip scoring, model_params, realtime_conquest_score, rtc, jaguar, ds13, membership db, cost_impression_log]
---

## TL;DR

**Q:** How does MNTN use IP scores across the bidding/targeting pipeline, and where do the scores live in the data?

**A:** TI-502 (done) is a reference-doc ticket documenting how MNTN uses IP scores. Findings: IP scores live in cost_impression_log.model_params as key=value pairs for per-impression scores, and in the membership DB for IP-level targeting scores applied at bid time. Key score types: realtime_conquest_score=10000 is the RTC (Real-Time Conquest) flag in model_params; Jaguar/DS13 audience-intent scores are stored in the membership DB and applied at bid time. Deliverable = a reference spreadsheet/PDF ("How We Use Scores"); the gsheet lives in the TI-501 Drive folder. Follow-up TI-541 covers the full IP scoring pipeline architecture.

**How:** Read the summary in full; checked for queries/ and outputs/ (neither folder exists). This is a documentation ticket with no BQ queries run — findings are the reference-doc content, not analysis results.

**Tables:** cost_impression_log

**Learned:**
- TI-502 is a reference/documentation ticket with no queries/ or outputs/ folders.

**Reuse when:**
- Someone asks where IP scores live in the data
- Someone asks what realtime_conquest_score=10000 means
- Someone wants a reference on how MNTN uses scores in bidding

---


# TI-502: IP Scoring — How We Use Scores

**Jira:** https://mntn.atlassian.net/browse/TI-502
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Documentation and reference ticket for how MNTN uses IP scores in the bidding and targeting pipeline. Covers score types, their sources, and how they flow from scoring systems into campaign decisions.

---

## 2. The Problem

No single reference document explaining how IP scores are used across the MNTN platform. Needed to document the score types, their meaning, and how they influence bidding.

---

## 3. Plan of Action

1. Document score types (Jaguar/DS13, RTC, etc.)
2. Explain how scores flow from scoring pipeline → bidder
3. Document `model_params` field structure in `cost_impression_log`

---

## 4. Investigation & Findings

**Local artifact:**
- `how_we_use_scores.pdf` (gitignored — see Drive for Google Sheets version)

**Drive:**
- `TI-502 How We Use Scores.gsheet` — reference spreadsheet (stored in TI-501 Drive folder)

Key score types:
- `realtime_conquest_score=10000` — RTC (Real-Time Conquest) flag in `model_params`
- Jaguar/DS13 scores — audience intent scores stored in membership DB, applied at bid time
- Scores appear in `cost_impression_log.model_params` as key=value pairs

---

## 5. Solution

Reference document created explaining score usage.

---

## 6. Questions Answered

- **Q:** Where do IP scores live in the data?
  **A:** `cost_impression_log.model_params` for per-impression scores; membership DB for IP-level targeting scores.

- **Q:** What is `realtime_conquest_score=10000`?
  **A:** The RTC flag — identifies impressions targeted via Real-Time Conquest.

---

## 7. Data Documentation Updates

- `cost_impression_log.model_params`: contains score signals including `realtime_conquest_score`.

---

## 8. Open Items / Follow-ups

- TI-541: Full IP scoring pipeline architecture documentation.

---

## Drive Files

📁 `Tickets/TI-501 Jaguar Analysis/` (stored here, not in a TI-502 folder)
- `TI-502 How We Use Scores.gsheet` — score reference document
