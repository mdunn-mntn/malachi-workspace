# TI-391: Analyze Pre-Post of Audience Intent Scoring Changes

**Jira:** https://mntn.atlassian.net/browse/TI-391
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Pre/post analysis of performance changes resulting from updates to the Audience Intent Scoring system (Jaguar/DS13). Audience Intent Scoring ranks IPs by their likelihood to convert; changes to the scoring model can shift which IPs get targeted.

---

## 2. The Problem

After an update to the Audience Intent Scoring model, needed to measure whether the new scores improved campaign performance (IVR, conversions, CPV) vs. the prior scoring model.

---

## 3. Plan of Action

1. Define pre/post windows around scoring model change date
2. Pull performance metrics segmented by score tier or model version
3. Statistical comparison
4. Report findings

---

## 4. Investigation & Findings

**All files on Drive** (no local files):
- `TI-391 Analyze Pre Post Audience Intent Changes.gdoc` — written findings
- `TI-391 Analyze Pre Post Audience Intent Changes.gsheet` — data

**Related architecture:** See `documentation/architecture/audience_intent_scoring.png` and `audience_intent_graph.png` for system diagrams. These were also found in the TI-541 Drive folder.

---

## 5. Solution

Pre/post analysis complete. Findings documented in Drive.

---

## 6. Questions Answered

- **Q:** Did the Audience Intent Scoring update improve performance?
  **A:** See Drive gdoc for results.

---

## 7. Data Documentation Updates

None documented locally.

---

## 8. Open Items / Follow-ups

- TI-541 (IP Scoring Pipeline) is the broader pipeline documentation ticket.

---

## Drive Files

📁 `Tickets/TI-391 Analyze Pre Post of Audience Intent Scoring Changes/`
- `TI-391 Analyze Pre Post Audience Intent Changes.gdoc` — investigation writeup
- `TI-391 Analyze Pre Post Audience Intent Changes.gsheet` — data
