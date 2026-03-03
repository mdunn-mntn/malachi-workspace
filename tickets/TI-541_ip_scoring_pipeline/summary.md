# TI-541: IP Scoring Pipeline — Architecture & Documentation

**Jira:** https://mntn.atlassian.net/browse/TI-541
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Documentation and investigation of the full IP scoring pipeline — how MNTN scores IP addresses for audience targeting using the DS13 / Audience Intent / Jaguar system. Includes architecture overview, walkthrough, and investigation of unscored IPs.

---

## 2. The Problem

The IP scoring pipeline was not well-documented. Stakeholders needed a clear understanding of:
1. How IPs get scored
2. What happens to IPs that don't get scored
3. The full pipeline from raw signals → membership DB → bidder

---

## 3. Plan of Action

1. Document the pipeline architecture end-to-end
2. Walk through each pipeline stage
3. Investigate unscored IPs — why some IPs don't receive scores
4. Produce architecture diagrams

---

## 4. Investigation & Findings

**Local artifacts:**
- `artifacts/ip_scoring_pipeline.html` — pipeline HTML document
- `artifacts/ip_scoring_pipeline_overview.docx` — overview document (gitignored)
- `artifacts/ip_scoring_walkthrough.docx` — step-by-step walkthrough (gitignored)
- `artifacts/unscored_ips_investigation.docx` — investigation of unscored IPs (gitignored)

**Architecture diagrams** (in `documentation/architecture/`):
- `audience_intent_graph.png`
- `audience_intent_scoring.png`
- `biddable_inventory_funnel.pdf` / `.png`
- `ds13_data_pipeline.png`
- `ecommerce_classification_architecture_api.png`

**Drive:**
- Same architecture images stored in Drive under TI-541 folder
- `TI-541 IP Scoring Pipeline Overview DRAFT.docx`
- `TI-541 IP Scoring Walkthrough DRAFT.docx`
- `TI-541 Unscored IPs Investigation.docx`
- `Scores Breakdown.pdf`

---

## 5. Solution

Full pipeline documented. Unscored IP investigation completed.

---

## 6. Questions Answered

- **Q:** How does the IP scoring pipeline work end-to-end?
  **A:** See `ip_scoring_pipeline.html` and architecture diagrams in `documentation/architecture/`.

- **Q:** Why are some IPs unscored?
  **A:** See `unscored_ips_investigation.docx` on Drive.

---

## 7. Data Documentation Updates

- DS13 pipeline: `bronze.raw.tmul_daily` → membership DB → bidder (see TI-644 complete_context.md for schema details)
- IP scores are applied at bid time from membership DB snapshots

---

## 8. Open Items / Follow-ups

- TI-542 (Max Reach Causal Impact) is the performance analysis that followed this documentation work.

---

## Drive Files

📁 `Tickets/TI-541 Max Reach Scores Analysis/`
(Note: Drive folder name says "Max Reach" but contains IP Scoring Pipeline docs)
- `TI-541 IP Scoring Pipeline Overview DRAFT.docx`
- `TI-541 IP Scoring Walkthrough DRAFT.docx`
- `TI-541 Unscored IPs Investigation.docx`
- `Scores Breakdown.pdf`
- Architecture images: `Audience Intent Graph.png`, `Audience Intent Scoring.png`, `Biddable Inventory Funnel.png`, `DS13 Data Pipeline.png`, `Ecommerce Classification Architecture API.png`
