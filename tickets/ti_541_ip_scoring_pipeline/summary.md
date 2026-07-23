---
doc_type: ticket
title: "TI-541: IP Scoring Pipeline — Architecture & Documentation"
status: done
date: 2026-03-04
summary: "End-to-end documentation of the DS13 IP scoring pipeline + unscored-IP investigation."
result: "IP scoring pipeline documented end-to-end + unscored-IP investigation completed."
keywords: [ip scoring, ds13, audience intent, jaguar, tmul_daily, membership db, unscored ips, bidder, bid time]
---

## TL;DR

**Q:** Summarize TI-541 (IP Scoring Pipeline) and extract any durable facts not already in the knowledge docs.

**A:** TI-541 documented the DS13 / Audience Intent / Jaguar IP scoring pipeline end-to-end and investigated why some IPs go unscored. Deliverables: a pipeline HTML doc, overview/walkthrough/unscored-IPs .docx files (gitignored, also on Drive), and architecture diagrams in documentation/architecture/ (audience_intent_graph, audience_intent_scoring, biddable_inventory_funnel, ds13_data_pipeline, ecommerce_classification_architecture_api). Pipeline: bronze.raw.tmul_daily → membership DB → bidder, with IP scores applied at bid time from membership DB snapshots. Status: complete (2026-03-04). TI-542 (Max Reach Causal Impact) followed as the performance analysis; TI-644 complete_context.md holds schema details.

**How:** Read summary.md in full; the outputs/ and queries/ dirs do not exist for this ticket. Grepped data_catalog.md, data_knowledge.md, experimentation.md, mntn_business.md for the Section 7 facts.

**Tables:** bronze.raw.tmul_daily

**Learned:**
- DS13 IP scoring pipeline runs bronze.raw.tmul_daily → membership DB → bidder
- IP scores are applied at bid time from membership DB snapshots, not stored long-term in BQ event tables
- TI-541 is a documentation ticket; deliverable docs are gitignored .docx files plus architecture diagrams in documentation/architecture/
- TI-542 (Max Reach Causal Impact) is the follow-on performance analysis; TI-644 complete_context.md has schema details

**Reuse when:**
- asking how MNTN scores IPs for audience targeting
- asking why some IPs are unscored
- looking for DS13 pipeline architecture diagrams
- referencing the membership DB / bid-time scoring flow

---


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
- `artifacts/ti_541_pipeline.html` — pipeline HTML document
- `artifacts/ti_541_pipeline_overview.docx` — overview document (gitignored)
- `artifacts/ti_541_walkthrough.docx` — step-by-step walkthrough (gitignored)
- `artifacts/ti_541_unscored_ips_investigation.docx` — investigation of unscored IPs (gitignored)

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
  **A:** See `ti_541_pipeline.html` and architecture diagrams in `documentation/architecture/`.

- **Q:** Why are some IPs unscored?
  **A:** See `ti_541_unscored_ips_investigation.docx` on Drive.

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
- `Audience Intent Graph.png`
- `Audience Intent Scoring.png`
- `Biddable Inventory Funnel.png`
- `DS13 Data Pipeline.png`
- `Ecommerce Classification Architecture API.png`
