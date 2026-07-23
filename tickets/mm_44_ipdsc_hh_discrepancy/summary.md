---
doc_type: ticket
title: "MM-44: IPDSC Household Discrepancy Investigation"
status: done
date: 2026-03-04
summary: "Trace household-count discrepancies in the IPDSC / MES IP-to-household pipeline"
result: "HH-count divergence traced through MES/IPDSC enrichment stages; see investigation doc"
keywords: [mm-44, ipdsc, mes, household discrepancy, membership enrichment service, ip to household, ti-684]
---

## TL;DR

**Q:** Where in the MES/IPDSC pipeline do household (HH) counts diverge, and how large is the discrepancy?

**A:** MM-44 investigated household-count discrepancies in the IPDSC (IP Data Source Category) pipeline, where the MES (Membership Enrichment Service) pipeline maps IPs to households and observed HH counts did not match expected values. The plan was to trace HH counts through each MES stage, find where they diverge, quantify the gap, and recommend a fix. The summary marks the ticket Complete but does NOT record the conclusion in-line: Solution is "TBD" and the answer to "Where do HH counts diverge?" is deferred to the investigation doc (artifacts/mm_44_investigation.md / .docx, artifacts/mm_44_household_discrepancy.doc, with a pipeline diagram in artifacts/mm_44_mes_pipeline.png). Documented takeaway captured in the summary: IP-to-HH mapping goes through IPDSC enrichment stages. Related follow-up: TI-684 (missing IPs from IPDSC).

**How:** Per the summary Plan: trace HH counts through each MES pipeline stage, identify the divergence stage, quantify the gap, document findings. Findings/Solution are not concluded in the summary itself; the actual results live in the artifacts investigation docs, which the summary points to without restating.

**Learned:**
- Summary is a stub: status=done but Solution and the Q6 answer are TBD/deferred to artifacts/mm_44_investigation.md — the divergence stage and gap size are not stated in the summary.
- MES = Membership Enrichment Service; IPDSC = IP Data Source Category; MES maps IPs to households via IPDSC enrichment stages (both already documented in knowledge/data_knowledge.md).
- TI-684 (missing IPs from IPDSC) is a related follow-up investigation.
- No queries/ or outputs/ directories exist for this ticket; work products are Word/PNG artifacts, not SQL.

**Reuse when:**
- Investigating IPDSC or MES household/IP mapping discrepancies
- Tracing HH counts through MES pipeline enrichment stages
- Following up on TI-684 missing IPs from IPDSC

---

# MM-44: IPDSC Household Discrepancy Investigation

**Jira:** https://mntn.atlassian.net/browse/MM-44
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Investigation into discrepancies in household (HH) counts within the IPDSC (IP Data Source Category) pipeline. The MES (Membership Enrichment Service) pipeline maps IPs to households — discrepancies between expected and actual HH counts indicated a data quality issue.

---

## 2. The Problem

Household counts from IPDSC did not match expected values. Needed to trace the discrepancy through the MES pipeline to identify where HH counts diverged.

---

## 3. Plan of Action

1. Trace HH counts through each MES pipeline stage
2. Identify the stage where discrepancy appears
3. Quantify the gap
4. Document findings and recommend fix

---

## 4. Investigation & Findings

**Artifacts:**
- `artifacts/mm_44_investigation.md` — written investigation
- `artifacts/mm_44_investigation.docx` — full doc version
- `artifacts/mm_44_household_discrepancy.doc` — discrepancy details
- `artifacts/mm_44_mes_pipeline.png` — MES pipeline diagram

---

## 5. Solution

TBD — review `artifacts/mm_44_investigation.md` for findings.

---

## 6. Questions Answered

- **Q:** Where in the MES pipeline do household counts diverge?
  **A:** See investigation doc in artifacts/.

---

## 7. Data Documentation Updates

- MES pipeline documented: IP → HH mapping goes through IPDSC enrichment stages
- See `documentation/architecture/mes_pipeline.png` for pipeline diagram

---

## 8. Open Items / Follow-ups

- TI-684 (missing IPs from IPDSC) is a related follow-up investigation.

---

## Drive Files

- (None found in Drive for MM-44)
