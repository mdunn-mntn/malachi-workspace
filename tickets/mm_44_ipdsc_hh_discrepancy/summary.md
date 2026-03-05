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
