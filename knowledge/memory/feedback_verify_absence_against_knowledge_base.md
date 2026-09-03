---
name: feedback_verify_absence_against_knowledge_base
description: Before writing "this data does not exist" into a deliverable, grep knowledge/ and the memory index, not just the BigQuery schema — TI-1313 shipped "audience size is not stored anywhere we could find" about a table documented in data_knowledge.md since TI-1026
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [absence claim, not available, does not exist, audience size, flight_cid_day_audience_sizes, TI-1313, TI-1026, Read me, negative claim, data availability, INFORMATION_SCHEMA, knowledge base grep]
domain: [workflow, data-catalog]
lifecycle: active
last_verified: 2026-09-02
---
**A claim that data does not exist is a finding and needs the same evidence as a positive one. Search
`knowledge/` before you write it, not only the schema.**

TI-1313's Read me shipped to Kirsa saying "total targetable audience size is not stored anywhere we could
find." `dw-main-silver.perml.flight_cid_day_audience_sizes` has been described in `data_knowledge.md`
since TI-1026 (Nick Martin / Matt Brorby / Jordan Piepkow, 2026-06-15), under a heading with the words
audience size in it. Coverage turned out to be **890 of 890** campaign groups. The reviewer found it by
reading the sheet and asking why it was blank.

**Why:** searching the warehouse schema answers "is there a column named this," which is the wrong
question. The table was named for the UI concept, not the analytical one, so no schema grep would have
surfaced it. Our own docs index by concept, which is exactly what a negative claim needs.

**How to apply:** before any "not available / does not exist / could not be found" line reaches a
deliverable, run the concept through `knowledge/_ROUTING.md`, `knowledge/START_HERE.md` and
`knowledge/_MEMORY_INDEX.md`. Two greps. Then, if it still holds, **write the narrow true statement rather
than the broad one** — name the column or table checked, not the concept: "does not vary" burned the same
ticket when the column read was `view_conversion_window` and the attribute lived in
`clickpass_acquisition_ttl`. Related: [[feedback_hold_evidenced_verdict]], [[feedback_bq_workflow]].
