---
name: feedback_one_resolver_for_shared_state
description: When two deliverables derive from the same state, they must share one resolver function; a regenerated file is not evidence of a regenerated result.
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [stale deliverable, shared state, single resolver, xlsx workbook drift, build script divergence, overlay chain, verify output against source, mtime not evidence, AUDI-431]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-11
---

When two outputs are built from the same evolving state (e.g. a CSV deliverable and an .xlsx workbook), resolve that state in ONE shared function both import. Never let each builder re-apply its own copy of the transform chain.

**Why:** AUDI-431 (2026-08-11). `build_lists.py` applied four adjudication overlays; `build_workbook.py` applied only the first. Every rebuild wrote a *fresh file with three-passes-stale contents* — the Drive workbook showed 10 whitelist rows when 102 shipped, and 1,373 manual rows when 10 remained. I reported "workbook rebuilt" repeatedly and it was technically true: the mtime moved every time. Malachi caught it by asking whether the .xlsx had actually been updated.

**How to apply:** (1) put the state resolution in one module (`load_designated_sheet()`) with the overlay list as data, so a new pass is registered once; (2) after generating any deliverable, ASSERT its contents against the source of truth (row counts per tab vs the shipped files) rather than trusting that the build ran; (3) treat "the file was rewritten" as zero evidence that the content changed — check a value that should have moved. Same class of error as the stall-detector bug: a check that always passes is worse than no check. [[feedback_background_work_liveness]] [[feedback_facts_not_presentation]]
