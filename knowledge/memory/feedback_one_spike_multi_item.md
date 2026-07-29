---
name: one-spike-multi-item
description: "Bryce (PMO) — multi-item investigations get ONE spike ticket with per-item outcome checklist, not an epic or per-item tickets"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 11120755-d5de-4ee7-83cd-aef7c4761482
doc_type: memory
keywords: [one_spike_multi_item, spike ticket, multi-item investigation, Bryce Wagg, PMO, per-item checklist, AUDI-1089, subfolder per item]
domain: [jira-process, workflow]
lifecycle: active
last_verified: 2026-07-09
---
For investigations that repeat the same evaluation across N items (e.g. AUDI-1089's 7 vendor evals), Bryce Wagg
(PMO rep) directed: **one spike ticket**, with the outcome description listing each item, marked off as each
completes — NOT an epic with N children and NOT N separate tasks. Malachi adopted it (2026-07-09) and added:
in the workspace, **one subfolder per item** inside the single ticket folder to keep them separated
(e.g. `tickets/audi_1089_ddp_vendor_evaluations/ds39_klickly/`).

**Why:** digestibility for PMO/grooming — one ticket to track, visible progress in the description.

**How to apply:** next multi-item spike → single AUDI ticket, wiki checklist `(x)`→`(/)` per item in the
description, per-item subfolders (each with summary.md, queries/, outputs/, artifacts/), shared cross-item
work at ticket root. Related: [[reference_jira_conventions]].
