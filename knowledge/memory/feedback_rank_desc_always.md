---
name: feedback-rank-desc-always
description: "Every table/chart ranks rows by the primary metric, most on top — descending, always"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [rank_desc_always, descending, most on top, tables and charts, sort by primary metric, whitespace, AUDI-1089, column widths]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-10
---
All tables and charts rank entities by the primary metric of that visual, **largest value on top / first**, descending. Stated 2026-07-10 on the AUDI-1089 q1 liveness table ("i always want most on top to least for all these charts and tables"). Also: minimize whitespace/dead space — specifically WITHIN rows (column widths must hug content: narrow the figure width / shorten headers rather than stretching sparse columns across a wide canvas). Normal row height, a small outer border, and a well-placed title are all fine and wanted. No captions unless asked, no separator label rows (gray styling alone distinguishes out-of-scope rows).

**Why:** the user reads these top-down; the biggest thing should be the first thing.

**How to apply:** before rendering any table/bar chart, sort by the column that is the visual's point (rows/day, spend, score...). Sectioned tables (in-scope vs context/internal) sort within each section. Related: [[feedback_runbook_artifacts_png_sql_only]], [[reference_deck_standards]].
