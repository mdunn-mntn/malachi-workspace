---
name: feedback-runbook-artifacts-png-sql-only
description: DDP runbook (and similar pipeline) steps produce ONLY the canonical SQL + the PNG chart — no ad-hoc HTML/TSV/wide-CSV exports unless explicitly asked
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [runbook_artifacts_png_sql_only, runbook, artifacts, png, sql, similar, pipeline, steps]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-16
---
For the DDP quality-score runbook build (AUDI-1089) and similar step-by-step pipelines: each step's deliverables are **the reproducible SQL query** (`runbook/queries/qN_*.sql`) and **the PNG visual** (`runbook/charts/qN_*.png`). The long-format query-output CSV in `outputs/run_<date>/` is fine as pipeline plumbing (the chart script reads it), but do NOT proactively generate wide-format CSVs, TSVs, HTML copy/paste tables, or other export conveniences — only make those if explicitly requested in the moment (2026-07-10).

**Why:** the user wants a lean, reproducible folder — anything derivable from the SQL is clutter; one-off exports went stale/deleted within the same session.

**How to apply:** after building a runbook step, stop at SQL + PNG. If the user asks to see data, show a table in chat from the query output. **Non-SQL measurements get the SAME discipline (user, 2026-07-15: "make sure all these other things we're doing we are saving the queries with a good name"):** a gsutil/shell measurement becomes a named `runbook/queries/qN_*.sh` with the standard header (claim/method/usage), its output saved as a CSV in the run dir, and a MANIFEST row — never a throwaway inline command (precedent: q14_gcs_ingest_bytes.sh). Hand-collected inputs
(sampled measurements, manual integrations) must be EMBEDDED in the script header itself — the
output CSVs are gitignored, so a script that can't reproduce its own numbers is not saved
(audit-caught 2026-07-15: q14's monthly samples existed nowhere durable). Before SHARING a
query package: run an outsider-lens header sweep — headers rot when mid-ticket facts change
(the May-2026 credit-regime change left two headers describing 1/N split as current; an anchor
comment stated a diagnostic comparison as an equality). Ship a VALIDATION_GUIDE.md (glossary +
windows + anchors + tooling-substitution note) with any package (AUDI-1089 precedent). Related: [[feedback-doc-style]], [[reference-ddp-valuation-framework]].

**2026-07-16 additions to the pre-share sweep:** (1) header FILLS/mapping lines must reference
sheet TABLES/COLUMNS BY TITLE, never row numbers — row maps rot as the workbook evolves (a stale
range pointed a validator at the wrong table's 99% cells). (2) Before sharing queries, EXECUTE
every header's run block VERBATIM from a clean folder holding only the .sql files (extract the
block programmatically, inject --dry_run) — proves the copy-paste promise, catches unstated
prereqs (python3, cross-project BQ reads) and broken redirect paths.
