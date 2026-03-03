# Prompt: Meticulous second pass — workspace audit and knowledge review

## Context

Workspace root: `/Users/malachi/Developer/work/mntn/workspace/`
This is a follow-up pass after a major audit session completed on 2026-03-03.

Read these files before starting anything:
- `knowledge/data_catalog.md`
- `knowledge/data_knowledge.md`
- `knowledge/folder_definitions.md`
- `.claude/CLAUDE.md`
- `~/.claude/CLAUDE.md`

Then go through every ticket and documentation area listed below. The goal is to catch anything
the previous session missed. Be meticulous. Don't skim.

---

## Pass 1 — Naming and structure

For every file and folder in the workspace, check:

1. **No uppercase letters** in file or folder names (except `README.md`)
2. **No dashes** as word separators (exception: `claude-prompts/` which is defined with a dash in `folder_definitions.md`)
3. **Ticket folder structure** — every ticket folder must have `queries/`, `outputs/`, `artifacts/` subdirs. Create missing ones silently.
4. **File placement** — every file is in the correct subfolder per `folder_definitions.md`:
   - `.sql` files → `queries/`
   - CSVs, JSONs, query results → `outputs/`
   - Notebooks, PDFs, scripts, deliverables → `artifacts/`
   - `summary.md` → ticket root
   - Nothing else should be loose in the ticket root

Run:
```bash
find /Users/malachi/Developer/work/mntn/workspace/tickets \
  -name "*[A-Z]*" -not -name ".DS_Store" -not -name "README.md" -not -path "*/.git/*"
```
and:
```bash
find /Users/malachi/Developer/work/mntn/workspace/tickets \
  -maxdepth 2 -not -path "*/.git/*" -not -name ".DS_Store" -type f | grep -v "summary.md\|queries/\|outputs/\|artifacts/"
```

Fix any violations. Use `git mv` for tracked files, `mv` for untracked.

---

## Pass 2 — Ticket summary accuracy

For each ticket in `tickets/`, open the `summary.md` and verify:

1. **Section 4 (Investigation & Findings)** — does it accurately describe what the files actually contain?
   Re-read the actual SQL/script files if needed to verify.
2. **File references in Section 4** — do the filenames listed match what's actually in `queries/`,
   `outputs/`, `artifacts/`? Check for stale or renamed references.
3. **Section 7 (Data Documentation Updates)** — does it list actual contributions to
   `knowledge/data_catalog.md` and `knowledge/data_knowledge.md`? If new knowledge was
   extracted in the 2026-03-03 session, document it here if not already.
4. **Section 8 (Open Items)** — are there follow-ups that have since been resolved? Mark them resolved.

Tickets to check (all of them):
- `dm_3118_rtc_monitor`
- `dm_3188_comparison_rt_and_non_rt`
- `mm_44_ipdsc_hh_discrepancy`
- `tgt_4016_ecomm_classifier_thresholds`
- `tgt_4103_common_crawl_coverage`
- `ti_033_vertical_classification_changes`
- `ti_200_whitelist_blocklist`
- `ti_221_pre_post_analysis`
- `ti_253_tpa_monitor`
- `ti_254_investigate_low_ntb_percentage`
- `ti_270_pre_post_analysis_ga`
- `ti_310_ntb_investigations`
- `ti_34_identity_sync_freshness`
- `ti_390_mmv3_performance`
- `ti_391_audience_intent_scoring`
- `ti_501_jaguar_kpi`
- `ti_502_ip_scoring`
- `ti_541_ip_scoring_pipeline`
- `ti_542_max_reach_causal_impact`
- `ti_644_root_insurance`
- `ti_650_stage_3_vv_audit`
- `ti_684_missing_ip_from_ipdsc`

---

## Pass 3 — Knowledge doc completeness check

### data_catalog.md
Open it and scan for:
1. Any table referenced in a ticket's SQL that is NOT in the catalog. Add an entry.
2. Any column detail mentioned in SQL (e.g., specific column names used in JOINs) that adds
   value to an existing entry — add it.
3. The Greenplum tables section — are all tables that appear in `.sql` files across ticket folders
   covered? Check specifically:
   - `tpa.membership_updates_logs` — is it documented?
   - `summarydata.sum_by_campaign_group_by_day` — is it documented?
   - `audience.audience_segment_campaigns` — is it documented?
   - `fpa.advertiser_verticals` — is it documented?
   - `dso.valid_campaign_groups` — is it documented?
   - `r2.advertiser_settings` — is it documented?

### data_knowledge.md
Open it and scan for:
1. Any business rule that appears consistently across ticket files but isn't captured
2. Any Greenplum vs. BQ behavioral difference noted in comments in `.sql` files
3. Any "gotcha" discovered in ticket investigation (read complete_context.md artifacts for anything missed)

---

## Pass 4 — Cross-reference integrity

1. **ticket → knowledge cross-refs**: Every summary.md Section 7 should list what was added
   to data_catalog.md / data_knowledge.md. If a section says "None" but the 2026-03-03 audit
   added knowledge from that ticket, update it.

2. **Drive → summary cross-refs**: Each `## Drive Files` section should reflect current Drive state.
   (If drive_sync_prompt.md has already been run, this should be clean. If not, run it first.)

3. **Inter-ticket references**: If a ticket says "see TI-XXX" in Open Items but TI-XXX is
   actually in the workspace, verify the referenced ticket exists and the path is correct.

---

## Pass 5 — documentation/ folder

Check `documentation/` for:
1. Files not in the right subfolder per `folder_definitions.md`
2. Any knowledge in `documentation/docs/` that hasn't been moved to `knowledge/data_catalog.md`
   or `knowledge/data_knowledge.md`
3. Any naming violations

---

## Pass 6 — .gitignore check

Verify `.gitignore` exists at workspace root and covers:
- `self_review/` (personal notes — should never be committed)
- `*.DS_Store`
- Any other local-only files that shouldn't be tracked

If `.gitignore` is missing or incomplete, create/update it and commit.

---

## Operating rules

- Commit and push after every meaningful change — no batching
- Never guess schema — if a table appears in SQL and you don't know the schema, note it as
  "schema unknown, needs verification" rather than inventing columns
- If you find something wrong, fix it. If you're unsure if it's wrong, note it in the relevant
  summary.md Open Items section
- Report summary at end: list every file changed and what was changed
