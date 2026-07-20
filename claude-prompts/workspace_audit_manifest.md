# Workspace Structure Audit — Manifest (review before execution)

Generated 2026-07-20 by `.claude/scripts/audit_structure.py` (deterministic, read-only) + human judgment.
**Nothing here has been executed.** Approve tiers/items and I run them in safe batches.

Regenerate anytime: `python3 .claude/scripts/audit_structure.py --json <path>`

## Headline

The workspace is **~95% structurally clean**. Root is tidy, the 65-ticket skeleton conforms
(`lint_tickets` 0 violations), git hygiene is good (0 untracked-non-ignored files). The audit found
**114 findings**, but most are trivial (30 empty dirs) or judgment calls (which committed CSVs to keep).
A blind "rename/move everything" pass would touch ~1,160 files to fix ~40 real ones and risks breaking
path references — so this is scoped into **safe** vs **judgment** vs **standard-reconciliation** tiers.

| Category | Count | Tier | Default action |
|---|--:|---|---|
| junk (Spark markers, .pyc) | 7 | **1 — safe** | delete + gitignore |
| empty scaffolded dirs | 30 | **1 — safe** | delete (recreated on demand) |
| queries/ non-.sql files | 10 | **2 — judgment** | move to `artifacts/` (or bless the runbook exception) |
| naming violations | 32 | **2 — judgment** | mixed — see breakdown |
| tracked data (committed CSVs/JSON) | 29 | **2 — judgment** | keep as record OR gitignore |
| root stray (.DS_Store, .vscode, vendored tool) | 3 | **2 — judgment** | gitignore / relocate |
| deep nesting (vendored PR copy) | 1 | **2 — judgment** | slim or keep |
| tracked `.claude/projects/` tree | 1 | **2 — judgment** | confirm intentional |
| ticket missing summary.md | 1 | **2 — judgment** | add card |
| root-spec + naming carve-outs stale | — | **3 — standard** | update `folder_definitions.md` |

---

## TIER 1 — SAFE (mechanical, reversible, recommend auto-run on approval)

### 1a. Junk — delete + gitignore (7 files)
Spark/Databricks write-markers and a compiled Python cache — should never have been tracked:
- `tickets/ber_2250_incrementality_overhaul/ti_933_select_lift_analysis/outputs/databricks_7d/result/{_SUCCESS,_started_*,_committed_*}`
- `…/databricks_14d_v3/result/{_SUCCESS,_started_*,_committed_*}`
- `tickets/ti_896_audience_composition_2025_drop/artifacts/__pycache__/generate_charts.cpython-311.pyc`
- **Also gitignore:** `_SUCCESS`, `_started_*`, `_committed_*`, `__pycache__/`, `*.pyc`, `.DS_Store`, `.vscode/` (20 `.DS_Store` currently sit untracked on disk).

### 1b. Empty scaffolded dirs — delete (30)
Ticket subfolders (`queries/ meetings/ outputs/ artifacts/`) created by the template but never used — pure
noise in a file tree. They're recreated on demand. (git doesn't track empty dirs, so this is disk-only tidy.)
Examples: `ti_1003_experiment_archive/{artifacts,queries,meetings,outputs}/`, `audi_1111_vendor_quality/{queries,meetings}/`, … (full list in the JSON manifest).

---

## TIER 2 — JUDGMENT (your call per group)

### 2a. Non-.sql files inside `queries/` (10) → recommend MOVE to `artifacts/`
The spec says `queries/` is SQL-only. These are runner scripts / indexes / guides:
- `.sh` runners: `ti_809/queries/ti_809_run_all_queries.sh`, `audi_1070/queries/reusable_diagnostic_pack/run_diagnostic.sh`, `documentation/docs/advertiser_yoy_diagnostic/queries/run_diagnostic.sh`, `audi_1089/runbook/queries/q14_gcs_ingest_bytes.sh`
- `.md` indexes/guides: `audi_1070/queries/QUERY_INDEX.md`, `audi_1089/runbook/queries/{MANIFEST.md,VALIDATION_GUIDE.md}`, `ti_837…/queries/ti_837_lift_analysis_plan.md`, `audi_1070/queries/reusable_diagnostic_pack/README.md`, `ti_650/queries/_archive/ti_650_zach_traced_ip_guide`
- **Recommendation:** move the `.sh`/`.md` to the sibling `artifacts/`. **Exception to consider:** the `audi_1089/runbook/queries/` MANIFEST+VALIDATION_GUIDE are a *deliberate self-contained handoff package* — arguably bless "a query pack may carry its own MANIFEST/README" rather than move it.

### 2b. Naming violations (32) — split by risk
- **Leave (machine round-trip):** 12 Mode-exported queries with spaces+hash under `ti_1037/perf_report/mode/batch1_queries/` (e.g. `00b Reach By Score.9b2f59dea917.sql`). These round-trip to/from Mode by that exact name — renaming breaks the sync. **Recommend: leave, document the exception.**
- **Rename with care (referenced in docs):** `ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py` is cited by name in `CLAUDE.md` + `experimentation.md`. **Recommend: keep as-is** (rename would need a coordinated ref update for zero real gain) OR rename + update both refs.
- **Rename (safe, low-stakes working docs):** `HANDOFF_PROMPT.md`, `TOMORROW_PLAN.md`, `PHASE5_PLAN.md`, `QUERY_INDEX.md`, `README_MODE.md` → lowercase. CamelCase scripts `TI_688_IP_Score_Eval.py`, `TI_704_Fangorn_DCG_Eval.py`, `TI_704_Fangorn_IP_DCG_Scoring.py` → snake_case (check for importers first). The `LEAN` in `ti_837_lift_analysis_30adv_7day_v5_xwin_LEAN_2segments.sql`. The colon+spaces file `ti_504/artifacts/BEST: Bayesian… .html`. The dash in `ti_797/meetings/…alex_-_project_discussion.txt`.
- **Borderline (kit convention):** `.claude/agents/{perf-analyst,reviewer-adversarial}.md`, `workflows/prompts/reviewer-adversarial.md`, `slack_bot/RECOVERY.md`, `todoist-mcp-transfer/{QUICK-START.md,mcp-server/SETUP.md}` — dashes/uppercase are the external tool's/kit's own convention. **Recommend: leave, carve out in the standard.**

### 2c. Tracked data — committed CSV/JSON outputs (29) → KEEP or GITIGNORE?
Small result CSVs force-added past `.gitignore` (`ti_790` feature rankings ×7, `ti_896` composition ×12,
`ti_832` importance ×5, `ti_921` lift ×4, `ti_650` ×2). They're the **analytical record** — cheap, and
they make a ticket's numbers reproducible without a rerun. **Recommendation: KEEP** (they're a feature, not
debt) — but if you'd rather the repo hold only code+docs, I can gitignore + `git rm --cached` them per-ticket.

### 2d. Root stray (3)
- `.DS_Store` (root) + `.vscode/` → **gitignore** (Tier-1 covers .DS_Store).
- `todoist-mcp-transfer/` (55 MB vendored MCP tool + a node `mcp-server/`) → **recommend relocate out of the analytics workspace** (it's a tool, not analysis). Options: move to a separate repo/dir, or keep but document it as a blessed exception. Your call — it's the single biggest "why is this here?" item.

### 2e. Deep nesting — vendored PR copy (1)
`ti_956/artifacts/targeting_infra_ml_pyproject_pr/` (a checked-in copy of a PR's package, 6 levels deep).
**Recommend:** keep only if it's an active reference; otherwise link to the PR and delete the copy.

### 2f. Tracked `.claude/projects/` tree (1)
A Claude session/memory tree is tracked **inside this repo** (`.claude/projects/-Users-…/memory/…`).
Memory canonically lives in **global** `~/.claude/`. **Recommend: confirm** — if it's a stray duplicate,
gitignore + `git rm --cached`; if intentional (a committed snapshot), document why.

### 2g. Ticket missing `summary.md` (1)
`tickets/ti_argocd_secrets_audit/` has no `summary.md` (fails the skeleton + won't appear in `INDEX`).
**Recommend:** add a card (it's the sibling of `ti_kafka_secret_sweep`, which does have one).

---

## TIER 3 — STANDARD RECONCILIATION (update the spec to match reality)

The audit proved `folder_definitions.md` is itself stale — its "root holds ONLY these 6" list predates
legitimate additions. **Recommend updating `folder_definitions.md` to:**
- Bless the post-spec root entries: `workflows/`, `self_review/`, `slack_bot/`, `README.md`, `.mcp.json`.
- Document the **naming carve-outs**: README-family + generated `INDEX.md`/`SKILL.md`/`MEMORY.md`; sanctioned
  dashed dirs (`claude-prompts/`, vendored tool trees); machine round-trip exports (Mode). This is what makes
  the audit repeatable without re-triaging the same false positives — and it's the "master standard" doc.

---

## Optional deeper pass (agents) — offer, not yet run
The above is the exhaustive **mechanical** audit. A **semantic** pass (a few agents over ticket clusters)
would add what a script can't see: superseded/duplicate deliverables, redundant output CSVs, and misfiled-
by-content files worth consolidating. Say the word and I'll run it.

## What I'll do on approval
Tell me which tiers/items to execute. Default plan if you just say "go": run **Tier 1** (junk + empty dirs +
gitignore) and **Tier 3** (update the standard) — both safe and reversible — and hold Tier 2 for per-group
decisions. All work on a branch, committed in small labeled batches.


---

## Semantic pass — content findings (2026-07-20, 12-agent review)

50 content proposals (junk already swept). Read-only agent proposals; **nothing executed**. Confidence is the agent's — many version calls are medium, so review before archiving/deleting deliverables.


### ARCHIVE — superseded versions → move to `_archive/` (reversible, no deletion)  (24)

- `tickets/ti_1037_audience_diagnostic_tool/perf_report/outputs/bouqs_test` — _high conf / low risk_ — params/bouqs_test.env states "TEST run of The Bouqs eCommerce (32147) — for exercising module order/renumbering only. Same inputs as bouqs_32147." Uses the old flat 00-21 numbering, superseded by the settled module-id scheme (00/00b/01/02/03/03b/...) used in the three real advertiser runs; not referenced by summary.md. Redundant with the real outputs/bouqs_32147/ run (identical AID/inputs).
- `tickets/audi_1089_ddp_vendor_evaluations/queries` — _medium conf / low risk_ — The 5 root-level SQL files (audi_1089_q1_scale_30d … q5_vr_membership, created 2026-07-09) are the Klickly-day-1 monolithic cut. They were superseded/split into the MANIFEST-driven canonical pack in runbook/queries/ (created 2026-07-14, generalized to all vendors, documented in runbook/README.md as 'canonical SQL, one per step, re-runnable each quarter/renewal' and the shareable run list). The root five are not referenced by summary.md, MANIFEST.md, or any tracked script/doc. Runbook pack is the blessed query home; archive the day-1 originals beside/under it (they retain historical context, so archive rather than delete).
- `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/build_phase2_deck_v1_pre_critique.py` — _high conf / low risk_ — Explicitly the pre-critique draft (filename '_v1_pre_critique'); superseded by build_phase2_deck.py in the same folder (newer mtime, post-critique). Latest is the blessed builder.
- `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_phase2_presentation_deck_v1_pre_critique.html` — _high conf / low risk_ — Pre-critique deck output ('_v1_pre_critique'); superseded by ti_837_phase2_presentation_deck.html / _standalone.html (post-critique, newer) which are the shared canonical deck per summary.md.
- `tickets/ber_2250_incrementality_overhaul/ti_933_select_lift_analysis/artifacts/ti_933_select_lift_notebook.py` — _medium conf / low risk_ — Original Spark-port notebook (v1); superseded by ti_933_select_lift_notebook_v3.py, which summary.md calls the current one and whose header documents fixes over v2. v3 is the blessed notebook.
- `tickets/ber_2250_incrementality_overhaul/ti_933_select_lift_analysis/artifacts/ti_933_select_lift_notebook_vs_v2.py` — _medium conf / low risk_ — Victor's v2 notebook; v3 (ti_933_select_lift_notebook_v3.py) header explicitly enumerates what it fixes vs this v2 and is the version referenced as ready/run in summary.md.
- `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_validation_run_prompt.md` — _high conf / low risk_ — Spent 'copy below the line into a new Claude Code session' launch prompt whose validation run has been fully executed (outputs/validation_run/ populated, v4 complete). Not referenced in summary.md's active-artifact list (Section 9), which keeps the executed guide (ti_650_validation_run_guide.md) but not this launch prompt. Belongs in artifacts/_archive/ alongside the ~7 sibling prompt files already there (ti_650_continuation_prompt.md, ti_650_next_session_prompt.md, ti_650_s3_resolution_execution_prompt.md, etc.).
- `tickets/ti_896_audience_composition_2025_drop/meetings/ti_896_01_war_room_audience_analysis_2026_04_22_local.txt` — _medium conf / low risk_ — Per-provider (mlx-whisper local) transcription byproduct from --keep-both; superseded by the merged blessed transcript ti_896_01_war_room_audience_analysis_2026_04_22.txt in the same folder. Kept only for provider comparison; the merged file is the canonical record.
- `tickets/ti_896_audience_composition_2025_drop/meetings/ti_896_01_war_room_audience_analysis_2026_04_22_openai.txt` — _medium conf / low risk_ — Per-provider (OpenAI whisper-1) transcription byproduct from --keep-both; superseded by the merged blessed transcript ti_896_01_war_room_audience_analysis_2026_04_22.txt in the same folder. Kept only for provider comparison; the merged file is the canonical record.
- `tickets/ti_921_fangorn_lift_dashboard/outputs/ti_921_pre_post_smoke.csv` — _high conf / low risk_ — 5-row early smoke test (May 5, schema has *_planned/still_treated scaffolding cols) superseded by the full ti_921_pre_post.csv sibling (May 28, 53 rows, final schema with pct_change cols). Smoke test served its purpose; the full pre_post is the real deliverable.
- `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/audi_1070_avon_deck.html` — _medium conf / low risk_ — Generic 'Avon - YoY Performance Review' deck (Jun 30 15:56), not referenced anywhere in summary.md. Sits between two named deliverables: blessed avon_case_deck.html ('Avon - Performance Diagnosis', summary line 17) and audi_1070_avon_full_deck.html ('Full Prospecting Story', line 410). Intermediate iteration; its audi_1070_avon_deck_standalone.html sibling is superseded too.
- `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/audi_1070_avon_yoy_no_change.html` — _medium conf / low risk_ — Earliest, single-message Avon cut ('Avon YoY - no significant change', Jun 30 14:04). Its finding is folded into the blessed avon_case_deck.html built 36 min later (summary line 399 states the no-significant-change conclusion). Its audi_1070_avon_yoy_no_change.png sibling is the same artifact and would archive with it.
- `tickets/ti_1053_elevenlabs_3p_segments/outputs/scored_v2.json` — _high conf / low risk_ — Explicit v2 scoring intermediate; summary.md §8 states 'v3 FINAL supersedes v1/v2'. scored_v3.json (269KB, kept) is the current version and the blessed csv/xlsx deliverable remains.
- `tickets/ti_1053_elevenlabs_3p_segments/artifacts/score_v2.py` — _high conf / low risk_ — v2 scorer, explicitly superseded by the v3 pipeline (build_v3.py) per summary §8. Kept only as historical reference in §7.
- `tickets/ti_1053_elevenlabs_3p_segments/artifacts/build_final.py` — _medium conf / low risk_ — v2 deliverable builder, superseded by build_v3.py which produced the blessed recommendations.{xlsx,csv}.
- `tickets/ti_1053_elevenlabs_3p_segments/artifacts/build_deliverable.py` — _medium conf / low risk_ — v1 deliverable builder, superseded by build_final.py (v2) then build_v3.py (v3).
- `tickets/ti_1053_elevenlabs_3p_segments/outputs/final_scored.json` — _medium conf / low risk_ — v1/v2 scored output superseded by final_v3_scored.json (the v3 recall-fixed version). Not referenced in summary as a deliverable.
- `tickets/ti_1053_elevenlabs_3p_segments/outputs/scored_by_name.json` — _medium conf / low risk_ — v1 exploratory scoring intermediates (this plus scored_deduped.json, genuine_shortlist.json, high_value_segments.json) — pipeline scratch superseded by the v3 run; not referenced in summary. Consolidate into an outputs/_archive/ leaving the v3 JSONs + blessed csv/xlsx.
- `tickets/ti_999_interest_segment_sizing/TOMORROW_PLAN.md` — _high conf / low risk_ — Ephemeral end-of-day planning scratch dated 2026-05-28 ('Tomorrow's primary task'); ticket work advanced well past it (outputs through 2026-06-01). Non-standard root file (only summary.md/presentation.md belong at ticket root).
- `tickets/ti_999_interest_segment_sizing/HANDOFF_PROMPT.md` — _medium conf / low risk_ — New-chat handoff prompt dated 2026-05-28 referencing 'Findings 1-14' while the ticket has since progressed; ephemeral session-continuity scratch at ticket root, not a standard deliverable.
- `tickets/mm_44_ipdsc_hh_discrepancy/artifacts/mm_44_household_discrepancy.doc` — _medium conf / low risk_ — Earlier Confluence HTML export (title 'Household Discrepancy', Feb 26 12:32) of the same MM-44 investigation. Superseded by mm_44_investigation.md (15:44, the RESOLVED writeup) and its Word export mm_44_investigation.docx (15:47).
- `tickets/dm_3188_comparison_rt_and_non_rt/queries/dm_3188_rtc_vs_nonrtc.sql` — _high conf / low risk_ — Identical to sibling queries/dm_3188_comparison_rtc_and_non_rtc.sql except for a trailing newline (diff shows only lines 67-68 whitespace). This is the older-named copy — the stale .idea/sqldialects.xml still points at 'dm-3188-rtc-vs-nonrtc.sql'. Keep comparison_rtc_and_non_rtc.sql (matches folder name); drop this duplicate.
- `tickets/ti_033_vertical_classification_changes/artifacts/ti_033_vertical_sizes.ipynb` — _medium conf / low risk_ — Earlier iteration superseded by sibling ti_033_vertical_sizes_final.ipynb (same date, near-identical size 63.7KB vs 64.5KB, explicit _final suffix = blessed version). Only the _final notebook matters; archive this one.
- `tickets/ti_790_bidstream_feature_inventory/artifacts/ti_790_presentation.md` — _medium conf / low risk_ — Earlier draft superseded by ti_790_presentation_new.md (rewritten with a Power Line, +3KB, later timestamp) which is the blessed deliverable and the source of ti_790_presentation_new.pdf. This old plain-title draft is the prior version, not the final. (Ambiguity: naming keeps _new suffix; blessed file is the _new pair, not this one.)

### MOVE — misfiled by content → correct subdir  (6)

- `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/PHASE5_PLAN.md` — _medium conf / low risk_ — Planning doc sitting at the child-ticket root; only summary.md belongs at ticket root. Peer plan docs (ti_837_phase2_cohort.md, ti_837_phase2a_30day_databricks_plan.md) already live in artifacts/ — move here to match.
- `tickets/ti_684_missing_ip_from_ipdsc/data_exports` — _high conf / low risk_ — Non-standard folder name; the two JSON audience-expression exports (ti_684_export.json, ti_684_export_2.json) are query-result data that belong in outputs/. The two files are distinct expression variants (v1 include/exclude vs a v2 query-language holdout expression), not duplicates — keep both, just relocate the folder to outputs/.
- `tickets/ti_832_feature_store_roas_cpa/outputs/ti_832_shap_combined.png` — _medium conf / low risk_ — Chart/image PNG in outputs/; per the standard images belong in artifacts/ (outputs/ is for query-result data).
- `tickets/ti_797_buk_knowledge_transfer/summary.pdf` — _medium conf / low risk_ — PDF export of summary.md sitting at ticket root (root should hold only summary.md/presentation.md). Stale snapshot (2026-03-31) of a summary.md that was updated 2026-07-20 — the live .md is the source of truth. Move to artifacts/ or drop.
- `tickets/ti_748_causal_impact_media_plan/summary.pdf` — _high conf / low risk_ — PDF sitting at the ticket root, which per the standard holds only summary.md/presentation.md; it is a stale rendered export of summary.md (pdf dated Mar 31, summary.md updated Jul 20) and is untracked in git. Move to artifacts/ (or delete — summary.md is the living source of truth).
- `tickets/tgt_4016_ecomm_classifier_thresholds/data/tgt_4016_product_lookup.csv` — _medium conf / low risk_ — 'data/' is a nonstandard ticket subfolder (standard is queries/ outputs/ meetings/ artifacts/). This 107MB input lookup CSV should live under outputs/ (data) or artifacts/ (3P/input data). File is tracked in git, so use git mv; flagging the folder placement, not the data itself.

### DELETE — redundant/stale data (**needs explicit approval**)  (4)

- `tickets/ti_644_root_insurance/outputs/data/ti_644_bid_and_served_ips.csv` — _high conf / low risk_ — 0-byte file (empty/failed export). A populated equivalent exists as outputs/data_exports/ti_644_bid_vs_served_ips.csv; the empty one has no analytical value.
- `tickets/ti_200_whitelist_blocklist/outputs/domain_lists/ti_200_ecomm_blocklist_export.csv` — _high conf / low risk_ — Byte-identical (md5 73ab624e...) to sibling ti_200_ecommerce_blocklist.csv in the same domain_lists/ folder — same 1464 domains. Keep ecommerce_blocklist.csv (cleaner name); this is a redundant export copy. Gitignored local file.
- `tickets/ti_200_whitelist_blocklist/outputs/ti_200_ecommerce_blocklist_2.csv` — _high conf / low risk_ — Named '_2 updated blocklist' in summary but byte-identical (md5 73ab624e...) to outputs/domain_lists/ti_200_ecommerce_blocklist.csv. Also misfiled in outputs/ root rather than the domain_lists/ subfolder holding the canonical set. Gitignored local file.
- `tickets/ti_200_whitelist_blocklist/outputs/ti_200_ecommerce_whitelist_2.csv.gz` — _high conf / low risk_ — Named '_2 updated whitelist' but decompresses to md5 6a5a447a... = exact copy of outputs/domain_lists/ti_200_ecommerce_whitelist.csv; just gzipped and dropped in outputs/ root. No new data. Gitignored local file.

### KEEP_FLAG — glance only, no action proposed  (16)

- `tickets/ti_1037_audience_diagnostic_tool/outputs/imemories/ti_1037_imemories_netnew_reach.json` — _medium conf / low risk_ — This conventioned .json (24KB) is a raw bq_run capture polluted with "Waiting on perf_..." progress lines and the BQ Performance footer (not valid JSON). The same net-new-reach data exists clean in sibling outputs/imemories/_netnew_reach_map.json (204B id->reach map). Human should reconcile — strip/delete the polluted capture and keep the clean map (ideally renamed to the ti_1037 convention).
- `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_lift_30adv_7day_v2_winrate_2026_04_20_to_26.json` — _medium conf / medium risk_ — Part of the v1->v5 lift-iteration cluster (lift_30adv_7day {v1,v2_winrate,v3,v4}, meta_analysis_30adv{,_v4}, per_cell_table_30adv{,_v4}). summary.md declares v5 canonical and v2/v3 'cancelled'. These are result data with provenance value — human should decide whether to collapse the v1-v4 iterations into outputs/final/ (leaving v5) or retain as run history. Not proposing deletion of result data.
- `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/meetings/audio1959855604.m4a` — _medium conf / low risk_ — 21 MB raw source recording; the merged + per-provider transcripts for the 2026-04-28 meetings already exist. Raw audio is superseded by the transcripts — human may want to remove the large binary from git.
- `tickets/ber_2250_incrementality_overhaul/meetings` — _low conf / low risk_ — 12 per-provider transcription intermediates (*_local.txt / *_openai.txt across epic-root and ti_837 meetings/) each have a merged .txt deliverable that supersedes them. These are the deliberate transcribe.sh --keep-both comparison outputs, so this is convention-sanctioned — flagging only for optional cleanup, not a violation.
- `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_zach_ray_comments.txt` — _low conf / low risk_ — Slack stakeholder transcript (Zach/Ray/Sharad) sitting in artifacts/, while a sibling Slack transcript (ti_650_slack_zach_lookback.txt) lives in meetings/. Placement is inconsistent for the same content type; per the standard, transcripts go in meetings/. Flagged only for a human glance -- summary.md explicitly blesses this as an active artifact, so not high-confidence.
- `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/audi_1070_avon_v4_deck.html` — _low conf / low risk_ — Newest Avon deck (Jul 1 15:57) and named 'v4' ('the control that proves the fix'), yet summary.md line 17 still names avon_case_deck.html as the latest Avon deck and never references v4. Either the summary line is stale or v4 is an abandoned iteration - a human should reconcile which is the blessed Avon deck before any archiving.
- `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/outputs/hexclad_daily_reach_jun_dec2025.csv` — _low conf / low risk_ — Raw 225-row reach file superseded by hexclad_daily_reach_jun_dec2025_clean.csv (215 rows, 10 outage/bad days removed, identical values on kept days). Clean version is the analysis input; raw is retained only as the pre-cleaning record - worth a glance to confirm the raw is still needed.
- `tickets/ti_504_causal_impact_experimentation/meetings/matt_and_malachi_meeting_1_1.txt` — _high conf / low risk_ — Second transcript of the SAME recording as matt_and_malachi_meeting_1.txt (identical opening: 'just had back-to-back meetings...'). meeting_1.txt is the OpenAI/paragraph transcription; meeting_1_1.txt is the local/timestamp-per-line transcription. Not exact-byte dupes, but one meeting represented twice under a confusing _1_1 name (convention is _openai/_local or a merged file). Human should pick/rename one.
- `tickets/ti_1053_elevenlabs_3p_segments/outputs/candidate_pool.json` — _low conf / medium risk_ — v1 candidate pool superseded by candidate_pool_v2.json, but summary §3 (and final_ranked.json in §4) still cite the v1 files — moving would break those links. Human glance before archiving; leave blessed csv/xlsx untouched.
- `tickets/ti_644_root_insurance/outputs/data_exports` — _medium conf / medium risk_ — outputs/ carries two parallel ad-hoc export subdirs (data/ and data_exports/) with same-named files of DIFFERENT content (e.g. cost_impression_log_ips.csv 61.6MB in data/ vs 42MB in data_exports/; impression_log_ips.csv 81MB vs 101MB — distinct md5s). Non-standard sprawl vs the flat outputs/ + outputs/final/ convention. A human should reconcile which pull is authoritative; a blind merge would collide on names.
- `tickets/ti_644_root_insurance/outputs/data_exports/ti_644_bid_vs_served_ips.csv` — _low conf / low risk_ — Three name/name_ips pairs in data_exports/ share identical byte sizes (bid_vs_served vs bid_vs_served_ips = 641176627; cost_impression_log vs _ips = 42072727; impression_log vs _ips = 101695082) but different md5s — near-duplicate variants, not exact dupes. Worth a human glance to confirm whether both cuts are needed; not proposing deletion since content differs.
- `tickets/ti_270_pre_post_analysis_ga/queries/ti_270_post_analysis_ga.sql` — _low conf / low risk_ — ~85% identical to sibling ti_270_pre_post_analysis_ga.sql (same 'RTC FEATURE IMPACT ANALYSIS' header; 186 differing lines). summary.md blesses pre_post as the 'main' query and notes this file as a fed-in carry-over, but this one is newer (Aug 28 15:28 vs 02:37) and longer — supersession direction is ambiguous. Also note summary line 45 references it as 'ti_254_post_analysis_ga.sql' (filename drift). Human glance only.
- `tickets/ti_644_root_insurance/artifacts/ti_644_llm_context.md` — _low conf / low risk_ — Overlaps heavily with sibling ti_644_complete_context.md — both are full-investigation resume/context dumps ('COMPLETE CONTEXT' vs 'Full LLM Context ... everything discovered so far'), llm_context being the later of the two. Transient LLM-scratch docs that duplicate each other and much of summary.md. Both are referenced in summary.md (lines 77-78), so flag for a human to merge into one rather than auto-remove.
- `tickets/ti_200_whitelist_blocklist/outputs/domain_lists/ti_200_whitelist_blocklist_domains.csv` — _medium conf / low risk_ — Summary labels this a 'combined' white+block list, but it is the 1464-domain blocklist plus a single 'domain' header row (1465 lines) — it does not contain the 3.3M-line whitelist. Near-duplicate of ecommerce_blocklist.csv; worth a human glance to confirm it is not the intended combined deliverable (domain_list.csv is the real combined file with designations).
- `tickets/audi_1111_vendor_quality/audi_1115_wtp_cpm/outputs/audi_1115_cpm_queries.zip` — _low conf / low risk_ — Zipped shareable query pack (README.md + audi_1115_l0f_fractional_credit_cpm.sql + q8b_solo_perf.sql) sitting in outputs/ (which is for query-result data); a bundle/archive belongs in artifacts/. l0f sql duplicates queries/audi_1115_l0f_fractional_credit_cpm.sql, but q8b_solo_perf.sql is not present elsewhere, so do not delete — human glance to relocate.
- `tickets/ti_kafka_secret_sweep/outputs/kafka_audit_report.md` — _low conf / low risk_ — Narrative markdown audit report in outputs/, which per the standard holds query-result data; report docs belong in artifacts/. Flagged for a glance only because it is effectively this ticket's main written deliverable (executive summary of 39 Kafka services) — human should decide whether to move it or leave it.
