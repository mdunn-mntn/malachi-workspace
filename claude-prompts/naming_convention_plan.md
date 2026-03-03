# Naming Convention Plan — MNTN Workspace

## Rules (confirmed by user)

1. **All lowercase** — no uppercase anywhere in file or folder names
2. **Underscores only** — never dashes as word separators (the Jira dash in ticket IDs becomes underscore too)
3. **Ticket folder pattern:** `ticket_###_short_description`
   - e.g., `TI-650_stage_3_vv_audit` → `ti_650_stage_3_vv_audit`
   - e.g., `DM-3118_rtc_monitor` → `dm_3118_rtc_monitor`
   - e.g., `TGT-4016_ecomm_classifier_thresholds` → `tgt_4016_ecomm_classifier_thresholds`

## Current folders to rename (tickets/)

| Current name | Target name |
|---|---|
| `DM-3118_rtc_monitor` | `dm_3118_rtc_monitor` |
| `DM-3188_comparison_rt_and_non_rt` | `dm_3188_comparison_rt_and_non_rt` |
| `TGT-4016_ecomm_classifier_thresholds` | `tgt_4016_ecomm_classifier_thresholds` |
| `TGT-4103_common_crawl_coverage` | `tgt_4103_common_crawl_coverage` |
| `TI-033_vertical_classification_changes` | `ti_033_vertical_classification_changes` |
| `TI-200_whitelist_blocklist` | `ti_200_whitelist_blocklist` |
| `TI-221_pre_post_analysis` | `ti_221_pre_post_analysis` |
| `TI-253_tpa_monitor` | `ti_253_tpa_monitor` |
| `TI-254_investigate_low_ntb_percentage` | `ti_254_investigate_low_ntb_percentage` |
| `TI-270_pre_post_analysis_ga` | `ti_270_pre_post_analysis_ga` |
| `TI-310_ntb_investigations` | `ti_310_ntb_investigations` |
| `TI-34_identity_sync_freshness` | `ti_34_identity_sync_freshness` |
| `TI-390_mmv3_performance` | `ti_390_mmv3_performance` |
| `TI-391_audience_intent_scoring` | `ti_391_audience_intent_scoring` |
| `TI-501_jaguar_kpi` | `ti_501_jaguar_kpi` |
| `TI-502_ip_scoring` | `ti_502_ip_scoring` |
| `TI-541_ip_scoring_pipeline` | `ti_541_ip_scoring_pipeline` |
| `TI-542_max_reach_causal_impact` | `ti_542_max_reach_causal_impact` |
| `TI-644_root_insurance` | `ti_644_root_insurance` |
| `TI-650_stage_3_vv_audit` | `ti_650_stage_3_vv_audit` |
| `TI-684_missing_ip_from_ipdsc` | `ti_684_missing_ip_from_ipdsc` |

## Files to rename inside tickets (tracked in git)

These have dashes that need to become underscores:

| Current path | Target path |
|---|---|
| `dm_3118_rtc_monitor/queries/dm-3118-rtc-monitor.sql` | `.../dm_3118_rtc_monitor.sql` |
| `dm_3188_comparison_rt_and_non_rt/queries/dm-3188-comparison_rtc_and_non-rtc.sql` | `.../dm_3188_comparison_rtc_and_non_rtc.sql` |
| `dm_3188_comparison_rt_and_non_rt/queries/dm-3188-rtc-vs-nonrtc.sql` | `.../dm_3188_rtc_vs_nonrtc.sql` |
| `ti_270_pre_post_analysis_ga/queries/ti-254_post_analysis_ga.sql` | `.../ti_254_post_analysis_ga.sql` |
| `ti_270_pre_post_analysis_ga/queries/pre_post_analysis_ga.sql` | already lowercase/underscore — keep |
| `ti_650_stage_3_vv_audit/legacy/stage3_vv_audit_handoff/Stage_3_VV_Audit_Consolidated_v5.md` | `stage_3_vv_audit_consolidated_v5.md` |
| `ti_650_stage_3_vv_audit/legacy/stage3_vv_audit_handoff/Stage_3_VV_Pipeline_Explained.md` | `stage_3_vv_pipeline_explained.md` |
| `ti_650_stage_3_vv_audit/legacy/stage3_vv_audit_handoff/HANDOFF_PROMPT.md` | `handoff_prompt.md` |
| `ti_650_stage_3_vv_audit/legacy/stage3_vv_audit_handoff/ZACH_REVIEW.md` | `zach_review.md` |
| `ti_650_stage_3_vv_audit/legacy/stage3_vv_audit_handoff/bqresults/README.md` | `readme.md` |
| `ti_221_pre_post_analysis/queries/pre_post_analysis_queries.sql` | already good |

## Files in documentation/ to check

- `documentation/ntb_documentation/MM-44_IPDSC_HH_Discrepancy_Investigation.md` → `mm_44_ipdsc_hh_discrepancy_investigation.md`
- `documentation/ntb_documentation/audit_trace_queries.sql` → already good

## Summary of subdir names (standard, already correct)

- `queries/` ✓
- `outputs/` ✓
- `artifacts/` ✓
- `knowledge/` ✓
- `documentation/` ✓
- `tickets/` ✓
- `_template/` ✓ (underscore prefix = reserved/template)

## Implementation approach for new chat

1. Use `git mv` for all tracked file and folder renames (preserves git history)
2. For untracked files, use `mv` directly
3. Update any internal references in `summary.md` files that reference old paths
4. Update `CLAUDE.md` template example paths if needed
5. Update `summary_template.md` to use lowercase in example paths
6. Commit with message: `chore: enforce lowercase underscore naming convention across workspace`
7. Push

## Open question for new chat

- Should `README.md` → `readme.md`? Convention is usually to keep README uppercase for visibility. Decide before executing.
- The `_template` folder uses underscore prefix to sort first — keep this pattern.
