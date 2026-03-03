# Prompt: Sync Google Drive file references in all ticket summary.md files

## Context

Workspace root: `/Users/malachi/Developer/work/mntn/workspace/`
Drive root: `/Users/malachi/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`
Drive Tickets folder: `…/My Drive/Tickets/`

Each ticket has a `## Drive Files` section in its `summary.md`. Those references need to match
what is **actually on Drive right now**. Run `ls` on each Drive folder to confirm, then update
the summary.md to reflect reality exactly — correct file names, correct folder name, full file list.

Commit after every ticket updated. Push after every 3–4 commits.

---

## Known Drive state (verified 2026-03-03, pre-populated to save you ls time)

The following is the **actual contents of each Drive folder** as of today.
Use this to update summaries without needing to re-ls unless you want to verify.

### `Tickets/DM-3188 Comparisson RT and Non-RT/`
- `Comparisson of Real Time To Non-Realtime IPs.csv`
- `comparisson_rtc_and_non-rtc.sql`

### `Tickets/TI-33 Vertical Sizes After Introduction/`
(mapped to local ticket: `ti_033_vertical_classification_changes/`)
- `TI-33 Vertical Classification Changes.gsheet`

### `Tickets/TI-200 Adding Domains to Whitelist   Blocklist/`
(three spaces in folder name)
- `[TI-200] - Whitelist Blocklist.gsheet`
- `_TI_200__Add_more_domains__Whitelist__Blocklist.csv`

### `Tickets/TI-221 Pre-Post Vertical Classification/`
- `TI-221 Pre-Post Vertical Analysis Planning .gdoc` (note trailing space in name)
- `[TI-221] - Pre Post Analysis .gsheet` (note trailing space)
- `[TI-270] - Pre Post Analysis GA Release.gsheet` ← belongs to TI-270, stored here

### `Tickets/TI-254 Investigate Low NTB Percentages/`
- *(empty)*

### `Tickets/TI-270 Pre Post Analysis Jaguar Release/`
- `[TI-452] - Pre Post Analysis Jaguar Release.gsheet` ← labeled TI-452 but belongs to TI-270

### `Tickets/TI-310 NTB Investigations/`
- `Copy of NTB_0801_0817.gsheet`
- `NTB Agenda.gdoc`
- `NTB Missing Page Views.gsheet`
- `New-to-Brand (NTB) Documentation.gdoc`
- `Notes - NTB Sync.gdoc`

### `Tickets/ID-34 Establish Freshness Measures for IP Blocklists/`
(mapped to local ticket: `ti_34_identity_sync_freshness/`)
- `ID-34 Establish Freshness Measure for IP Blocklist.gdoc`
- `ID-34 Establish Freshness Measure for IP Blocklist.gsheet`
- `Quality and Identity Graph.gdoc`

### `Tickets/TI-390 Investigate MMv3 Performance/`
- `TI-390 MMv3 Performance Investigation.gdoc`
- `TI-390 MMv3 Performance Investigation.gsheet`

### `Tickets/TI-391 Analyze Pre Post of Audience Intent Scoring Changes/`
- `TI-391 Analyze Pre Post Audience Intent Changes.gdoc`
- `TI-391 Analyze Pre Post Audience Intent Changes.gsheet`

### `Tickets/TI-501 Jaguar Analysis/`
(also stores TI-502 file)
- `TI-452 Jaguar Analysis.gdoc` ← new discovery, not yet in any summary
- `TI-502 How We Use Scores.gsheet`

### `Tickets/TI-541 Max Reach Scores Analysis/`
(Drive folder named "Max Reach" but contains IP Scoring Pipeline docs)
- `TI-541 IP Scoring Pipeline Overview DRAFT.docx`
- `TI-541 IP Scoring Walkthrough DRAFT.docx`
- `TI-541 Unscored IPs Investigation.docx`
- `Scores Breakdown.pdf`
- `Audience Intent Graph.png`
- `Audience Intent Scoring.png`
- `Biddable Inventory Funnel.png`
- `DS13 Data Pipeline.png`
- `Ecommerce Classification Architecture API.png`

### `Tickets/TI-644 Root Insurance Analysis/`
- `TI-644 Root Insurance.gdoc`
- `TI-644 Root Insurance Audience List.gsheet`
- `TI-644 Root Insurance Kale Talk Track.gdoc`
- `bid_vs_served_ips_root_insurance.csv`
- `city_analysis.csv`
- `conversion_ips.csv`
- `conversion_ips.gsheet`
- `cost_impression_log_ips_root_insurance.csv`
- `impression_log_ips_root_insurance.csv`
- `ipdsc_excluded_ips_root_insurance.csv`
- `ipdsc_included_ips_root_insurance.csv`

### `Tickets/TI-650 Stage 3 Audit/`
- `Advertiser creates a campaign.gdoc`
- `Stage_3_VV_Audit_Consolidated_v4.docx`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Stage_3_VV_Audit_Consolidated_v5.md`
- `Stage_3_VV_Audit_Summary.docx`
- `Stage_3_VV_Audit_Summary.md`
- `Untitled spreadsheet.gsheet`

### No Drive folder found for:
- `dm_3118_rtc_monitor` — (results are in DM-3188 Drive folder)
- `mm_44_ipdsc_hh_discrepancy` — no Drive folder
- `tgt_4016_ecomm_classifier_thresholds` — no Drive folder
- `tgt_4103_common_crawl_coverage` — no Drive folder
- `ti_253_tpa_monitor` — no Drive folder
- `ti_502_ip_scoring` — no dedicated folder (file stored in TI-501 folder)
- `ti_542_max_reach_causal_impact` — no Drive folder
- `ti_644_root_insurance` — HAS folder (listed above)
- `ti_684_missing_ip_from_ipdsc` — no Drive folder

---

## Tasks per ticket

For each ticket below, open the `summary.md` and update `## Drive Files` to match the actual
Drive state above. Be precise: exact filenames, exact folder path (copy the folder name character
for character, including spacing quirks). Note any anomalies inline.

### `dm_3118_rtc_monitor/summary.md`
No Drive folder. Current summary says "(None in Drive for DM-3118 specifically; comparison results
are in DM-3188 Drive folder)". This is accurate — leave as-is.

### `dm_3188_comparison_rt_and_non_rt/summary.md`
Drive has 2 files. Current summary matches. Verify and leave if correct.

### `ti_033_vertical_classification_changes/summary.md`
Drive has 1 file. Update summary to include it:
- Folder: `Tickets/TI-33 Vertical Sizes After Introduction/`
- File: `TI-33 Vertical Classification Changes.gsheet`

### `ti_200_whitelist_blocklist/summary.md`
Drive has 2 files. Update summary:
- Folder: `Tickets/TI-200 Adding Domains to Whitelist   Blocklist/` (three spaces)
- Files: `[TI-200] - Whitelist Blocklist.gsheet`, `_TI_200__Add_more_domains__Whitelist__Blocklist.csv`

### `ti_221_pre_post_analysis/summary.md`
Drive has 3 files (one belongs to TI-270). Update:
- Folder: `Tickets/TI-221 Pre-Post Vertical Classification/`
- `TI-221 Pre-Post Vertical Analysis Planning .gdoc`
- `[TI-221] - Pre Post Analysis .gsheet`
- `[TI-270] - Pre Post Analysis GA Release.gsheet` ← note: belongs to TI-270, stored here

### `ti_253_tpa_monitor/summary.md`
No Drive folder. Note this explicitly if not already.

### `ti_254_investigate_low_ntb_percentage/summary.md`
Drive folder exists but is empty. Note this.

### `ti_270_pre_post_analysis_ga/summary.md`
Drive has 1 file (labeled TI-452). Update:
- Folder: `Tickets/TI-270 Pre Post Analysis Jaguar Release/`
- `[TI-452] - Pre Post Analysis Jaguar Release.gsheet` ← labeled TI-452, is TI-270 content

### `ti_310_ntb_investigations/summary.md`
Drive has 5 files. Read current summary and update file list to match all 5.

### `ti_34_identity_sync_freshness/summary.md`
Drive is `ID-34` folder (not TI-34). Has 3 files. Current summary already references these —
verify they match exactly.

### `ti_390_mmv3_performance/summary.md`
Drive has 2 files. Update if not already accurate.

### `ti_391_audience_intent_scoring/summary.md`
Drive has 2 files. Update if not already accurate.

### `ti_501_jaguar_kpi/summary.md`
Drive has 2 files. Note: `TI-452 Jaguar Analysis.gdoc` is a new discovery not yet in summary.
Update to include both files.

### `ti_502_ip_scoring/summary.md`
Stored in TI-501 folder. Already noted in summary. Verify and leave if correct.

### `ti_541_ip_scoring_pipeline/summary.md`
Drive folder named "Max Reach Scores Analysis" but contains IP Scoring docs. Has 9 files.
Update to list all 9 files. Already noted the naming mismatch in the Drive folder name.

### `ti_542_max_reach_causal_impact/summary.md`
No Drive folder. Note this explicitly if not already.

### `ti_644_root_insurance/summary.md`
Drive has 11 files. This is probably not fully listed in current summary. Read the current
summary and update to list all 11 files.

### `ti_650_stage_3_vv_audit/summary.md`
Drive has 7 items. Update to list all:
- `Advertiser creates a campaign.gdoc`
- `Stage_3_VV_Audit_Consolidated_v4.docx`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Stage_3_VV_Audit_Consolidated_v5.md`
- `Stage_3_VV_Audit_Summary.docx`
- `Stage_3_VV_Audit_Summary.md`
- `Untitled spreadsheet.gsheet`

### `ti_684_missing_ip_from_ipdsc/summary.md`
No Drive folder. Current summary already says this — leave as-is.

---

## Additional cross-references to fix

1. **`ti_221_pre_post_analysis/summary.md`**: The `[TI-270]` file stored in TI-221's Drive folder
   should also be cross-referenced in `ti_270_pre_post_analysis_ga/summary.md` (note it's physically
   stored in the TI-221 folder). Already partially noted — just make sure both summaries are consistent.

2. **`ti_501_jaguar_kpi/summary.md`**: The `TI-452 Jaguar Analysis.gdoc` file in the TI-501 Drive
   folder is labeled TI-452 but lives there. Document this anomaly clearly.

---

## Operating rules

- Read each summary.md before editing it — don't overwrite content blindly
- Preserve all existing sections; only update `## Drive Files`
- Use the exact folder path with correct spacing/capitalization as it exists on Drive
- Commit message format: `docs: sync drive file references — ti_XXX, ti_YYY`
- Commit after every 3–4 tickets, push after each commit
