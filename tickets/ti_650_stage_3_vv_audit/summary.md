# TI-650: Stage 3 VV Audit — IP Mutation & NTB Disagreement

**Jira:** TI-650
**Status:** Complete
**Date Started:** ~2026-02-10 (estimate)
**Date Completed:** ~2026-02-24
**Assignee:** Malachi

---

## 1. Introduction

Investigation into the MNTN verified visit (VV) pipeline to understand IP address mutation between pipeline stages and the NTB (new-to-brand) classification disagreement between `clickpass_log` and `ui_visits`. The audit was scoped to advertiser 37775 as a representative mid-size advertiser.

The broader goal: determine where in the pipeline IP addresses change, how much they change, and how that change drives NTB misclassification — with quantified, campaign-level granularity.

---

## 2. The Problem

- **IP mutation:** IP addresses observed at ad-serving (win_log) often differ from IPs observed at visit (ui_visits). The aggregate mutation rate was known to be ~21%, but the cause and pipeline location were unknown.
- **NTB disagreement:** `clickpass_log.is_new` and `ui_visits.is_new` frequently disagree for the same event — estimated at ~42% disagreement rate for NTB=TRUE population.
- **Impact:** IP mutation driving NTB misclassification means returning visitors are counted as new-to-brand, affecting campaign targeting accuracy, ROAS reporting, and advertiser trust.

---

## 3. Plan of Action

1. Build IP trace query: join clickpass_log → ui_visits to observe IP at each hop
2. Extend trace to win_log to capture IP at ad-serving (v2: 4-checkpoint trace)
3. Pivot to clickpass_log as starting point when it proved a better anchor than ui_visits (v3: 5-checkpoint trace)
4. Quantify mutation rate at each hop
5. Decompose by campaign, device type, and cross-device flag
6. Build NTB disagreement × mutation cross-tabulation
7. Document findings and propose remediation

---

## 4. Investigation & Findings

### IP Trace Methodology

Three query versions were developed:
- **v1:** 3-checkpoint trace (clickpass → ui_visits → conversion)
- **v2:** 4-checkpoint trace (win → redirect → clickpass → ui_visits)
- **v3:** 5-checkpoint trace, starting from clickpass_log as the anchor (better than ui_visits as starting point)

**Queries:**
- `queries/audit_trace_queries.sql` — full 5-checkpoint IP trace (v1/v2/v3 versions)
- `queries/questions_summary.sql` — questions for Zach / schema investigation queries

**Outputs:**
- `outputs/bqresults/` — BQ result JSON files (gitignored); `outputs/bqresults/readme.md` describes contents
- `outputs/column_definitions.tsv` — column definitions reference (gitignored)

**Artifacts:**
- `artifacts/stage_3_vv_audit_consolidated_v5.md` — comprehensive audit document (v5)
- `artifacts/stage_3_vv_pipeline_explained.md` — pipeline architecture explanation
- `artifacts/stage_3_vv_audit.docx` — Word version of audit report (gitignored)
- `artifacts/zach_review.md` — review notes with Zach
- `artifacts/handoff_prompt.md` — Claude session handoff prompt
- `artifacts/meeting_zach_1.txt` — meeting notes
- `artifacts/questions_for_zach.txt` — open questions for Zach
- `artifacts/questions_for_zach.docx` — formatted questions doc (gitignored)
- `artifacts/membership_updates_proto_generate.py` — protobuf generation script
- `artifacts/mes_pipeline.pdf` / `mes_pipeline.png` — MES pipeline diagrams (gitignored)

### Key Findings

**1. Aggregate mutation rate: ~21.2%**
For advertiser 37775, ~21.2% of events show a different IP between win_log and clickpass_log.

**2. ALL mutation occurs at the redirect hop**
Zero meaningful mutation occurs between clickpass_log and ui_visits. The visit hop is essentially stable. All 21.2% of mutation happens between the ad win and the clickpass (the redirect step).

**3. Per-campaign variance: 0.6% to 20.6%**
The aggregate 21.2% masks large campaign-level differences. Some campaigns are near-clean (0.6%) while others are severely affected (20.6%). Targeted fixes should start with worst-offending campaigns.

**4. Cross-device is the primary driver**
`is_cross_device = TRUE` events show 61.2% IP mutation vs ~10-15% for non-cross-device events. Cross-device tracking drives the NTB misclassification problem.

**5. Phantom NTB events: ~4,006/day for advertiser 37775**
Intersection of IP mutation + NTB disagreement: ~4,006 events/day where both `clickpass.is_new=TRUE` AND `ui_visits.is_new=TRUE` AND IP mutation is present. These are returning visitors misclassified as new-to-brand.

**6. clickpass_log is a 99.6% proxy for ui_visits VVs**
For audit purposes, starting from clickpass_log is nearly equivalent to starting from ui_visits but provides richer IP audit columns.

**7. BQ data gap discovered**
`raw.visits` and `cost_impression_log` stopped ingesting in BQ after 2026-01-31. Pivoted to Greenplum for post-Jan analysis.

**8. win_log.device_ip is always NULL**
Cannot use win_log for device-level IP trace.

**9. win_log uses Beeswax IDs, not MNTN ad_served_id**
Direct join on `ad_served_id` between win_log and clickpass_log is invalid — different ID systems.

---

## 5. Solution

- Delivered quantified analysis: mutation rate, hop breakdown, campaign decomposition, cross-device breakdown, NTB phantom event count
- Documented methodology as reusable 5-checkpoint IP trace pattern
- Produced per-campaign mutation table (BQ result JSON files in `outputs/bqresults/`; gitignored)
- Documented BQ pipeline gap and Greenplum workaround
- Provided remediation recommendations (targeted campaign fixes, cross-device infrastructure)

---

## 6. Questions Answered

- **Q:** Where in the pipeline does IP mutation occur?
  **A:** 100% at the redirect hop (win → clickpass). The visit hop (clickpass → ui_visits) is stable.

- **Q:** How much IP mutation occurs in aggregate?
  **A:** ~21.2% for advertiser 37775.

- **Q:** What drives IP mutation?
  **A:** Primarily cross-device events (61.2% mutation rate vs ~10-15% non-cross-device).

- **Q:** How many NTB events are phantom (returning visitors misclassified)?
  **A:** ~4,006/day for advertiser 37775.

- **Q:** Is clickpass_log a reliable VV proxy?
  **A:** Yes — 99.6% overlap with ui_visits VVs, with better IP audit columns.

- **Q:** Why does win_log join fail?
  **A:** win_log uses Beeswax IDs, not MNTN ad_served_id. device_ip is also always NULL in win_log.

---

## 7. Data Documentation Updates

Added to `data_catalog.md` (2026-03-03):
- clickpass_log entry with join keys, gotchas, and BQ tips
- ui_visits entry with UUID type caveat
- win_log entry with Beeswax ID and NULL device_ip warnings
- cost_impression_log entry with BQ data gap note

Added to `data_knowledge.md` (2026-03-03):
- Visits table disambiguation
- IP address columns per-table guide
- NTB disagreement between tables
- ID types (Beeswax vs MNTN)
- BQ data gaps
- Pipeline flow documentation
- Cross-device IP mutation stats
- Per-campaign mutation variance
- clickpass_log as VV proxy guidance

---

## 8. Open Items / Follow-ups

- Schema crawl: run `bq show --schema` for key tables to fill in complete column lists in data_catalog.md
- ext_visits table: unknown purpose, needs schema check (may need special permissions)
- mntn-coredw-prod: key tables not yet documented
- Remediation: no code changes were part of this audit ticket — follow-up ticket needed for fixes
- Greenplum port: BQ versions of post-Jan queries need to be ported when data resumes

---

## Performance Review Tags

**Speed:** Built v1→v2→v3 trace pipeline iteratively within weeks. Independently resolved 5+ blockers (IP type mismatch, win_log Beeswax ID, NULL device_ip, BQ data gap, clickpass/ui_visits VV overlap) without escalation. Managed parallel BQ and Greenplum investigation simultaneously.

**Craft:** Designed novel 5-checkpoint IP trace methodology. Decomposed 21.2% aggregate mutation into hop-level precision — discovered 100% occurs at redirect hop, correcting a prior assumption that mutation was distributed. Built NTB disagreement × mutation cross-tab revealing 4,006 phantom NTB events/day. Documented 18 discrete findings with quantified results. Created reusable methodology documented as a template.

**Adaptability:** When BQ pipeline gap was discovered (raw.visits/CIL stopped ingesting Jan 31), pivoted to Greenplum-first approach and documented BQ port requirements for when data resumes. Reframed v2 findings when v3 revealed mutation at visit was actually at redirect. Switched starting anchor from ui_visits to clickpass_log when the latter proved to be a better analytical starting point.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser directly impacts revenue retention — returning visitors misclassified as new-to-brand degrades campaign targeting accuracy and advertiser trust. Per-campaign variance (0.6% to 20.6%) enables targeted fixes. Cross-device mutation insight (61.2%) informs infrastructure investment decisions.

---

## Drive Files

📁 `Tickets/TI-650 Stage 3 Audit/`
- `Advertiser creates a campaign.gdoc`
- `Stage_3_VV_Audit_Consolidated_v4.docx`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Stage_3_VV_Audit_Consolidated_v5.md`
- `Stage_3_VV_Audit_Summary.docx`
- `Stage_3_VV_Audit_Summary.md`
- `Untitled spreadsheet.gsheet`
