# Plan 2: Ticket Tracking System

## Goal
Create a structured, repeatable system for documenting every Jira ticket — from problem to solution — with templates, consistent folder structure, version-controlled storage, and automatic cross-pollination with data documentation.

---

## Folder Structure

```
workspace/
├── tickets/
│   ├── TI-650_stage_3_vv_audit/
│   │   ├── summary.md                    # Main ticket document
│   │   ├── queries/                      # SQL files used
│   │   │   ├── v3_trace_query.sql
│   │   │   └── ntb_disagreement.sql
│   │   ├── outputs/                      # Results, CSVs, screenshots
│   │   │   ├── mutation_by_campaign.csv
│   │   │   └── ntb_crosstab.png
│   │   └── artifacts/                    # Other supporting files
│   │       └── pipeline_diagram.png
│   ├── TI-684_missing_ip_from_ipdsc/
│   │   ├── summary.md
│   │   ├── queries/
│   │   ├── outputs/
│   │   └── artifacts/
│   └── _template/
│       └── summary_template.md           # Copy this for new tickets
```

### Naming Convention
- Folder: `{TICKET-ID}_{short_description}` (lowercase, underscores)
- Summary: always `summary.md`
- Queries: descriptive name, e.g., `trace_query_v3.sql`, `ntb_validation.sql`
- Outputs: descriptive name with format, e.g., `mutation_rates_by_campaign.csv`

---

## Ticket Summary Template

```markdown
# {TICKET-ID}: {Title}

**Jira:** {link}
**Status:** In Progress | Complete | Blocked
**Date Started:** YYYY-MM-DD
**Date Completed:** YYYY-MM-DD
**Assignee:** Malachi

---

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket. (Cross-reference with Plan 1.)

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.

---

## Performance Review Tags
<!-- Used by the self-review feedback loop (Plan 4) -->
**Speed:** {how this demonstrated speed}
**Craft:** {how this demonstrated craft}
**Adaptability:** {how this demonstrated adaptability}
```

---

## Storage & Sync Strategy

### Option A: Personal GitHub Repo (Recommended)
Create a private GitHub repo for ticket documentation.

**Setup:**
```bash
# One-time setup
cd /Users/malachi/Developer/work/mntn/workspace
git init tickets
cd tickets
gh repo create mntn-tickets --private --source=.
```

**Workflow:**
1. Work locally in `workspace/tickets/{TICKET-ID}_description/`
2. After each session or ticket milestone, commit and push
3. Claude can be instructed to remind you to commit at end of sessions

**Pros:**
- Full version history of every ticket
- Searchable from anywhere
- Easy to share specific tickets if needed
- Markdown renders nicely on GitHub
- Can be cloned to any machine

**Cons:**
- Must remember to push (can be automated)
- Screenshots/images add repo size (use `.gitattributes` for LFS if needed)

### Option B: Google Drive Sync
Use a local folder that syncs to Google Drive.

**Setup:**
- Place tickets folder inside Google Drive's local sync folder
- OR use `rclone` to sync periodically

**Pros:**
- Automatic sync
- Accessible from Google Drive web UI
- Easy to share with colleagues

**Cons:**
- No version history (unless using Drive's version history)
- Conflict potential if editing from multiple places
- Less developer-friendly

### Recommendation
**Start with Option A (GitHub).** It's more natural for a developer workflow, gives you version history, and integrates with Claude Code. You can always export/copy to Google Drive for sharing with non-technical stakeholders.

---

## Migration: Existing Tickets

You already have ticket folders in `workspace/`. We should:

1. **Migrate existing folders** into the new `tickets/` structure
2. **Retroactively create summary.md** files for completed tickets (at least a skeleton)
3. **Move the VV audit docx** content into `TI-650_stage_3_vv_audit/summary.md`

Existing folders to migrate:
- `TI-684 Missing IP from IPDSC` → `TI-684_missing_ip_from_ipdsc/`
- `ti_650_stage_3_audit` → `TI-650_stage_3_vv_audit/`
- `ti_033_vertical_classification_changes/`
- `ti_200_whitelist_blocklist/`
- `ti_221_pre_post_analysis/`
- `ti_253_tpa_monitor/`
- `ti_254_investigate_low_ntb_percentage/`
- `ti_270_pre_post_analysis_ga/`
- `ti_34_identity_sync_freshness/`
- `ti_501_jaguar_kpi/`
- `ti_502_ip_scoring/`
- `ti_541_ip_scoring_pipeline/`
- `ti_542_max_reach_causal_impact/`
- `ti_644_root_insurance/`

Other folders (`daily_trend_by_vertical_ab_test`, `dm_3118_rtc_monitor`, etc.) can stay in `workspace/` as non-ticket work, or be given ticket IDs if they have them.

---

## Feedback Loop: Ticket → Data Docs

At the end of every ticket (or during, if significant findings emerge):

1. Claude reviews findings in the ticket summary
2. Claude proposes additions to `data_catalog.md` and `data_knowledge.md`
3. Section 7 of the ticket summary records what was added
4. This ensures every ticket enriches the shared knowledge base

---

## CLAUDE.md Integration

Add to global instructions:
- When starting a new ticket, create the folder structure from template
- When closing a ticket, prompt for data documentation updates
- When closing a ticket, prompt for performance review tags (Plan 4)

---

## Estimated Effort
- **Template creation:** ~15 min
- **GitHub repo setup:** ~10 min
- **Migrate existing tickets (skeleton summaries):** ~2 hours
- **VV audit docx → markdown conversion:** ~30 min
- **CLAUDE.md integration:** Part of Plan 3
- **Ongoing:** 5-10 min per ticket (fill in template as you go)

---

## Success Criteria
- Every new ticket gets a folder with consistent structure
- Any past ticket can be found by ID and has at least a skeleton summary
- Queries and outputs are preserved alongside the narrative
- Knowledge flows from tickets into the shared data docs
- The full history is version-controlled and searchable
