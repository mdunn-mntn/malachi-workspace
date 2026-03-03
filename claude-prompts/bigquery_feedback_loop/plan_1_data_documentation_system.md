# Plan 1: Data Documentation System

## Goal
Build a living, progressively-improving documentation system for MNTN's data tables — schemas, relationships, hidden knowledge, and querying tips — that grows automatically as we work.

---

## Deliverables

### File 1: `data_catalog.md` — Table Registry & Schema Reference
**Location:** `/Users/malachi/.claude/projects/-Users-malachi-Developer-work-mntn-workspace-bigquery-feedback-loop/memory/data_catalog.md`

**What goes in it:**
- Table fully-qualified name (project.dataset.table)
- Environment (BigQuery, Greenplum, or both)
- Column list with types
- Partition column and type (date, timestamp, etc.)
- Clustering columns (if any)
- Approximate row counts / date ranges observed
- Key columns for joins (what joins to what)
- Known query tips (e.g., "always cast ad_served_id::text on ui_visits side")

**Template per table:**
```markdown
## `dw-main-silver.logdata.clickpass_log`
- **Environment:** BigQuery + Greenplum
- **Partition:** `_PARTITIONTIME` (date)
- **Clustering:** unknown
- **Row estimate:** ~32K/day/advertiser (observed for adv 37775)
- **Key columns:** `ad_served_id` (TEXT), `page_view_guid`, `ip`, `advertiser_id`, `is_new`, `is_cross_device`
- **Join keys:**
  - → `ui_visits` via `ad_served_id` (cast ui_visits side to text)
  - → `conversion_log` via `page_view_guid = guid`
  - → `cost_impression_log` via `ad_served_id`
- **Query tips:**
  - Always date-filter; table is date-partitioned
  - `ad_served_id` is text type here, uuid in ui_visits
- **Date range (BQ):** TBD — needs verification
```

### File 2: `data_knowledge.md` — Tribal Knowledge & Insights
**Location:** `/Users/malachi/.claude/projects/-Users-malachi-Developer-work-mntn-workspace-bigquery-feedback-loop/memory/data_knowledge.md`

**What goes in it:**
- Relationships between tables that aren't obvious from schemas
- "Gotchas" discovered during analysis (e.g., `win_log.device_ip` is always NULL)
- Business logic encoded in columns (e.g., what `is_new` actually means at each stage)
- Known data quality issues (e.g., `is_new` disagrees 42% between clickpass and ui_visits)
- Which table is the "source of truth" for which concept
- Pipeline flow documentation (how data flows from Stage 1 → 2 → 3)
- ID format references (Beeswax IDs vs MNTN IDs)
- BQ vs Greenplum differences for the same conceptual table
- Any "there are 8 visits tables" type disambiguation

**Template per topic:**
```markdown
## Visits Tables — Disambiguation
- **ui_visits** (summarydata): The enriched visit record. Has VV flags, NTB classification.
- **raw.visits** (bronze): Raw ingestion, stopped Jan 31 2026 in BQ.
- **ext_visits**: Unknown — schema check pending (permissions needed).
- **Source of truth for VVs:** `ui_visits` for verified visits, but `clickpass_log` is a near-perfect proxy (99.6%) with better audit columns.
- **Key insight:** Stages are additive, not sequential. Stage 2 overlaps ~97% with Stage 1.
```

---

## The Feedback Loop Mechanism

### How it works
After every Claude Code session that involves BigQuery/data work:

1. **Claude checks** both doc files at the start of every session (they'll be in the memory directory, auto-loaded)
2. **During work**, any time we discover something new about a table — a column meaning, a join pattern, a gotcha — Claude proposes an update
3. **At the end of the session**, Claude reviews what was learned and drafts additions/corrections to both files
4. **The user approves** the updates (Claude will propose edits, not silently write)

### What triggers an update
- Schema inspection reveals new columns or types
- A join succeeds or fails (confirming/denying a relationship)
- A query reveals data quality issues
- Business logic is clarified by a colleague
- A new table is encountered
- An existing doc entry is found to be wrong

### Integration with CLAUDE.md (see Plan 3)
A directive will be added to the global CLAUDE.md instructing Claude to:
- Read both doc files at session start
- Propose updates when new knowledge is discovered
- Never assume schema relationships not documented in `data_catalog.md`

---

## Bootstrap: Initial Population

### Step 1 — Seed from existing knowledge
We already know a lot from the VV audit work. Immediately populate both files with:
- All tables referenced in the Stage 3 audit doc
- All join keys, gotchas, and insights from that work
- The pipeline flow (Stage 1 → 2 → 3)

### Step 2 — Schema crawl
Run `bq show --schema` and `bq ls` for key datasets to fill in column-level detail:
- `dw-main-silver.logdata.*`
- `dw-main-silver.summarydata.*`
- `mntn-coredw-prod` (key tables)

### Step 3 — Visits table disambiguation
Specifically enumerate every table with "visit" in the name across all known projects/datasets, document what each one is, and identify the hierarchy.

---

## Estimated Effort
- **Bootstrap (seed from VV audit):** ~30 min
- **Schema crawl:** ~1 hour (mostly bq commands + organizing)
- **Visits disambiguation:** ~30 min
- **CLAUDE.md integration:** Part of Plan 3
- **Ongoing:** 2-5 min per session (incremental updates)

---

## Success Criteria
- Any new Claude session can immediately understand which table to use for a given concept
- No more guessing at join keys — they're documented with types and caveats
- The "8 visits tables" problem is resolved with a clear disambiguation
- Knowledge compounds over time instead of being rediscovered
