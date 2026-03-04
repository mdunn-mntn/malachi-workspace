# Meeting Notes: Zach Meeting 3 (2026-03-04)

## Key Decisions

### 1. Campaign ID = Stage (one-to-one)
- Campaign ID and stage/segment/funnel are **one-to-one**
- Determine stage via `campaign_template_id` or `funnel_id` on the campaigns table
- Objective ID rough mapping: 1 = Stage 1, 5 = Stage 2, 6 = Stage 3
- Zach recommends using `campaign_template_id` over objective_id
- The **bidder doesn't even understand stages** — it treats each campaign as an independent entity with its own audience and budget

### 2. IPs accumulate stages, never removed
- When an IP qualifies for Stage 2, it is NOT removed from Stage 1
- Frequency capping (14-day window) prevents duplicate serving, not targeting removal
- "Trying to move it between the stages just adds a level of complexity that gets us nothing"

### 3. Budget allocation by stage
- Stage 1: ~75-80% of budget (wide net, prospecting)
- Stage 2: ~5-10% (already made first contact, capitalize)
- Stage 3: Remainder (highest intent — already visited site, drive repeat visit)
- Each campaign has its own budget, bidder just sees independent campaigns

### 4. VV attribution = stack model
- "Verified visits acts on a stack. We put impressions on the stack."
- When a page view comes in, system checks the TOP of the stack (most recent impression)
- Everything behind that impression on the stack is not eligible
- Only first-touch and last-touch matter; intermediate impressions don't

### 5. TABLE MUST SUPPORT ALL STAGES (major redesign)
- Current table: one row per VV with first-touch and last-touch
- **New requirement:** each row should show the full stage chain for that VV
  - Stage 1 VV: only Stage 1 data populated, Stage 2 and 3 columns NULL
  - Stage 2 VV: Stage 1 + Stage 2 data populated, Stage 3 NULL
  - Stage 3 VV: **entire row should be full** (Stage 1 → Stage 2 → Stage 3)
- Need to add campaign_id → stage lookup to classify each VV
- Need to trace back through the stages (Stage 3 VV → find the prior VV → find the Stage 2 impression → find the Stage 1 impression)

### 6. Non-CTV: use impression_log instead of event_log
- Display ads don't fire VAST events
- For display, the equivalent of vast_impression is the impression event in impression_log
- Zach: "you can really just use the impression log for display instead of the event log"
- vast_start → display equivalent is "viewed" but IP drift is less likely

### 7. Retention: 90 days confirmed
- "90 sounds like a good starting point. That's not going to break the bank."
- May need more or less, but 90 is fine to start

### 8. Table location: ask D Platt
- "You should confirm with D Platt where this should live"

### 9. Pipeline: SQLMesh
- "You're gonna want to create a SQLMesh job and go through that process for it"
- Not PySpark, not raw BigQuery DDL — SQLMesh

## Implications for Table Design

The current v2 schema traces one VV at a time with first-touch and last-touch. Zach wants the table to be **stage-aware**:

1. **Add stage classification**: join campaign_id → campaigns table → funnel_id to determine what stage each impression belongs to
2. **Add stage-level columns**: for a Stage 3 VV, we need:
   - Stage 3 impression IP lineage (current last-touch columns)
   - The VV that put the IP in Stage 3 (current prior_vv columns)
   - Stage 2 impression IP lineage (NEW — need to trace back one more hop)
   - Stage 1 impression IP lineage (current first-touch columns)
3. **NULL semantics change**: Stage 2/3 columns are NULL when the VV came from an earlier stage (not just when data is missing)

## Action Items
- [ ] Look up campaigns table schema (funnel_id, campaign_template_id)
- [ ] Design stage-aware column layout
- [ ] Ask D Platt about table location
- [ ] Learn SQLMesh job creation process
- [ ] Update production table SQL (A4 v3)
