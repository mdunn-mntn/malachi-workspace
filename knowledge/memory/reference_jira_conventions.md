---
name: jira-conventions
description: "All Jira conventions: wiki markup, curl REST v2 writes, v3 search endpoint, Task type, required fields, assignee, links, story points"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [jira conventions, wiki markup, curl rest v2, search jql api v3, task issuetype, story points, customfield, bug origin, sprint transitions, assignee]
domain: [jira-process]
lifecycle: active
last_verified: 2026-07-28
---
## from feedback_jira_formatting.md

The Jira MCP tool (`mcp__jira__jira_add_comment`) sends body text as plain ADF paragraphs via REST API v3. ALL markup passes through as literal characters — nothing renders.

Use `curl` with REST API v2 instead — wiki markup renders correctly.

**Why:** API v2 interprets wiki markup natively. API v3 (used by MCP tool) expects ADF and treats strings as plain text.

**How to apply:**
```bash
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
  -X POST -H "Content-Type: application/json" \
  "https://mntn.atlassian.net/rest/api/2/issue/TI-XXX/comment" \
  -d '{"body": "wiki markup here"}'
```

Wiki markup reference:
- Bold: `*text*` (single asterisks)
- Headers: `h3. Header Text`
- Bullets: `* item` (asterisk + space at line start)
- Links: `[text|url]`
- Code inline: `{{code}}`
- Code block: `{code}block{code}`

Style: keep the structured/formal format (headers, bullet sections, bold labels). User likes the formality — the issue was only that markup wasn't rendering, not the tone.

## from feedback_jira_create_curl.md

Use `curl` with REST API v2 for ALL Jira write operations (creating tickets, posting comments, updating descriptions) — not just comments.

**Why:** The MCP Jira tools (`jira_create_issue`, `jira_add_comment`) use API v3 which renders wiki markup (`*bold*`, `h3.`, `#` numbered lists) as literal text instead of formatting it. REST API v2 via curl correctly renders wiki markup.

**How to apply:** MCP tools are fine for READ operations (searching, getting issues). For any WRITE operation that includes formatted text, use curl with REST API v2.

## from feedback_jira_task_type.md
When creating Jira tickets in the TI project, always use `"issuetype": {"name": "Task"}`. Never use "Story" type.

**Why:** User preference confirmed 2026-04-20. The TI team's workflow treats all work as Tasks regardless of size or scope — Stories aren't used.

**How to apply:** Every `POST /rest/api/2/issue` call must have `"issuetype": {"name": "Task"}`. This applies to all TI-prefixed tickets including epics' child tickets (though the epic itself — BER-2250 style — is a separate issuetype set at epic creation time, not relevant to day-to-day ticket creation).

## from feedback_jira_links_must_be_http.md
When writing Jira ticket descriptions or comments in wiki markup:

- `[label|https://...]` ✓ renders as a clickable link
- `[label|tickets/path/to/file.md]` ✗ renders as literal text in the description body — looks like `[label|tickets/...]`

**Why:** Jira's wiki renderer doesn't know what to do with a workspace-relative path; it has no context for the user's git checkout. The viewer is in their browser, not a filesystem.

**How to apply:**
- For source attribution to workspace files, write plain text: *"Source: 2026-04-30 Alex K deck review (see TI-837 meetings/05)"*
- For real cross-references that need clicking, use the GitHub URL: `https://github.com/mdunn-mntn/malachi-workspace/blob/main/path/to/file.md`
- Default: drop the link and just describe where it lives. Readers can navigate the ticket tree.

## from feedback_jira_required_fields.md

Every TI Jira ticket must include these at creation time:

- **PMO Rep:** Bryce Wagg (`customfield_15612`, option ID `17863`)
- **Labels:** Quarterly label in format `q{quarter}_{year}` (e.g. `q2_2026` for April–June 2026). Quarters: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec.

**Release Type (`customfield_15783`) — OPTIONAL at creation; default to OMITTING it:**
- **Omit entirely** — **the default for most TI work.** Analysis, research, investigation, audits, documentation, Looms/presentations, dashboards/notebooks, internal tooling, advisory/design-review tickets. Nothing deployed to a prod service. The field is not required — leave it off the `curl` payload (verified: TI-1039 created fine with no `customfield_15783`). User 2026-06-17: "we don't need to add the release type."
- **Backend** (`id 14522`) — ONLY when backend application/pipeline code actually ships to prod: airflow-ti models, bidder changes, gary-ql/API resolvers, SQLMesh models that deploy. These also need a **fix version** attached.
- **UI** (`id 14521`) — front-end / premier-ui changes.
- (N/A = `id 14523` exists, but prefer omitting the field over stamping N/A for non-prod work.)

**Why:** I had been auto-stamping `Backend` on every ticket. Most TI work is analysis/tooling, so this mismarked dozens of tickets. `Done` + `Backend` + **no fix version** trips the release automation and finance reports — Bryce had to sweep and clean them up (TI-961, TI-1019, TI-962 caught 2026-06-09). User: "we don't want to mark every ticket we have as backend." Refined 2026-06-17: for non-prod tickets just leave Release Type off entirely.

**How to apply:** On every `curl` ticket creation, set PMO Rep + quarterly label always. **Leave Release Type off** unless the ticket genuinely ships prod code — then set Backend AND attach a fix version (or UI for front-end). Linked: [[feedback_jira_create_curl]], [[feedback_jira_task_type]].

## from feedback_jira_bug_required_fields.md
Creating a `{"issuetype": {"name": "Bug"}}` ticket in the TI Jira project requires two extra fields beyond what Task tickets need:

- **`customfield_16028` Bug Origin** (multi-checkbox; required). Allowed values:
  - `15161` Automated Testing / Monitors
  - `15162` Integration Testing
  - `15163` Regression Testing
  - `15164` Exploratory Testing
  - `15165` Demo Testing
  - `15166` Feature Acceptance Testing
  - `15167` Release Testing
  - `15168` Customer Submission
  - `15169` Other
- **`customfield_16001` Bug Environment Details** (single-select; required). Allowed values:
  - `15079` N/A
  - `15080` Dev
  - `15081` QA
  - `15082` Burn In
  - `15083` Prod

**Why:** Discovered when creating TI-931 — first attempt returned `400` with `customfield_16028: "Bug Origin is required"` and `customfield_16001: "Bug Environment Details is required"`. These fields don't apply to Task issuetype.

**How to apply** (full Bug ticket payload for TI project, on top of `feedback_jira_required_fields.md`):
```json
{
  "fields": {
    "project": {"key": "TI"},
    "issuetype": {"name": "Bug"},
    "summary": "<terse, clarity-first>",
    "description": "<wiki markup, objective/task/results>",
    "assignee": {"accountId": "712020:3c684a7b-50a1-4639-8cb1-e488aca288e7"},
    "customfield_10012": <story_points>,
    "customfield_15612": {"id": "17863"},
    "customfield_15783": {"id": "14522"},
    "customfield_15614": {"accountId": "712020:3c684a7b-50a1-4639-8cb1-e488aca288e7"},
    "customfield_16028": [{"id": "15161"}],
    "customfield_16001": {"id": "15083"},
    "labels": ["q2_2026"]
  }
}
```

**Defaults to use when in doubt:**
- Bug Origin: `15161` Automated Testing / Monitors — fits when caught by Airflow/scheduler/PagerDuty/CI failures (most TI bugs).
- Bug Environment Details: `15083` Prod — most TI bugs are prod-discovered.

**Discovery tool** (if these allowed values change):
```bash
curl -s -u "$EMAIL:$JIRA_API_TOKEN" \
  "$JIRA_BASE_URL/rest/api/2/issue/createmeta?projectKeys=TI&issuetypeNames=Bug&expand=projects.issuetypes.fields" \
  | jq '.projects[0].issuetypes[0].fields | {bug_origin: .customfield_16028, bug_env: .customfield_16001}'
```

## from feedback_jira_assign.md

When creating Jira tickets, assign them to Malachi by default.
Account ID: `712020:3c684a7b-50a1-4639-8cb1-e488aca288e7`

**Why:** Malachi asked for this — tickets he creates should be assigned to him automatically.
**How to apply:** Use `jira_assign_issue` after creating any ticket, or pass assignee in the create call.

## from reference_jira_practices.md

TAR team Jira best practices: `documentation/architecture/TAR-JIRA Best Practices-270326-052529.pdf`

**Story Points Scale:**
| Points | Time |
|--------|------|
| 1 | Half day or less |
| 2 | 1 day |
| 3 | 1-2 days |
| 5 | 3-5 days |
| 8 | 1 week+ (should be broken down) |
| 13+ | Turn into an epic |

**Field ID:** `customfield_10012` (Story Points)
**Estimate field:** `customfield_15521` (Story point estimate)

**Ticket types:** Task preferred. Stories for multi-team work (FE+BE+Design). Avoid subtasks.
**Linking:** Use "Included in Master Package" to link related tasks.

**Required fields when creating tickets:**
- Story Points: `customfield_10012` (required for Tasks)
- PMO Rep: `customfield_15612`, value `{"id": "17863"}` (Bryce Wagg)
- Release Type: `customfield_15783`, value `{"id": "14522"}` (Backend — auto-set on creation, update if UI or N/A)
- Priority: defaults to P3, adjusted during grooming/planning
- Developer: `customfield_15614` (userpicker) — REQUIRED on all tickets (feeds software capitalization audit). Value: `{"accountId": "712020:3c684a7b-50a1-4639-8cb1-e488aca288e7"}` (Malachi)
- Assignee: Malachi Dunn, accountId `712020:3c684a7b-50a1-4639-8cb1-e488aca288e7`
- Labels: Quarterly label (e.g. `q2_2026`)
- Sprint: add to current sprint if work starts this sprint, otherwise leave for backlog

**Rules:**
- Tasks should always have points
- Do NOT move tickets directly from In Progress → Done (violates workflow rules per Bryce Q2 update) — BUT that rule is really for CODE/release tickets (skipping RFD loses fix version + release Slack notif). For an ANALYSIS/non-release ticket, direct-to-Done (transition id 6) is fine. Also the `Developer` field is NOT reliably auto-assigned, so the `In Review` transition can be blocked ("Please add a developer to the ticket") — for analysis tickets, go straight to Done. **Ids for a manual close (verified AUDI-1083 2026-07-24, deployed SQLMesh model routed the full path):** set `Developer` = `customfield_15614` `{accountId:<assignee>}` (unblocks `In Review` = transition 711), then `Done` = transition 6 REQUIRES `resolution` (`{id:"10000"}` = Done; other resolutions: Won't Do 10100, Duplicate 3). Release Type left None for a silver analytics table (not serving/product code; Backend-without-fix-version trips finance automation). Detail in knowledge/mntn_business.md Jira ticket-hygiene.
- When creating follow-up tickets, link from parent ticket comment — note: wiki link syntax `[TI-XXX|url]` does NOT work in MCP tool (renders literal). Use plain ticket key `TI-XXX` which Jira auto-links, plus raw URL on next line.
- Comment on all changes explaining why
- Always estimate story points when creating new tickets
- Update tickets before standup and planning meetings (status, not data entry in meetings)

**Boards, Spikes & sprints (AUDI-1148, 2026-07-22):** Spikes + sprints live in **AUDI** (Scrum board **1814**, active sprint via `GET /rest/agile/1.0/board/1814/sprint?state=active`). **INCR is a Kanban board (3013) with NO sprints and NO Spike issue type** — file spike/sprint work under AUDI even when it's incrementality work. AUDI issue types: Spike=`11467`, Task=3, Story=6, Bug=1, Epic=27. **An AUDI Spike requires only project + issuetype + summary** — Story Points, PMO Rep, Developer, Release Type are NOT required for a Spike (unlike Tasks). Add to sprint: `POST /rest/agile/1.0/sprint/<sid>/issue {"issues":["AUDI-XXXX"]}`. Attach files: `POST /rest/api/2/issue/AUDI-XXXX/attachments` with header `X-Atlassian-Token: no-check` + `-F "file=@path"` (multiple `-F` OK); replace a stale attachment by `DELETE /rest/api/2/attachment/<attid>` first (updating the local file does NOT update the Jira copy).

## from reference_jira_search_api_v3.md

Atlassian **removed** the old `GET /rest/api/2/search` (and `/rest/api/3/search`) endpoint — a curl search now returns `"The requested API has been removed. Please migrate to the /rest/api/3/search/jql API."`

Use instead:

```bash
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
  -X POST -H "Content-Type: application/json" \
  "https://mntn.atlassian.net/rest/api/3/search/jql" \
  -d '{"jql": "project = AUDI ORDER BY created DESC", "maxResults": 20, "fields": ["summary","status","labels","created"]}'
```

Single-issue reads (`/rest/api/2/issue/KEY`), comment/create writes (REST v2, see [[feedback_jira_create_curl]]) are unchanged. Project fully migrated TI→AUDI with numbers preserved (TI-1037 = AUDI-1037); new tickets go in project key `AUDI`, "Backlog" is the default workflow status. See [[reference_team_name]], [[feedback_jira_required_fields]].

## Sprint, status transitions, delete (verified AUDI-1172/1177, 2026-07-28)

- **Sprint field = `customfield_10321`** (NOT 10020 — that read None). To find the active sprint: AUDI scrum board id **1814** → `GET /rest/agile/1.0/board/1814/sprint?state=active`. To add an issue: `POST /rest/agile/1.0/sprint/<id>/issue -d '{"issues":["AUDI-XXXX"]}'` (204); to pull it back out: `POST /rest/agile/1.0/backlog/issue -d '{"issues":[...]}'`.
- **Transitions:** `GET .../issue/KEY/transitions` then `POST .../transitions -d '{"transition":{"id":"6"}}'`. On the AUDI workflow, **Done=6**, Backlog=671, In Progress=771, On Hold=3, Blocked=61, Ready(Dev)=461. There's a direct Backlog→Done, fine for logging already-complete work as Done.
- **No DELETE permission (403)** on the AUDI project ("You do not have permission to delete issues"). To neutralize a mis-created ticket: transition to Backlog, rename `[VOID - duplicate of AUDI-XXXX]`, move out of the sprint, link Duplicate (`POST /rest/api/2/issueLink -d '{"type":{"name":"Duplicate"},"outwardIssue":{"key":"<void>"},"inwardIssue":{"key":"<keep>"}}'`), comment, and flag a human to delete.
- **A ticket represents the STAKEHOLDER DELIVERABLE (the ask), not the internal work done in service of it.** Logged AUDI-1177 "xlsx format-system uplift" for a session that was really about giving Kirsa the Select lift numbers — Malachi corrected it; the deliverable = AUDI-1172 (the numbers). Tooling/format improvements made along the way are not their own ticket unless separately requested. See [[feedback_ticket_writing_rule]].
