---
name: jira-conventions
description: "All Jira conventions: wiki markup, curl REST v2 writes, v3 search endpoint, Task type, required fields, assignee, links, story points"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [jira conventions, jira comment, progress update, when to post, comment template, jira auth, set_auth, wiki markup, curl rest v2, search jql api v3, task issuetype, story points, customfield, bug origin, sprint transitions, assignee, spike issuetype, 11467, spike routes to AUDI, spike project routing, retroactive spike, 0 story points, zero SP, transition 6 Close, AUDI-1207, unticketed investigation, issueLink Relates To, resolution ids, wont do 10100, duplicate 3, triage bug spec, AUDI-1054 parent epic, bug priority mapping, two put task to bug conversion, nextPageToken only paging, startAt ignored, search 410 removed, DEV board devops request form, request type infrastructure improvement, DEV-8821, sprint 8649 hackathon, future sprint issue move, assignee put endpoint, sprint ids 8303 8649 8650, sprint_pull.sh, my open sprint issues, board 1814 sprint list, ticket description standard, laymen BLUF description, file links in tickets, github line anchor, verify link target on main, epic create fields, epic name 10528, epic re-parent agile api, AUDI-1290, AUDI-1302 wont do same day, backlog issue sprint removal, hackathon q3_2026 labels, pipeline optimization hackathon epic]
domain: [jira-process]
lifecycle: active
last_verified: 2026-09-02
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

**How to apply:** On every `curl` ticket creation, set PMO Rep + quarterly label always. **Leave Release Type off** unless the ticket genuinely ships prod code — then set Backend AND attach a fix version (or UI for front-end). Linked: [[reference_jira_conventions]], [[reference_jira_conventions]].

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

**Debugger triage-Bug spec (Bryce Wagg, 2026-08-27; implemented in airflow-ti `include/airflow_debugger/triage.py`, PR #1240):** type Bug, parent = Q3 tech-debt epic **AUDI-1054**, `customfield_16001` Bug Environment Details = `{"value": "Prod"}` (single value), `customfield_16028` Bug Origin = `[{"value": "Automated Testing / Monitors"}]` — it is an ARRAY and 400s if sent as a single object. Priority mapping: infra/upstream signature class = `P1 - Critical`, unclassified = `P3 - Minor`, else `P2 - Normal`.

**Converting an existing Task to Bug takes TWO PUTs** (verified converting AUDI-1227..1240 + AUDI-1245, 2026-08-28): FIRST PUT sets `issuetype` + `parent`; SECOND PUT sets the Bug-only screen fields (`customfield_16001`, `customfield_16028`) — they are not on the Task edit screen until the type has changed, so a single combined PUT 400s.

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

**Scope grew on an already-Done ticket → re-estimate SP + document the added ask, keep it Done (user pref, AUDI-1172 2026-07-29).** When a completed ticket's real scope ends up materially larger than its original estimate (a follow-up ask, an extra deliverable), bump `customfield_10012` to the true effort and post a short `*[Scope update | <date>]*` comment listing what was added beyond the original delivery — do NOT reopen it. So the board reflects actual effort and the ticket self-documents the growth. (AUDI-1172: 1→3 SP after the CPIV/CPIA follow-up + Matt reconciliation + explainer.)

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

**Boards, Spikes & sprints (AUDI-1148, 2026-07-22):** Spikes + sprints live in **AUDI** (Scrum board **1814**, active sprint via `GET /rest/agile/1.0/board/1814/sprint?state=active`). **INCR is a Kanban board (3013) with NO sprints and NO Spike issue type** — file spike/sprint work under AUDI even when it's incrementality work. AUDI issue types: Spike=`11467`, Task=3, Story=6, Bug=1, Epic=27. **An AUDI Spike requires only project + issuetype + summary** — Story Points, PMO Rep, Developer, Release Type are NOT required for a Spike (unlike Tasks). **Spike routing is org-forced to AUDI (verified 2026-08-05):** issuetype `11467` exists in BOTH the TI and AUDI create-meta, and POSTing `project={key:"TI"}` + `issuetype={id:"11467"}` still lands the issue in the **AUDI** project regardless of the create-project key — I posted project TI and got key `AUDI-1195`. So "file a TI spike" resolves to AUDI; matches the CLAUDE.md convention that spikes file under AUDI. Add to sprint: `POST /rest/agile/1.0/sprint/<sid>/issue {"issues":["AUDI-XXXX"]}`. Attach files: `POST /rest/api/2/issue/AUDI-XXXX/attachments` with header `X-Atlassian-Token: no-check` + `-F "file=@path"` (multiple `-F` OK); replace a stale attachment by `DELETE /rest/api/2/attachment/<attid>` first (updating the local file does NOT update the Jira copy).

**Retroactive 0-SP spike for unticketed investigation time (AUDI-1207, 2026-08-17).** When a question
arrives outside the board (a Slack ask, a PS escalation) and eats real time before anyone thinks to open a
ticket, log it afterwards as an **AUDI Spike with `customfield_10012: 0`** rather than leaving it invisible
or back-dating an estimate. Zero points says "this consumed a session but is not sprint capacity we
planned," which is the honest signal. Set `framing_state: "skip: retroactive — <why>"` in the local
`summary.md`; the framing gate is a pre-work ceremony and cannot apply to work already finished.

**Verified create-and-close path for an AUDI Spike (AUDI-1207, 2026-08-17):** `POST /rest/api/2/issue` with
`project={"key":"AUDI"}`, `issuetype={"id":"11467"}`, plus `customfield_10012`, `customfield_15612`
(`{"id":"17863"}` = Bryce) and `labels:["q3_2026"]` all accepted in the same payload. Then
`POST /rest/agile/1.0/sprint/<sid>/issue` (204). The Done transition on a Spike is listed as
**`6 Close`**, not "Done" — same id, different label, and it takes `resolution={"id":"10000"}` straight
from Open with **no `Developer` field required** (that block only bites the `In Review` route). Active
sprint at the time: board 1814 → sprint 8270.

## Comment cadence + template (migrated from global CLAUDE.md §9, 2026-08-11)

**When to post:** end of a work session (what was accomplished, what's next), ticket completion (final summary, key findings, follow-up tickets needed), and the moment a blocker is hit (what's blocked, what's needed to unblock). Nothing else.

**Comment shape (wiki markup, lint with `.claude/scripts/lint_comms.py --kind comment|completion`):**
```
*[Progress Update | Completed | Blocked]: YYYY-MM-DD*

Answer line: the one thing the reader needs, stated first.

h3. Done
* Bullet one

h3. Key Findings (if applicable)
* *Bold label:* Finding details

h3. Next
* Bullet one
```

**Link other tickets as** `[TI-XXX|https://mntn.atlassian.net/browse/TI-XXX]` in curl-posted wiki markup (in the MCP tool that syntax renders literal — use the bare key there).

**Ticket-create payload (REST v2, Task):**
```bash
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
  -X POST -H "Content-Type: application/json" \
  "https://mntn.atlassian.net/rest/api/2/issue" \
  -d '{"fields": {"project": {"key": "TI"}, "issuetype": {"name": "Task"}, "summary": "Title", "description": "wiki markup here", "assignee": {"accountId": "712020:3c684a7b-50a1-4639-8cb1-e488aca288e7"}, "customfield_10012": 3, "customfield_15612": {"id": "17863"}, "labels": ["q2_2026"]}}'
```

**MCP auth (READ tools only).** Before using any `mcp__jira__*` read tool, call `mcp__jira___internal_jira_set_auth` with `baseUrl` https://mntn.atlassian.net, `email` malachi@mountain.com, `apiToken` from `$JIRA_API_TOKEN`, `persist=false`. Writes never go through MCP.

## from reference_jira_search_api_v3.md

Atlassian **removed** the old `GET /rest/api/2/search` (and `/rest/api/3/search`) endpoint — a curl search now returns `"The requested API has been removed. Please migrate to the /rest/api/3/search/jql API."` (Now an HTTP **410**, verified 2026-08-28.) **`/rest/api/3/search/jql` pages ONLY by `nextPageToken` and SILENTLY IGNORES `startAt`** — startAt-based paging returns page 1 forever (a gauntlet fixer introduced it on PR #1240; would infinite-loop past 100 results; reverted).

Use instead:

```bash
curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
  -X POST -H "Content-Type: application/json" \
  "https://mntn.atlassian.net/rest/api/3/search/jql" \
  -d '{"jql": "project = AUDI ORDER BY created DESC", "maxResults": 20, "fields": ["summary","status","labels","created"]}'
```

Single-issue reads (`/rest/api/2/issue/KEY`), comment/create writes (REST v2, see [[reference_jira_conventions]]) are unchanged. Project fully migrated TI→AUDI with numbers preserved (TI-1037 = AUDI-1037); new tickets go in project key `AUDI`, "Backlog" is the default workflow status. See [[reference_team_name]], [[reference_jira_conventions]].

## Sprint, status transitions, delete (verified AUDI-1172/1177, 2026-07-28)

- **Sprint field = `customfield_10321`** (NOT 10020 — that read None). To find the active sprint: AUDI scrum board id **1814** → `GET /rest/agile/1.0/board/1814/sprint?state=active`. To add an issue: `POST /rest/agile/1.0/sprint/<id>/issue -d '{"issues":["AUDI-XXXX"]}'` (204); to pull it back out: `POST /rest/agile/1.0/backlog/issue -d '{"issues":[...]}'`.
- **Transitions:** `GET .../issue/KEY/transitions` then `POST .../transitions -d '{"transition":{"id":"6"}}'`. On the AUDI workflow, **Done=6**, Backlog=671, In Progress=771, On Hold=3, Blocked=61, Ready(Dev)=461. There's a direct Backlog→Done, fine for logging already-complete work as Done.
- **issueLink type names (verified 2026-08-24, AUDI-882→AUDI-1100):** the relates link is `{"type":{"name":"Relates To"}}` on this instance — POSTing `"Relates"` returns 404 ("No issue link type with name 'Relates' found"). `"Duplicate"` works as-is. Resolution ids for the Close transition (re-verified on the 2026-08-24 backlog-audit closes): Done=`10000`, Won't Do=`10100`, Duplicate=`3`; AUDI transition id 6 = Close.
- **DEV board (re-verified 2026-08-28, DEV-8821):** issueLink REST v2 type name is `"Relates To"` (`"Relates"` 404s). DEV uses the DevOps Request form requiring **Request Type** (Infrastructure Improvement for new infra), **Due Date**, **Environment**, **Squad**.
- **No DELETE permission (403)** on the AUDI project ("You do not have permission to delete issues"). To neutralize a mis-created ticket: transition to Backlog, rename `[VOID - duplicate of AUDI-XXXX]`, move out of the sprint, link Duplicate (`POST /rest/api/2/issueLink -d '{"type":{"name":"Duplicate"},"outwardIssue":{"key":"<void>"},"inwardIssue":{"key":"<keep>"}}'`), comment, and flag a human to delete.
- **A ticket represents the STAKEHOLDER DELIVERABLE (the ask), not the internal work done in service of it.** Logged AUDI-1177 "xlsx format-system uplift" for a session that was really about giving Kirsa the Select lift numbers — Malachi corrected it; the deliverable = AUDI-1172 (the numbers). Tooling/format improvements made along the way are not their own ticket unless separately requested. See [[feedback_ticket_writing_rule]].

## Sprint fan-out + assignee via the agile API (verified 2026-08-31, hackathon filing AUDI-1269..1281)

- **Move issues into a sprint (batch):** `POST /rest/agile/1.0/sprint/{sprintId}/issue -d '{"issues":["AUDI-1269","AUDI-1270"]}'` (204). Works on a FUTURE sprint — no need to wait for it to start.
- **AUDI scrum board = 1814.** Sprint ids AS OF 2026-08-31: **8303** (active, ends 09/07) · **8649** (the fall tech-debt hackathon sprint, 09/07-09/21) · **8650** (the one after). List them: `GET /rest/agile/1.0/board/1814/sprint?state=active,future`.
- **Assignee:** `PUT /rest/api/2/issue/{key}/assignee -d '{"accountId":"..."}'` (204).
- **Story points stay `customfield_10012`** — settable in the create payload or a later PUT.

## Ticket description standard (user rule 2026-08-31, verified AUDI-1269..1281 + epic AUDI-1290)

Every ticket description, every time:
- Shape: BLUF first line (what + payoff, plain English a non-owner can read), then `*Why:*`
  explaining the problem and defining every internal term at point of use ({{config key}},
  tier label, mechanism), then `*Task:*` as a bulleted list, then `*Done-when:*` measurable.
- Wiki markup, written via curl REST v2: `*bold*` labels, `* ` bullets, `{{monospace}}` for
  config keys, `[text|url]` links.
- Link everything named: each DAG/model file links its GitHub blob on main
  (`[name|https://github.com/SteelHouse/<repo>/blob/main/<path>#Lnn]`, line anchor when the
  change targets a known line); dashboards (Mode), PRs, and runbook files get links too.
- Verify each link target exists on origin/main first (`git ls-tree -r origin/main` /
  `git cat-file -e origin/main:<path>`). This catches dead findings: intent_score_household_map
  was deleted on main 2026-08-26 (PR 1209), so it was dropped from two tickets, not linked.
- The 400-char description cap yields to this standard (user call 2026-08-31): a linked,
  laymen-readable description may run long; the BLUF line still has to carry the ticket alone.
- **CONTRADICTED 2026-09-04.** Malachi, seeing AUDI-1326..1329 filed at ~2,000 chars each under
  that exception: "Why are the tickets so wordy? Don't we have a limit on size?" All four were
  rewritten to ~400 chars and the exception should be treated as retired. Both statements are his,
  five days apart; the later one is the live rule. **Default to the 400/60/4 cap.** The links
  survive the trim: what runs long is prose, not URLs. Note the structural conflict the cap has
  with the BLUF / *Why:* / *Task:* / *Done-when:* shape — `lint_comms.py` counts each of those
  three labels as a bullet, so the mandated structure alone spends 3 of the 4-bullet budget and
  a Task list of separate bullets cannot fit. Run the Task items as one sentence, not a list.
- Initiative/hackathon batches: parent all tickets to one Epic (Epic create needs
  customfield_10528 Epic Name + customfield_16455/16456 Feature-Audit/Monitor Required
  Yes/No options; re-parent via POST /rest/agile/1.0/epic/<KEY>/issue) and put the shared
  label on epic and children both.

- **Epic AUDI-1290 "Pipeline Optimization Hackathon" (created 2026-08-31)** parents the 13
  hackathon tickets AUDI-1269..1281; labels `hackathon` + `q3_2026` on the epic AND every child;
  all 13 descriptions rewritten to this standard.
- **Mis-filed ticket, same-day close (AUDI-1302, 2026-08-31):** DELETE still 403s without admin;
  the accepted path is transition to Won't Do (resolution `10100`) the same day + pull it from
  the sprint via `POST /rest/agile/1.0/backlog/issue`. Do not file a ticket for PR-only follow-on
  work the user is already driving — flag first ([[feedback_auto_capture_and_ticket_flag]]).

## Reading a sprint from the CLI (`sprint_pull.sh`, 2026-09-02)

`.claude/scripts/sprint_pull.sh` is the one command for "what is assigned to me this sprint" — it
resolves the sprint live (never hardcode an id), runs the JQL, and matches each issue to its local
ticket folder. `--next` for the next sprint, `--sprint <id>`, `--all` to include Done, `--json`.
Output: `key type status points folder title`, folder `-` when none exists.

Verified 2026-09-02: `GET /rest/agile/1.0/board/1814/sprint?state=active,future` returns 8303
(active, ends 09/07), 8649 (09/07-09/21), 8650 (09/21-10/05). `POST /rest/api/3/search/jql` with
`sprint = <id> AND assignee = currentUser() AND statusCategory != Done` is the working search —
the removed `/rest/api/2/search` still 410s. `GET /rest/agile/1.0/sprint/<id>` gives name/state/
endDate. Consumed by [[reference_sprint_skill]].

## AUDI Triage service account (provisioned 2026-09-04, ITS-6496)

The debugger files its triage Bugs as **`audi-triage-reporter@mountain.com`** ("AUDI Triage",
accountId `712020:44bd38ee-c426-4734-a0ed-4f6a69a508ce`), NOT as Malachi. Robin Fox created it after
Malachi declined a personal API token: the tool is meant to be shared company-wide, so the identity
has to be user-agnostic.

Access it needs, and why both halves matter:
- **Jira**: `Developers` role on the AUDI project (Alyson Lefkowitz, 2026-09-03). Operations used are
  create issue (`/rest/api/2/issue`), add remote link (`/issue/{key}/remotelink`) and search
  (`/rest/api/3/search/jql`). It never transitions or deletes.
- **Confluence**: View + Pages **Add** on the **Targeting (TAR)** space (Alyson, 2026-09-04). Confluence
  Cloud has no separate Edit permission, so Add is what allows editing; the triage step appends a row to
  the TI On Call Playbook (page `2908061697`). Granting only the Jira role leaves this half 403ing, which
  is easy to miss because the Bug still files.

Credentials live on the Astro prod deployment as the secret env vars `JIRA_API_TOKEN` and
`JIRA_USER_EMAIL`. Rotating the token is an Astro variable edit, not a code change.
