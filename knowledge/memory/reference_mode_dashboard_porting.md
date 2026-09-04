---
name: reference_mode_dashboard_porting
description: "How to port a Python/SQL report tool into a Mode dashboard: repo, window.datasets->Chart.js, params, the fragment-injection blank-page fixes, and what the REST API can and cannot do"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 84b2cb82-cfd8-4180-9827-1f0a7ea16899
doc_type: memory
keywords: [mode dashboard, mode blank page, mode layout not rendering, mode fragment injection, mode duplicate root getelementbyid, mode schedule api, mode publish api, window.datasets, mode rest api, mode-assets, chart.js, deploy_mode.sh, liquid params, multiselect all sentinel, modeanalytics bot, tableau migration]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-09-03
---
Mode (BI tool; MNTN moving off Tableau). A **report** = SQL queries + optional Python notebook + one
`index.html` + `settings.yml`. Repo = **`SteelHouse/mode-assets`** (private); structure
`Mode/mntn/spaces/<space>/<Report Name>.<12hex token>/` with `<Query Name>.<token>.sql`, `settings.yml`,
`index.html`, `notebook/cell-number-N.<token>.py`. AUDI space = "🗂️ Audience Intelligence". Applied at
[[project_audi_1037_mode_dashboard]].

**Data bridge (the crux):** every query becomes a named dataset; the HTML reads them from
`window.datasets` = array of `{name, content:[…rows]}`, resolved **by query name** (row keys = the SQL
column aliases). Render with **Chart.js** (CDN) + HTML/CSS. Notebook cells whose last line is a DataFrame
add more named datasets. **Mode CANNOT render matplotlib PNGs — rebuild every chart as HTML/JS.**

**Parameters:** a query with a `{% form %}` block (2-space indent, UNQUOTED defaults:
`type: text`/`date`/`select`/`multiselect`) defines the inputs; consumers reference `{{ Param }}` and wrap
dates in quotes in SQL (`'{{ Period_Start }}'`). Canonical examples live in Mode's own
"💡 Sample Code / Sample Code - Parameters" report.

**GitHub↔Mode sync — CORRECTS Nick's "UI-only push" claim:**
- Sync is **two-way**. Commits to the **default branch `main` auto-sync to Mode**; a **branch does NOT sync**
  (safe to develop on). Mode→GitHub is via the **`modeanalytics[bot]`** ("[Mode] Update Report …" commits).
- BUT **Mode will NOT create a NEW report from a hand-authored GitHub folder** (verified 2026-07-07: merged a
  new `<Report>.<self-made-token>/` to main → no bot reaction, report never appeared). GitHub→Mode only syncs
  **edits to reports Mode already knows**. So a report must be **born once in the Mode UI** (gets a real token
  + Push-to-GitHub). **git-side EDITS do NOT flow INTO Mode** (settled 2026-07-07, Nick right / repo README
  wrong: Mode only accepts changes made in the UI first — mode-assets PR #10 closed unmerged for this reason).
  (Paste-into-UI was the deploy path until 2026-07-16 — superseded by the API deploy below. Push to GitHub
  still syncs the archive afterwards; the bot bypasses the ruleset — no PR/review on that path.)
- **DEPLOY = REST API (PROVEN 2026-07-16, replaces the paste relay ENTIRELY — HTML included):** Mode REST API
  `PATCH /api/mntn/reports/{report}/queries/{tok}` `{"query":{"raw_query":…}}` + `POST …/runs` (poll state)
  both work; `POST …/runs` also takes `{"parameters": {...}}` (multiselects as arrays) to run with explicit
  params. Script: CPD staging dir `deploy_mode.sh` (check/diff/apply [--run]/run; backs up remote SQL to
  ~/.cache/mode_deploy, verifies every PATCH by re-GET). Creds: MODE_API_TOKEN/SECRET in ~/.zshrc (Member API
  key — Workspace Settings > Personal > My API Keys; Malachi created one directly 2026-07-16, NO admin step
  was needed at MNTN; workspace tokens are admin-only).
  Gotchas: member keys CANNOT call `/account` (use the report GET as auth probe); live query names are
  underscored variants of display names (match case/underscore-insensitively); UI pastes append a trailing
  blank line (normalize trailing newlines when diffing); report payload includes `layout` (HTML) and
  `PATCH {"report":{"layout":…}}` WORKS (undocumented; proven 2026-07-16) — reports are fully zero-paste.
  Run required after SQL changes (window.datasets = last run); HTML-only changes render on page refresh, no
  Run needed.
- **Multiselect 'ALL' sentinel trap (user-caught 2026-07-16):** `('ALL' IN ({{ P }}) OR col IN ({{ P }}))`
  silently stops filtering when users check ALL + a specific together (Mode can't make options exclusive).
  Fix = specific-overrides-ALL: `(col IN ({{ P }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ P }}]) v))`.
  Belt-and-suspenders: index.html carries an exclusive-ALL JS (document-level click listener; clicking a
  specific option synthetically unchecks ALL; double-injection-guarded, try/catch no-op) — UX effect not yet
  user-confirmed against Mode's live widget markup.

**READ a report's SQL + results via the API (PROVEN 2026-07-21 — to review someone else's report):**
- `GET /api/mntn/reports/{token}` (meta) · `GET …/reports/{token}/queries` (each `raw_query` = the full SQL) ·
  `GET …/reports/{token}/runs?order=desc&per_page=1` (latest run; its `query_runs` embed is EMPTY here).
- Fetch results in 2 hops: `GET …/runs/{run}/query_runs` (list, with `_links.result.href`), then
  `GET …/runs/{run}/query_runs/{qr}/results/content.csv`. **The `content.csv` body is GZIP** — pass
  `curl --compressed` (or pipe through `gunzip -c`); curl does not auto-decompress by default.
  Rows/name come back null in the list; the CSV has the header.
- **`MODE_API_TOKEN`/`MODE_API_SECRET` (in ~/.zshrc) are NOT exported into a non-interactive shell** — load
  them first: `eval "$(grep -E '^export MODE_API_(TOKEN|SECRET)=' ~/.zshrc)"`, then `curl -u "$TOKEN:$SECRET"`.
  Member keys can't hit `/account`; a report GET is the auth probe. (Applied: found the live "Campaign Groups
  Hitting Goal Percentage" report this way — see [[reference_goal_attainment_report]].)

**REST API capability boundary on a MEMBER key (mapped 2026-09-03 building the MDE calculator report):**
- WORKS: `PATCH /reports/<t>/queries/<q>` (raw_query and name) · `PATCH /reports/<t>` with
  `{"report":{"layout": html}}` · `POST /reports/<t>/runs` then poll `/runs/<r>` ·
  `GET /runs/<r>/query_runs/<qr>/results/content.csv` (pass `curl --compressed`, else the body is gzip).
- **CANNOT create a schedule.** `POST /reports/<t>/schedules` rejects every cron format tried
  (`0 6 * * 1`, 6-field, `@weekly`, named day) with "Cronline must be formatted as a cron string" on a
  member key. The API path is closed, so treat schedule creation as UI-only and set the weekly
  refresh in the Mode UI (that UI path was not exercised here).
- **CANNOT publish.** There is no `/publish` endpoint (404) and `published_at` stays null over the API.
- Reconfirmed: git cannot create a report (a hand-authored git folder never appears in Mode), so the
  report has to exist in Mode already. Creation over the API DOES work, `POST /api/mntn/reports` with a
  `space_token` (verified 2026-08-28, [[reference_mode_api]]); it was not exercised this session.

**The layout is a FRAGMENT injected into Mode's own live page (five blank renders, 2026-09-03, MDE
calculator report `9a5afa55ca99` in "Audience Intelligence"). Symptom first, since that is what a future
session sees:**
- **Page blank, no console error:** the layout was pushed as a full `<!DOCTYPE html><html><head><body>`
  document. Mode injects the layout into its page, so inside a document wrapper none of the scripts
  execute. Emit CDN tags, a scoped `<style>`, markup under one root div, then the scripts. No wrapper.
- **Markup renders, controls dead, numbers stuck on placeholder dashes:** boot was registered on
  `DOMContentLoaded`, which has ALREADY fired by the time Mode injects, so the handler never runs. Call
  boot directly when `document.readyState !== 'loading'`.
- **Blank after a re-injection, console says "Identifier has already been declared":** the script
  re-executes in the SAME window, so every top-level `const`/`let` throws and kills the whole script
  before any guard can run. Wrap the app in an IIFE and export onto `window` only the functions that
  inline `onclick` attributes name.
- **Two root elements after a re-injection:** Mode can APPEND a re-injected layout rather than replace
  it, and duplicate ids then make EVERY `document.getElementById` resolve to the stale FIRST copy,
  including any boot guard. Keep only the last: `querySelectorAll('#root')`, remove the rest, boot that
  one. That also restores id uniqueness for the rest of the app.
- **Placeholder dashes after every Refresh, never on first load, and NO console error at all. THE ONE
  THAT ACTUALLY MATTERED.** Mode re-renders the report body by ASSIGNING HTML, and
  [scripts inserted that way never execute](https://developer.mozilla.org/en-US/docs/Web/API/Element/innerHTML#security_considerations).
  So a Refresh swaps in fresh placeholder markup and nothing runs to fill it. Every JS-side fix is
  unreachable code in this state, which is why the three fixes above each looked correct, tested clean and
  changed nothing the user could see. **Fix: the first execution installs a watchdog that outlives the
  markup** — `if (!window.__watchdog) window.__watchdog = setInterval(boot, 400)` — where `boot()` finds any
  root without the booted flag, re-hydrates from the current `window.datasets`, and boots it. Do this
  FIRST on any Mode port; the other four are real defects but this is the one that survives a Refresh.
- **Scope every CSS rule to the root** (the fragment shares Mode's DOM). Trap: a bare `*` selector scopes
  to `#root *`, NOT `#root`, so the reset silently skips the root element itself.

**Verify a port before shipping by replaying what Mode actually does, not what you assume it does.**
The jsdom harness used `host.innerHTML = frag` and then hand-recreated every `<script>` so it would run.
Both halves of that were wrong: Mode may append rather than replace, and it does NOT re-execute scripts.
The harness therefore passed clean through three rounds of a failure the user kept seeing (2026-09-03).
A test that encodes the same assumption as the code cannot fail the way the code fails. It now runs three
cases: first load with scripts executing, a re-render that APPENDS, and a re-render where scripts are
NOT re-created (the real Refresh path) — the last one must still render, via the watchdog.
[[feedback_test_must_not_share_code_assumption]]

**Gotchas that cost time:**
- **Mode parses Liquid tags (`{% %}` and `{{ }}`) INSIDE SQL `--` comments.** A stray `{% form %}` in a comment
  = a duplicate form-open tag → "query couldn't be parsed." Keep ALL form/param tags out of comments.
- **`window.datasets` = the LAST RUN, not the current SQL** (burned 2026-07-07: report rendered a draft-era
  dataset — 4 dead zero-spend groups — while the synced SQL correctly returned 21). A git deploy updates query
  definitions only; **always hit Run in the Mode UI after merging**, else the HTML renders stale data.
- **No Chart.js date adapter is loaded** (only `chart.umd.min.js`): a `type:"time"` scale throws "This method
  is not implemented: Check that a complete date adapter is provided." Use `type:"linear"` over epoch-ms + a
  month tick callback instead (CPD modules 03/11 pattern).
- **Mode injects the report HTML into the page TWICE** — `document.getElementById` may hit the hidden
  duplicate (charts draw invisibly, no error). Resolve elements via `root.querySelector` within your own
  section (burned 2026-07-07 on the 09rt mixed chart). (Refined 2026-09-03: the second copy is APPENDED,
  the first is not removed, and `getElementById` resolves to the STALE FIRST copy, not the visible one.
  De-duplicate the root on boot per the fragment-injection section above.)
- **Never start a commit message with `[Mode]`** (repo README — malfunctions the sync; that prefix is the bot's).
- **Attaching a pre-built Mode "Dataset" object to a report DISABLES JavaScript in the HTML embed.** Use plain
  queries and keep a `SELECT 1` query present to preserve scripting.
- Each dataset output must be < ~10MB. Pick the right DB per query (BigQuery vs core dw) or it fails.
- **mode-assets `main` is protected: PR + approval from ANOTHER engineer required** (no auto-merge; Alex Knorr /
  rkleck reviewed prior CPD PRs). But PRs are NOT the deploy path (edits don't sync into Mode) — avoid them;
  deploy via the REST API above (it replaced the paste-in-the-UI relay on 2026-07-16), then the report's
  Push-to-GitHub archives via the bot (bypasses the ruleset).
- **Query-backed select params:** the `{% form %}` must live IN the query whose result feeds it;
  `options: labels: <col> / values: <col>` (label shown, bare value substituted — consumers untouched).
  Dynamic options capped ~1,000/1MB. **QUOTE the default** (`default: '1900-01-01'`) — unquoted date-like
  YAML datifies, matches no option, and the param goes EMPTY.
- **An undefined/broken param substitutes as EMPTY STRING in every consumer query** — the whole report reads
  "no data" at once. Never remove a form block before its replacement exists; delete leftover queries whose
  forms define the same params.
- **Date params take only STATIC defaults** — dynamic defaults live in SQL sentinels (start 1900-01-01 ->
  Jan 1 of current year; end clamped to first-of-current-month so a far default = "last full month"); parse
  every date param as `DATE(LEFT(p, 10))` to survive serialization quirks.
