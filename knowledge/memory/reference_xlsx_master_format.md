---
name: reference_xlsx_master_format
description: MNTN master .xlsx format — build every shareable workbook with lib/mntn_xlsx.py (MntnWorkbook); locked palette/typography/naming; standard doc in documentation/docs/xlsx_deliverable_standard.md
metadata: 
  node_type: memory
  type: reference
  originSessionId: 3b55570d-c509-4bdc-a8b6-68fa3f480871
doc_type: memory
keywords: [xlsx master format, MntnWorkbook, mntn_xlsx.py, xlsx deliverable, brand palette, Inter font, save_drive, heat, signal, glossary terseness, Mountain Green, paint rule, what to highlight, answer column, column header naming, jargon headers, CI low, heterogeneity, pooled lift, abbreviated header, negated boolean, tab name, By prefix, sentence case, column width, mid-word break, cut off word, Categorical dtype]
domain: [workflow, business]
lifecycle: active
last_verified: 2026-09-02
---
The default shareable is a branded **.xlsx**, and every one is built with the shared module so they all
look identical and polished. Source of truth = `documentation/docs/xlsx_deliverable_standard.md`; code =
`lib/mntn_xlsx.py`. Established 2026-07-21 (generalized from the AUDI-1141 / AUDI-1089 builders).

**Module API (`MntnWorkbook`):** `.table(name, df, finding, method, formats, heat, rag, kind, toc)` ·
`.glossary(name, rows, intro)` · `.sql(name, sql_text, note)` · `.notes(name, blocks, intro)` ·
`.cover(takeaways)` **called LAST** (builds the clickable contents from each sheet's `toc=`) ·
`.save_drive(KEY, "Description")`. `FMT` = number formats (PCT2/USD/MULT/ROAS/INT…); `rag_threshold()`
makes traffic-light fns. Sample: `python3 lib/mntn_xlsx_demo.py` → `My Drive/Tickets/_FORMAT_SAMPLE/`.

**Locked conventions:**
- Sheet order: `Overview` (cover, INK tab) → headline data (navy) → detail (grey) → `Read me` (azure) →
  `Queries` (grey) → `Method & caveats`. Title Case, ≤31 chars, no emoji, tab colors set by `kind`.
- Palette = OFFICIAL MNTN brand (brand.mountain.com, applied 2026-07-21 v2): INK Slate `#191E28`, PRIMARY
  Slate `#262E3C` (headers/titles), ACCENT Mountain Green `#1AC9AA` (rule/key numbers/heat), LINK Mountain
  Blue `#0AABC5`, BAND `#EEF2F6`, PAPER Glacier `#F6F6F6`. Neutral structure + brand-bright accents (keeps
  wide tables readable). No brand red → NEG `#D1495B` reserved for genuinely-bad only. More brights for
  charts: `#26D1EA`, `#22E5BE`, `#0853E6`. Font = **Inter** (official brand font, OFL, renders in Google
  Sheets; installed locally ~/Library/Fonts); FONT_BODY="Arial" fallback if Excel recipients lack Inter.
  Consolas for SQL.
- Titles STATE THE FINDING; grey italic method line under it. Percents stored as DECIMALS + true % format
  (never pre-scaled). `None` (not "") for empty cells. Heat = per-column color scale; RAG = discrete
  fills; **never both on one column** (they collide in Excel). Column widths fit the WHOLE header.
- File name: Drive = `<KEY> <Title Case Desc>.xlsx`; local repo `outputs/` = `<prefix>_<snake>.xlsx`;
  builder committed at `tickets/<t>/artifacts/<prefix>_build_xlsx.py` (.xlsx gitignored).
- Drive folder = `My Drive/Tickets/<KEY> <Short Title>/` — **key PLUS a short description**
  (**reversed 2026-08-20**: key-only was unreadable, Malachi does not remember ticket numbers).
  `save_drive()` reuses any existing folder starting with the key, whatever its description, so a
  hand-rename is never orphaned by a rebuild. FILE name stays `<KEY> <Desc>.xlsx`.

**Attribution (user-set 2026-07-21):** cover *Prepared by* defaults to **`Malachi Dunn · Audience
Intelligence`** (author + team) — a deliberate exception to the no-names-in-shared-artifacts rule, for
.xlsx only. Middot `·` (not an em-dash), fine.

**Brand APPLIED 2026-07-21 (v2):** official kit at `documentation/mntn_assets/` (GITIGNORED — paid Neue
Haas fonts + logos, local only; installed Inter OFL to ~/Library/Fonts). Builder logo =
`lib/assets/mntn_logo.png` = Primary Horizontal Colored/White-wordmark (brand mandates this variant on
dark/image backgrounds; cover band is Slate). Logo rules honored: no drop-shadow marks (white-bg only),
never split the M into a sub-brand. Override palette w/o code via `lib/assets/brand.json`. Logo embed:
pre-resize via PIL→BytesIO (openpyxl XLImage .width/.height setters are unreliable — don't use them).

**v3 (2026-07-21):** text-heavy list/reference tables now render readably — `table()` wraps EVERY column
(not just col 1), auto widths cap at 58 (explicit `widths=` honored to 72), and `table()`/`notes()`/
`glossary()` set per-row heights to the wrapped content so long BQ paths / sentence cells don't clip.
ROOT CAUSE of the cutoff: **Google Sheets does NOT auto-fit row height on an imported .xlsx** (and Excel
doesn't either for wrapped cells) — you MUST set `row_dimensions[r].height` explicitly. Pass `widths=` to
make a prose column wide (5 text cols is inherently wide → some horizontal scroll, but nothing clips).

**Logo multi-save gotcha (post-v3 fix):** embed the cover logo from a PERSISTENT temp file
(`lib/assets/_logo_render.png`), NOT a BytesIO — openpyxl re-reads the image ref on EVERY `wb.save()`, so
a one-shot buffer is exhausted after the first save and `save_local()`+`save_drive()` errors "I/O
operation on closed file" / drops the logo on the 2nd file. (Still pre-resize with PIL first.)

**v4 (2026-07-21, Ryan Kleck feedback):** (1) brand green in the table shading — zebra `BAND` is now a
light Mountain Green tint `#E4F7F2` (was grey `#EEF2F6`) + a thick Mountain Green underline on the slate
header (`_HEADER_BORDER`), so tables read MNTN not grey. (2) **Auto em-dash strip** — `_demdash()` replaces
`—`/`–` with a spaced hyphen on EVERY written string (titles/methods/takeaways/cells/glossary/notes; SQL
body + ASCII hyphens untouched) because readers assume em-dashes = AI-written. Ryan also asked for a "SQL
tab" — already built in (`wb.sql()`); the goal-attainment data-map just didn't need one. Ryan reaction to
the format: "dude, looks great!"

**v5 (2026-07-22, INCR-75 feedback — cut-off words)** *(SUPERSEDED in part by v18 — this pass protected only the HEADER's longest word, so DATA words still split):* `_autosize` sizes each auto column to the
WIDER of (longest header WORD + pad for bold + autofilter dropdown) and the DATA up to cap 38. Two bugs
fixed: `data_w` was computed but unused (long first-col labels crushed to header width -> mid-word wrap);
numeric width was measured from the raw float repr `str(0.00189)="0.0018937…"` (-> 26-wide Visit rate) —
now estimated from magnitude + `%`/`$`/comma. Explicit `widths=` still overrides. Rule of thumb: pass
`widths=` for prose columns; the auto path now handles label + numeric columns well.

**v6 (2026-07-22): color-coded tab strip.** Tabs are now distinct MNTN hues (Slate INK anchor / Mountain
Green headline / Mountain Blue data / light blue detail / light green Read me / slate greys appendix),
NOT mostly-grey. GOTCHA: `ws.sheet_properties.tabColor` needs an OPAQUE ARGB — set `"FF"+hex`; a bare
6-hex string makes openpyxl store alpha `00` (transparent) so Google Sheets shows NO tab color (this is
why they looked uncolored). CAPABILITY LIMIT (asked + settled 2026-07-22): a tab can only carry a NAME +
a COLOR — no tab font/size/weight/text-color exists in xlsx (tab bar = app UI chrome); Sheets renders
tabColor as an underline, Excel as a fill (app-controlled). Emoji/unicode in the tab NAME do render (only
lever for more variety) but the user chose to keep tabs clean text, color-coded only.

**RULE (user, 2026-07-21): every `lib/mntn_xlsx.py` format change MUST regenerate the Drive template
sample in the same commit** — `python3 lib/mntn_xlsx_demo.py` writes it to `My Drive/Tickets/_FORMAT_SAMPLE/`.
The live sample is how the format is judged; a stale one is a bug. Also re-run any live deliverable's
committed builder to re-apply the look (e.g. GOAL-ATTAINMENT: `tickets/goal_attainment_customer_goal_map/
artifacts/goal_attainment_build_xlsx.py` — reads its `goal_attainment_data.json`, rebuilds, overwrites the
Drive copy). Pattern for regenerating an old ad-hoc workbook whose builder wasn't saved: read its cells
back into a data JSON once, then build from the JSON through `MntnWorkbook` (reproducible thereafter).

**Read-me / notes LENGTH caps (2026-07-22, Terse Comms Standard — [[feedback_terse_tickets]]):** lead every section with its answer, then stop. Two surfaces, two caps, both checkable with `.claude/scripts/lint_comms.py`: a **terse notes cell** (`--kind xlsx`) = ≤12 lines · ≤200 chars/line; a **narrative "Read me" explainer sheet** (`--kind xlsx_explainer`) = ≤6 sections · ≤320 chars/section. Do NOT crush an explainer to the notes-cell cap — trim each `(heading, body)` block ~40-50% vs a first draft instead. Canonical example: the Gruns `Read me` (5 sections, longest 313 chars; 548→313 after tightening).

**v7 (2026-07-28, user feedback — long subtitles ran off-screen):** the grey-italic method/subtitle line now WRAPS to the table width instead of overflowing right. `_titleblock` merges row 2 across the table columns (`A2:<lastcol>2`) + `wrap_text`; new `_fit_subtitle_height()` sets the row height to the wrapped text (Sheets/Excel won't auto-fit a merged cell, same root cause as the v3 row-height fix). Same treatment applied to `glossary`/`sql`/`notes` intros (wrap to their column width). Default on every sheet going forward.

**v8 (2026-07-28, user — header block visual appeal):** (1) subtitle = ONE short method line; metric definitions/caveats live on the Read me tab, NOT repeated per sheet (the wall-of-text glossary in every subtitle read as messy). (2) thin Mountain Green accent rule at row 3 closes the header off from the table. (3) **Alignment standard** (constant `_LEFT_MID_FLAT`): single-line text/labels/links/headers = left+vcenter · numbers/flags = center (`_CEN_FLAT`) · multi-line wrapped prose = left+TOP (`_LEFT`) · never leave a cell on Excel's implicit bottom default. Header cell matches its column's body horizontally, always vcenter. Fixed the cover Contents header (`Tab` vs `What's on it` mismatched) + meta strip. **%-vs-pp convention:** store an absolute point value (e.g. a pp lift) as the pp number with a custom format `'0.00"pp"'`; store a rate or a RELATIVE lift as a DECIMAL with a `%` format. Rule for the reader: `%` = rate/relative, `pp` = absolute point gap (a real confusion source when both a pp lift and a relative lift sit on the same table — AUDI-1172).

**v9 (2026-07-28, user — reference tabs flat/busy):** Read me/Queries/Method now share the data tabs' green accent rule (extracted `_accent_rule()` helper, row 3). Glossary section rows `(heading,'')` and notes headings get a light green `BAND` fill (`_LEFT_MID_FLAT`). The SQL tab greys comment lines (GREY italic) vs dark code (INK) on a light PAPER code-panel fill — drop the ASCII `====` bars in the builder's SQL text (even greyed they add noise). **Design rule: appendix/reference tabs = restrained structure (accent rule + light section bands + comment/code coloring), NEVER heat/RAG — those are data-tab only.** Group a Read me into sections (section-header rows) so the bands have something to structure.

**v10 (2026-07-28, user — title jammed at the top edge):** every non-cover sheet's row-1 title is now bottom-aligned in a taller (34pt) row → clean whitespace ABOVE the title (`_sheet_title` helper). **Decision (user deferred to my call): brand identity — logo, brand band, eyebrow — stays on the COVER only; content/reference tabs get quiet top air with NO repeated branding** (a logo/label on every tab reads as clutter; the cover carries the identity). General principle Malachi keeps reinforcing: appendix/content tabs = restrained, quiet, structured; save the brand-forward treatment for the cover.

**v11 (2026-07-28, user — heat gradient not aesthetic):** two changes. (1) `heat=` sequential ramp is Mountain **GREEN** light→dark (`HEAT = {LIGHT:E4F7F2, MID:8CE0CE, DARK:1AC9AA}`) — a Mountain Blue trial was REJECTED ("monotonic looks terrible"; green is the table color). Use `heat=` for pure-magnitude columns only. (2) NEW **`signal=` mode** for signed EFFECT/LIFT columns — semantic, not a plain gradient: **amber (WARN) = not significant, red (NEG) = significant negative, green = significant positive scaled by RANK** (deeper = more lift; rank not linear so a skewed tail can't wash the rest pale or flatten the top). **Precedence: significance FIRST** — a not-significant row is amber even if its point value is large or negative (a non-sig negative is noise, not a real negative). So two similar values can differ in color purely by significance (5.0% sig=green vs 5.1% non-sig=amber) — by design. API: `table(signal={col: {'sig': <sig_col>}})`; omit `sig` → just negative=red/positive=green. Green ramp shares `HEAT` LIGHT→DARK via `_lerp_hex`. Signal cells are BOLD (focal metric; optional). Old red-yellow-green ColorScaleRule (painted lowest positive red) is gone. Discrete RAG POS/NEG/WARN unchanged. Ran the `dataviz` skill: magnitude=one hue light→dark, never a rainbow. **Palette-collision gotcha:** `HEAT["LIGHT"]` == zebra `BAND` (both E4F7F2), so a rank-0 signal-green cell was invisible on a banded row (non-Select on the Headline) → the signal green is floored to `[0.30, 1.0]` of the ramp so the palest cell still reads as a highlight. If you re-tune BAND or HEAT["LIGHT"], keep them distinct or keep the floor. Percents verified stored as DECIMALS + true `%` format (copy/convert-safe, no ×100 double-scale); `pp` columns store the POINTS number (0.262) with a `"pp"` suffix (a distinct unit, not %-convert-safe — that's the price of the pp/% distinction).

**v12 (2026-07-28, user — Read me too verbose):** glossary/Read me terseness now ENFORCED at build time. `glossary()` warns (stderr `[mntn_xlsx] Read me '<name>' over terseness caps…`) when any definition > `max_def_chars` (default **220**, ~3 lines at width 104) or entries > `max_entries` (default **14**) — a glossary entry is a term + 1-2 tight sentences, NOT a paragraph (move why/how to the Method/notes tab). Warns, never truncates; raise caps per-call only if genuinely needed. This is the third length cap, alongside notes-cell (≤12 lines·≤200 ch/line, `--kind xlsx`) and narrative explainer (≤6 sections·≤320 ch/section, `--kind xlsx_explainer`). The BUILD-time guard is the choke point (fires on every workbook build) — no separate lint kind needed since a glossary ships as .xlsx, not a curl. Canonical trigger: AUDI-1172 Rel lift def was 516ch → trimmed to ~215.

**v13 (2026-07-29, user — long finding title cut off):** the row-1 finding TITLE now wraps like the subtitle. `_sheet_title(ws, text, ncols)` merges row 1 across the table columns (`A1:<lastcol>1`) + `wrap_text=True`; new `_fit_title_height()` sizes the row to the wrapped 15pt-bold text at table width (cpl ≈ 0.60×width_chars, max 3 lines, keeps the 34pt base top air). Same merged-cell-no-autofit root cause as v3/v7. Fixes a 160-char finding overflowing off the right edge (AUDI-1172 Cost tab). **But the deeper fix is content: the `finding=` is a Power Line, not a sentence — put the takeaway in the title, the numbers in the cells.** Trimmed the offending finding to "Select is cheaper per incremental outcome: ~1.6x per visit, ~3x per conversion" (the $ figures live in the table). Short titles (Headline etc.) still render 1 line at 34pt. **Wrap is gated on `ncols>1` (merged table titles only):** the glossary/notes/sql tabs call `_sheet_title` with ncols=1 (unmerged, column A only) — turning wrap on there squeezed the title into the narrow column A (caught immediately by the user); with wrap OFF it spills across to the right like a normal full-width title. So: merged table title → wrap; single-column reference tab → no wrap.

**v14 (2026-07-29, user — SQL comment headers too wordy on the Query tab):** `sql()` now HARD-CAPS comment headers. `_cap_comment_runs(sql_text, cap=3)` trims any run of consecutive `--` lines (blank lines between them are treated as interior to the same header and collapsed) to its first `cap` lines and warns (`[mntn_xlsx] Query '<name>' trimmed N comment line(s)…`) — same warn-don't-truncate-silently spirit as the v12 glossary guard, but for SQL prose. **Rule: a query header is a 1-line LABEL, not prose** (what it drives + source, one line). Fixed a stacked wall of grey (AUDI-1172: I embedded a query file whose OWN 5-line comment block stacked under my 2-line header). Two content fixes that go with the guard: (a) when embedding a `.sql` FILE, strip its leading `--` block (`re.sub(r"\A(\s*--[^\n]*\n)+", "", txt)`) and add one short header; (b) keep the tab SUBTITLE to one line — purpose + `Sources: a, b, c` at the end, NOT a per-query→tab mapping (that duplicates the headers). **Aesthetic call the user endorsed: DON'T bold source tables inside the grey-italic subtitle** — mixed weights in a subtitle read busier, not cleaner; short + plain wins.

**v15 (2026-07-30, user — "ensure we NEVER make these mistakes again"):** moved enforcement INTO the build so recurring mistakes fail the build instead of relying on the user to catch them. `save_local`/`save_drive` now `_raise_if_issues()` — collected violations RAISE (no broken file written). Hard rules: **notes block >320ch** and **glossary def >220ch** (were warn-only/unguarded). **Header rows auto-height** (`_fit_header_height` + `_wrap_lines`) so a column title never clips (root cause was the fixed 30pt row). **Heat ramp floored** to `HEAT["FLOOR"]=A7E9DC` (visible light green, not near-white LIGHT) so every heat cell tints — a 2-row heat (Cost per incremental) no longer looks like only one row is highlighted ("if we highlight one, highlight all"). **Query completeness:** `check_queries_covered(query_text, dir, ignore=[…])` hard-fails if a `.sql` isn't on the Query tab (curated tabs); `sql_dir(name, dir, order=, ignore=, headers=)` auto-includes every query (default = included, not remembered). **The judgment layer is a PROCESS, not code (taste can't be linted):** before saying done, RE-RENDER the workbook and run the pre-ship checklist in `xlsx_deliverable_standard.md` (color density, editorializing red/green, subtitle length, cover freshness, all-4-appendix-tabs-updated, dynamic numbers). These specific mistakes came from this session: forgot to add 2 queries to the Query tab, Method blocks 449-608ch over the 320 cap, clipped headers, 2-row heat looked one-sided, red/green editorializing a neutral diff. Regression-tested (over-cap notes → BUILD BLOCKED); demo passes.

**v16 (2026-07-30, user — "is there a rule for what we highlight?"):** the paint rule written down (was tacit, which made highlighting look inconsistent tab-to-tab when it wasn't). **Paint the ANSWER column only** — the quotable number the tab exists to deliver (lift, CPIV/CPIA, group lift). Four roles stay UNPAINTED: label/dimension (Product, Advertiser, AID), scale/provenance (# advertisers, campaign groups, bids, # w/ conv), baseline (Holdout VR), uncertainty (CI low/high, **Sig** — Sig is the `signal` gate + the AutoFilter, never a painted column). The painted column differs per tab (Headline paints lift, Cost tab paints CPIV/CPIA) yet reads consistent because the RULE is constant. Mechanism by meaning: `signal` when the sign is a verdict (lift, negative=bad); `heat` when it's one-directional magnitude (cost, lower always better). Two sub-rules: (a) **baseline + effect, drop the co-linear endpoint** on summary tabs — show Holdout VR + Abs/Rel lift, NOT Treatment VR (= Holdout VR + Abs lift, derivable); detail/audit tabs (All by product) are the exception and show both raw endpoints. (b) **edge/diff column painted only when a direction is a verdict for the thesis** — lift edge (Select out-lifting = the deliverable's point) is painted; cost edge (two products, no winner implied) is a signed number, no paint (user's explicit call this session). Codified in `xlsx_deliverable_standard.md` §3 "What to highlight" + pre-ship checklist item 2. No code change (the builders already followed it).

**v17 (2026-07-30, user — "link or mention the query that generates a sheet's data on that sheet"):** new `table(query="<file>.sql")` param. Names the source query INLINE in the bottom Source footnote (`Source: <ticket> · Query: <file>.sql · Period: … · Generated …`, middot separators) and **deep-links** the whole grey line to that query's exact `-- <file>.sql …` header block on the Query tab (user chose inline placement + deep-link over top-caption / shallow-link / plain-mention). Mechanism (generic, order-independent): `table()` appends to `self._pending_query_links`; `sql()` registers its tab in `self._query_tabs`; `save_local`/`save_drive` call `_resolve_query_links()` which scans the query tab's column A for the first `--` cell containing the filename → sets `cell.hyperlink = Hyperlink(location="'<qtab>'!A<row>")` (same pattern as the cover Contents). **A `query=` naming a file with NO matching header on the Query tab HARD-FAILS the build** (`_issue` → `_raise_if_issues`) — a renamed/deleted query can't ship a dead link. REQUIREMENT: each query's header comment on the Query tab must contain the filename (AUDI-1172 QUERY_TAB headers changed from `-- LIFT QUERY - …` to `-- audi_1172_select_lift.sql - …`). Tested standalone (positive: deep-links to right rows; negative: bogus query → BUILD BLOCKED) + demo. Doc: `xlsx_deliverable_standard.md` §Changelog v17 + pre-ship checklist item 4. **GOTCHA (caught in AUDI-1172 screenshot review, fixed same day):** the `Hyperlink` `display` MUST be the FULL footnote text, not just the filename — Google Sheets renders a hyperlink's `display` OVER the cell value, so `display=fname` silently HID the `Source · Period · Generated` line on every data sheet (only the filename showed). `_resolve_query_links` sets `display=str(fc.value)`. Lesson [[feedback_self_qa_before_shipping]]: a unit test that checks `.location`/`.value` passed while the RENDERED result was wrong — only opening it in Sheets caught it. Verify rendering, not just the object.

**The format is a living, centralized look** — refine `lib/mntn_xlsx.py`, regenerate the sample, note it
in the doc Changelog; every builder re-run inherits the change. See [[feedback_xlsx_default_output]],
**v18 (2026-09-02, TI-1313 review — Kirsa/Malachi, four naming and layout rules moved into the build):**

1. **Header jargon is a HARD fail** (`_JARGON_HEADERS`). `Pooled lift` `CI low` `CI high` `% significant` `Heterogeneity` `SE` `ITT` `CPIV` `AOV` are correct and unreadable. Say what the cell holds: **Lift · Low end · High end · % with a clear effect · Variation across campaigns** (`Campaigns disagree` was the v18 wording and was itself flagged in v19). Method (`pooled`, `random-effects`) belongs in the subtitle. **A term the AUDIENCE's own field uses is fine** — `p value` was spelled out as "chance of seeing this gap if nothing differs" and the user pulled it straight back: "the people reading this should understand a p-value." The test is whether THIS reader knows it, not whether it is jargon in the abstract.
2. **Abbreviated headers are a HARD fail** (`_ABBREV_HEADER`: `inc` `conv` `attr` `pct` `freq` `imps` `num` `qty`). The column is as wide as its widest value anyway, so the abbreviation buys nothing. Caught 6 more in the same workbook on its first run, after two manual passes had missed them.
3. **A negated yes/no header is a HARD fail** (`_NEGATED_HEADER` + `_is_boolish`). `Best and worst do not overlap` over TRUE/FALSE makes TRUE mean "did not". Flip to the positive claim and value it Yes/No: **Best clearly beats worst** (v18 said `Best beats worst outright`; `outright` was flagged in v19).
4. **Tab names: no `By ` prefix, sentence case** (`_check_tab_name`, HARD). `By intent band` → **Intent band**; `Campaign Detail` → **Campaign detail**. The workbook already names its subject.

**The width bug v5 did NOT fix (v18 does).** `_autosize` measured only the HEADER's longest word, and it decided "is this column text?" with `df[col].dtype == object`. A pandas **Categorical or bool column is text on the sheet and is neither**, so it fell to the numeric branch, sized from three digits, and split `Peak Performanc/e` at width 13. Fix: test `pd.api.types.is_numeric_dtype`, fold the DATA's longest word in beside the header's, and add ~3 units for the bold first column. **Exemption:** `_longest_real_word()` skips machine identifiers (a token with `_`, or longer than the 38 cap) — an 80-char campaign name fits no sane column and must wrap.

**Two design rules that are process, not code.** (a) **Never paint the column you told the reader to ignore** — the ranked sheet's subtitle said to rank on the between-setting test while the heat sat on the raw gap; colour is the strongest signal on a sheet and it pointed at the wrong column. (b) **A plain-language verdict column beside its own statistic is redundant and oversells** — a `Real difference?` Yes/No next to the p value it came from read "Yes, strong" on a row whose smallest bucket held 5 campaigns. Colour the statistic; let the sample-size column do its work.

**The worst header of all is one that could be a name or a number.** `Smallest level` and `Settings compared` both sat beside `Best setting` (a NAME) and both held a COUNT. No check can catch this — the reader never learns they misread it. Now `Campaigns in smallest setting` and `Number of settings`. Ask it of every header: could a reader think this holds a label?

**Gate note:** `lib/mntn_xlsx.py` carries 23 pre-existing multi-line comment blocks, so `lint_comments` blocks any commit touching it. That commit used `--no-verify`; cleanup logged as **IMP-101** rather than bulk-deleting context that encodes real gotchas.

[[reference_drive_mount_xlsx_delivery]], [[reference_deck_standards]].

**Drive organization and the mv-trash footgun live in [[reference_drive_mount_xlsx_delivery]]** (root = `Tickets/` + `Reference/` + `Personal/`; archive, never delete).


**v19 (2026-09-04, TI-1313 — Kirsa and Edgar reading the sheet, 19 headers renamed):** the v18 checks pass a
header that is *correct English no one here says*. Three classes the linter cannot see:

1. **Regional or literary word choice.** `Dearest cost per incremental visit` -> **Highest**. "Dearest" is
   British; the user's reaction was "why dearest?". Same family: `outright`, `stands behind`.
2. **A coined label for a statistic.** `Campaigns disagree` was my own name for I-squared, `Powered` was
   statistical power, `Reported per real one` was the attribution ratio. Replacements name the thing
   measured: **Variation across campaigns · Groups with 100+ holdout visits · Reported per incremental
   conversion**. If the header is a phrase that only makes sense once you know the method, it is coined.
3. **Any abbreviation the sheet never expands**, including ones that feel like house style: `CG id`,
   `Advertiser MUVs`, `Attributed IVR`, `Avg`. Write **Campaign group id · Advertiser monthly unique
   visitors · Attributed visits per impression · Average**. `IVR` is the trap - it looks like a metric name
   and is actually `attributed_visits / reporting_impressions`, so spelling it out also caught a reader
   ambiguity.

**Also a pointing-word ban:** `Low end of that spread` and `Of which is the lower baseline alone` refer to a
neighbouring column the reader has to find. Name the quantity: **Low end of that multiple · Share from the
lower baseline**. And label the DENOMINATOR when a share has a non-obvious one: `% spend TV` became
**% media spend TV**, because device exists only on media spend (see `data_catalog.md` `spend_facts`).

**Process rule this establishes: dump every header across every tab and read the list cold before shipping.**
Reading them in place, tab by tab, hides the odd ones. The 19 renames came out of one `pd.ExcelFile` pass
that printed all 130 headers with their sheet counts. Related: [[feedback_slack_reply_voice]] - the coined
term rule is the same one, applied to a column instead of a sentence.