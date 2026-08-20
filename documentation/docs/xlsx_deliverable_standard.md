# MNTN .xlsx Deliverable Standard

**The default MNTN analysis deliverable is a branded `.xlsx` workbook** — not a deck, not a markdown
file. Build every one with `lib/mntn_xlsx.py` so they all read as one polished system. Read this before
building any shareable spreadsheet.

> People judge the work on the *perception of effort*, not the volume. A workbook that is clearly,
> deliberately designed — coloring, spacing, typography, borders, a branded cover, a clickable index —
> lands as "this person cares." That perception is the job of this standard. The numbers stay fully
> auditable underneath it.

---

## 1. When to produce an .xlsx (the default)

| Ask | Deliverable |
|-----|-------------|
| "analyze X / value Y / evaluate Z / scorecard / by-vertical / vendor eval" | **`.xlsx`** (this standard) |
| "make a deck / slides / presentation / a Loom" | RevealJS deck (`revealjs_guide.md`) |
| "just the numbers / a quick markdown / drop it in the ticket" | markdown / Jira comment |

Unless the user explicitly asks for a deck or a markdown file, **assume the shareable is an `.xlsx`.**
The audiences here (finance/billing, eng, RevOps) pull numbers apart cell-by-cell and re-run the SQL —
a spreadsheet is their native workflow. A deck is extra work that is usually not wanted.

---

## 2. The builder — `lib/mntn_xlsx.py`

One import, one look. Never hand-roll fills/fonts/borders again; the module owns the look so refinements
propagate to every future workbook.

```python
import sys, os
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import MntnWorkbook, FMT, rag_threshold

wb = MntnWorkbook(
    title="MM vs 3P Segment Scorecard",
    ticket="AUDI-1141",
    subtitle="Prospecting performance by vertical — trailing 6 months",
    period="Jan-Jun 2026",
    generated="2026-07-21",          # pass a date string for reproducible files
    logo_path=None,                  # PNG; falls back to an "MNTN" wordmark when None
)

wb.table("MM vs 3P by vertical", df,
         finding="MNTN Matched leads visit rate in every vertical",   # STATE THE FINDING
         method="Advertiser-weighted medians; prospecting only; visits = views + clicks.",
         formats={"MM IVR": FMT.PCT2, "3P IVR": FMT.PCT2, "IVR advantage": FMT.MULT,
                  "MM CPV": FMT.USD, "MM ROAS": FMT.ROAS},
         heat={"MM IVR": "high", "3P IVR": "high", "MM CPV": "low"},   # per-column color scale
         rag={"IVR advantage": rag_threshold(good_above=4.0, bad_below=2.0)},
         kind="headline", toc="The headline — MM vs 3P by vertical")

wb.glossary("Read me", intro="How to read this workbook.", rows=[("IVR", "Visit rate ..."), ...])
wb.sql("Queries", open("queries/....sql").read(), note="BigQuery SQL used to produce these numbers.")
wb.notes("Method & caveats", blocks=[("ROAS is directional", "Prospecting, last-touch ...")])

wb.cover(takeaways=["…one-line finding…", "…", "…"])   # CALL LAST — builds the clickable contents
wb.save_drive("AUDI-1141", "MM vs 3P Scorecard")        # -> My Drive/Tickets/AUDI-1141/AUDI-1141 MM vs 3P Scorecard.xlsx
```

**Open the reference sample** to see the target look: run `python3 lib/mntn_xlsx_demo.py`, or open
`My Drive/Tickets/_FORMAT_SAMPLE/_FORMAT_SAMPLE MNTN xlsx Format Sample.xlsx`.

### Sheet-builder methods
| Method | Produces | Tab color |
|--------|----------|-----------|
| `cover(takeaways, name="Overview")` | Branded title sheet: logo band, title, meta strip, Rule-of-Three takeaways, **clickable contents** (auto-built from every sheet's `toc=`). Call **last**. | INK (deep navy) |
| `table(name, df, finding, method, formats, heat, rag, kind, …)` | A styled data sheet: finding-led title + grey method line + navy banded table + frozen panes + autofilter + footnote. `kind="headline"` for the money sheet. | navy / grey |
| `glossary(name, rows, intro)` | Two-column term/definition "Read me". A `("Header","")` row is a bold sub-head; `("","")` is a spacer. | azure |
| `sql(name, sql_text, note)` | The SQL behind the numbers, monospaced. | light grey |
| `notes(name, blocks, intro)` | Long-form method & caveats, `(heading, body)` blocks. | light grey |

**Cover takeaways — guideline, warns but does not block.** Three (Rule of Three; a 4th is silently dropped), each **≤160 chars**, each leading with its number. Range across the reference workbooks: AUDI-1204 89–96, AUDI-1172 104–155. `cover()` prints to stderr past either bound. Deliberately softer than the subtitle caps: a long takeaway is a judgment call, a 382-char method line is not.

### Plain language and fact-only annotations (applies to every artifact, not just xlsx)

Two rules from global `CLAUDE.md` §9, restated here because workbooks break them most often:

- **Never use internal vocabulary this workbook doesn't define.** A constant name, tier label, function or script name, or internal column name cannot appear in reader-facing text unless a tab defines it. If the reader would have to open a source file to decode the word, it is a variable, not a word. Real misses: *"above the 12% saturation band"* (INCR-75's `IVR_SATURATED`) and *"Ceiling is Mid tier"* on a cover, in a workbook with no tier column.
- **A `Note` column carries fact only** — composition ("36,965 visiting of 285,909 served IPs"), a benchmark ("cohort median $27.54"), or a unit qualifier ("both arms, 10% holdout"). If the label already says it, leave the Note empty. Interpretation goes on Method & caveats where it has room to be justified.

### Column headers must read without the Read me, and derived groups carry their range

Two failure modes caught in review on AUDI-1210 (2026-08-19), both of which passed every existing check because the terms *were* defined on the Read me:

- **A header that only makes sense after a lookup is a variable, not a word.** `Rank vs peers` and `Reading` both had correct Read me entries and still stopped the reader. Rewrite the header as the sentence the number answers: `Reading` → **Tracking history**, `Tier` → **Test readiness**, `Score` → **Candidate score**. If you cannot name a column without a definition, the column is doing two jobs.
- **Give the reader the comparison, not the rank.** `Rank vs peers` became `Similar sites we beat` and was still wrong twice over: "beat" implied MNTN won a contest when the number is a rank on one ratio, and a percentile makes the reader do arithmetic to recover the thing they actually want. **Put the benchmark in its own column and let the row read straight across** — `Share of site visits 0.02%` · `Typical for this size 0.61%` · `Compared to sites with: Over 1.4M visits`. Three plain numbers beat one clever one. Never name a column with a verb that implies winning, losing, or beating.
- **Check whether the distribution even supports a rank.** Inside the largest site-size band the share ran p10 0.0036% · p25 0.0297% · **p50 0.6075%** — a 20x break between the 25th and 50th percentile. That is two populations, not a gradient, so "bottom quartile" reads as *slightly behind* when the truth is *different cluster*. Look at the quantiles before shipping a percentile; if it breaks, show the median and say so on Method & caveats.
- **Never label a derived group by its ordinal position.** `Smallest fifth / Second fifth / Fourth fifth / Largest fifth` tells the reader nothing about what the group *is*, and "fourth fifth" is actively hard to parse. Carry the actual range instead. Same rule for score bands, spend tiers and date buckets.
- **A range carries its unit, in the header or in the value — never neither.** The first fix above produced `Under 25K · 350K to 1.4M · Over 1.4M`, and the immediate question back was *"over 1.4M what?"*. Landed as header **Compared to sites with** + values **`Under 25K visits` … `Over 1.4M visits`**. Put the unit once where it reads naturally: on the header when every value shares it and repetition would be noise, on the values when the header is already carrying a different job.

Five tests before shipping, all cheap:
1. Read each header aloud with no other context. If "what would this cell contain?" is a guess, rename it.
2. Read one cell value aloud on its own. If it prompts "…of what?", the unit is missing.
3. If a value names a bucket, ask whether the reader can tell *why this row landed there*. Ordinal labels always fail this; ranges always pass it.
4. For any column holding a rank, percentile, index or score: can you replace it with the raw number plus its benchmark in the next column? If yes, do that.
5. Read one full row aloud as a sentence. If you have to jump columns or do mental arithmetic to reach the point, the columns are in the wrong order or one of them is doing too much.

**Partly enforced.** `MntnWorkbook.table()` raises on ordinal-position group labels (`fourth fifth`, `third quintile`, `2nd decile`) and on placeholder headers (`Reading`, `Value`, `Status`, `Category`, `Group`, `Type`, `Rank`, `Score`, `Band`, `Tier`, `Metric`, `Result`, `Flag`, `Notes`). Tests 2, 4 and 5 are on you — no check can see that `Over 1.4M` is missing a noun, or that a percentile would read better as a benchmark.

### Tab title + method subtitle caps (HARD — the build refuses to write the file)

**`finding` ≤ 125 chars · `method` ≤ 200 chars.** Enforced in `MntnWorkbook.table()` via `_check_titleblock`; over-cap raises at `save_*()` and no file is produced. Derived from **AUDI-1172 `Select vs Non-Select Incrementality.xlsx`, the hand-edited reference workbook** (finding 72–122, method 91–192) and capped just above its longest.

**The pattern that makes ~130 chars enough: state the basis, then delegate.** Almost every 1172 method line ends `"... See Read me for definitions."` or `"... See Read me / Method for the formula."` The subtitle says what the numbers are and on what basis; the *why*, the formula, and the caveats live on the **Read me** and **Method & caveats** tabs. Two consequences worth stating plainly:

- A method line that needs >200 chars is a signal you are missing a **Read me** tab, not that the cap is wrong. Add `glossary()` and delegate.
- Do **not** over-correct to a bare 70 chars either. 1172's median method line is ~130 and carries real content (grain, window, weighting). Terse is not the same as uninformative.

`finding` states the answer **with its number** ("Select prospecting drives ~5x the relative visit lift of non-Select (+23% vs +5%)"), not the topic.

**Why this is a hard fail and not a lint rule:** these two fields went uncapped until 2026-08-12 and practice drifted to a 382-char method line, because the only guidance was one short example in this doc and nothing ever checked. A doc rule that is not enforced decays; a build that will not produce a file cannot be ignored.

**Read-me / notes length caps (Terse Comms Standard — global `CLAUDE.md §9`).** Lead every section with its answer, then stop. Two surfaces, two caps, both checkable with `.claude/scripts/lint_comms.py`:
- **Terse notes cell** (`--kind xlsx`): ≤12 lines, ≤200 chars/line — clipped facts, one per line.
- **Narrative "Read me" / "Method & caveats" sheet** (`--kind xlsx_explainer`): ≤10 sections, ≤340 chars/section — plain-English prose, but each `(heading, body)` block leads with the answer. Do NOT compress an explainer to the notes-cell cap; do trim each block ~40-50% versus a first draft. Caps track **AUDI-1172**, the reference workbook: 9 sections, longest 330 chars. (The doc previously said ≤6 while the code enforced 7, and both were tighter than the reference.)

---

## 3. Visual system (locked)

### Palette — `BRAND` dict in `lib/mntn_xlsx.py` (official MNTN brand, brand.mountain.com 2025)
| Token | Hex | MNTN name | Use |
|-------|-----|-----------|-----|
| `INK` | `#191E28` | Slate Grey | Cover band bg, footer, dark copy |
| `PRIMARY` | `#262E3C` | Slate Grey | Table header fills, finding titles, row labels |
| `ACCENT` | `#1AC9AA` | Mountain Green | Cover rule, key numbers, takeaway ticks, positive heat |
| `LINK` | `#0AABC5` | Mountain Blue | Contents hyperlinks |
| `BAND` | `#E4F7F2` | light Mtn Green | Zebra band on data rows (brand-tinted, not grey) |
| `PAPER` | `#F6F6F6` | Glacier White | Off-white fill / neutral heat start |
| `GREY` / `MUTE` | `#5C6675` / `#98A2B3` | Slate | Method subtitles / footnotes |
| `LINE` | `#DCE3EA` | — | Thin cell borders |
| `POS` / `NEG` / `WARN` | `#1AC9AA` / `#D1495B` / `#E9A23B` | Mtn Green / — / — | RAG traffic-lights & deltas |

The **structure is neutral (Slate Grey), the brand brights are accents** — the logo, the green rule, the
green key numbers/heat, the blue links. That's the sophisticated read of a bright brand: it keeps wide
data tables readable while still being unmistakably MNTN. (MNTN has no brand red, so `NEG` is a reserved
tasteful red for genuinely-bad values only.) Other official brights available for charts: Mountain Blue
`#26D1EA`, Mountain Green `#22E5BE`, Pacific Blue `#0853E6` / `#0E44BF`.

**Brand assets (in place as of v2).** The official kit lives at `documentation/mntn_assets/` (gitignored —
it holds paid Neue Haas fonts + logos, local only). The builder's logo is
`lib/assets/mntn_logo.png` = the **Primary Horizontal Colored / White-wordmark** mark, which brand
guidelines mandate for dark/image backgrounds (the cover band is Slate Grey). To override the palette
without code, drop `lib/assets/brand.json` (e.g. `{"PRIMARY": "…", "ACCENT": "…"}`). See
`lib/assets/README.md`. Logo rules honored: no drop-shadow marks (white-bg only), the "M" symbol is never
split into a sub-brand.

**Attribution** — the cover's *Prepared by* defaults to **`Malachi Dunn · Audience Intelligence`**
(author + team). Override per-workbook with `owner=`. This is the deliberate exception to the
"no names in shared artifacts" rule, made by the user for `.xlsx` deliverables.

- **Red reads as "bad" to execs** — reserve `NEG` for genuinely-bad values only, never for emphasis.
- Heat = a per-column color scale (`"high"` green-is-large, `"low"` green-is-small e.g. cost,
  `"neutral"` white→navy). RAG = discrete traffic-light fills. **Never put `heat` and `rag` on the same
  column** — a color-scale and a manual fill collide in Excel.

### What to highlight (the paint rule)

**Paint the answer, not the arithmetic.** Color lands only on the column(s) the tab exists to deliver — the
number a reader would quote. Every other column stays unpainted. This is why the painted columns differ tab
to tab (Headline paints lift, the cost tab paints CPIV/CPIA) yet the workbook still reads as consistent: the
*rule* is constant even though the *answer* changes. Five column roles, one gets color:

| Role | Examples | Paint |
|---|---|---|
| **Answer** — what the tab is for; the quotable number | lift (abs/rel), CPIV, CPIA, group lift | **Yes** |
| **Label / dimension** — the row's identity | Product, Advertiser, AID, Group | No |
| **Scale / provenance** — sizes & lets you trust the answer | # advertisers, campaign groups, treated/holdout bids, # w/ conv | No |
| **Baseline** — anchors the answer but isn't it | Holdout VR | No |
| **Uncertainty** — qualifies the answer | CI low/high, Sig | No paint; Sig is the `signal` *gate* + the AutoFilter |

- **Mechanism by meaning:** `signal` when the *sign* is a verdict (lift — negative is bad) → sig-gated
  red/green; `heat` when it's pure magnitude with one better direction (cost — lower always better) → ramp.
- **Baseline + effect, not both endpoints (summary tabs).** Show the baseline (Holdout VR) + the effect
  (Abs/Rel lift) and drop the third co-linear value (Treatment VR = Holdout VR + Abs lift — the reader
  derives it). **Detail/audit tabs are the exception** — show both raw endpoints (Treated VR *and* Holdout
  VR) because that tab is the receipts.
- **Difference / "edge" columns:** paint only when one direction is a verdict *for the deliverable's
  thesis* (a lift edge where Select-wins = good → paint). If it's just "two things differ" with no winner
  implied (a cost edge across two products), leave it a signed number, no paint.

### Typography
- **Inter** everywhere — the official MNTN body/UI font (open-license/OFL), and it renders natively in
  Google Sheets, which is the actual delivery surface. **Consolas/Menlo** for SQL. Set `FONT_BODY =
  "Arial"` in the module only if recipients open in desktop Excel without Inter installed and you need
  guaranteed-identical column metrics. (Neue Haas Grotesk is the brand *headline* font but is paid/not in
  Sheets, so we standardize on Inter for both.)
- Cover title 24 · finding title 15 bold slate · method subtitle 10 italic grey · header 11 bold white ·
  body 10 · footnote 9 grey.

### Number formatting (auditable)
- **Store rates as DECIMALS** (`0.0046`) with a true `%` format (`FMT.PCT2` → `0.46%`). **Never**
  pre-scale to `0.46` and add a fake `"%"` suffix — Excel/Sheets must recognize them as real percents so
  they survive a re-type.
- Money `FMT.USD`/`USD0`, multiples `FMT.MULT` (`4.1x`) / `FMT.ROAS` (`3.40x`), counts `FMT.INT`.
- Use `None` (not `""`) for truly-empty cells so an adjacent long label can overflow.

### Layout
- Gridlines **off**, frozen header, autofilter, content-fit columns (width fits the **whole** header +
  padding for the bold text and the filter dropdown; keep headers ≤ 2 words — the tab title says which
  group). Landscape, fit-to-width print setup. Clean workbook metadata (title/creator/keywords).

---

## 4. Naming (locked)

### Sheets / tabs
- Sheet 1 is always **`Overview`** (the cover).
- Then headline data → supporting/detail → **`Read me`** → **`Queries`** → **`Method & caveats`**.
- Title Case, ≤ 31 chars, **no emoji**. Tab colors are set by `kind`/method (a legend in the tab strip).
- **A tab can only carry a name + a color** — the `.xlsx` format has no tab font/size/weight/text-color,
  so those can't be styled (the tab bar is app UI chrome). Google Sheets renders `tabColor` as an
  underline, Excel as a fill; that's app-controlled, not file-controlled. Emoji/unicode in the tab *name*
  do render (the only lever for more per-tab variety) but we keep tabs clean text, color-coded only.

### File name
- **Drive (shareable):** `<KEY> <Title Case Description>.xlsx` — e.g. `AUDI-1141 MM vs 3P Scorecard.xlsx`.
- **Local repo (gitignored `outputs/`):** `<prefix>_<snake_description>.xlsx` — e.g.
  `audi_1141_mm_vs_3p_scorecard.xlsx`.
- **Builder (committed):** `tickets/<ticket>/artifacts/<prefix>_build_xlsx.py`.

### Drive folder
- **`My Drive/Tickets/<KEY>/` — ticket key ONLY** (e.g. `AUDI-1141/`), matching recent folders. Create it
  if missing; `save_drive()` does this for you. (Older folders carry a description suffix — that has
  drifted; new folders are key-only.)

---

## 5. Workflow

1. **Analysis** → save results to the ticket's `outputs/*.csv` (data comes from CSVs, never hardcoded).
2. **Write** `tickets/<ticket>/artifacts/<prefix>_build_xlsx.py` importing `lib.mntn_xlsx`.
3. **Build** sheets — `table` / `glossary` / `sql` / `notes` — then `cover()` **last**.
4. **`save_drive(KEY, "Description")`** — writes straight into the mounted Drive (auto-syncs, no upload).
5. **Verify** — reopen with `openpyxl.load_workbook`, check the sheet list + a footing row (`save_drive`
   returns the path; the demo shows the pattern).
6. **Reference** the Drive path in the ticket `summary.md` (Drive files aren't committed to git).
7. **Commit** the builder `.py` (the `.xlsx` output is gitignored).

**Editing an existing .xlsx in place:** openpyxl round-trips data + basic formatting, but can drop native
Excel charts / pivots / macros / some conditional formatting on save. If the file has those, work on a
copy and flag it. Prefer regenerating from the builder — the builder is the source of truth.

**Gotchas:** don't have the file open in Excel/Sheets while writing (Drive spawns a conflict copy). A
native Google Sheet is a `.gsheet` pointer, not a real file — keep the source as `.xlsx`.

---

## 6. Governing the format itself (the "make it beautiful" loop)

This standard is a **living format**. The look lives in one place (`lib/mntn_xlsx.py`), so we refine it
over time — pulling in what worked from past workbooks (AUDI-1089 heat scales, AUDI-1141 read-me + column
sizing) and reacting to how shared files actually land. When we change the look:

1. Edit `lib/mntn_xlsx.py` (palette, a sheet method, spacing).
2. **Regenerate the Drive template sample — REQUIRED, never skip.** Run `python3 lib/mntn_xlsx_demo.py`;
   it writes the reference workbook to **`My Drive/Tickets/_FORMAT_SAMPLE/`**. Eyeball it. **The live
   template sample must always reflect the current format** — every `lib/mntn_xlsx.py` change is followed
   by this regen in the same commit (user rule, 2026-07-21). The Drive sample is how the format is judged;
   a stale sample is a bug.
3. Note the change in the Changelog below and commit.
4. Regenerate any live deliverables that should carry the new look (each has a committed builder — e.g.
   `tickets/goal_attainment_customer_goal_map/artifacts/goal_attainment_build_xlsx.py`; re-running it
   re-applies the standard and overwrites the Drive copy).

Every existing builder re-run picks up the new look automatically. That is the point of centralizing it.

### Changelog
- **2026-07-30 · v17** — `table(query="<file>.sql")` names each sheet's source query inline in the bottom
  Source line and **deep-links** it to that query's block on the Query tab. The Source footnote becomes
  `Source: <ticket> · Query: <file>.sql · Period: … · Generated …` (middot separators) and the whole grey
  line is clickable, jumping to the exact `-- <file>.sql …` header (not just the tab top). Mechanism:
  `table()` registers a pending link; `save_*` runs `_resolve_query_links()`, which scans the Query tab for
  the header naming that file and sets the hyperlink. **A `query=` naming a file with no matching header on
  the Query tab HARD-FAILS the build** (so a renamed/missing query can't ship a dead reference — serves the
  "not missing anything" criterion). For the link to resolve, each query's header comment on the Query tab
  must contain the filename (e.g. `-- audi_1172_cost.sql - drives Cost by advertiser`). GOTCHA: the
  hyperlink `display` must be the FULL footnote text — Google Sheets renders a hyperlink's `display` over
  the cell value, so `display=<filename>` alone hides the Source/Period/Generated line (fixed post-review).
- **2026-07-30 · v16** — The paint rule written down (see "What to highlight" in §3). Color lands on the
  **answer** column only — the quotable number the tab exists to deliver; labels, scale/provenance,
  baseline, and uncertainty stay unpainted. `signal` for signed verdicts (lift), `heat` for one-directional
  magnitude (cost). Sub-rules: summary tabs show baseline + effect and drop the co-linear endpoint
  (Holdout VR yes, Treatment VR no; detail/audit tabs show both); an edge/diff column is painted only when a
  direction is a verdict for the thesis (lift edge yes, cost edge no). Codifies the previously-tacit choice
  that made highlighting look inconsistent across tabs when it wasn't.
- **2026-07-30 · v15** — Build-time ENFORCEMENT (mistakes fail the build, not the reviewer). `save_local`/
  `save_drive` now RAISE on collected rule violations, so a broken workbook is never written: **notes block >
  320 chars** and **glossary def > 220 chars** are hard fails (were warn-only / unguarded). **Header rows
  auto-height** to their wrapped text (`_fit_header_height`) so a column title can never clip — the old fixed
  30pt row was the root cause. **Heat ramps are floored** to a visible light green (`HEAT["FLOOR"]`), so every
  heat cell reads as a tint (a 2-row heat no longer looks like only one row is highlighted). **Query-tab
  completeness:** `check_queries_covered(query_text, dir, ignore=[…])` hard-fails if a `.sql` file isn't on the
  Query tab; `sql_dir(name, dir, …)` auto-includes every query so a new one can't be forgotten. **The
  judgment-call layer is a process, not code:** before declaring a workbook done, RE-RENDER it and scan the
  pre-ship checklist below — color density, editorializing, subtitle length, cover freshness — because taste
  can't be linted. Ship after that pass, not after the user catches it.

#### Pre-ship checklist (run every time, before saying "done")
1. **Open the rebuilt file in Google Sheets (the delivery surface) / read each tab as the recipient sees it.**
   Don't ship a tab you haven't looked at rendered. A passing unit test on the object model is NOT proof:
   AUDI-1172's footer hyperlink passed its `.location`/`.value` test but Sheets rendered the hyperlink
   `display` over the value, hiding the Source line — only opening it in Sheets caught it.
2. **Color is signal, not decoration.** Heat/gradient on summaries (few rows); plain on many-row lookup tables.
   Diverging red/green ONLY where one direction is genuinely good/bad — a neutral two-product diff is a signed
   number, not red/green. **Paint the answer column only** (§3 "What to highlight"): labels, scale/provenance,
   baseline, and CI/Sig stay unpainted; Sig is the gate, not a painted column.
3. **Titles/subtitles:** finding = a Power Line; subtitle = ONE line (definitions live on Read me / Method).
4. **Overview + Read me + Method + Query all updated for every new tab** — a takeaway, a glossary row, a caveat,
   and the query. Result numbers are f-string DYNAMIC (never hardcoded — they drift as data accumulates).
   **Every data sheet passes `query="<file>.sql"`** so its Source line names + deep-links the query that built
   it (build hard-fails if the file isn't on the Query tab).
5. **Rebuild is idempotent + clean:** no `BUILD BLOCKED`, no terseness warnings.
6. **No person-names in any cell.** They slip into Method/notes blocks (AUDI-1172 shipped "Matt Brorby
   confirmed", caught in review). Shared deliverables name no people; put the who in the ticket, not the sheet.
- **2026-07-29 · v14** — SQL comment headers hard-capped. `sql()` trims any run of `--` comment lines
  (blank-separated blocks merged) to `max_comment_run` (default 3) and warns, so the Query tab never
  becomes a wall of grey. A query header is a 1-line label (what it drives + source), not prose. When
  embedding a `.sql` file, strip its own leading comment block first; keep the tab subtitle to one line.
- **2026-07-29 · v13** — Finding TITLE wraps to the table width. Row 1 is now merged + `wrap_text` across
  the table columns with `_fit_title_height()` sizing it (Excel won't auto-fit a merged cell), so a long
  `finding=` wraps in place instead of running off the right edge. The deeper rule stands: `finding=` is a
  Power Line (the takeaway), not a sentence — put numbers in the cells, not the title.
- **2026-07-28 · v12** — Read me / glossary terseness guard. A glossary entry is a term + 1-2 tight
  sentences, not a paragraph. `glossary()` now warns at BUILD time (stderr) when any definition exceeds
  `max_def_chars` (default 220, ~3 lines) or the sheet exceeds `max_entries` (default 14), so a Read me
  can't silently sprawl into prose — move why/how reasoning to the Method tab. Warns, never truncates;
  raise the caps per-call if a deliverable genuinely needs it. Trimmed the AUDI-1172 Read me (Rel lift was
  516ch). Glossary cap sits alongside the existing notes-cell (≤12 lines · ≤200 chars/line) and narrative
  explainer (≤10 sections · ≤340 chars/section) caps.
- **2026-07-28 · v11** — Heat/scaling colors reworked. (a) The magnitude `heat=` ramp is a SEQUENTIAL
  single-hue Mountain **Green** ramp light→dark (a brief Mountain Blue trial was rejected — green is the
  table color). (b) NEW `signal=` mode for signed EFFECT/LIFT columns — semantic, not a plain gradient:
  **amber = not significant, red = significant negative, green (deeper = more lift) = significant
  positive.** This replaces the old red-yellow-green ColorScaleRule, which anchored on the column min/max
  and painted the *lowest positive* value red (a +104% lift looked "bad"). The green is **rank-scaled**
  (not linear) so a skewed tail can't wash the column pale or flatten the top. Precedence: a not-significant
  negative reads amber (noise), not red. API: `table(signal={col: {'sig': <sig column>}})`; omit `sig` →
  just negative=red / positive=green. Discrete RAG pass/fail fills unchanged. Use `signal` for lift
  columns, `heat` for pure magnitude.
- **2026-07-28 · v10** — Top breathing room on content/reference sheets. The row-1 title was jammed
  against the top edge; it's now bottom-aligned in a taller row (`_sheet_title` helper) so there's clean
  whitespace above it. Decision: brand identity (band/logo/eyebrow) stays on the COVER only; content and
  reference tabs get quiet top air with no repeated branding (repeating it on every tab would clutter).
- **2026-07-28 · v9** — Reference tabs get restrained structure (they read flat/busy before). Read me /
  Queries / Method now carry the same Mountain Green accent rule as the data tabs; Read me and Method
  section headers get a light green band; the Queries tab greys its comment lines (readable subtitle-grey
  italic) and sits the code on a subtle code-panel fill, so code reads clean and comments recede (drop the
  ASCII `====` bars in builder text). Design rule: appendix/reference tabs = light structure + one brand
  accent + comment/code coloring, NEVER the data tabs' heat/RAG.
- **2026-07-28 · v8** — Header block cleaned up for visual appeal. (1) The subtitle is now ONE short
  method line; all metric definitions/caveats live on the Read me tab, not repeated on every sheet.
  (2) A thin Mountain Green accent rule (row 3) closes the header block off from the table. (3) Alignment
  standard codified and applied: single-line text/labels/links/headers = left + vertical-center; numbers/
  flags = center; multi-line wrapped prose = left + top; never leave a cell on Excel's implicit bottom
  default. Fixed the cover Contents header (`Tab` vs `What's on it` had mismatched vertical alignment) and
  the meta strip.
- **2026-07-28 · v7** — Sheet subtitles wrap to the table width. The grey-italic method line under each
  title used to run off to the right on wide-text sheets; it now merges across the table columns
  (`A2:<lastcol>2`) with `wrap_text`, and `_fit_subtitle_height()` sets the row height to the wrapped text
  (Sheets/Excel won't auto-fit a merged cell — same root cause as the v3 row-height fix). The `Read me` /
  `Queries` / `Method` intros wrap to their own column width. Default on every sheet.
- **2026-07-22 · v6** — Color-coded tab strip now actually renders. Two fixes: (1) tab colors were stored
  with alpha `00` (transparent) because a bare 6-hex string makes openpyxl prepend `00` — now applied as
  `FF`+hex (opaque), so they show in Google Sheets. (2) The `TAB` palette was mostly slate/grey; it's now
  distinct MNTN hues — Slate INK anchor (Overview), Mountain Green (headline), Mountain Blue (data), light
  Mountain Blue (detail), light Mountain Green (Read me), slate greys (Queries/Method). The tab strip is
  now a color-coded legend: bright greens/blues = content, greys = appendix, dark = the cover.
- **2026-07-22 · v5** — Column auto-sizing fixed (INCR-75 feedback: cut-off words). `_autosize` now sizes
  each auto column to the WIDER of its longest header word (+ padding for bold text and the autofilter
  dropdown, so `Spend`/`Group` no longer break to `Spen/d`) and its actual data (so long first-column
  labels like `Ad served (treatment)` fit instead of crushing to header width). Two bugs fixed: `data_w`
  was computed but never used; and numeric width was measured from the raw float repr
  (`str(0.00189)="0.0018937…"`) — now estimated from magnitude + the `%`/`$`/comma format, so `Visit rate`
  is ~12 wide, not ~26. Pass `widths=` to override.
- **2026-07-21 · v4** — Stakeholder-feedback pass: (1) **brand green in the table shading** — zebra bands
  are now a light Mountain Green tint (`#E4F7F2`) instead of grey, and the slate header carries a thick
  Mountain Green underline, so the tables read as MNTN, not generic grey. (2) **Automatic em-dash strip**
  — every string written (titles, methods, takeaways, cells, glossary, notes) has `—`/`–` replaced with a
  spaced hyphen via `_demdash()`; ASCII hyphens and SQL bodies are untouched. Readers associate em-dashes
  with AI-generated text, so no deliverable ships one. Reminder: `table()`'s `sql()` tab already satisfies
  the common "add a SQL tab" ask — call `wb.sql(name, sql_text)`.
- **2026-07-21 · v3** — Text-heavy tables now render readably: body cells wrap in every column (not
  just the first), auto-computed column widths cap at 58 (was 46) while explicit `widths=` are honored
  up to 72, and `table()`/`notes()`/`glossary()` set per-row heights sized to the wrapped content so
  long prose no longer clips. Root cause: **neither Google Sheets (on an imported .xlsx) nor Excel auto-fits
  row height** — it must be set explicitly via `row_dimensions[r].height` (the earlier "Sheets auto-fits"
  assumption was wrong; the cutoff reproduced in Sheets). Fixes the
  reference/list-style deliverable (many long BQ paths + sentence-length cells) that previously
  truncated. Pass explicit `widths=` to make a prose column genuinely wide.
- **2026-07-21 · v2** — Official brand applied: real MNTN logo on the cover (Primary Horizontal
  Colored/White), Slate Grey structure + Mountain Green/Blue accents from brand.mountain.com, **Inter**
  font (installed locally; renders in Google Sheets). Licensed kit gitignored under
  `documentation/mntn_assets/`. Fixed logo scaling (pre-resize via PIL→BytesIO).
- **2026-07-21 · v1** — Initial standard + `lib/mntn_xlsx.py` (`MntnWorkbook`): branded cover with
  clickable contents, finding-led table sheets with heat + RAG, glossary/SQL/notes sheets, color-coded
  tabs, locked palette/typography/naming/Drive structure. Sample at `My Drive/Tickets/_FORMAT_SAMPLE/`.

### No label badges in descriptive slots

A contents line, method subtitle, caption or column note **states what the thing is**. Do not prefix it
with a rhetorical badge and colon: `The headline:`, `Best case:`, `The realistic average:`, `Key point —`.
Write "Every MM campaign vs 3P, by vertical", not "The headline: every MM campaign vs 3P, by vertical".

In a contents list every line is a description, so the badge carries no information and reads as
salesmanship. Conclusion-first phrasing belongs where a conclusion is the point: cover takeaways and the
sheet `finding=` title. `MntnWorkbook` hard-fails the build on a badge in `toc=` or `method=`.

### Descriptive slots must read on their own

A contents line, method subtitle or caption is read by someone with no one standing next to them. Two
things break that, and `MntnWorkbook` now hard-fails the build on both:

- **Coined shorthand the workbook never defines** — `whale-robust`, `apples-to-apples`, `the former`.
  Say it plainly: "the middle advertiser, each counting once", not "advertiser-weighted and whale-robust".
- **A pointer that never names its target** — "pair it with the blended tab", "see the other column".
  Name the actual tab or column: "MM vs 3P by vertical has all of them".

When a subtitle describes a subset, give its size rather than a label: "the 1,254 of 2,613 MM advertisers
whose intent threshold is above 0" beats "MM with the intent gate on". Derive those counts from the data
in the builder so they cannot drift on the next refresh.
