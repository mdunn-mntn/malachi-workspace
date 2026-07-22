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
