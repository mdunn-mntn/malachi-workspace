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

### Palette — `BRAND` dict in `lib/mntn_xlsx.py`
| Token | Hex | Use |
|-------|-----|-----|
| `INK` | `#10263B` | Cover band background, deepest text |
| `PRIMARY` | `#1A3C5E` | Table header fills, finding titles (**MNTN identity navy**) |
| `ACCENT` | `#1F8FE5` | Cover rule, active tab, key numbers, contents links |
| `BAND` | `#F1F5F8` | Zebra band on data rows |
| `PAPER` | `#FBFCFD` | Off-white sheet fill (never pure white) |
| `GREY` / `MUTE` | `#667085` / `#98A2B3` | Method subtitles / footnotes |
| `LINE` | `#D9E1E8` | Thin cell borders |
| `POS` / `NEG` / `WARN` | `#1B9E77` / `#D1495B` / `#E9A23B` | RAG traffic-lights & deltas |

**Swapping in official MNTN brand (zero code change):**
- **Logo** — drop a transparent PNG (white/reversed reads best on the navy band) at
  `lib/assets/mntn_logo.png`. Every cover uses it automatically in place of the "MNTN" wordmark.
- **Colors** — drop official hexes in `lib/assets/brand.json` (e.g. `{"PRIMARY": "…", "ACCENT": "…"}`);
  they override the `BRAND` defaults. Any subset is fine. See `lib/assets/README.md`.
- (Both asset files are gitignored. Until they're provided, the palette above is built on the navy
  already used across our deliverables.)

**Attribution** — the cover's *Prepared by* defaults to **`Malachi Dunn · Audience Intelligence`**
(author + team). Override per-workbook with `owner=`. This is the deliberate exception to the
"no names in shared artifacts" rule, made by the user for `.xlsx` deliverables.

- **Red reads as "bad" to execs** — reserve `NEG` for genuinely-bad values only, never for emphasis.
- Heat = a per-column color scale (`"high"` green-is-large, `"low"` green-is-small e.g. cost,
  `"neutral"` white→navy). RAG = discrete traffic-light fills. **Never put `heat` and `rag` on the same
  column** — a color-scale and a manual fill collide in Excel.

### Typography
- **Arial** everywhere (it round-trips identically into Google Sheets; Calibri is Excel-only and gets
  substituted). **Consolas/Menlo** for SQL.
- Cover title 24 · finding title 15 bold navy · method subtitle 10 italic grey · header 11 bold white ·
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
2. Regenerate the sample (`python3 lib/mntn_xlsx_demo.py`) and eyeball it.
3. Note the change in the Changelog below and commit.

Every existing builder re-run picks up the new look automatically. That is the point of centralizing it.

### Changelog
- **2026-07-21 · v1** — Initial standard + `lib/mntn_xlsx.py` (`MntnWorkbook`): branded cover with
  clickable contents, finding-led table sheets with heat + RAG, glossary/SQL/notes sheets, color-coded
  tabs, locked palette/typography/naming/Drive structure. Sample at
  `My Drive/Tickets/_FORMAT_SAMPLE/`. Built on the existing navy; official MNTN logo/hexes are a one-line
  swap (`BRAND` + `logo_path`) once provided.
