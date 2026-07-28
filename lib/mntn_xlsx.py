#!/usr/bin/env python3
"""
mntn_xlsx — the MNTN shared .xlsx deliverable builder.

One import, one look. Every ticket's shareable workbook is built with this module so they all read
as one polished system: a branded cover, finding-led table sheets, a glossary, the SQL behind the
numbers, and method notes — color-coded tabs, frozen headers, autofilters, decimal-stored %/$ formats.

Design goals (from the deliverable standard, documentation/docs/xlsx_deliverable_standard.md):
  * The audience should feel real care went into it — coloring, spacing, typography, borders.
  * The numbers stay auditable: percents are stored as DECIMALS with true % number formats (survive
    a re-type in Excel/Sheets), the SQL rides along on its own tab, and nothing is pre-scaled.
  * Swapping in official MNTN brand hexes / logo is a ONE-LINE change (edit BRAND / pass logo_path).

Quick start
-----------
    import sys; sys.path.insert(0, "<workspace-root>")   # or install editable
    from lib.mntn_xlsx import MntnWorkbook, FMT

    wb = MntnWorkbook(
        title="MM vs 3P Segment Scorecard",
        ticket="AUDI-1141",
        subtitle="Prospecting performance by vertical — trailing 6 months",
        period="Jan-Jun 2026",
    )
    wb.table("MM vs 3P by vertical", df,
             finding="MNTN Matched leads visit rate in every vertical",
             method="Advertiser-weighted medians; prospecting only; visits = views + clicks.",
             formats={"MM IVR": FMT.PCT2, "3P IVR": FMT.PCT2, "IVR advantage": FMT.MULT},
             heat={"MM IVR": "high", "3P IVR": "high"},
             kind="headline", toc="The headline: MM vs 3P visit rate, CPV and ROAS by vertical")
    wb.glossary("Read me", intro="How to read this workbook.", rows=[("IVR", "Visit rate ..."), ...])
    wb.sql("Queries", open("....sql").read(), note="BigQuery cohort SQL used to produce these numbers.")
    wb.cover(takeaways=["...", "...", "..."])            # call LAST — builds the clickable contents
    wb.save_drive("AUDI-1141", "MM vs 3P Scorecard")     # -> My Drive/Tickets/AUDI-1141/AUDI-1141 MM vs 3P Scorecard.xlsx

Notes
-----
  * Store rates as DECIMALS (0.0046) and pass a % format — never pre-scale to "0.46".
  * Use None (not "") for truly-empty cells so an adjacent long label can overflow.
  * Keep headers <= 2 words; the sheet title says which group. Column widths fit the WHOLE header.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.hyperlink import Hyperlink
from openpyxl.worksheet.properties import PageSetupProperties

# ---------------------------------------------------------------------------
# BRAND — the single source of truth for the look. Swap these for official MNTN
# hexes when we have them; everything downstream re-colors automatically.
# ---------------------------------------------------------------------------
BRAND = {
    # Official MNTN brand palette (brand.mountain.com, 2025 guidelines).
    "INK":     "191E28",  # Slate Grey (deepest) — cover band bg, footer, brand copy on light
    "PRIMARY": "262E3C",  # Slate Grey — table header fills, finding titles, row labels
    "ACCENT":  "1AC9AA",  # Mountain Green (core brand color) — cover rule, key numbers, takeaway ticks
    "LINK":    "0AABC5",  # Mountain Blue — hyperlinks (better small-text contrast than the green)
    "BAND":    "E4F7F2",  # zebra band on data rows — light Mountain Green tint (brand, not grey)
    "PAPER":   "F6F6F6",  # Glacier White — off-white fill / neutral heat start
    "GREY":    "5C6675",  # subtitles, methodology lines (mid slate)
    "MUTE":    "98A2B3",  # footnotes, appendix tab color
    "LINE":    "DCE3EA",  # thin cell borders / rules
    "POS":     "1AC9AA",  # Mountain Green — good delta / RAG green
    "NEG":     "D1495B",  # bad delta / RAG red (brand has no red; reserved — reads "bad" to execs)
    "WARN":    "E9A23B",  # caution / RAG amber
    "WHITE":   "FFFFFF",
}

# Assets + brand override. Drop the official MNTN logo at assets/mntn_logo.png and it is used on every
# cover automatically. Drop official hexes in assets/brand.json ({"PRIMARY": "...", "ACCENT": "..."}) and
# they override the defaults above — no code edit needed. See lib/assets/README.md.
_ASSET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
_DEFAULT_LOGO = os.path.join(_ASSET_DIR, "mntn_logo.png")
try:
    import json
    _bpath = os.path.join(_ASSET_DIR, "brand.json")
    if os.path.exists(_bpath):
        with open(_bpath) as _bf:
            BRAND.update({k: str(v).lstrip("#").upper() for k, v in json.load(_bf).items()})
except Exception:
    pass  # malformed override never breaks a build; fall back to defaults

# Typography. Inter is the official MNTN body/UI font (open-license) and renders natively in Google
# Sheets — the actual delivery surface. Set FONT_BODY = "Arial" if recipients open in desktop Excel
# without Inter installed and you need guaranteed-identical metrics. Consolas/Menlo for SQL.
FONT_BODY = "Inter"
FONT_MONO = "Consolas"


@dataclass(frozen=True)
class _Fmt:
    """Number formats. Percents assume the cell holds a DECIMAL (0.0046 -> '0.46%')."""
    INT: str = "#,##0"
    NUM1: str = "0.0"
    NUM2: str = "0.00"
    PCT1: str = "0.0%"
    PCT2: str = "0.00%"
    PCT3: str = "0.000%"
    USD: str = '"$"#,##0.00'
    USD0: str = '"$"#,##0'
    USD2: str = '"$"#,##0.00'
    MULT: str = '0.0"x"'      # advantage ratios: 4.1x
    ROAS: str = '0.00"x"'
    DATE: str = "yyyy-mm-dd"


FMT = _Fmt()

# Tab color by role, so the tab strip itself is a color-coded legend. Distinct MNTN brand hues:
# a dark anchor for the cover, bright greens/blues for content, muted greys for the appendix.
# NOTE: apply with a leading "FF" (opaque) — a bare 6-hex string makes openpyxl store alpha 00
# (transparent), which renders as NO tab color in Google Sheets. See _new_sheet().
TAB = {
    "cover":    "191E28",  # Slate INK — dark anchor / "start here"
    "headline": "1AC9AA",  # Mountain Green — the hero content tab
    "data":     "0AABC5",  # Mountain Blue — content
    "detail":   "26D1EA",  # Mountain Blue (light) — supporting detail
    "glossary": "22E5BE",  # Mountain Green (light) — the Read me / reference
    "sql":      "667085",  # Slate grey — appendix
    "notes":    "98A2B3",  # Light slate grey — appendix
}

# Reusable style objects -----------------------------------------------------
_THIN = Side(style="thin", color=BRAND["LINE"])
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
_NOBORDER = Border()
# header cells get a thick Mountain Green underline — ties the slate header to the brand
_HEADER_BORDER = Border(left=_THIN, right=_THIN, top=_THIN,
                        bottom=Side(style="thick", color=BRAND["ACCENT"]))
_CEN = Alignment(horizontal="center", vertical="center", wrap_text=True)
_CEN_FLAT = Alignment(horizontal="center", vertical="center")
_CEN_WRAP = Alignment(horizontal="center", vertical="center", wrap_text=True)
_LEFT = Alignment(horizontal="left", vertical="top", wrap_text=True)
_LEFT_MID = Alignment(horizontal="left", vertical="center", wrap_text=True)
_RIGHT = Alignment(horizontal="right", vertical="center")


def _font(size=10, bold=False, italic=False, color="000000", name=FONT_BODY):
    return Font(name=name, size=size, bold=bold, italic=italic, color=color)


def _fill(hex_):
    return PatternFill("solid", fgColor=hex_)


_DASH_RE = re.compile(r"\s*[—–]\s*")


def _demdash(s):
    """Replace em/en dashes with a spaced hyphen. People read '—' as AI-written, so no MNTN
    deliverable should ship one. ASCII hyphens ('sub-vertical', '2026-07-21') are untouched."""
    return _DASH_RE.sub(" - ", s) if isinstance(s, str) else s


def _to_native(v):
    """numpy/pandas -> json-native, NaN/NaT -> None so Excel shows a truly empty cell.
    String cells are em-dash-sanitized on the way in."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if pd.isna(v) else float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return _demdash(v) if isinstance(v, str) else v


class MntnWorkbook:
    """A branded, multi-sheet MNTN deliverable. Build content sheets, then call cover() last."""

    def __init__(self, title, ticket, subtitle="", period="", owner="Malachi Dunn · Audience Intelligence",
                 logo_path=None, generated=None, status="Final"):
        self.title = _demdash(title)
        self.ticket = ticket.upper().strip()
        self.subtitle = _demdash(subtitle)
        self.period = period
        self.owner = owner
        # explicit logo_path wins; else use the canonical asset if it's been dropped in
        cand = logo_path or _DEFAULT_LOGO
        self.logo_path = cand if (cand and os.path.exists(cand)) else None
        self.generated = generated  # 'YYYY-MM-DD' string; pass one for reproducible files
        self.status = status
        self._toc = []  # (sheet_name, one-line description, role)

        self.wb = Workbook()
        self.wb.remove(self.wb.active)  # start empty; cover() is added last and moved to front
        p = self.wb.properties
        p.title = f"{self.ticket} — {title}"
        p.subject = subtitle
        p.creator = owner
        p.keywords = f"MNTN, {self.ticket}, Audience Intelligence"
        p.category = "Analysis deliverable"

    # -- internal sheet scaffolding -----------------------------------------
    def _new_sheet(self, name, role):
        ws = self.wb.create_sheet(name[:31])  # Excel 31-char tab limit
        ws.sheet_view.showGridLines = False
        ws.sheet_properties.tabColor = "FF" + TAB.get(role, BRAND["PRIMARY"])  # FF = opaque (bare hex -> alpha 00 = invisible)
        ws.sheet_view.zoomScale = 100
        ws.page_setup.orientation = "landscape"
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
        ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
        for side in ("left", "right", "top", "bottom"):
            setattr(ws.page_margins, side, 0.4)
        return ws

    def _titleblock(self, ws, finding, method, ncols=1):
        """Finding-led title (states the finding) + grey italic methodology line.

        The subtitle is merged across the table columns and wrapped, so it never runs off to the
        right past the table below it. Its row height is fitted later in table() once column widths
        are known (Excel/Sheets won't auto-fit a merged cell). Applies to every table sheet."""
        c = ws.cell(row=1, column=1, value=_demdash(finding))
        c.font = _font(15, bold=True, color=BRAND["PRIMARY"])
        if method:
            m = ws.cell(row=2, column=1, value=_demdash(method))
            m.font = _font(10, italic=True, color=BRAND["GREY"])
            m.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
            if ncols > 1:
                ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=ncols)
        # accent tick under the title
        ws.cell(row=3, column=1, value=None)

    def _fit_subtitle_height(self, ws, row, text, total_width_chars, per_line=13.5, pad=4.0, maxlines=5):
        """Set a merged, wrapped subtitle row's height to fit its text at the table width.
        total_width_chars = summed width of the merged columns; Excel won't auto-fit merged cells."""
        if not text:
            return
        cpl = max(int(total_width_chars * 0.92), 24)   # ~chars per line across the merged span
        lines = max(1, -(-len(_demdash(text)) // cpl))
        ws.row_dimensions[row].height = max(16.0, min(lines, maxlines) * per_line + pad)

    def _footnote(self, ws, text, row, ncols):
        if not text:
            return
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max(ncols, 1))
        c = ws.cell(row=row, column=1, value=text)
        c.font = _font(9, italic=True, color=BRAND["MUTE"])
        c.alignment = _LEFT

    def _autosize(self, ws, df, widths=None, first_col=None, cap=38, filter_pad=7):
        """Size each column to the WIDER of (a) its longest header WORD + padding for bold text and the
        autofilter dropdown icon — so a header word never breaks mid-word ("Spend" -> "Spen/d") — and
        (b) its actual DATA, up to `cap` (longer data wraps on word boundaries).

        Returns {column_name: final_width_chars} so row heights can be sized to the wrapped text.
        Explicit `widths` are honored as-is (up to a sane ceiling) so a caller can make a prose column
        genuinely wide.
        """
        widths = widths or {}
        final = {}
        for j, col in enumerate(df.columns, 1):
            if col in widths:
                w = min(max(int(widths[col]), 8), 72)   # honor caller intent, sane ceiling
            else:
                header = str(col)
                longest_word = max([len(x) for x in header.split()] or [len(header)])
                cd = df[col].dropna()
                if df[col].dtype == object:
                    data_w = int(cd.astype(str).map(len).head(200).max()) if len(cd) else 0
                else:
                    # numbers are display-FORMATTED (%, $, commas) — never measure the raw float repr
                    # (str(0.00189)="0.00189372…"). Estimate from magnitude + room for separators/symbol.
                    mx = pd.to_numeric(cd, errors="coerce").replace([np.inf, -np.inf], np.nan).abs().max()
                    int_digits = len(f"{int(mx):,}") if (pd.notna(mx) and mx >= 1) else 3
                    data_w = int_digits + 5
                # header word must fit (never break mid-word); data fits up to cap (wraps beyond); floor 10
                need = max(longest_word + filter_pad, min(data_w, cap) + 2, 10)
                w = min(need, cap)
            ws.column_dimensions[get_column_letter(j)].width = w
            final[col] = w
        if first_col:
            ws.column_dimensions["A"].width = first_col
            final[df.columns[0]] = first_col
        return final

    def _fit_row_heights(self, ws, df, start, colw, per_line=15.0, pad=6.0, minh=18.0, maxlines=10):
        """Size each data row to its tallest wrapped cell so nothing clips (Excel won't auto-fit).

        colw = {column: width_chars} from _autosize. Slightly over-estimates lines (chars-per-line
        uses 0.85 * width) so text errs tall rather than clipped.
        """
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rr = start + i
            lines = 1
            for col in df.columns:
                v = r[col]
                if v is None:
                    continue
                s = str(v)
                cpl = max(int(colw.get(col, 12) * 0.85), 4)
                need = sum(max(1, -(-len(seg) // cpl)) for seg in s.split("\n"))
                lines = max(lines, need)
            ws.row_dimensions[rr].height = max(minh, min(lines, maxlines) * per_line + pad)

    @staticmethod
    def _wrap_rows(ws, col_letter, rows, width_chars, per_line=15.0, pad=6.0, minh=16.0, maxlines=40):
        """Size a single wrapped text column's rows (used by notes/glossary prose cells)."""
        cpl = max(int(width_chars * 0.9), 8)
        for rr, text in rows:
            if text is None:
                continue
            need = sum(max(1, -(-len(seg) // cpl)) for seg in str(text).split("\n"))
            ws.row_dimensions[rr].height = max(minh, min(need, maxlines) * per_line + pad)

    # -- public: table sheet -------------------------------------------------
    def table(self, name, df, finding, method="", formats=None, heat=None, rag=None,
              band=True, kind="data", toc="", widths=None, first_col_width=None, freeze="A"):
        """Add a styled table sheet.

        finding  : sheet title that STATES THE FINDING (not the metric).
        method   : one grey line of methodology under the title.
        formats  : {column_name: number_format}. Store %/$ as decimals; pass FMT.PCT2 etc.
        heat     : {column_name: 'high'|'low'|'neutral'} -> per-column color scale
                   ('high' = green is good/large, 'low' = green is small/good e.g. cost).
        rag      : {column_name: fn(value)->'POS'|'NEG'|'WARN'|None} -> traffic-light cell fills.
        kind     : 'headline' | 'data' | 'detail' (drives tab color).
        """
        formats = formats or {}
        heat = heat or {}
        rag = rag or {}
        ws = self._new_sheet(name, "headline" if kind == "headline" else kind)
        ncols = len(df.columns)
        self._titleblock(ws, finding, method, ncols)
        start = 4

        # header row (slate fill, white text, Mountain Green underline)
        for j, col in enumerate(df.columns, 1):
            c = ws.cell(row=start, column=j, value=str(col))
            c.font = _font(11, bold=True, color=BRAND["WHITE"])
            c.fill = _fill(BRAND["PRIMARY"])
            c.alignment = _CEN
            c.border = _HEADER_BORDER
        ws.row_dimensions[start].height = 30

        # body
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rr = start + i
            for j, col in enumerate(df.columns, 1):
                v = _to_native(r[col])
                c = ws.cell(row=rr, column=j, value=v)
                c.border = _BORDER
                # numbers stay centered on one line; text wraps so long cells never clip
                c.alignment = (_LEFT_MID if (j == 1 and isinstance(v, str))
                               else (_CEN_WRAP if isinstance(v, str) else _CEN_FLAT))
                if col in formats:
                    c.number_format = formats[col]
                if j == 1:
                    c.font = _font(10, bold=True, color=BRAND["PRIMARY"])
                if band and i % 2 == 0:
                    c.fill = _fill(BRAND["BAND"])
                if col in rag and v is not None:
                    key = rag[col](v)
                    if key in ("POS", "NEG", "WARN"):
                        c.fill = _fill(BRAND[key])  # solid semantic fill
                        c.font = _font(10, bold=True, color=BRAND["WHITE"])

        n = len(df)
        last_col = get_column_letter(ncols)
        # per-column heat scales
        for col, direction in heat.items():
            if col not in df.columns:
                continue
            jj = get_column_letter(list(df.columns).index(col) + 1)
            rng = f"{jj}{start+1}:{jj}{start+n}"
            if direction == "neutral":
                rule = ColorScaleRule(start_type="min", start_color=BRAND["PAPER"],
                                      end_type="max", end_color=BRAND["PRIMARY"])
            else:
                lo, hi = (BRAND["NEG"], BRAND["POS"]) if direction == "high" else (BRAND["POS"], BRAND["NEG"])
                rule = ColorScaleRule(start_type="min", start_color=lo,
                                      mid_type="percentile", mid_value=50, mid_color="FFF4C2",
                                      end_type="max", end_color=hi)
            ws.conditional_formatting.add(rng, rule)

        ws.freeze_panes = f"{freeze}{start+1}"
        ws.auto_filter.ref = f"A{start}:{last_col}{start+n}"
        colw = self._autosize(ws, df, widths=widths, first_col=first_col_width)
        self._fit_row_heights(ws, df, start, colw)
        # subtitle (row 2) is merged across the table; size its height to the wrapped text at table width
        self._fit_subtitle_height(ws, 2, method, sum(colw.get(c, 12) for c in df.columns))
        self._footnote(ws, f"Source: {self.ticket}."
                       + (f"  Period: {self.period}." if self.period else "")
                       + (f"  Generated {self.generated}." if self.generated else ""),
                       start + n + 2, ncols)
        if toc:
            self._toc.append((ws.title, toc, kind))
        return ws

    # -- public: glossary / read-me -----------------------------------------
    def glossary(self, name, rows, intro="", toc="How to read this workbook", body_width=104):
        """Two-column term/definition sheet (term bold in A, definition wrapped in B).
        A row of ('', '') renders a blank spacer; a row of ('Header', '') renders a bold sub-head."""
        ws = self._new_sheet(name, "glossary")
        ws.cell(row=1, column=1, value=self.title).font = _font(15, bold=True, color=BRAND["PRIMARY"])
        sub = f"{self.ticket}." + (f"  {_demdash(intro)}" if intro else "")
        subc = ws.cell(row=2, column=1, value=sub)
        subc.font = _font(10, italic=True, color=BRAND["GREY"])
        subc.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        ws.merge_cells("A2:B2")
        r = 4
        def_rows = []
        for k, v in rows:
            kc = ws.cell(row=r, column=1, value=(_demdash(k) or None))
            kc.font = _font(10, bold=True, color=BRAND["PRIMARY"] if v else BRAND["INK"])
            kc.alignment = _LEFT
            vc = ws.cell(row=r, column=2, value=(_demdash(v) or None))
            vc.alignment = _LEFT
            vc.font = _font(10)
            def_rows.append((r, v))
            r += 1
        a_width = max((len(str(k)) for k, _ in rows), default=24) + 3
        ws.column_dimensions["A"].width = a_width
        ws.column_dimensions["B"].width = body_width
        self._wrap_rows(ws, "B", def_rows, body_width)
        self._fit_subtitle_height(ws, 2, sub, a_width + body_width)
        if toc:
            self._toc.append((ws.title, toc, "glossary"))
        return ws

    # -- public: SQL / queries ----------------------------------------------
    def sql(self, name, sql_text, note="", toc="The SQL behind the numbers", width=120):
        ws = self._new_sheet(name, "sql")
        ws.cell(row=1, column=1, value="Queries used (for validation)").font = _font(15, bold=True, color=BRAND["PRIMARY"])
        if note:
            nc = ws.cell(row=2, column=1, value=_demdash(note))
            nc.font = _font(10, italic=True, color=BRAND["GREY"])
            nc.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        r = 4
        for line in sql_text.split("\n"):  # SQL body left verbatim (never sanitized)
            c = ws.cell(row=r, column=1, value=line)
            c.font = _font(9, name=FONT_MONO, color=BRAND["INK"])
            r += 1
        ws.column_dimensions["A"].width = width
        self._fit_subtitle_height(ws, 2, note, width)
        if toc:
            self._toc.append((ws.title, toc, "sql"))
        return ws

    # -- public: long-form notes / method -----------------------------------
    def notes(self, name, blocks, intro="", toc="Method & caveats", body_width=110):
        """blocks = list of (heading, body). heading '' -> continuation paragraph."""
        ws = self._new_sheet(name, "notes")
        ws.cell(row=1, column=1, value=name).font = _font(15, bold=True, color=BRAND["PRIMARY"])
        if intro:
            ic = ws.cell(row=2, column=1, value=_demdash(intro))
            ic.font = _font(10, italic=True, color=BRAND["GREY"])
            ic.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        r = 4
        body_rows = []
        for head, body in blocks:
            if head:
                hc = ws.cell(row=r, column=1, value=_demdash(head))
                hc.font = _font(11, bold=True, color=BRAND["PRIMARY"])
                r += 1
            bc = ws.cell(row=r, column=1, value=_demdash(body))
            bc.font = _font(10)
            bc.alignment = _LEFT
            body_rows.append((r, body))
            r += 2
        ws.column_dimensions["A"].width = body_width
        self._wrap_rows(ws, "A", body_rows, body_width)
        self._fit_subtitle_height(ws, 2, intro, body_width)
        if toc:
            self._toc.append((ws.title, toc, "notes"))
        return ws

    # -- public: branded cover (call LAST) ----------------------------------
    def cover(self, takeaways=None, name="Overview"):
        """Create the branded cover and move it to the front. Builds the clickable contents
        from every sheet added so far. takeaways = up to 3 headline bullets (Rule of Three)."""
        takeaways = (takeaways or [])[:3]
        ws = self._new_sheet(name, "cover")
        SPAN = 8
        wide = get_column_letter(SPAN)

        # brand band (rows 1-3): INK fill, wordmark / logo left
        for rr in (1, 2, 3):
            for cc in range(1, SPAN + 1):
                ws.cell(row=rr, column=cc).fill = _fill(BRAND["INK"])
            ws.row_dimensions[rr].height = 22
        ws.merge_cells(f"A1:{wide}3")
        if self.logo_path:
            try:
                from PIL import Image as PILImage
                target_h = 44  # px; fits inside the 3-row band with breathing room
                lg = PILImage.open(self.logo_path).convert("RGBA")
                w = max(1, int(lg.width * target_h / lg.height))
                # Render the resized logo to a PERSISTENT temp file, not a BytesIO: openpyxl re-reads the
                # image ref on EVERY wb.save(), so save_local()+save_drive() would exhaust a one-shot
                # buffer ("I/O operation on closed file"). A file path is re-readable across saves.
                render_path = os.path.join(_ASSET_DIR, "_logo_render.png")
                lg.resize((w, target_h)).save(render_path, format="PNG")
                self._logo_render = render_path  # keep a ref; the file persists (gitignored)
                ws.add_image(XLImage(render_path), "A2")  # row 2 -> vertically centered in the band
            except Exception:
                self.logo_path = None
        if not self.logo_path:
            wm = ws.cell(row=1, column=1, value="MNTN")
            wm.font = _font(26, bold=True, color=BRAND["WHITE"])
            wm.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        # accent rule (row 4)
        for cc in range(1, SPAN + 1):
            ws.cell(row=4, column=cc).fill = _fill(BRAND["ACCENT"])
        ws.row_dimensions[4].height = 5

        # title + subtitle
        ws.merge_cells(f"A6:{wide}6")
        t = ws.cell(row=6, column=1, value=self.title)
        t.font = _font(24, bold=True, color=BRAND["INK"])
        t.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.row_dimensions[6].height = 34
        if self.subtitle:
            ws.merge_cells(f"A7:{wide}7")
            s = ws.cell(row=7, column=1, value=self.subtitle)
            s.font = _font(12, italic=True, color=BRAND["GREY"])
            s.alignment = Alignment(horizontal="left", vertical="center", indent=1)

        # meta strip
        meta = [("Ticket", self.ticket), ("Period", self.period or "—"),
                ("Prepared by", self.owner), ("Status", self.status)]
        if self.generated:
            meta.insert(2, ("Generated", self.generated))
        r = 9
        for label, val in meta:
            ws.cell(row=r, column=1, value=label).font = _font(9, bold=True, color=BRAND["MUTE"])
            c = ws.cell(row=r, column=2, value=val)
            c.font = _font(11, bold=True, color=BRAND["PRIMARY"])
            r += 1

        # key takeaways (Rule of Three)
        r += 1
        if takeaways:
            ws.cell(row=r, column=1, value="Key takeaways").font = _font(13, bold=True, color=BRAND["INK"])
            r += 1
            for i, tk in enumerate(takeaways, 1):
                ws.cell(row=r, column=1, value=str(i)).font = _font(12, bold=True, color=BRAND["ACCENT"])
                ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=SPAN)
                c = ws.cell(row=r, column=2, value=_demdash(tk))
                c.font = _font(11, color=BRAND["INK"])
                c.alignment = _LEFT_MID
                ws.row_dimensions[r].height = 30
                r += 1

        # contents (clickable)
        r += 1
        ws.cell(row=r, column=1, value="Contents").font = _font(13, bold=True, color=BRAND["INK"])
        r += 1
        ws.cell(row=r, column=1, value="Tab").font = _font(9, bold=True, color=BRAND["WHITE"])
        ws.cell(row=r, column=1).fill = _fill(BRAND["PRIMARY"])
        ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=SPAN)
        hc = ws.cell(row=r, column=2, value="What's on it")
        hc.font = _font(9, bold=True, color=BRAND["WHITE"])
        hc.fill = _fill(BRAND["PRIMARY"])
        ws.cell(row=r, column=1).alignment = _CEN_FLAT
        r += 1
        for sheet_name, desc, role in self._toc:
            link = ws.cell(row=r, column=1, value=sheet_name)
            link.hyperlink = Hyperlink(ref=f"A{r}", location=f"'{sheet_name}'!A1", display=sheet_name)
            link.font = _font(10, bold=True, color=BRAND["LINK"], name=FONT_BODY)
            link.alignment = _LEFT_MID
            ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=SPAN)
            d = ws.cell(row=r, column=2, value=_demdash(desc))
            d.font = _font(10, color=BRAND["GREY"])
            d.alignment = _LEFT_MID
            r += 1

        # footer
        r += 1
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=SPAN)
        f = ws.cell(row=r, column=1,
                    value=f"MNTN · Audience Intelligence · {self.ticket}"
                          + (f" · generated {self.generated}" if self.generated else "")
                          + " · Internal")
        f.font = _font(9, italic=True, color=BRAND["MUTE"])

        # column widths for the cover — col A must fit the longest tab name in Contents
        longest_tab = max((len(n) for n, _, _ in self._toc), default=16)
        ws.column_dimensions["A"].width = min(max(longest_tab + 2, 16), 28)
        for cc in range(2, SPAN + 1):
            ws.column_dimensions[get_column_letter(cc)].width = 16

        # move to front and select
        self.wb.move_sheet(ws, -(len(self.wb.worksheets) - 1))
        self.wb.active = 0
        return ws

    # -- save ---------------------------------------------------------------
    def save_local(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.wb.save(path)
        return path

    def save_drive(self, ticket_key, filename_desc, drive_root=None):
        """Write straight into the mounted Google Drive: My Drive/Tickets/<KEY>/<KEY> <Desc>.xlsx"""
        root = drive_root or os.path.expanduser(
            "~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/Tickets")
        folder = os.path.join(root, ticket_key.upper().strip())
        os.makedirs(folder, exist_ok=True)
        fname = f"{ticket_key.upper().strip()} {filename_desc}.xlsx"
        path = os.path.join(folder, fname)
        self.wb.save(path)
        return path


# RAG helpers ----------------------------------------------------------------
def rag_threshold(good_above=None, bad_below=None, reverse=False):
    """Return a fn(value)->'POS'|'WARN'|'NEG' for use in table(rag=...)."""
    def f(v):
        try:
            x = float(v)
        except (TypeError, ValueError):
            return None
        hi, lo = good_above, bad_below
        if hi is not None and x >= hi:
            return "NEG" if reverse else "POS"
        if lo is not None and x <= lo:
            return "POS" if reverse else "NEG"
        return "WARN"
    return f


if __name__ == "__main__":
    print("mntn_xlsx — import this module. See documentation/docs/xlsx_deliverable_standard.md")
