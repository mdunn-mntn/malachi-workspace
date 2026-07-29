---
name: reference-matplotlib-dollar-mathtext
description: matplotlib text with two $ signs renders as italic mathtext (drops the $); escape as \$ in chart labels/titles/subtitles
metadata: 
  node_type: memory
  type: reference
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [matplotlib, dollar sign, mathtext, italic latex, escape dollar, chart labels, cpm chart, helvetica arrow glyph]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-17
---
matplotlib interprets `$...$` in ANY text string (title, label, annotation, tick) as LaTeX
mathtext — so a string with **two dollar signs** renders the middle in italic math font and
**drops the `$` characters**. E.g. `f"${lo:.2f}–${hi:.2f}"` shows as italic "1.07–3.22", and a
subtitle "≈ $1–3 ... (~$10.7 CPM)" renders "1–3...(" in garbled italics.

**How to apply:** in any matplotlib text, escape literal dollar signs as `\$` (in a normal
Python string `"\\$"`, or a raw f-string). A SINGLE `$` per string is usually harmless (no
closing delimiter to pair with), but two-or-more always trips it. Bit me twice in the
AUDI-1115 chart work (`audi_1115_l0f_generate_charts.py`, `audi_1115_generate_charts.py`) — the
workspace builds a lot of `$`-heavy CPM/dollar charts, so this recurs. Also: the `→` glyph is
missing from Helvetica Neue → use `->`.
