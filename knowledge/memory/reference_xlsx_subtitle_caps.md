---
name: reference_xlsx_subtitle_caps
description: "xlsx tab title <=125 / method subtitle <=200 chars, hard-failed in MntnWorkbook.table(); AUDI-1172 is the reference workbook, and the pattern is delegate-to-Read-me"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [mntn_xlsx, xlsx standard, finding cap, method subtitle, tab title, MntnWorkbook table, AUDI-1172, reference workbook, Read me tab, glossary, build blocked, _check_titleblock, deliverable standard]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-12
---
**`finding` ≤ 125 chars, `method` ≤ 200 chars, enforced as a HARD BUILD FAILURE** in `MntnWorkbook._check_titleblock` (`lib/mntn_xlsx.py`) — over-cap raises at `save_*()` and no file is written. Caps derived from **AUDI-1172 `Select vs Non-Select Incrementality.xlsx`**, the workbook Malachi hand-edited and named as the standard: finding 72–122 (median ~80), method 91–192 (median ~130).

**The reference workbook is `My Drive/Tickets/AUDI-1172/AUDI-1172 Select vs Non-Select Incrementality.xlsx`. Open it before building a new one.** Its structure: Overview → data tabs → **Read me** (glossary, 13 term/def pairs grouped under bold sub-heads, defs ≤213 chars) → Query → Method & caveats (9 blocks, bodies ≤299 chars).

**The pattern that makes ~130 chars sufficient: state the basis, then delegate.** Nearly every 1172 method line ends `"... See Read me for definitions."` The subtitle says what the numbers are and on what basis; formula, caveats, and definitions live on Read me / Method & caveats. **A method line that needs >200 chars means you're missing a Read me tab, not that the cap is wrong** — add `glossary()`. `finding` states the answer WITH its number ("Select prospecting drives ~5x the relative visit lift of non-Select (+23% vs +5%)"), never the topic.

**Do NOT over-correct to ~70 chars.** On AUDI-1204 I first trimmed to 72–84 and stripped real basis; the reference median is ~130. Terse is not the same as uninformative. Correct range is roughly 90–190.

**Why this became a hard fail (2026-08-12).** These two fields were **never capped**. The only guidance was one 72-char example in `documentation/docs/xlsx_deliverable_standard.md`, and practice drifted to a **382-char** method line (`incr_75_gruns`), median 175 across 24 shipped lines. When Malachi flagged AUDI-1204's subtitles as too long I first claimed they violated "the standard" — **wrong, no cap existed**; they sat between the p50 and p90 of shipped practice. **Lesson: before asserting a rule was broken, check that the rule is written and enforced; an illustrative example is not a spec.** The durable fix is enforcement in the builder, not a doc line — a doc rule nobody checks decays, a build that refuses to write a file cannot be ignored. 8 over-cap lines across 4 existing workbooks were trimmed at the same time so nothing breaks on re-run. **Companion caps, also realigned to 1172 the same day:** `lint_comms.py --kind xlsx_explainer` was 7 sections / 320 chars (the doc said 6) against a reference that runs **9 sections / 330 chars** — so 1172 failed the standard it defines. Now **10 / 340**; `glossary(max_entries)` 14 → **18** (1172 has 18 rows). **Pattern: when a cap and the reference artifact disagree, the artifact wins — check the reference before trusting a written cap.**

**Column-direction trap (same review):** a ratio column formatted `FMT.MULT` reads as "higher is better" by convention. AUDI-1204 showed required-spend ÷ their-typical-month as `0.6x`, where LOWER is better, with nothing stating the direction — and it duplicated the Verdict column. Fixed to `Share of typical` as a plain percent (60%, and 1206% for the out-of-reach CVR row); added `FMT.PCT0`. **Rule: express a sub-1 ratio as a percentage, or rename the header so the good direction is unambiguous.**

**No internal vocabulary in reader-facing workbook text (2026-08-12, two corrections in one review).** A workbook may only use a term it defines on its own tabs. Both offenders came from borrowing INCR-75's scoring code: **"above the 12% saturation band"** (its `IVR_SATURATED` constant) and **"Ceiling is Mid tier"** (its Top/Mid/Low tiering) — the second was on the COVER, in a workbook with no tier column anywhere. Replacements are plain statements of the same fact: "a large share of the people we serve already visit the site"; "a powered test is not the same as proven incrementality, that needs a live holdout." Same test for code identifiers: `spend_required` in prose became "these budgets".

**Note / annotation columns carry FACT ONLY.** A `Note` is one of three things: composition ("36,965 visiting of 285,909 served IPs"), a benchmark ("cohort median $27.54"), or a unit qualifier ("both arms, 10% holdout"). Never interpretation. Cut on sight: "the defensible number", "the direct power cross-check denominator", "they ramped up before pausing", "their exit run-rate". If the label already says it, the Note is empty. Interpretation belongs in Method & caveats, where it gets room to be justified.

See [[reference_xlsx_master_format]], [[feedback_xlsx_default_output]], [[feedback_minimize_complexity]].
