---
name: present
description: >-
  Build any deck, chart set, or *_presentation.md to the MNTN standard — resolving persuasion-vs-plain-facts
  by audience, applying the playbook and Tufte chart rules, and running the mandatory critique pass before it
  ships. Invoke when the user says "make a deck", "build a presentation", "build the slides", "put together
  slides", "RevealJS", "presentation.md", "present this to leadership", "walk us through what you found",
  "share-out", "build the charts for this", or "generate_charts.py".
---

# /present — Build a presentation, deck, or chart set

The default MNTN deliverable is a branded `.xlsx`, not a deck. Build a presentation only when the user asked
for slides, a live share-out, a leadership briefing, or a recommendation that needs buy-in. If they only need
the numbers, stop here and build the `.xlsx` instead (memory `feedback_xlsx_default_output`).

## Step 0 — Settle the audience question FIRST

This decides everything downstream, and getting it wrong is the most common failure.

| Audience | Framing |
|---|---|
| **Internal / technical** (team, engineers, Paulo, a working session) | **Playbook framing OFF.** No Power Line, no three-act, no Cialdini, no story. Plain facts, tables, exact numbers, caveats stated. Per memory `feedback_facts_not_presentation`. |
| **Leadership / cross-functional / external** (a decision or buy-in is being asked for) | Full playbook applies: Power Line, three-act, one number per slide, Rule of Three, Cialdini checklist. |

When mixed, lead with the BLUF headline and push detail to an appendix.

## Step 1 — Load the standards

Read before writing a single slide or line of chart code:

- `documentation/docs/presentation_playbook.md` — the authoritative guide. Power Line, three-act structure, the five openers, data-slide rules, Rule of Three, Cialdini checklist, Billboard Test, Part 8 visualization standards.
- `documentation/docs/revealjs_guide.md` — config, font sizes, cutoff-prevention rules, standalone build. Read before building any RevealJS deck.
- `documentation/docs/bluf_comms.md` — lead with the bottom line.
- memory `knowledge/memory/reference_deck_standards.md` — chart generation standards, no-double-titles rule, author name on the title slide, no named attributions, and the share step.
- memory `knowledge/memory/feedback_facts_not_presentation.md` — the plain-facts carve-out from Step 0.

**Conflict with the global `dataviz` skill:** `dataviz` ships a brand-neutral placeholder palette. For any MNTN
deliverable the MNTN standards win — Helvetica Neue, `#FAFAFA` background, 200 DPI, red accent for the insight,
navy supporting, gray context. Use `dataviz` for its form/interaction heuristics, not its colors.

## Step 2 — Write the Power Line (skip if Step 0 said plain-facts)

One sentence, ten words or fewer, the thing the audience will still remember tomorrow. Write it before
building anything. Every slide that does not serve it goes in the appendix or stays in `summary.md`.

## Step 3 — Build the artifact

- `summary.md` is the source of truth and the raw material. **Mine it, do not reorder it.** The presentation is a
  new document built from scratch; it intentionally omits most of the summary and must never contradict it.
- Presentations live at `tickets/ti_xxx_name/artifacts/ti_xxx_presentation.md`. Charts at
  `artifacts/ti_xxx_chart_*.png`. RevealJS at `artifacts/ti_xxx_presentation_deck.html`.
- Structure (persuasion mode): Disruption → Revelation → Resolution. Open with a Startling Stat, Question,
  Story, Bold Claim, or Contrast — never "today I'm going to talk about". Close on the Power Line or a clear
  call to action — never "that's all I have" or "any questions?".
- One number per data slide. Anchor before the reveal. Contrast over absolutes. Round for business audiences,
  exact for technical ones. Full tables go in the appendix.
- At least one story (character + emotion + moment + specific detail) in persuasion mode only.

## Step 4 — Charts

Every presentation with quantitative findings gets charts. Write `generate_charts.py` in the ticket's
`artifacts/`, reading from `outputs/*.csv` — never hardcode numbers. Apply the chart generation standards and
Tufte rules from memory `reference_deck_standards` (Helvetica Neue, `#FAFAFA`, 200 DPI, direct labels, no
gridlines/legends/3D, color encodes meaning only, lie factor 1, small multiples past ~5 categories, one-line
business implication annotating each chart).

Static PNGs always. Add a RevealJS HTML deck when presenting live.

## Step 5 — Critique pass (mandatory, do not skip)

Run `claude-prompts/presentation_critique.md` against the finished artifact. It scores ten areas 1-5 (Power
Line, Opening, Narrative, Story, Data Persuasion, Cialdini, Billboard Test, Close, Audience Adaptation,
Boldness) and produces a prioritized fix list. **Apply the fixes before calling it done.**

In plain-facts mode, run only the applicable dimensions (Data, Billboard Test, Close, Audience Adaptation) and
say which were skipped and why.

## Step 6 — Share

After building any RevealJS deck, run `.claude/scripts/share_deck.sh` and deliver the githack URL unprompted.
Exception (2026-07-10, AUDI-1089): when the user is assembling the doc themselves and asked only for "the
graphs", deliver the PNG set locally in `artifacts/` with no share link unless they ask.

Reference the charts from both `summary.md` and the presentation. Commit and push.
