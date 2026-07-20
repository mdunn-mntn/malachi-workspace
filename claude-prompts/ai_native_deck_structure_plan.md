# Plan — The Deck Structure Layer (Findings-Communication Unit)

> Component plan for the super-structure. Scope: **how a finding becomes a deck that reads as human
> work, not AI slop.** Sibling plans cover the knowledge base, the ticket/work layer, tooling, and
> agents; the work plan (`ai_native_work_structure_plan.md`) parks decks in `deliverables/` but does
> not say what one should contain. This plan is that contract: the *findings-communication unit* and
> the machine-checkable rules that make every deck to-the-point, single-line-titled, and
> evidence-led — the same way, every time.

---

## 0. Thesis

**The title makes the claim. The chart proves it. Everything else is deleted — and a linter, not
willpower, does the deleting.**

Slop is not a style problem; it is a *specificity* problem. When the model lacks the exact fact it
fills the gap with generic language, and reviewers now pattern-match that filler as "AI did this, the
analyst didn't." The cure is a structure that (a) forces one specific, sourced claim per slide and
(b) mechanically strips every word that isn't that claim or its proof. This makes "looks like real
work" a *property of the format*, not a thing the author has to remember to do.

---

## 1. The diagnosis — why AI decks read as slop

Attack root causes, not symptoms. Every slop tell traces to one of eight causes:

| Slop symptom | Root cause | The tell a reviewer reads |
|---|---|---|
| Title is a topic phrase — "Analysis of Visit Rate Across Keyword Buckets" | Model describes the slide instead of asserting a finding | "Generated, not thought through" |
| 5 full-sentence bullets | Model fills space it couldn't fill with a visual | "You're reading the slide *to* me — where's the synthesis?" |
| Vague quantifiers — "significant," "substantial," "several," "meaningfully" | Model didn't have the exact number | "You didn't actually run the numbers" |
| Title + subtitle + bullet + chart caption all say the same thing | Model restates instead of advancing | "Padding to look thorough" |
| Every slide the same title-over-bullets rhythm | One template stamped N times | "A machine produced this" |
| Register words — "leverage," "robust," "holistic," "delve," "it's worth noting" | Default LLM prose register | "This is straight out of ChatGPT" |
| Methodology on the main slides | Model dumps everything it did | "Hiding the absence of a point behind process" |
| No caveat, no recommendation, no opinion | Model has no stake in the conclusion | "Nobody actually owns this — so why should I trust it?" |

**Corollary (the load-bearing insight):** specificity *is* the proof of work. A named advertiser, an
exact number, a provenance path, one caveat you could only know by doing the analysis — these are the
signals of a human who did the work. Generic language is the signal of a machine that didn't. The
whole plan optimizes for the first and bans the second.

---

## 2. The core model — Assertion–Evidence

Two slide models exist. AI defaults to the first. This plan mandates the second (Alley,
*The Craft of Scientific Presentations* — the assertion–evidence structure).

**Default (topic + bullets) — the slop generator:**
```
┌──────────────────────────────────────────┐
│  Visit Rate by Keyword Rank Bucket         │ ← topic, not a claim
│  • Visit rate varies across rank buckets   │
│  • Higher-ranked buckets tend to perform   │ ← sentences the presenter reads aloud
│  • The difference is statistically robust  │
│  • Lower buckets show weaker performance   │
│  • Further analysis is recommended         │
└──────────────────────────────────────────┘
```

**Assertion–Evidence — the mandate:**
```
┌──────────────────────────────────────────┐
│  Top-ranked keywords drive 184× more       │ ← THE claim, one line
│  visits than bottom-ranked                 │
│                                            │
│     ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇  184×               │ ← ONE visual carries the proof
│     ▇ 1×                                   │
│                                            │
│  So keyword selection is the lever — not   │ ← one-line "so what" (judgment)
│  bid price.              data/final/kw_vr.csv│ ← provenance
└──────────────────────────────────────────┘
```

The mandate in one line: **the title is a single-line sentence stating the finding; the body is one
chart, table, or number that proves it; there are no bullets restating either.** "Single-line title"
means one line that *carries the message* — not a topic fragment, not a full paragraph.

---

## 3. Design principles (the "why")

1. **Subtraction is the work.** The default is delete. A word, bullet, or slide earns its place by
   advancing the claim or proving it; if it does neither, it is cut. This is the opposite of the AI
   default (add until it looks complete).
2. **Specificity over completeness.** One exact, sourced number beats five vague statements. Named
   entities, real figures, and provenance are non-negotiable — they *are* the proof of work (§1).
3. **The title carries the message; the visual carries the proof.** Never split the finding across
   both. If the reader gets the point from the title and the chart alone, the slide is done.
4. **One assertion per slide.** If a slide needs two claims, it is two slides. Slides are free;
   attention is not.
5. **Judgment, not description.** Every finding slide ends in a "so what" — the interpretation only a
   person who did the analysis could write. Description is what AI does; judgment is what proves you
   did the thinking.
6. **Vary the rhythm on purpose.** A deck of identical slides reads as machine-stamped. Mix slide
   *types* (§4): a big-number slide next to a chart slide next to a one-row table. Sameness is a tell.
7. **Round for the room, keep precision in the appendix.** "184×" on the slide; "183.7×, n=2,041" in
   the appendix. Business audiences read confidence in round numbers; the exact figure stays sourced
   and available.
8. **Not everything is a deck.** Internal analytical records stay plain facts in `summary.md` — no
   Power Line, no persuasion (per `feedback_facts_not_presentation`). Manufacturing a persuasion
   artifact where plain facts suffice is itself a form of slop. Build a deck only when presenting to a
   room (see the work plan's `deliverables/` trigger).
9. **Enforced by tooling, not discipline.** Every rule below is machine-checkable and runs in a
   linter + build step (§8), mirroring the knowledge base's `lint_coverage`/`build_index` pattern.
   Structure is never left to memory.

---

## 4. Slide taxonomy (a small, fixed, deterministic set)

Six types. Determinism: the author picks a type, the type dictates the layout, drift is impossible.
`type:` is a front-matter field on every slide (§8) so the linter and builder both key off it.

| Type | Contains | Count in a typical deck | Rule |
|---|---|---|---|
| `title` | Deck's one-line message + author + date | exactly 1 | The Power Line IS the deck title. No sub-tagline. Author only — no named attributions to others (`reference_deck_standards`). |
| `context` | One line: the question and why it matters now | 0–1 | The setup, not a background dump. If it takes more than one line, cut it. |
| `finding` | Assertion title + ONE visual + one-line "so what" + provenance | 3–7 (the body) | The assertion–evidence workhorse (§2). No bullets. Every quantitative claim sourced to `data/final/`. |
| `bignumber` | One number, one label, one line of context | 0–2 | The hero stat. Anchor context before the number. One number only. |
| `recommendation` | The action(s) — rule of three max | exactly 1 | Imperative verbs, specific owners/dates where known. Not "we should explore." |
| `appendix` | Methodology, full tables, caveats, exact figures | as needed | Referenced, never presented. This is where every detail the main deck omitted lives — so omitting it costs nothing. |

A minimal deck is `title → 3× finding → recommendation`. Everything else is optional and earns its
place. Charts follow the workspace Tufte standard (playbook Part 8) and rank descending by the
primary metric, most on top (`feedback_rank_desc_always`); the finding lives in the slide *title*,
never in a matplotlib chart title (`reference_deck_standards`).

---

## 5. The per-slide contract (machine-checkable rules)

Every rule is a lint check. A slide that fails does not ship.

| Rule | Check | Threshold |
|---|---|---|
| **Single-line title** | title char/line count | ≤ 14 words, renders on one line |
| **Title is a claim, not a topic** | title does not start with a banned topic-stem | no "Analysis of / Overview of / Summary of / Breakdown of / Deep dive into / A look at" |
| **One visual per finding slide** | count of charts+tables+big-numbers in body | exactly 1 |
| **No bullets on finding slides** | bullet count | 0 (bullets allowed only on `recommendation`, ≤ 3, ≤ 6 words each, fragments) |
| **Body word budget** | non-label prose words per slide | ≤ 25 |
| **Provenance on every number** | each quantitative slide names a `data/final/*` source | present |
| **The "so what" line** | each `finding` has a one-line interpretation | present, ≤ 20 words |
| **No banned language** | body + title vs the de-slop list (§6) | 0 hits |
| **No vague quantifier without a number** | "significant/substantial/several/many/various/meaningful" not adjacent to a figure | 0 hits |
| **Rhythm variety** | ≥ 2 distinct slide `type`s across the body | pass |
| **Round on slide, precise in appendix** | slide numbers ≤ 3 sig figs; exact figure exists in appendix | pass |

---

## 6. The de-slop list (find-and-kill)

The linter greps for these. They are the fingerprints reviewers now read as "AI wrote this."

- **Register / filler:** leverage, utilize, robust, holistic, comprehensive, seamless, delve, unlock,
  unleash, elevate, tapestry, landscape, realm, "in today's fast-paced," "it's worth noting,"
  "it is important to note," "at the end of the day," "needless to say."
- **Throat-clearing openers:** "This slide shows/demonstrates," "As we can see," "In order to,"
  "Let's dive in," "Without further ado," "So today I'm going to talk about."
- **Hedges (kill or commit):** "it seems," "it appears," "arguably," "one might argue," "I think
  maybe," "this could potentially suggest." State the finding or don't put it on a slide.
- **Vague quantifiers (replace with the number):** significant, substantial, several, various,
  numerous, a lot of, meaningfully, considerably, dramatically — *unless* the exact figure sits
  right beside it.
- **Redundancy patterns:** a subtitle that restates the title; a bullet that restates the chart; a
  chart caption that restates the axis; "as mentioned above."
- **Tricolon reflex:** not every point needs three parallel clauses. One clean clause beats a rhythmic
  three when two of them are filler.

The rule for each hit is not "soften" — it is **replace with the specific fact or delete.**

---

## 7. Proof-of-work checklist (the "a human did this" signals)

Slop reads as machine work because it is *generic*. A deck reads as real work when it carries signals
the model could not have invented without doing the analysis. Every deck should hit at least four:

- [ ] **Named entities** — a specific advertiser, table, keyword, IP, vertical (not "an advertiser").
- [ ] **Exact numbers with provenance** — every headline figure traces to `data/final/`.
- [ ] **A non-obvious insight** — one finding the audience would not have guessed (Greene's Law 6:
      surprise is what earns attention).
- [ ] **One earned caveat** — a limitation you could only know by doing it ("n_post < 28d, so CVR is
      noisy — visit rate is the headline"). This is the single strongest anti-slop signal: AI omits
      caveats because it has no stake.
- [ ] **A point of view** — the `recommendation` slide takes a position, not a menu of options.
- [ ] **A judgment line per finding** — the "so what" that interprets, not describes.

---

## 8. Deck spec format + build pipeline (structure by machine)

A deck is authored as **structured content, not free-form slides**, so it is lintable and buildable —
mirroring the knowledge base's front-matter → linter → generator trio.

**Source format:** one markdown file, `deliverables/ti_<num>_<slug>_deck.md`, with per-slide
front-matter blocks:

```markdown
---
deck: TI-804 — Keyword selection is the dominant visit-rate lever
author: malachi
date: 2026-04-08
power_line: Top-ranked keywords drive 184× more visits — selection beats bid.
---

## slide
type: finding
title: Top-ranked keywords drive 184× more visits than bottom-ranked
visual: charts/ti_804_vr_by_rank.png
so_what: Keyword selection is the lever, not bid price.
source: data/final/keyword_vr_by_rank.csv
```

**Tooling (three scripts + a hook, modeled on `bq_introspect`/`lint_coverage`/`build_index`):**

- **`new_deck.sh <TICKET>`** — stamps the deck skeleton from `_template/`, pulls the ticket's
  `result:` field (from the work plan's README front-matter) as the seed Power Line, scaffolds one
  `finding` slide per blessed output in `data/final/`. The only sanctioned way a deck is born — so it
  is born conforming.
- **`lint_deck.py`** — the enforcer. Runs every §5 check and greps the §6 de-slop list; fails the
  build on any hit. Also fails a `finding` slide whose `source:` is missing or points outside
  `data/final/`. Runs in a pre-commit hook and at session Stop.
- **`build_deck.sh`** — renders the markdown to a self-contained RevealJS HTML deck (team format —
  Jason Mills, Mike Dolt) plus static PNG fallbacks for Jira/Slack, charts embedded inline. Then
  `share_deck.sh` publishes via githack (`reference_deck_standards`). Charts come from the ticket's
  `generate_charts.py`, never hardcoded.
- **Hook:** on a deck file change, run `lint_deck.py`; block commit on failure, print the specific
  slop hits and their fix ("replace vague quantifier with the number from data/final/...").

**Default critique gate:** after build, run `claude-prompts/presentation_critique.md` against the
deck (existing workspace default). The linter catches *structural* slop; the critique catches
*narrative* slop (weak Power Line, findings in discovery order, no story).

---

## 9. Before / after — one slide

**Before (ships as slop):**
> **Title:** Analysis of Conversion Performance Across Advertiser Segments
> **Bullets:** • We looked at conversion data across several segments • Performance varied
> significantly • Some segments performed substantially better than others • Further analysis is
> recommended to leverage these robust findings

Six slop tells: topic title, five vague-quantifier bullets, zero numbers, zero provenance, register
words ("leverage," "robust"), no judgment. A reviewer's read: *"AI wrote this; you didn't do the
analysis."*

**After (assertion–evidence):**
> **Title:** HI-tier advertisers convert at 3.2× the PP tier
> **Visual:** one bar chart, 2 bars, direct-labeled 3.2× vs 1×, ranked descending
> **So what:** Concentrate spend in HI — the PP premium doesn't pay back.
> **Source:** data/final/cvr_by_intent_tier.csv

One claim, one proof, one judgment, one source. Same underlying analysis — but now it reads as the
work of someone who ran the numbers.

---

## 10. Anti-patterns this eliminates

| Today | Cause | Fixed by |
|---|---|---|
| Topic titles ("Analysis of…") | title describes the slide | assertion-title rule + linter (§5) |
| 5-bullet walls of text | space filled without a visual | one-visual, zero-bullet finding contract (§4, §5) |
| "significant / substantial / several" everywhere | exact number missing | vague-quantifier check + provenance rule (§5, §6) |
| "leverage / robust / delve / it's worth noting" | LLM register | de-slop grep list (§6) |
| Title + subtitle + bullet + caption all agree | restatement, not advance | word budget + redundancy check (§5, §6) |
| Every slide identical | one template ×N | rhythm-variety check (§4, §5) |
| Methodology on main slides | dump everything done | `appendix` type; referenced, not presented (§4) |
| Reads as "AI did it" | no specific, sourced, opinionated content | proof-of-work checklist (§7) |
| A deck built for internal facts | persuasion where plain facts belong | "not everything is a deck" (§3.8) |

---

## 11. Why it composes into the super-structure

- **Declared seam to the work plan.** A deck is exactly the artifact the work plan parks in
  `deliverables/`; this plan defines its contents. `new_deck.sh` reads the ticket's `result:`
  front-matter as the seed Power Line — the two plans wire together with no translation.
- **Declared seam to the viz standard.** Slide visuals inherit the workspace Tufte rules (playbook
  Part 8) and `generate_charts.py`; this plan owns the *slide*, the viz standard owns the *chart*.
- **Declared seam to the playbook.** The playbook owns *narrative and delivery* (Power Line, three-act
  structure, story, the room); this plan owns the *artifact and its language*. Structural slop dies
  here; narrative slop dies in the critique gate (§8). No overlap, clean handoff.
- **Uniform contract.** Per-slide front-matter is a typed API: a master orchestrator can assemble a
  deck from a ticket's `result:` + `data/final/` sources, lint it, and build it — without reading
  prose. Same front-matter → linter → generator idiom as every sibling plan, so they merge without
  seam translation.

**One-line summary for the synthesizer:** *a deck is a set of assertion–evidence slides — single-line
claim as title, one sourced visual as proof, a judgment line, zero filler — authored as typed
front-matter, enforced by a de-slop linter, and built to RevealJS; specificity is mandated because
specificity is the proof that a human did the work.*
