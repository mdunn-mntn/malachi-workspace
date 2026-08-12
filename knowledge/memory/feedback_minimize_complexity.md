---
name: feedback_minimize_complexity
description: "Keep analyses/deliverables as simple as possible — don't invent terms, lenses, or extra columns; one clear frame in plain language"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [minimize_complexity, simple deliverable, no invented terms, fewer columns, one value frame, audi-1148, platform evidence tab, lean deliverable, xlsx notes]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-22
---
Keep every deliverable **as simple as possible**. New terms, extra columns, second "lenses", and long
explanation cells all add complexity the user doesn't want. Default to the leanest version that still makes
the point.

**Why:** every invented term or added column is something the audience has to decode. The user repeatedly
strips these out (removed the "data-licensing" lens, the "Read" column, the big explanation cell, asked for
one simple frame). Complexity is a cost, not thoroughness.

**Be specific; only broaden if it strengthens (AUDI-1148).** Keep analyses to the specific case at hand.
Add broader/contextual information ONLY when it clearly strengthens the argument — if it's better or less
confusing without the extra info, cut it. Canonical miss: a "Platform evidence" tab showing the platform-wide
intent gradient (all advertisers) next to a single-campaign result — it read as if the campaign spanned those
bands and confused the user, so it was removed and its one load-bearing point ("this audience is ~100%
no-score → ~0 expected") was stated in one plain sentence instead. When in doubt, cut.

**How to apply:**
- **One value frame, not two.** Pick the single most defensible lens (e.g. money-made = impressions × measured
  eCPM × margin, as a range) and use it everywhere. Don't run a parallel lens (domain/licensing "fee-band")
  next to it — it just confuses.
- **Don't coin terms.** Prefer plain language ("did it pay for itself", "what we'd pay") over coined labels
  ("dependency ceiling", "cap at fair", "data-licensing value").
- **Fewer columns.** Cut any column that isn't load-bearing (Read/notes/secondary-lens columns). Fold the
  reasoning into the one action/recommendation cell, briefly.
- **Short notes.** One line under a table, not a paragraph. No oversized merged explanation cells.
- **Show ranges, not point estimates, when a parameter is a range** (margin) — but compute the exact inputs
  (eCPM measured, not guessed) so the range comes only from the genuine uncertainty.
- Applies to xlsx sheets, decks, tickets, docs. See [[feedback_doc_style]], [[feedback_xlsx_default_output]],
  [[feedback_terse_tickets]], [[feedback_facts_not_presentation]].
- **Triage asks to the MINIMAL necessary set before handing them to a human (2026-07-29).** Asked for "questions to bring to Matt," I gave 23; Malachi pushed back twice ("are they really all necessary?"). The right output was ~1: before asking a person, first (a) verify empirically what you can (I verified the bidder-leg myself and it flipped the premise), (b) route documented questions to Compass / the catalog, and (c) hand the human ONLY the genuine judgment/tribal-knowledge unknowns, tagged by owner (us / Compass / the-human). Don't dump the full brainstorm; deliver the filtered short list. Same spirit as [[feedback_no_unsolicited_suggestions]] and [[feedback_terse_chat_replies]].

## Two rules that apply to EVERYTHING generated, not just xlsx (2026-08-12, AUDI-1204)

Malachi: *"I want it to be consistent everywhere."* These govern every artifact — xlsx, decks, Jira, Slack, PRs, commits, docs, chat.

**1. Never use internal vocabulary the artifact doesn't define.** If a term comes from code or config — a constant name, a tier label, a function or script name, an internal column name — it cannot appear in reader-facing text unless that same artifact defines it. Say the thing plainly instead. Two live misses in one review, both borrowed from INCR-75's scoring code into a workbook that never mentions tiers:
- *"above the 12% saturation band"* (its `IVR_SATURATED` constant) → **"a large share of the people we serve already visit the site"**
- *"Ceiling is Mid tier while they stay paused"* — **on the cover**, in a workbook with no tier column → **"a powered test is not the same as proven incrementality; that needs a live holdout"**
- `spend_required` sitting in prose → **"these budgets"**

The test: would the reader have to open a Python file to know what this word means? Then it is not a word, it is a variable.

**Scope is EVERY surface, including `summary.md`, console output and commit bodies (Malachi, 2026-08-12: "literally everywhere"). The rule is DEFINE AT POINT OF USE, not never-use.** He qualified it "as long as that's more beneficial to AI", and a blanket ban is not: **identifiers are retrieval keys.** `_ROUTING.md` is keyword→doc, and a future session greps `IVR_SATURATED` or `can_hit_ivr_5pct_8w` to find a fact. Stripping them from analytical records and `knowledge/` would make facts unfindable — a net loss. So in those surfaces **pair the identifier with the plain phrasing**, never drop it: "INCR-75's final tier is POWER x CONFIRMED-LIFT; `confirmed +` needs >=20 holdout visits at p<.05" is correct because it defines as it goes. What was wrong was the bare undefined use: a `result:` frontmatter field reading "tier caps at Mid" (it renders alone in INDEX.md) and a console line opening "a-priori tier ceiling: Mid" before anything defined a tier.

**2. An annotation carries FACT ONLY — never interpretation.** A Note, caption, label, footnote, or sub-line is exactly one of three things: **composition** ("36,965 visiting of 285,909 served IPs"), a **benchmark** ("cohort median $27.54"), or a **unit qualifier** ("both arms, 10% holdout"). If the label already says it, the annotation is empty. Interpretation moves to the section that has room to justify it (Method & caveats, the analysis body). Cut on sight: *"the defensible number"*, *"the direct power cross-check denominator"*, *"they ramped up before pausing"*, *"their exit run-rate"*, *"worth knowing"*.

Both are now in global `CLAUDE.md` §9 Delete-on-sight. Enforcement for the xlsx surface: [[reference_xlsx_subtitle_caps]].
