---
name: feedback_minimize_complexity
description: "Keep analyses/deliverables as simple as possible — don't invent terms, lenses, or extra columns; one clear frame in plain language"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [minimize_complexity, minimize, complexity, keep, analyses, deliverables, simple, possible]
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
