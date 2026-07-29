---
name: Document style preference
description: Presentation-style docs — single table, tells a story, gets to the point. No sprawling multi-doc artifacts.
type: feedback
doc_type: memory
keywords: [doc style, single table, presentation doc, findings doc, ti_790, story, one doc]
domain: [workflow]
lifecycle: active
last_verified: 2026-04-01
---
When creating analysis/findings docs, follow the ti_790_presentation.md pattern:
- One doc, not many
- Start with the question, then what we did, then what we found
- One master ranked table with metadata columns (tag, source, etc.) — not separate sections per category
- End with clear takeaways and actionable next steps
- Include methodology details at the bottom for people who want to dig in
- Clean, tells a story, gets right to the point

**Why:** Multiple overlapping artifacts get convoluted. The user needs to share these with leadership and team — one clean doc they can walk through like a presentation.

**How to apply:** For any analysis ticket, consolidate findings into a single presentation doc rather than building up multiple reference docs. If supporting detail is needed, keep it in the queries/code — not in additional markdown files.
