---
name: feedback_no_label_colon_titles
description: "No editorial label-then-colon lead-ins in titles, findings, or takeaways ('Frequency is the lever:', 'Too early to call:'); state the fact directly, verdict phrase goes at the end or nowhere"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [colon titles, label lead-in, finding format, xlsx finding, deck takeaway, too early to call, frequency is the lever, editorial label, title style]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-24
---
Titles, sheet findings, and takeaways must not open with an editorial label followed by a colon ("Frequency is the lever: ...", "Too early to call: ..."). State the fact first; if a verdict phrase is needed it trails the sentence.

**Why:** flagged twice in one ticket (AUDI-1215, 2026-08-21 and 2026-08-24). Malachi: "I don't like titles/subtitles like that with colons, its supposed to be a rule." A claim-then-numbers colon ("Visit lift positive both periods: +11.1% pre...") is fine; the ban is on the short editorial framing label.

**How to apply:** before shipping any workbook finding, deck headline, or chart annotation, scan for a leading label+colon and rewrite it as a plain declarative. See [[feedback_facts_not_presentation]] [[feedback_minimize_complexity]].
