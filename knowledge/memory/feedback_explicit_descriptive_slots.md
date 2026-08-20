---
name: feedback_explicit_descriptive_slots
description: Descriptive slots (captions, subtitles, contents lines) must read standalone — no coined shorthand, no unnamed pointers; give the tab name or the number
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [whale-robust, coined shorthand, unnamed tab pointer, the blended tab, caption reads standalone, name the tab, give the number, mntn_xlsx _check_explicit, xlsx subtitle, derived counts]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-20
---
A caption, subtitle, contents line or column note is read by someone with nobody there to explain it.
Two things break that and both shipped on AUDI-1141 before review caught them:

- **Coined shorthand the artifact never defines** — "advertiser-weighted and whale-robust". Say it
  plainly: "the middle advertiser, each counting once".
- **A pointer that never names its target** — "pair it with the blended tab". Name it: "MM vs 3P by
  vertical has all of them".

When the slot describes a subset, **give its size instead of a label**: "the 1,254 of 2,613 MM advertisers
whose intent threshold is above 0" beats "MM with the intent gate on". Derive the count in the builder so
it cannot drift on a refresh.

**Why:** this is the global §9 "no internal vocabulary the artifact doesn't define" rule; it kept being
missed in captions because captions felt too small to audit. Malachi, 2026-08-20.

**How to apply:** enforced, not honour-system — `lib/mntn_xlsx.py::_check_explicit` fails the build on a
coined term or an unnamed tab/column pointer in `toc=` or `method=`. Standard doc:
`documentation/docs/xlsx_deliverable_standard.md`. Companion rule: [[feedback_no_label_colon_prefix]].
Related: [[feedback_minimize_complexity]] [[reference_xlsx_master_format]]
