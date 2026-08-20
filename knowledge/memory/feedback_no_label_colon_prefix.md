---
name: feedback_no_label_colon_prefix
description: Don't prefix descriptive labels with a rhetorical tag and colon ("The headline:", "Best case:") — say what the thing is; BLUF belongs in takeaways only
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [label badge, the headline colon, best case colon, rhetorical prefix, caption wording, toc line, xlsx subtitle, mntn_xlsx guard, BLUF scope, descriptive slot]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-20
---
In descriptive slots — tab-of-contents lines, captions, column notes, method subtitles — **say what the
thing is, with no rhetorical label in front of it.** Cut prefixes like `The headline:`, `Best case:`,
`The realistic average:`, `Key point —`. "Every MM campaign vs 3P, by vertical", not "The headline: every
MM campaign vs 3P, by vertical".

**Why:** the label is the author telling the reader how to feel about the row before they've read it. In a
contents list every line is by definition a description, so the tag carries no information and reads as
salesmanship. BLUF is for takeaways and answer lines, where a conclusion is the point — it does not
generalize to every piece of text in the artifact. (Malachi, 2026-08-20, on the AUDI-1141 workbook.)

**How to apply:** in a descriptive slot, start with the noun. Reserve conclusion-first phrasing for
takeaways, sheet `finding=` titles, Jira answer lines, and chat line 1. If a caption needs a qualifier,
put it after the description as a clause, not before it as a badge.

Related: [[feedback_facts_not_presentation]] [[feedback_no_unsolicited_suggestions]]
[[feedback_terse_chat_replies]] [[reference_xlsx_master_format]]
