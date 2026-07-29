---
name: feedback_plain_voice_internal_docs
description: "Internal Confluence/spec docs must read like a human engineer wrote them, not an AI report — no draft banners, no callout boxes, no rhetorical openers, no em-dashes, sparse bold"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 604faaf9-ab5f-4b71-bb07-1a88aa0b430e
doc_type: memory
keywords: [plain_voice_internal_docs, plain, voice, internal, docs, confluence, spec, must]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-22
---
The user rejected the first cut of the AUDI-1083 Confluence spec: "reads like an AI report, and not
very good." Internal reference/spec docs must read like a sharp human engineer wrote them.

**Why:** the AI-report voice (over-structured, salesy, hedged) reads as low-quality and undercuts the
work, even when the content is right.

**How to apply — cut the AI tells:**
- No "DRAFT FOR FEEDBACK" / info / note callout-macro banners at the top.
- No rhetorical or salesy openers ("Why this exists", "How big is the gap?", "MM is not one thing").
  Open by stating what the thing IS.
- **No em-dashes** (standing rule, see [[feedback_no_emdash_no_namedrop]]) — use periods, commas, parentheses.
- Sparse bold; don't bold-label every bullet. Fewer section headers.
- Let tables carry the density; write plain declarative sentences around them.
- Drop punchy one-liner "insights" and "instructive edge cases" editorializing.
- Keep it tight — a human spec is shorter than an AI one.

Applies to Confluence pages, spec docs, internal reference docs. Related:
[[feedback_facts_not_presentation]], [[feedback_no_unsolicited_suggestions]], [[feedback_doc_style]].
