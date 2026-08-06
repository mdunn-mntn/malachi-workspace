---
name: feedback_plain_voice_internal_docs
description: "Internal docs, PR/Jira descriptions must read like a human engineer wrote them, not an AI report — no draft banners, no rhetorical openers, no em-dashes, and no invented shorthand/jargon/metaphors (fleet-wide, get fuel, guarded/off-switch) that mean nothing to other readers"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 604faaf9-ab5f-4b71-bb07-1a88aa0b430e
doc_type: memory
keywords: [plain_voice_internal_docs, plain voice, AI report tells, Confluence spec, em-dash, callout banner, AUDI-1083, internal docs, PR description, jargon, invented shorthand, fleet-wide, metaphor, be direct]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-06
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
- **No invented shorthand, jargon, or metaphors that only make sense to me.** Write words another
  reader already uses. Concrete rejects from the AUDI-1191 PR #1169 description (2026-08-04): "fleet-wide"
  (say "for Dataproc batch jobs"), "get fuel" (say what the data is for), "the 2 agreed extras" (name the
  two things), "guarded, off-switch" (say "disable with `MNTN_SPARK_OBSERVE=0`"). Cut whole lines that
  add nothing to a reader (the "Not in scope:" line). Be direct. This applies to PR/Jira descriptions too,
  not just Confluence. Also applies to Slack replies (2026-08-06, INC-012): "flat-lists" rejected as odd;
  say "lists every file under the folder". Prefer the common phrase over the technical-sounding compound.

Applies to Confluence pages, spec docs, internal reference docs, PR/Jira descriptions. Related:
[[feedback_facts_not_presentation]], [[feedback_no_unsolicited_suggestions]], [[feedback_doc_style]].
