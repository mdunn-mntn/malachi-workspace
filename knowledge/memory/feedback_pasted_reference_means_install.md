---
name: feedback_pasted_reference_means_install
description: A reference/skill doc pasted into chat is an INSTALL request — wire it into the workspace system (skill + routed annex), don't follow its own interactive instructions or start a Q&A.
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [pasted reference, pasted skill doc, install request, skill install, compiled reference, other chat handoff, reference doc intent, companion memory doc]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-27
---
When the user pastes a compiled reference or skill-formatted doc (often built in another chat),
the intent is installation into the workspace system, not a live consultation. On 2026-08-27 I
followed the pasted PySpark reference's own "ask which platform" instructions through two
question rounds before the correction: "the point of this is for you to have a reference."

**Why:** the user compiles references elsewhere and hands them over so future sessions act on
them; the doc's embedded assistant instructions describe its post-install behavior, not what to
do at handoff.

**How to apply:** on receiving a pasted reference doc, plan the install — verbatim skill under
`.claude/skills/`, a companion `knowledge/memory/reference_*.md` carrying keywords for
`_ROUTING.md` plus any MNTN divergences (appended, never edited into the artifact), START_HERE
row if a task class fits. Ask questions only about placement decisions that are genuinely his.
Pattern instance: [[reference_pyspark_optimization_skill]]. [[feedback_contradictions_are_appended]]
