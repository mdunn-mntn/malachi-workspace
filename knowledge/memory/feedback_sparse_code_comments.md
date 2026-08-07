---
name: sparse_code_comments
description: "Code comments must be sparse, one line max if ever. Write self-documenting code; put the why in the PR/commit, not in block comments."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b9798d39-963b-4b08-ba77-d3be373da680
doc_type: memory
keywords: [sparse_code_comments, sparse, code, comments, must, line, ever, write]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-07
---
Comments in code must be sparse. One line max, and only if ever. The code should be self-documenting through naming and structure, not narrated by block comments. Malachi flagged the INC-005 PR for adding 5-6 line block comments above each change.

**Why.** Multi-line rationale comments are clutter, they drift out of sync with the code, and they duplicate what belongs in the PR description, commit message, or ticket. Dense comments also read as machine-generated.

**How to apply (all code I write).**
- No multi-line block comments. If a comment is genuinely needed, one terse line.
- Default to none. A one-liner is justified only for a non-obvious magic constant or a real gotcha, nothing else.
- Put the why (rationale, incident refs, tradeoffs) in the PR description, commit message, or runbook/ticket, not in the code. Specifically: no ticket IDs (AUDI-XXXX) and no env-var / kill-switch explanations in comments — the `Variable.get("SPARK_EVENT_LOG_ENABLED")` guard names itself (AUDI-1191, 2026-08-04).
- A one-line comment is warranted only where the logic is genuinely non-obvious, e.g. a platform constraint the reader can't see: `# Dataproc Serverless allows event logging or a history server, not both`.
- Prefer self-documenting code: clear names, small functions, obvious structure.
- Script headers too (reinforced 2026-08-07): a methodology essay in a shell-script header is the same violation. Header = usage + a pointer; the methodology goes in `docs/<topic>.md`.
- Docstrings: a concise one-line what-it-does is fine. Don't turn a docstring into a rationale essay.

Same family as [[feedback_minimize_complexity]] and [[feedback_no_unsolicited_suggestions]].
