---
name: feedback_read_full_source_before_verdict
description: Before asserting what a ticket/PR fixed, read the FULL comment thread and OPEN the linked PR to check its real state — a "Done" status and a truncated read lie.
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [read the full thread, open the linked PR, ticket status lies, Done doesnt mean deployed, PR merged vs draft, truncated comments, verify before concluding, AUDI-1072, PR 1147, gh pr view, full comment thread]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-29
---
When diagnosing "what was done / was this fixed" from a Jira ticket, **read the entire comment thread and OPEN every linked PR/commit/dashboard to check its real state — before asserting a verdict.** A ticket's `Done`/`Resolved` status is not evidence the root cause shipped, and a partial (head/tail) read of the comments is not "reading the comments."

**Why:** AUDI-1072 (aud22 geo audit, 2026-07-29). I first read only the head+tail of the comments and never opened PR #1147, then told Malachi it was a "2-ZIP patch, closed as fixed." He pushed back twice — "are we SURE it's AUDI-1072? did we read all the comments and the links... there's GitHub PRs mentioned." Reading everything reversed two claims: PR #1147 is a GENERAL fix (not 2 ZIPs), and it is still an **open draft, never merged/deployed** (the ticket was marked Done only because DM suppressed the audit noise). My first answer sounded evidenced but wasn't — it came from an incomplete read.

**How to apply:**
1. Pull ALL comments (not `head`/`tail`) and enumerate every link (`grep` the bodies for `github.com/.../pull/` etc.).
2. **Check the PR's actual state before citing it:** `gh pr view <n> --repo <org/repo> --json state,mergedAt,isDraft` + `gh pr diff`. Open ≠ merged; draft ≠ shipped; "fixed on their end" often means a suppression/workaround, not the model fix.
3. Treat a ticket's `Done` as a claim to verify, not a fact — reconcile it against the PR/deploy reality.
4. **When challenged with "are you sure / did you read it all," re-verify the primary source — don't defend the prior answer.** Revising a claim that came from an incomplete read is correcting an unevidenced assertion, NOT folding under pressure (that distinction matters vs [[feedback_hold_evidenced_verdict]]: hold a verdict only once it's genuinely sourced; if it wasn't, go read and fix it).

Related: [[feedback_hold_evidenced_verdict]] (the complement — don't fold once the verdict IS fully evidenced), [[reference_aud22_geo_reporting_sync]].
