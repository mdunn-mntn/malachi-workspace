---
name: feedback_verify_claims_against_code
description: "A capability claim must be checked against the implementing code, never against another doc — a false claim in a source comment launders itself through the generated inventory into every doc that cites it"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [false capability claim, doc laundering, generated inventory, COMPONENTS.md, dry-run gate, bq_run.sh no cost gate, claim vs code, drift check, unwired check, verify against source, adversarial review, pairing rule]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-08-12
---
When a doc says the system does X, verify X against the code that would implement it — not against
another doc that says the same thing. Docs cite each other and a single false line propagates.

**Why:** 2026-08-12, an adversarial review of the workflow kit found two claims that had been true
nowhere for months:
- **"`bq_run.sh` has a dry-run gate."** It does not; its own comment reads `Pure instrumentation: NO cost
  gate, NO warning, NO preemption` (deliberate, per [[feedback_bq_workflow]] — cost is not a blocker here).
  The false phrase originated in `enforce_bq_wrapper.sh`'s **header comment**, which `build_kit_manifest.sh`
  reads to generate `COMPONENTS.md` — so the drift-proof, generated inventory was laundering the error
  into every doc that cited it as the source of truth. A generated doc is only as true as what it reads.
- **"A diff in the component inventory means a component was added without updating the docs."** Nothing
  called `build_kit_manifest.sh` — not `verify.sh`, not `workflow_audit.sh`, not the commit gate. The
  drift detector was described in three files and existed in none. Now wired into `verify.sh`.

The general shape: an unwired check reads exactly like a working one, and a claim in a comment reads
exactly like a verified fact. Both cost nothing to write and are invisible until someone tries to rely
on them.

**How to apply:** grep for the mechanism, not the sentence. For "X is enforced", find the line that
exits non-zero. For "Y is generated", find the caller. Every prose rule that matters needs a partner
that fires when it is skipped — an instruction with no detector is a wish. When correcting the claim,
fix it at the SOURCE (the comment the generator reads), then regenerate, or it comes straight back.
See [[feedback_hold_evidenced_verdict]], [[reference_commit_gate]].
