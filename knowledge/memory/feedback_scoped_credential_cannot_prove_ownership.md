---
name: feedback_scoped_credential_cannot_prove_ownership
description: "An inventory taken through a scoped credential bounds only what that credential can see — our OpenAI key lists only files WE uploaded, so no sweep of ours could ever settle who filled a company-shared 2.5 TB project cap; state the instrument's scope before turning a measurement into an ownership claim"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [scoped credential, instrument blind spot, ownership claim, shared quota, shared project cap, files.list only own uploads, absence of evidence, cannot prove absence, measurement scope, api key scope, openai 2.5TB, AUDI-1321, AUDI-1301, quota ownership, inventory does not prove ownership, green submit proves headroom not ownership, person knows what instrumentation cannot see]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-09-05
---

**Before a measurement becomes an ownership claim, say what the instrument can see.** A sweep run through a
credential that is scoped to your own objects bounds YOUR footprint and nothing else — it cannot distinguish
"nobody else is here" from "I cannot see anybody else."

**Why (AUDI-1321, 2026-09-03 → 2026-09-05).** OpenAI's 2.5 TB file-storage cap is per PROJECT, and the MNTN
Match pipeline runs in the **company-shared default project**. Our API key can list only the files we
uploaded. Three days of work read our own inventory as proof of ownership anyway:

- 09-03: a sweep deleted its full eligible set and the next `batch_submit` went green ⇒ "the storage WAS ours,
  no other producer was holding it." **A submit succeeding right after our own deletions is equally consistent
  with a shared pool we merely made room in** — and `batch_submit` dies on the FIRST ~40 MB upload, so a green
  submit proves "there was room for one file", never "the store is clean."
- 09-04: a per-file byte inventory read 129 files / 4.2 GiB, "1.4 GiB of it not ours" ⇒ read as settling it.
  Same defect one layer up: 1.4 GiB is what our key can SEE that is not ours.
- 09-05: Malachi named the shared project from direct knowledge. Contradicting evidence had been sitting in
  the logs the whole time — two consecutive sweeps read `6221 files holding 198.8 GiB, 7.8% of the 2.5TB
  project limit` and `6040 files holding 191.8 GiB` while `files.create` kept returning a deterministic `400`
  exceeded-quota. Our own numbers never approached the cap; the cap kept firing.

**What actually settled it was a person's knowledge of an account our instrumentation cannot inspect** — not a
better measurement. That is the shape to expect whenever the question is about a resource outside the boundary
of your credentials.

**How to apply:**
1. When writing a conclusion from an inventory/listing/scan, write the SCOPE in the same sentence: "everything
   our key can list", not "the whole store". The scope is what a future reader needs to re-derive the claim.
2. A negative ("no other producer") needs evidence that could have shown the positive. If the instrument
   cannot render other producers at all, the negative is unprovable — mark the question OPEN and name who or
   what could close it (an org dashboard, an owner, an audit log).
3. Prefer the cheap human check when it is the only thing that CAN answer it. The kill criterion here
   ("escalate for dashboard access") never fired precisely because a self-scoped measurement kept looking
   conclusive; three sessions of instrumentation work would have been shortened by one question.
4. Corollary for planning: when a cap is shared, headroom is not yours to predict. Keep standing inventory
   small and never size a backfill against the whole cap.

[[feedback_hold_evidenced_verdict]] [[feedback_contradictions_are_appended]] [[feedback_validated_is_not_correct]]
[[reference_mntn_matched_batch_pipeline]] [[reference_openai_sdk_pagination]]
