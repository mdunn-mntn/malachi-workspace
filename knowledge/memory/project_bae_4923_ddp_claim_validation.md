---
name: project-bae-4923-ddp-claim-validation
description: "BAE-4923 (Done, Sherwin Ocampo) independently validated the free-log preemption thesis at ~$43K/mo (~$516K/yr) vs our $273.7K/$412.4K per yr — reconcile the grain gap; two Google Sheets are the evidence and both are 401-locked to current creds"
doc_type: memory
domain: vendor
lifecycle: active
last_verified: 2026-08-05
keywords: [bae-4923, ddp business claim validation, sherwin ocampo, mike dolzer, preemption validation, free log preemption, 43k per month, audi-1093, audi-1113, audi-1111, vendor renewal, drive access blocker]
metadata:
  type: project
---

**BAE-4923 "DDP Business Claim Validation" is BAE's own confirmation of our free-log preemption
thesis, and it is important to the vendor work — Malachi flagged it explicitly 2026-08-05.**

Ticket: https://mntn.atlassian.net/browse/BAE-4923 — type Support, **status Done**, assignee
Sherwin Ocampo, reporter Mike Dolzer, created 2026-07-21, labelled P2 for mid-August. Mike's ask:
have Sherwin/Maya validate his vendor-quality claims, "could save us as much as 800k/yr."

**Sherwin's finding (comment 602686, 2026-08-04):** on winning MM segments where the winners
include **both** a free source (guid or auglog) **and** one or more usage-based DDPs, impression
credits are currently spread across all winning sources; shifting the usage-based-DDP share to the
free sources saves **~$43K/month (~$516K/yr)**.

**The gap to reconcile:** ours is $273.7K/yr at (ip × domain × date) visit grain and $412.4K/yr at
DS13 vertical/category grain — see [[project_audi_1111_vendor_quality]] and
[[project_audi_1089_ddp_evals]]. Sherwin's ~$516K/yr is ~1.9x the visit-grain number. Prime
suspects for the divergence: **credit grain** (his "winning MM segments" reads as the
impression-winner grain, ours as visit grain — those swing ~5x per AUDI-1115), the 1/N-split vs
full-preemption rule ([[reference_ddp_billing_logic]]), and the month/window used.

Mike's "$800K/yr" is a different claim from the preemption slice — our metered CPM roster is
~$812K/yr at June run-rate, so $800K reads as *drop everything metered*, not preempt.

**Evidence (both 401-locked as of 2026-08-05):**
- Sherwin's results: `docs.google.com/spreadsheets/d/150Robua_GKHyfnI0JuvEuAjPya7F3938eXpJQ5exGNs`
- Mike's queries/analyses: `docs.google.com/spreadsheets/d/1tVhe2vBr6q8VV9tbdvB4wZpDNqi0hwvc`

**Access blocker + fix:** the Drive MCP connector is authed to Malachi's personal gmail, not
`malachi@mountain.com`, and the `gcloud` credential for `malachi@mountain.com` has no Drive scope
(`cloud-platform`/`compute`/`email` only), so `docs.google.com/.../export?format=csv` returns 401.
Unblock with `gcloud auth login --enable-gdrive-access`, or a Drive shortcut into `My Drive/` so the
local mount syncs it, or a manual CSV export into the ticket's `outputs/`.

**Note the ticket is already Done** — if BAE considers this settled, the live ask is implementation
(AUDI-1113), not further proof.

Review checklist and full context: `tickets/audi_1111_vendor_quality/summary.md` §5b.
