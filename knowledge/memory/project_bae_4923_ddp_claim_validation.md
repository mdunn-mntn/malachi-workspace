---
name: project-bae-4923-ddp-claim-validation
description: "BAE-4923 (Sherwin) validated free-log preemption at ~$43K/mo; reviewed 2026-08-05 — his 6 months reproduce to the cent but he recomputed the 1/N denominator as ARRAY_LENGTH (double-counts 33Across DS28+DS40, +15-19%) and averaged a growing series; corrected July run-rate = $64,076/mo = $768,916/yr, which lands on Mike's $800K claim"
doc_type: memory
domain: vendor
lifecycle: active
last_verified: 2026-08-05
keywords: [bae-4923, ddp business claim validation, sherwin ocampo, mike dolzer, preemption validation, free log preemption, ddp_mm_winners_imp, mm_dsid_count, array_length denominator, 33across dedup, ds28 ds40, audi-1093, audi-1113, audi-1111, 768916, run rate vs average]
metadata:
  type: project
---

**BAE-4923 "DDP Business Claim Validation" is BAE's own confirmation of the free-log preemption
thesis — and the true number is HIGHER than they reported.** Malachi flagged this ticket as
important to the vendor work 2026-08-05; reviewed same day.

Ticket: https://mntn.atlassian.net/browse/BAE-4923 — Support, **status Done**, assignee Sherwin
Ocampo, reporter Mike Dolzer, created 2026-07-21, P2 for mid-August. Mike's ask: have Sherwin/Maya
validate his vendor-quality claims, "could save us as much as 800k/yr." Mike's linked sheet is
**our own** `audi_1089_verify_claims.xlsx` (owner malachi@mountain.com), not a BAE artifact.
Sherwin's results sheet `150Robua_GKHyfnI0JuvEuAjPya7F3938eXpJQ5exGNs` (owner sherwin@mountain.com,
2 tabs: data + `queries`).

**His claim (comment 602686):** on winner sets containing both a free source and a usage-based DDP,
shifting the DDP credit share to free saves ~$43K/mo. Source = `dw-main-gold.reporting.
ddp_mm_winners_imp_YYYYMM`, the same gold anchor AUDI-1115 uses — so it measures the meter
directly, at the impression-winner grain, which is the RIGHT grain for preemption (our $273.7K/yr
visit-grain and $412.4K/yr vertical-grain numbers answer different questions, see
[[project_audi_1111_vendor_quality]] and [[reference_ddp_billing_logic]]).

**Review verdict: method sound, conclusion holds, number too low.** All six of his monthly cells
reproduce to the cent. Two independent errors pointing opposite ways:
1. **Double-counts 33Across (inflates 15-19%/mo).** He shadowed the native `mm_dsid_count` column
   with a recomputed `ARRAY_LENGTH(mm_dsids_winner)`. They differ on 34.2% of rows and the gap is
   exactly the DS28+DS40 dedup — see [[reference_ddp_billing_logic]] and the data_catalog entry.
2. **Averaged a steeply growing series (understates far more).** Jan $36.2K → Jun $60.1K, volume
   2.35x. July (`_202607`) existed and he did not use it.

Corrected (vendor-deduped): Jan 31,400.80 · Feb 33,094.91 · Mar 33,046.63 · Apr 33,747.91 · May
41,115.40 · Jun 50,934.30 · **Jul 64,076.31**. So: his stated $43,257/mo = $519,084/yr; corrected
May-Jul $52,042/mo = $624,504/yr; **corrected July run-rate $64,076/mo = $768,916/yr**.

**The convergence worth quoting:** corrected July annualizes to ~$769K/yr, landing on Mike's
"$800K/yr" from an independent direction, against a metered roster of ~$812K/yr. **Preemption alone
recovers ~95% of what dropping every metered vendor would, without dropping anyone.**

**The ticket is already Done** — the live ask is implementation (AUDI-1113), not more proof. The one
question only BAE can settle: does 33Across get one credit share or two (i.e. is `mm_dsid_count` the
denominator billing actually applies)?

Query + full series: `tickets/audi_1111_vendor_quality/queries/bae_4923_preemption_recon.sql`,
`outputs/bae_4923_savings_reconciliation.csv`. Narrative: that ticket's `summary.md` §5b.

**Drive access (fixed 2026-08-05):** the Drive MCP connector is authed to a personal gmail and
cannot see MNTN files. Read MNTN Sheets via the gcloud token instead — `gcloud auth login
--enable-gdrive-access` once, then `curl -H "Authorization: Bearer $(gcloud auth
print-access-token)" https://sheets.googleapis.com/v4/spreadsheets/<id>`.
