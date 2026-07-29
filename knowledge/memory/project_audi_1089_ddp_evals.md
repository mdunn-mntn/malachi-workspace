---
name: audi-1089-ddp-evals
description: "AUDI-1089 DDP evals — billing-review workbook (Drive Tickets/AUDI-1089) w/ 2 grains: DOMAIN same-visit $275K + VERTICAL $412K preemption; BAE crediting mechanism confirmed (1/N incl free logs at $0; ours=incremental full-preemption); read the actual credit script + change point; plan Mike Dolzer→Andy Everson→implement (backlog AUDI-1143); Malachi likely to own pipeline"
metadata: 
  node_type: memory
  type: project
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [audi_1089_ddp_evals, audi, 1089, ddp, evals, billing, review, workbook]
domain: [project]
lifecycle: active
last_verified: 2026-07-22
---
**2026-07-21 CURRENT STATE (supersedes the older prior-day framing below):** Primary deliverable is now the
**billing-review workbook** `artifacts/audi_1089_billing_review.xlsx` (in **Google Drive `Tickets/AUDI-1089/`** —
Drive mounted locally, write straight there). Sheet 2 = TWO preemption grains: **DOMAIN (same exact ip×domain×date)
$274.6K** + **VERTICAL (same MM vertical, how MM bids) $412.4K** → roster $812K→$400K. (The prior-day "$200K /
augmentor-fix" detour was REVERTED — IP-grain check proved same-day cohold is genuine, not tautology; see
[[reference_ddp_billing_logic]].) Worth-vs-bill uses MONEY-MADE only (no metered vendor pays for itself). Verify-
claims query pack + pipeline reference also in Drive. **BAE meetings 7/20 (pipeline) + 7/21 (crediting sync):**
credit = 1/N split incl free logs (23/30) at $0 CPM (they take unpaid slots via `mm_dsid_count`) → free ARE
partially preempted; **ours ($275-412K) = INCREMENTAL saving from FULL preemption** (pay $0 when free won).
Read the actual credit script (`SteelHouse/bae-sql-utility/ddp/usage reporting`) — walkthrough + exact change
point in `artifacts/ddp_pipeline_and_crediting_reference.md`. guid-causality caveat DOWNGRADED (guid=MNTN pixel,
household's own visits). **PLAN:** Mike Dolzer (BAE PM) validates → vendor decision → Andy Everson (contracts)
blessing incl. can-3P-interest-vendors-cover-same → implement credit change = **backlog AUDI-1143**. Alyson→Kale;
Malachi likely to OWN the DDP pipeline. Ownership: nobody owns credit logic today; Andy owns contracts+flat fees.

**AUDI-1089** renewal evals — deliverable workbook COMPLETE (`outputs/audi_1089_quality_template_filled.xlsx`,
139 rows x 14 vendors, 4 sheets; see [[reference-audi-1089-template-workbook]]). Verdicts stand:
KEEP 5x5/Predactiv (lock flat; HEM blocker) · KEEP-trim Justuno · NEGOTIATE 33Across+API combined
(~$598K/yr, one vendor) · DROP Sovrn/Cybba · **Klickly drop-unless-~free APPROVED in team review
2026-07-13** (transcript audi_1089_01; answer to Paulo due same day). **Cybba flip-option:** top-2
genuine per-unit economics ($228-247/1M pairs) — offer keep IF 50-100x scale at flat/capped price.

**2026-07-13 discoveries:** (1) **AUDI-1092 RESOLVED** — meter regime changed at 2026-05
(Jan-Apr fractional 1/N, May+ integer single-credit; residue analysis); June q3b LOO savings
VALID (exact-to-conservative floors). (2) **AUDI-1093** — free logs do NOT preempt paid credit
(Sean Yang); exact recoverable $273.7K/yr at visit grain (33Across $222K); preemption SUBSTITUTES
for drops on the overlap slice. (3) q3c visit grain: free logs 59.4% coverage; augmentor DS30
dominant (48.8% alone); 33Across 66% same-day-duplicated. (4) Net-of-free ladder: 33Across
standalone 0.94x bill (revenue basis; pay-up-to = x15-30% margin → converges with $30-100K WTP
band); every other metered vendor 0.02-0.34x AT ANY position. (5) q7e/q7f: sole-IP low VR is real
darkness, not attribution loss. (6) **Roster P&L at kept margin: $812K metered bills buy $47-141K
of margin → net -$670-765K/yr** ("we pay as if we keep 100% of CPM; we keep ~20%"). (7) Flats are a
TWO-AXIS call: revenue break-even fees ($4-20K) vs domain-axis bands ($0.15-3M) differ 30-300x —
breadth vendors priced as classifier coverage, not media performance. User independently replicated
the standalone analysis — same conclusion (methods convergence). (8) **Fangorn endgame (Matt
Brorby): most actives already on DS46 (guid-only), forcing the DS13 tail is fine (Alex owns) —
vendor value trends to DS19-MM-Core-only; Tuesday framing = sunset roster on the DS13 migration
timeline; ask Alex for the date.** (9) **q3d: scored HI/PP audiences VENDOR-INDEPENDENT — k=4
keeps 99.9991% of HI (53 of 5.96M IPs lost); free-only 99.94% HI / 99.25% PP; vendor reach value
concentrates in ~10 ad-invisible verticals (health/beauty/travel 0-26% retained free-only; k=4
>=94% everywhere). Charts q3d_score_coverage/vertical_impact.png; scenario table has HI/PP cols.**

**2026-07-14: SOLO sheet COMPLETE** (user ask — each vendor as ONLY paid source, overlap vs
free logs only; see [[reference-audi-1089-template-workbook]] for mechanics; q8a/q8b landed
same day, all anchors passed, 0 pending). MEASURED solo: T2_solo runs 3-5x ABOVE density
estimates (33Across $724K vs $397K est — estimates inherit sole-cohort adverse selection);
33Across feeds clear bills on REVENUE basis (1.6-2.5x — vendor will argue this lens) but
**no metered pay range (10-30% margin) reaches its bill even solo** (33Across $72-217K vs
$422-456K; Sovrn 0.97x / Justuno 0.48x / Cybba 0.45x revenue basis). Flats measured: 5x5
$281K → pay $28-84K; Predactiv $202K → $20-61K; Klickly $13K → $1-4K (vs Maya's fees).
Only 33Across feeds have solo-bill upside (DS28 +8%, DS40 +75%); small meters' junk credit
uncontested → solo bill == today's. 33Across solo HI = 2,727 (vs 92 sole-HI) — paid overlap
drives tiny sole counts. NEVER sum solo columns (overlapping cohorts; ladder marginal = only
additive lens). Convergence: visit-day dup-with-free == AUDI-1093 preemption shares exactly.
Sovrn junk signature: raw-lens solo-HI +68% vs usable lens.

**2026-07-15: post-preemption economics answered** (user Q: bills if free logs stop paying for
co-held data): **$812K → $539K/yr (−$273.7K, −33.7%), data kept, pay ranges unchanged by
construction.** NO vendor flips on the portfolio lens; on the measured-solo ceiling lens 33A API
lands exactly AT fair ($134.0K == pay top) and the combined 33Across pair at 1.05x — preemption +
renegotiation STACK to fair for 33Across only; Sovrn (−0.2%)/Justuno (−4.9%) barely move (junk/
unique credit, not overlap) — DROPs unchanged. Workbook: 144 rows, decisions block 5
(POST-PREEMPTION), chart q12_post_preemption.png; anchor tripwire asserts total stays ~$273.7K.
Decisions sheet blocks (actual, corrected): calls / scenarios / ladder / post-preemption —
vertical impact lives in charts (q3d_vertical_impact.png), NOT a decisions block.

**2026-07-15 pm:** (a) **WASTE tab** shipped (boss ask): measured GB/day per vendor via gsutil on
svs data_source_id partitions (q14 script; paid ~156 GB/day ≈ 57 TB/yr, 33Across 106; no-TTL footprint
39.3TB → storage floor $9.4K/yr; Kafka/DAG/classifier compute needs Data Eng — draft ask delivered);
junk-that-bills framing (33Across 22% thrown + 36% webmail/Googlebot that PASSES DS19 and bills).
(b) **DS19 exposure ANSWERED (q13a+q13b landed):** DS19-only free coverage 64.3% pair / 64.2%
visit-day / 61.6% true-path (BETTER than the 60.4/59.4 union); lost 30% of member IPs is dark;
every scored tier ≥99% free-covered incl. Max Reach 99.4%; **collapsing keyword categories are
ad-infra junk** (Paid Advertising 0.2% / Ad Platforms 0.3% retained — cookie-sync URLs as
keywords; real categories hold: Search 98%, News 81%; Baked Goods 46M/17% = the one legit-looking
dependent, likely content-farm). DS19 lens does NOT rescue the roster. Old partial note:  free logs keep 69.7% of 271M DS19-member IPs; vendor-only 30% is
dark (0.48% serve, VR 0.061%, 92% unscored); every scored tier ≥99% free-covered incl. max-reach 99.4%
— keyword UI counts shrink ~30% but scored/servable keyword audience is vendor-independent. q13a
(~40TB: coverage masks + per-category sizes w/ taxonomy names) in flight. (c) User-caught grain fix:
q2c funnel numbers are PER-DAY (1-day sample) — sheet now grain-marked.

**2026-07-15 eve — ALL scans landed, eval measured end-to-end:** q13a (DS19-only coverage
64.3/64.2/61.6% vs union 60.4/59.4; collapsing keyword categories = ad-infra junk; scenario DS19
columns + decisions DS19 block + 2 charts); q15/q15b/q15c (combined free_logs column COMPLETE:
**T2-free $602.9K/yr > any vendor's sole-T2**; union sole 44M IPs / 2.15B pairs / 50.8K classified
domains; fresher-or-tied 89.7%; closing chart q15_free_union_value.png "free logs are the biggest
vendor"). 3-lens completeness audit PASSED after fixes (q14 samples embedded — was the only
non-regenerable artifact; 4 legacy headers retrofitted; durable-homes policy README §6b). Boss zip
final: 34 files. THE POPULATION PRINCIPLE named: vendors inflate audience-COUNT denominators;
served members stay 97.6-99.9% free-covered (tables now label SERVED vs ALL-members populations).

**2026-07-15 night — org-share validation package ready:** runbook/queries/ +
VALIDATION_GUIDE.md (glossary/windows/anchors/wrapper-substitution) + genericized MANIFEST;
zip = 35 files. Outsider header review (4-agent fan-out) fixed header rot: q0/q1d stale credit
regime, q8b anchor equality→diagnostic, q3d k4 def, q7e claim/output mismatch. THREE population
traps now documented at point of use (SERVED vs ALL members; degenerate own-world share;
summed-pool 13.3% vs distinct 99.9% — avg HI household delivered by ~7.5/10 sources).

**Ticket closures 2026-07-22:** AUDI-1092 (credit model CONFIRMED 1/N fractional, not first-reporter — Done),
AUDI-1093 (preemption spec Done; impl→AUDI-1113), AUDI-1143 (dup of 1113, closed), AUDI-1091 (augmentor
full-source spike Done = NO-GO: svs DS30 = augmentor BANNER slice per Sean's
`dsid30_augmentor_log_processing.py`; the dropped VIDEO placements are 75% of rows but 99.6% URL-less
CTV/video → not site visits → net-new ~1% of rows; folder `tickets/audi_1091_augmentor_full_source/`).

**Remaining:** flat fee amounts + renewal schedule (Maya — the ONLY blocker); Sherwin
engagement on AUDI-1093 fix (execution AUDI-1113/1144/1145); DS13-tail migration timeline from Alex (sets vendor-sunset date);

Data Eng reply on ingestion compute (Kafka/DAG/classifier).
DONE 07-13/15: q3d, MANIFEST.md (30 files incl. q14 gsutil script), saved() boundary fix,
SOLO sheet complete (q8a/q8b landed, 0 pending), post-preemption block, WASTE tab, q13b. Sequencing: lock flat fees BEFORE drops; renegotiate before dropping;
preemption fix before/instead of overlap-motivated drops.

**2026-07-16 — stakeholder package COMPLETE:** deck sheet xlsx (9 tables, 0 pending), 8 deck queries
(deck_d1-d8, one per table; copy-paste proven from clean folder; zip byte-verified), 9-slide technical
TLDR deck (RevealJS gist; user's Net Kept Margin formula). All proof runs landed and reconciled
(d8: served signal 63.66% free-covered; d2/d7 exact). Free-row standalone convention = vs ALL paid.
d1 relaunch LANDED 07-16 (anchors exact, workbook final — 'upload this one' delivered). Follow-on work moved to epic [[audi-1111-vendor-quality]]. Still external: flat fees (Maya), Data Eng ingest costs, AUDI-1093 rollout.
Related: [[reference-ddp-billing-logic]] [[reference_ddp_valuation_framework]] [[reference-audi-1089-template-workbook]].
