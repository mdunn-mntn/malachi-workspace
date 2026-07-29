---
name: reference-ddp-billing-logic
description: "DDP billing REGIME 2026-05: usage-table imps integer May+ BUT cross-PATH fractional split alive upstream (gold winners table; 3P-segment path in denominator); free logs never bill (tv_cpm=0), paid bills on mixed rows = preemption gap 291M imps/mo; DS17 segments $0.95 CPM; gold ddp_* family = BAE billing tables; 8-step pipeline billed FUNNEL-1/CTV-ONLY; row-level targeted_signal is BQ-queryable (external.targeted_signal, source_data_source_id partition — NOT Athena-only); enriched_impressions access-denied (PAM)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [ddp billing, targeted_signal, usage_reporting_data, 1/N fractional split, free log preemption, 33across, augmentor, ddp_mm_winners_imp, DS13, DS19, tv_cpm, AUDI-1089, AUDI-1092, AUDI-1093, enriched_impressions, sherwin ocampo, mntn id crediting]
domain: [pricing, business]
lifecycle: active
last_verified: 2026-07-29
---
DDP billing (**AUDI-1092 CLOSED/Done 2026-07-22** — credit model CONFIRMED in code + BAE testimony,
supersedes the residue-only reading below). **The current model is a 1/N FRACTIONAL split, NOT
first-reporter** — confirmed by reading the crediting script (`SteelHouse/bae-sql-utility/ddp`, 07-21)
+ BAE meeting owners (07-20/21): credit = `impression_cnt / mm_dsid_count`, N = all sources with the
category in `targeted_signal` in the 30d before the imp, free logs (23/30) in N at $0 CPM, winner layer
OR=lowest-CPM/AND=highest-CPM (see the "BAE MEETINGS" block below + data_knowledge § "DDP MM crediting
mechanism"). The 07-13 residue read below (May+ "INTEGER single-vendor credit / first-reporter") was a
usage_reporting_data rollup artifact — the upstream winners table (`ddp_mm_winners_imp_202606`) shows
fractional cross-path 0.5 splits still alive in June; ±7-42%/vendor winners→meter residual unreconciled
but doesn't change the model. HISTORY (07-13 residue analysis): Jan-Apr 2026 usage rows ~100% decimal
(halves/thirds/quarters), May-Jun 2026 ~100% integer; switch coincides with augmentor entering svs.
**June-bill q3b LOO savings VALID as FLOORS** (metered-to-metered overlap negligible 0.03-8.6% → drop
recovers 81-100% of bill; Sovrn ≥$109K, 33Across ≥$386K/yr). Never use Jan-Apr bills for LOO arithmetic. Billed MM imps
fell 36% Apr→Jun. Signal is **paid only if used** — "used" is an OR across
consumers (DS13 verticals OR DS19 product-categories; DS19 has NO blocklist/parse gate, so junk and yahoo
bill through it). Row-level used table = `targeted_signal` — **queryable in BQ (corrected 2026-07-20; the
earlier "Athena only" was wrong):** `dw-main-bronze.external.targeted_signal`, BQ external over
`gs://mntn-data-archive-prod/signals/targeted_signal/` parquet, hive-partitioned on `data_source_id`
(4=CRM/13=MM-vert/19=MM-prodcat) × `dt` (2025-07-31→current) × `source_data_source_id` (credited vendor);
partition-col aggregations bill $0. ⚠ rows = RAW used events (uid×ip×dscid), NOT billed imps (33Across
~591M rows/day vs ~70M billed/mo) — DS13/19×vendor DECOMPOSITION only; $ still needs first-reporter/split.
This unblocks the per-vendor used-row split that AUDI-647/1089/1115 kept punting to Athena/Victor. Companion
`external.targeted_signal_domain` (uid→domain). Chain: svs → targeted_signal → mntn_matched_reporting
(Athena, not re-checked) → `coredw.usage_reporting_data`
(BQ; **dt = month-end snapshots only**; domains.list = billed domains, MM CPM vendors only) → Maya Triman pays.

**Exact reassignment (q3b, 30d masks):** metered-to-metered overlap is NEGLIGIBLE (0.03-8.6%; 18.7% only
between the 33Across sibling feeds) — dropping a metered vendor recovers 81-100% of its bill (Sovrn 94%:
its overlap is 81% with FLAT-FEE vendors which absorb credits free). Free logs alone cover 60.4% of usable
pairs; 97-99% of sole-IP serves are prospecting (membership-gated → vendor-dependent, max-reach included).
augmentor in svs since 2026-05-12 → displaced ~$29K/mo of 33Across-family credits by June.
Flat-fee vendors (5x5/Predactiv/Klickly) are paid regardless of use — renewal date is the only lever.
**Free logs do NOT preempt paid credit** (Sean Yang 2026-07-13): vendors earn day-grain credit on
signals guid/augmentor also capture — **$273.7K/yr recoverable** with a free-preemption rule, exact
at (ip,domain,date) grain from q3c (33Across $221.7K) — AUDI-1093. Preemption substitutes for drops.
**Business case finalized 2026-07-15:** post-preemption bills $812K → $539K/yr (33Across $200.4K,
API $134.0K, Sovrn $115.6K, Justuno $73.3K, Cybba $15.4K); cuts are cost-only (vendor unique value
untouched by construction). NO vendor flips worth-its-bill on the portfolio lens; 33A API lands
exactly AT its measured-solo ceiling pay top, combined 33Across pair 1.05x — preemption +
renegotiation stack; Sovrn/Justuno bills are junk/unique credit, barely cut. Pair-grain strict
variant ≈ $284K (barely more than visit grain). Workbook decisions POST-PREEMPTION block +
q12_post_preemption.png.

Full detail: data_knowledge.md § Site Visit Signal (credit-model evidence 2026-07-13); runbook/dependency_valuation.md (LOO + frontier).
Related: [[reference_ddp_valuation_framework]], [[audi-1089-ddp-evals]], [[feedback-take-rates-sensitive]].

**2026-07-17 upstream nuance (AUDI-1115 §4f, gold `dw-main-gold.reporting.ddp_*` family — Alyson's
pointer):** the "May+ integer" reading applies to `usage_reporting_data.impressions`, NOT the
allocation upstream — `ddp_mm_winners_imp_202606` shows **cross-PATH fractional splitting alive in
June** (impression matching a 3P segment path AND the MM path → impression_cnt 0.5 on the MM row;
exhibit ad_served_id f05c2bac-e547-4eb0-b49d-1abe16d3955c: DS17 ShareThis segments @ **$0.95 CPM**
+ DS19/33Across @ $0.50). `tv_cpm`=0 on 100% of free-only-winner rows (free logs never bill);
$0.50 on 91.7% of mixed free+paid rows = **AUDI-1093 preemption gap visible in the billing table
(291.1M imps/mo)**. NO simple winners-table aggregation reproduces the usage meter exactly
(±7–42% by vendor) — exact BAE downstream allocation = 2026-07-20 billing-sync question.
Canonical recon: audi_1115_l0b_bae_winners_recon.sql.

**BAE MEETINGS 2026-07-20/21 — the ACTUAL crediting mechanism + reconciliation of the whole preemption thesis
(Sherwin Ocampo, who runs the meter; transcripts audi_1089 meetings 03/04):** MM/CRM impression credit is a
**1/N fractional split** across ALL vendors that have the category ID in `targeted_signal` in the **30d prior**
to the impression (simple lookup, NOT most-recent-vendor). **Free logs (guid 23, aug 30) ARE in the divisor N
but at $0 CPM** (unpaid slots) → they DILUTE paid credit (bigger N) but do NOT eliminate it: a paid vendor still
earns (1/N)×$0.50 on free-covered imps. This RECONCILES our `ddp_mm_winners_imp.tv_cpm`=$0.50-on-free-covered
finding (the $0.50 = the paid vendor's residual fraction; free's fraction is $0). Separate AND/OR layer: OR →
lowest-CPM provider wins (free log at $0 wins OR), AND → highest-CPM; the OR-lowest-CPM was a deliberate later
"big cost saving". Interest-segment co-targeting (LiveRamp/ShareThis) takes a slot first, further shrinking MM
vendor share. **So the AUDI-1089 preemption $ (~$275K same-date / $412K vertical) is the INCREMENTAL saving from
FULL preemption (pay paid vendors $0 when a free log covers the imp), on top of the partial preemption already in
place** — Malachi's proposal; Sherwin: "we're onto something", valid direction. Sherwin floated a
causality caveat (guid coverage might be vendor-caused: vendor → impression → advertiser-site visit → guid) but
**per Malachi DON'T over-weight it** — guid_log is MNTN's own pixel and those visits are overwhelmingly the
household's own behavior (organic/direct), not vendor-manufactured; it's a speculative edge case. **Keep free
logs = guid + augmentor TOGETHER (do NOT build an augmentor-only carve-out).** The $275K/$412K numbers stand.
**OWNERSHIP: NOBODY owns the credit logic** — BAE (Sherwin/Maya under Kristen Colley) just execute what they were
told at MM launch; **Andy Everson owns vendor contracts/terms + has the flat-fee $ (BAE has no visibility)**;
changing it needs Andy's blessing + contract-terms check; Mike Doltz/Kristen formalize review time. NEXT: Malachi
→ digestible queries → Mike Doltz → BAE validation → maybe implement full free-log preemption (needs Andy). Not urgent.

**REVERTED 2026-07-21 — the DOMAIN measure is SAME-DATE cohold ($274.6K), NOT prior-day ($200.4K).** The
prior-day adjustment below assumed same-day augmentor cohold was tautological; the IP-grain check DISPROVED
that (same-DOMAIN cohold 52.9% vs any-domain 88.5% — if augmentor were tautological same-domain would be
~88%). So 52.9% is a GENUINE same-visit overlap, not augmentor noise. Correct domain measure = "did a free
log have the same exact (ip,domain,date)?" = q3c/`q3g_domain_sameday_cohold.sql` = **$274.6K** (33Across
$223.3K, API $41.7K, Cybba $6.1K, Justuno $3.4K, Sovrn $0.2K). The billing WORKBOOK sheet 2 now shows DOMAIN
(same-visit) $274.6K + VERTICAL $412.4K; the "Augmentor Fix / Why $200K not $274K" sheet was REMOVED (confusing).
Ignore the prior-day $200.4K / $612K residual below — kept only as history.

**2026-07-20 augmentor prior-day detour (SUPERSEDED — see above):** augmentor
(DS30) is the SSP bid stream → it necessarily logs an IP the DAY it's bid on, so SAME-DAY free-cohold (q3c,
basis of $273.7K) was FEARED circular. Fair test = did a free log have (ip×domain) on a PRIOR day within the 30d
window AND is it still ≥ as fresh (`free_prior_dominant`); query `q3e_v2_free_prior_lookback.sql` (37d scan /
7d measure = full 30d lookback, ALL IPs; reproduces same-day 52.9% for 33Across as a check). 33Across
same-day 52.9% → fair 38.4%. Prior-day preemption = $200.4K/yr (−24.7%), roster $812.4K → $612.0K
(upper bound $243.5K if vendor recency not credited). Because less is recovered, residual bills
are higher → EVERY metered vendor now < 1.0× worth-vs-bill even at most-generous (best-of-both-lenses) value:
API 0.94×, 33Across 0.83×, Justuno 0.79× (domain-driven), Sovrn 0.29×, Cybba 0.27×. Deck+audit pack in
audi_1089 artifacts (billing_review_deck_standalone.html / billing_review.md / audit_map.md). Meter-does-NOT-
preempt spine proof (live): `ddp_mm_winners_imp.tv_cpm`=$0.50 whenever ANY paid vendor wins, $0 only if none
(free co-presence ignored; 268.9M June imps). Two-lens rule (REVISED 2026-07-20 per user): the WORTH ("did it
pay for itself") = the MONEY-MADE / dependency lens = media revenue on the vendor's unique serves × margin —
lead with this everywhere (harsher: Justuno 0.15×, Cybba 0.17×, Sovrn 0.29×, 33Across 0.83×, API 0.94× — none
>1.0×). The domain fee-band (unique domains × per-domain rate) is a DATA-LICENSING comp, NOT money-made — show
it as a SECONDARY column, and it's what justifies KEEPING the flat-fee vendors (5x5/Predactiv). The max-of-both
is the CAP (most you'd pay to keep, incl. coverage value), distinct from WORTH. [superseded the earlier "take max
as fair value" — that mixed a profit basis with a licensing basis under one label.]
GRAIN CAVEAT (user 2026-07-20): q3e_v2 matches on exact REG_DOMAIN, but TARGETING keys off the CATEGORY the
domain falls into (DS13 vertical/wcv, DS19 keyword/pc) — "did this IP have a prior visit in this vertical/keyword".
A free log with a DIFFERENT same-category domain prior already makes the IP targetable but domain grain misses it.
**MEASURED (q3f, 2026-07-21, FULL all-IPs): VERTICAL-grain (DS13) preemption = $412.4K/yr (~2× the $200.4K domain
floor)** → roster $812.4K → $400.0K. prior_dominant shares: 33Across 60.7% (vs 38.4% domain), API 47.4%, Sovrn
**43.8% (vs 0.1% domain — niche domains but IPs sit in free-covered verticals)**, Cybba 42.0%, Justuno 17.0%.
Cross-check: free logs see 88.5% of 33Across IPs same-day (different domains) vs 52.9% same-domain. q3f =
`q3f_category_prior_coverage.sql` (DS13 vertical only, 1 cat/domain = cheaper than domain grain; DS19 keyword
multi-cat explode blew the 6h/shuffle limits → deferred; vertical is broad = close proxy for DS13+DS19 union, so
slightly conservative). Workbook sheet 2 shows BOTH grains (floor $200K + targeting $412K).

**2026-07-20 billing-sync — canonical 8-step pipeline delivered** (`audi_1089_ddp_steps.xlsx`, from BAE
billing team; script = `SteelHouse/bae-sql-utility/ddp/`; AUDI-1089 summary §4f + data_knowledge § "Canonical
DDP usage-reporting pipeline"). Steps: (1) `integrationprod.direct_data_partners` reference → (2) taxonomy
`tpa.categories`/`tpa.liveramp_categories`/`external.sharethis_categories` → (3) impression↔audience↔IPDSC
30-day-lookback match, persisted as **`mntn-analytics-prod-01.analytics_curated.enriched_impressions`** (moved
OUT of the DDP script into the UI Audience Segment Reporting pipeline for perf — this is *the* intermediate the
meter consumes) → (4) business logic → (5) CRM/MM→DDP mapping (`external.targeted_signal(_domain)`) → (6)
reporting → (7) audit (`coredw.usage_reporting_audits`) → (8) email from `partnerbilling@`. Finance view =
Tableau "DDP Monthly Usage Report." **NEW hard scoping rule: the billed base is PROSPECTING-FUNNEL-1 (CTV)
impressions ONLY** — F2 (already F1-served), F3 (F1-served + site-visited), and RT all run on FIRST-PARTY
engagement not the 3P audience signal → excluded from DDP billing. So the meter counts F1/CTV serves on
MM(DS13/19)- or CRM(DS4)-targeted impressions, credit-split across co-matching vendors. (Transcript to be added
to `meetings/`; reconcile any deltas then.)

**BILLING GRAIN = the WON impression (CONFIRMED 2026-07-17, AUDI-1115):** `ddp_mm_winners_imp`
is keyed on `ad_served_id` (a served impression) — vendors are billed ONCE, per credited WON
impression × contract CPM, NOT for ingestion and NOT separately for DS13/DS19 usage (that's the
eligibility gate). Only ~0.2% of ingested rows ever bill (33Across 1.08B rows/day → 70.3M billed
imps/mo). Ingestion cost is ours (WASTE lens: storage + Data Eng compute). **Two grains of
"vendor-unique" swing the value ~5×** — IP-membership (~5.6M/mo 33Across = marginal/keep-drop)
vs impression-winner (~27.5M/mo = credited/pricing); the crediting system's grain (07-20 sync)
sets how much bill survives preemption. Per-credited-impression media CPM ~$10.7 is MNTN's CTV
media rate (vendor-independent) → break-even ~$1-3 for all → $0.50 below break-even on the
residual → the rate is fine; preemption (volume) is the lever, not a rate cut. See
[[audi-1111-vendor-quality]], data_knowledge § billing, valuation framework § 2026-07-17.

**MNTN ID crediting wrinkle (2026-07-29, [[project_fangorn_on_mntn_id]]):** under MNTN ID, crediting splits in
two. (1) **Graph-vendor crediting** — every ID→household translation in the feature store must be logged to
`dw-main-silver.identity.graph_translation_signal` (Weiang Li dev, modeled on `hashed_email_signal`), required
even when the FS sources only internal logs (the graph holds licensed-vendor data); Sean wires it into AUDI-1167
via an ID-team pyspark interface. (2) **DDP-vendor crediting** — Fangorn uses NO DDP so it's unaffected, but
**DS13/DS19 use DDP** and their crediting may need to change under MNTN ID (Alyson/Jack, open), by ~mid-October
for real-campaign testing.
