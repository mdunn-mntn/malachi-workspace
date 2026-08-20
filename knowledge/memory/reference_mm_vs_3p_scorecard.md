---
name: reference_mm_vs_3p_scorecard
description: "AUDI-1141 MM vs 3P by-vertical scorecard: OR-vs-AND classifier, gate split, IVR/CVR defs, WGU pooled trap"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [mm vs 3p, audi-1141, scorecard, ivr, cvr, cpv, roas, hhst gate, wgu, or vs and classifier, vertical]
domain: [audience-scoring, business]
lifecycle: active
last_verified: 2026-08-20
---
AUDI-1141 (Sales request, 2026-07-20): MM vs 3P prospecting perf by the 8 sales verticals, trailing 6mo.

**Classifier** (S1 prospecting obj=1/funnel=1, delivered 180d, latest targeted segment): MM signal=DS13/19/38/46, 3P=DS17/18/35. **Do NOT lump MM+3P as "Mixed"**: ~85% join 3P by OR (additive, stays MM), ~15% by AND (narrows). Use TI-999 Pass 26 JS UDF (LCA tree-walk of categories.where) for OR_include vs AND_include. Groups: MM (3P OR-additive/absent, geo broad) split by HHST gate; MM restricted (AND-3P or narrow geo zip/city/radius); 3P (no MM); Neither. Spend mix MM 51 / restricted 24 / 3P 15 / Neither 10.

**Rates over IMPRESSIONS (TI-999 Pass 26): IVR=visits/imp, CVR=conv/imp, CTR=clicks/imp.** visits=views+clicks. CPV=spend/visits, ROAS=rev/spend.

**Headline (advertiser-weighted MEDIAN):** MM(gated) IVR 0.46% / CPV $9 / ROAS 0.92 beats 3P 0.07% / $37 / 0.40 (~6.6x IVR, ~4x cheaper); wins IVR+CPV all 8 verticals. Both restrictions hurt: MM-no-gate 0.13%, MM-restricted 0.18%. HHST gate is the norm (median gated-frac 0.99).

**Two lenses (Jon wanted both):** MM (all) = every MM campaign blended (realistic average) still beats 3P ~4x IVR / ~3x CPV / ~2.3x ROAS (0.28% / $13 / 0.92 vs 0.07% / $37 / 0.40); MM (gated) = best-configured subset. Blended answers "is gated a perfect-scenario average?" (no, MM wins either way). ROAS directional (prospecting/last-touch). **Gate plain-English (Malachi→Jon):** score threshold >0 = bidder only bids model-scored high-intent IPs; set to 0 (Max Reach / many short flights lowering it for deliverability / over-narrowed HI pool exhausted) bypasses the model, bids broadly like 3P. Gate = the threshold setting, NOT the scoring model.

**WGU (31357) pooled trap:** ~39% of pure-3P imps → impression-pooled 3P looks competitive; always use advertiser-weighted median. [[reference_wgu_pixel_case]]

**CPA added** (only metric taken from the TI-1037 Mode dashboard set; reach + frequency intentionally omitted per Jon): CPA = spend/conversions (median over advertisers w/ conv). MM(gated) $253 vs 3P $658 (~2.6x cheaper); directional like ROAS. By-vertical tabs also carry an ROAS-advantage column next to IVR-advantage (Jon request).

**STATUS: DONE 2026-07-21.** Deliverable = .xlsx (upload as Google Sheet OR just open from Drive — synced to `My Drive/Tickets/AUDI-1141/`, see [[reference_drive_mount_xlsx_delivery]]). 7 tabs (Read me / MM vs 3P by vertical [blended] / MM gated vs 3P / Full scorecard [+CPA] / Overall / Campaign detail / Queries); rates decimals formatted % and $. Build: artifacts/audi_1141_build_xlsx.py. Vertical crosswalk 37->8 interim (needs RevOps). Files in tickets/audi_1141_mm_vs_3p_by_vertical/. [[reference_mm_component_taxonomy]] [[reference_mntn_1p_3p_mm_definitions]]

## REFRESHED + REBUILT 2026-08-20 (pitch-deck rollout)

Sales (Jon Zucker) is building the new pitch deck on this sheet. Re-ran the cohort SQL (window moved to
**2026-02-21 → 2026-08-20**, 2.29 GB, 23s), rebuilt the workbook on **`lib/mntn_xlsx.MntnWorkbook`** (the
original hand-rolled openpyxl builder predated the standard the module was generalized from).

**Numbers moved:** MM(gated) IVR 0.46%→**0.43%**, no-gate 0.13%→0.14%, 3P 0.07% flat, so the gated-vs-3P
IVR advantage is **6.1x** (was 6.6x). MM still wins IVR and CPV in every vertical on both lenses.
CPA: MM(all) **$339** vs 3P **$733** (2.16x); MM(gated) $253.

**8 tabs:** Overview · MM vs 3P by vertical · MM gated vs 3P by vertical · Full scorecard · Campaign
detail · Read me · Queries · Method & caveats. `Overall` was cut (derivable from Full scorecard).
Both comparison tabs now carry **CPA + CPA advantage** and **MM/3P with conversions** (the advertiser
count each CPA rests on). `Campaign detail` gained **Peak Performance / Scoring engine / Intent tiers
reachable** from `audience.mm_campaign_classifier` (answers "which campaigns are Peak Performance?",
Alex Knorr) — 40% read `Not classified`.

**The CPA advantage does NOT survive a B2B cut** — 2.16x overall, 1.23x on advertisers with no
prospecting revenue, 1.02x on B2B Software & Services, and on B2B-with-no-revenue **3P is cheaper**
(n=19). A "CPA on non-revenue accounts" tab was built and then **cut**: Jon asked for the CPA *column*,
not a cohort split, and "no revenue tracked" did not mean what it said (see `data_knowledge.md`). Finding
kept for the conversation and for the follow-up spike.

**Two things still open with the requester:** (a) **Jon's original zip rule was never implemented** — he
said "omit zip codes for all verticals except auto and professional services"; the SQL instead routes
zip/city/radius into `MM restricted` in EVERY vertical, and `MM restricted` is folded INTO `MM (all)`,
the lens the deck uses. Applying his rule drops 43% of MM campaigns ($26M) and makes MM look BETTER:
IVR 0.26%→0.34%, CPA $339→$295. (b) Jon's circulated link
`1m5RKXYN219eGH_JiYwPdL9pmzwmerunJ` is the **2026-07-21 file**, not the rebuild — and it is Malachi's own
file (Jon has editor access), not a copy.

**Hand-edit lesson:** the July sheet was NOT reproducible from its builder — Jon had editor access and the
live file carried a descending re-sort, an Education ROAS footnote, and two hand-blanked "N/A" cells.
**Read the live Drive file before regenerating over it.** [[feedback_no_label_colon_prefix]]
[[feedback_explicit_descriptive_slots]]
