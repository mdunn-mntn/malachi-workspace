---
name: reference-audi-1089-template-workbook
description: "The DDP vendor-eval workbook format (locked 2026-07-12): one question per row, numbers+notes sheets, exact section/vendor order; reproduced by fill_template.py [run_dir] [bill_month] from the q0-q7d CSVs"
metadata: 
  node_type: memory
  type: reference
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [audi_1089 template workbook, ddp vendor eval, fill_template.py, xlsx format locked, solo sheet, decisions sheet, notes numbers sheets, one question per row, vendor column order]
domain: [pricing, project]
lifecycle: active
last_verified: 2026-07-16
---
**The AUDI-1089 vendor-eval workbook format is LOCKED (user-approved 2026-07-12; v3 adds
index + decisions sheets) — reuse it for every future DDP eval pass. FOUR sheets in order:
`index` (all 131 row definitions: meaning, formula, source query), `decisions` (8 paid vendors
ranked by curved score: bill vs WTP band vs T1/T2, exact drop savings, coverage lost, dep
revenue at risk, DECISION + negotiation plan + asks; then the 7-scenario roster frontier from
q3b masks), `numbers`, `notes`.** `tickets/audi_1089_ddp_vendor_evaluations/outputs/
audi_1089_quality_template_filled.xlsx`, generated ONLY by `runbook/charts/fill_template.py
[run_dir] [bill_month]` (defaults run_2026_07_10, 2026-06). The `SPEC` list in that script IS
the canonical question order — never hand-edit the xlsx.

Format rules (user-corrected twice to get here — do not regress):
- **One question per row, one value per cell** (compound "count + % + share" questions split
  into separate rows; 131 rows). No prose in cells; "—" = not applicable.
- **Real typed values**: percents stored as FRACTIONS with true `%` number formats (0.0004 +
  `0.00%`, NOT 0.04 + a fake `"%"` suffix — Sheets/Excel must recognize them as percents);
  counts `#,##0`; money `$#,##0`; CPM 2dp; ROAS `0.00"x"`.
- **Sections in order**: CONTRACT & IDENTITY → FEED SCALE (30d) → DATA QUALITY → USABLE
  FUNNEL → UNIQUENESS & FRESHNESS → SERVING & WON BIDS → SCORE QUALITY → PERFORMANCE
  touched → PERFORMANCE sole → ECONOMICS COST → ECONOMICS WORTH → PORTFOLIO (LOO) → VERDICT.
- **Vendor columns in order**: 33Across(28), 33Across API(40), Sovrn(33), Justuno(24),
  Cybba(36), 5x5(25), sharethis_predactiv(26), Klickly(39), LaunchLabs(27 disabled),
  augmentor_log(30), guid_log(23), LiveRamp IP(35), ShareThis(17), deepsync(29) — last 3
  out-of-MM context columns (contract+bill rows only).
- **Per-row heat scales** (Excel ColorScaleRule across vendor columns; 121 rows): direction
  per metric in `DIR` — green = high for value metrics, green = LOW for junk/cost metrics;
  direction-less rows (CPMs, billed domains, contract rate) get a white->steel-blue NEUTRAL
  scale. All-equal rows (e.g. all 0%) render one flat color (min=max) — expected, not a bug.
  '% of pairs usable' capped at 100 (sub-1% q3-vs-q2 window mismatch).
- **All text on the styled `notes` sheet** (per-vendor: scope, rate, renewal, ingestion,
  blast radius, full verdict, asks; + numbered CONVENTIONS block; verdict color-coded).

Reproduction: run queries q0→q7d per their headers into `outputs/run_<date>/`
(q4 also emits q3_pair_recency.csv; ~1h background: q3,q3b,q5,q6,q7,q7b,q7c), then run the
fill script — must print `empty: none`. Full instructions: runbook/README.md §7 "Reproduce
the workbook from scratch"; **shareable validation package = runbook/queries/MANIFEST.md** (25
ordered queries mapped to rows/charts, standalone bq patterns, validation anchors). User lens (apply in future evals): thinks at VISIT grain (ip x domain x DATE) — always offer the
date-aware variant; wants standalone-vs-free-only AND marginal-at-position economics side by side;
label estimates as dependent REVENUE with an explicit PAY RANGE column at the 10-30% blended margin (user's range, avg ~20% — not 15/20/30); everything must land IN the xlsx,
not just chat/PNGs. Related: [[project_audi_1089_ddp_evals]], [[reference_ddp_valuation_framework]],
[[feedback_rank_desc_always]].

**2026-07-15 final:** decisions gained block 6 (DS19 EXPOSURE: tier table labeled "SERVED members
only — the targeting reality" + category table labeled "ALL member IPs — the audience-COUNT lens";
label the POPULATION on any table pair like this or it WILL be misread). FREEC share rows: never
show a degenerate 100% (own-world share) — show the all-sources share or em-dash (user-caught).
Charts through q15 (q13 scenarios/cats, q14 waste, q15_free_union_value closer). Completeness-audit
pattern: 3-lens workflow (queries↔MANIFEST↔zip / xlsx cell-walk / charts registry+freshness) before
declaring a deliverable set done; hand-collected inputs EMBED in their script header (q14 precedent).

**2026-07-15 later:** numbers/solo gained a 15th column — `free_logs (guid+aug)` pseudo-vendor
(ds 99, after guid_log): UNION semantics, never sums (usable pairs 3.605B union vs 3.62B naive
sum). Mechanism: `_synth_free_combo()` synthesizes exact sums (rows/GB/weighted shares) +
mask-exact unions; measured cohorts INJECT from q15 into q5/q6/q7b/q7c/q7/q8b under ds 99 so
every existing row formula works unchanged (touched = either log; sole = no paid vendor; solo
sheet column == free-only counterfactual). Union-underivable cells stay em-dash. share()
denominators must EXCLUDE ds 99 (double-count guard). **Column-width rule:** render_grid caps
columns at max_w=36; long text cells wrap (left/top) with row heights sized to line count —
never let text stretch columns (user, 2026-07-15).

**2026-07-15:** 144 rows (post-preemption row); SIX sheets — `waste` added at position 3 (boss-facing:
measured GB/day [q14 gsutil script], % used vs thrown away, USED-BUT-SHOULDN'T-BE junk-that-bills
section, storage floor, stop-sending asks; columns = 10 ACTIVE sources via render_grid cols param).
Decisions scenario table gained DS19-only pair+visit-day coverage columns (q13a masks). **GRAIN
MARKERS are mandatory** (user misread daily 839M against 30d 30.4B): any section mixing grains must
say so in the header AND row labels (USABLE FUNNEL counts are PER-DAY, 1-day sample 2026-07-01).

**Current state 2026-07-14:** 143 rows; FIVE sheets — `solo` added between numbers and notes
(user ask: every vendor recomputed as the ONLY paid source, overlap vs free logs only).
Solo mechanics: `SOLO_OVERRIDE` dict annotates SPEC rows (copy/derive/mask/est/scan — 75/5/12/6/45),
`build_solo_spec()` mirrors SPEC 1:1 (length+key guards; renames inherit DIR heat);
`other_free(d)` = both free bits for paid vendors, the OTHER free log for guid/augmentor columns.
Solo bill = bounded est: LOW = today's run-rate (hard floor), HIGH = max(LOW, visit-day-proportional
share of metered pool) — clamp needed because junk-billed credit (Sovrn/Cybba/Justuno) is uncontested.
Measured solo serving/perf from q8a/q8b (MANIFEST rows 26-27); script runs pre-scan with
"pending scan (q8)" cells and asserts anchors when CSVs land (mask solo pairs == q3 netnew EXACT;
q8b >= q6 sole; HI/PP == q3d masks). Decisions = 4 blocks (calls / scenarios+HI-PP 3dp /
net-of-free ladder / vertical impact); shareable package = runbook/queries/MANIFEST.md (27 queries);
chart q11_solo_pnl.png. THREE counterfactuals coexist — sole (vs all 10) / solo (vs free only) /
ladder standalone (= solo at pair grain, density $) — never conflate them in prose.

**Deck-sheet conventions (2026-07-16):** percent cells store TRUE FRACTIONS with % number formats
(survive Excel re-typing — user rule); data-driven column widths + footnotes merged across the
table width (no manual resizing); free-log standalone = vs ALL 8 paid (each free row ⊆ union);
profit = UNIQUE cohort only (touched bands rejected as misleading ceiling); generator
artifacts/audi_1089_deck_workbook.py.
