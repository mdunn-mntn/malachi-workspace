# Graph vendor crediting — ID / AUDI working session

**Attendees:** Weiang Li, Jack Barbey (Identity) · Sean Yang, Alyson Lefkowitz (AUDI)
**Ticket:** [AUDI-694](https://mntn.atlassian.net/browse/AUDI-694) · **Blocks:** ID-407 · **Prep:** all figures measured 2026-08-17, sources in `summary.md` §4

---

## Why we're meeting

As CRM inclusion audiences move from DS4 to DS63, vendor crediting moves with them. Two things need
settling before DS63 goes GA: **which SQL is actually the candidate**, and **how the impression gets
split across the sources that enabled it**. The second one changes what deepsync gets paid by 4.7x.

Nothing here is a blocker on the graph rollout itself. It is a billing-correctness gate on the
crediting leg.

---

## 1. The PR under review is not the current design (5 min, informational)

`bae-sql-utility#24` has been open since 2026-07-29 and Blocked on BAE review since 08-06. It reads
`ats.translation_date` / `gts.translation_date` in five places. That column has never existed on
either signal, including in the pre-refactor dev tables from 08-02/03 — the column is
`translation_timestamp`. So the PR as committed cannot compile.

Meanwhile the outputs exist: `dw-main-gold.reporting.ddp_graph_matches*` (built 08-10) and a newer
iteration (`ddp_crm_graph_cpm`, `ddp_crm_graph_matches_cpm`, built 08-13) that splits the two
crediting legs into separate vendor arrays and carries `auction_signal_timestamp`.

**Ask:** point BAE at the SQL that is actually the candidate for merge, and close or rewrite #24.
Reviewing the committed diff reviews the wrong artifact, and it is the item blocking ID-407.

---

## 2. The divisor decision (the main item, 25 min)

Terms: an impression is credited to the sources that enabled it, and each source's share is
`1 / (number of sources counted)`. "The divisor" is what goes in that count. Today the
MNTN-Matched leg counts our own free logs (guid_log DS23, augmentor_log DS30) in the divisor, so they
take unpaid slots and dilute every paid vendor. PR #24's graph leg removes them **before** counting.

Measured on real DS63 output — `ddp_crm_graph_cpm`, 2026-08-06..08-12, 214,251 impressions:

| source | on % of impressions | how it bills |
|---|---|---|
| augmentor_log (30) | 98.7% | free, ours |
| Experian (22) | 97.4% | flat fee, not externally reported |
| deepsync (29) | 96.6% | **$0.50 CPM — the only per-impression-billable source** |
| Audience Acuity (58) | 96.0% | not in the partner registry at all |
| guid_log (23) | 82.1% | free, ours |

Average 4.7 sources per impression. **99.6% of deepsync-credited impressions also carry one of our
free logs.**

Deepsync's credited share for the week under each rule:

| rule | shares | usage @ $0.50 | vs PR #24 |
|---|---|---|---|
| **A** PR #24 — count billable partners only | 209,076 | $104.54 | 1.0x |
| **B** match the MNTN-Matched leg — count all sources | 44,627 | $22.31 | 4.7x less |
| **C** free-log preemption — free source present, paid gets nothing | 807 | $0.40 | 259x less |

Under rule A deepsync is the sole billable source on nearly every DS63 impression, so it takes 100% of
each one. That is a different pricing regime from the DS4 leg it replaces, arriving silently with a
data-source migration.

**Context:** AUDI-1113 costs rule C at $768,916/yr across the whole metered roster, independently
confirmed by BAE on BAE-4923. Shipping rule A on the graph path moves in the opposite direction.

**Decide:** which rule the graph leg uses. Whatever we pick should apply to both legs — running the
DS4 leg on one rule and the DS63 leg on another guarantees a discontinuity in vendor bills exactly at
the rollout boundary, on a meter vendors self-report against.

**Scale:** deepsync bills $22,380 Jan–Jul 2026 (~$38K/yr) through DS4 today. No DS63 credit has ever
been billed. Today's DS63 footprint is 4 audience uploads; GA is this quarter.

---

## 3. Per-touchpoint or per-vendor crediting? (10 min)

Leg 1 credits sources that resolved the auction IP to a household. Leg 2 credits sources that
translated the segment ID into that household. **Leg 2 contributes a vendor leg 1 does not carry on
22.7% of impressions** (49,016 of 216,409).

PR #24 flattens both legs into one deduplicated vendor set, so a source present at both touchpoints is
credited once. The 08-13 build keeps them separable. The design doc's worked example credits all four
vendors across three touchpoints, which implies per-touchpoint.

**Decide:** one credit per vendor per impression, or one per vendor per touchpoint.

---

## 4. Three arithmetic defects to confirm before merge (10 min)

All three are live in `direct_data_partners` today, and all three inflate what we pay:

1. **33Across is counted twice.** DS40 (33Across API) carries `primary_data_source_id = 28`, so it
   credits to DS28 — that is the dedup that makes them one vendor. The graph leg counts raw
   `report_data_source_id`, where 28 and 40 are distinct, so 33Across takes two divisor slots and two
   usage rows. This is the same error BAE hit on BAE-4923, where the two counts differ on 34.2% of
   MNTN-Matched rows. The MNTN-Matched leg guards it with a credit-divisor table; the graph leg has no
   equivalent.
2. **LiveRamp fans out.** `report_data_source_id = 35` matches two registry rows (35 LiveRamp IP, 11
   LiveRamp). The graph partner table does not dedup, so the join duplicates rows before the usage sum.
3. **Variable-CPM partners bill zero but still dilute.** LiveRamp (35/11) and Bombora (51) have a null
   fixed CPM, so usage computes to $0 while they still occupy a divisor slot and shrink every paid
   vendor's share. Bombora lands in both signal tables via ID-421 and airflow-ti#1201.

---

## 5. Crediting fails open in production (5 min)

airflow-ti #1200 and #1201 wrap the crediting write in a try/except that re-raises only in dev. In prod
a failure means a missing partition, a green DAG, and silently short bills. No alerting in either diff;
the SQLMesh audits are non-blocking with a zero-row threshold, so partial loss is invisible end to end.

Separately, both PRs still point at the **dev** `mntn_graph.zip` bucket, and the umbrella signal views
read only the CRM source — the union branches those PRs' output depends on do not exist yet, so their
crediting logs currently have no reader.

**Ask:** a blocking partition-presence check on both signals before either PR merges, and confirmation
of what lands the union branches.

---

## 6. Ownership and sequencing (5 min)

ID-407 says the graph billing logic should live in the AUDI/audience-platform crediting pipeline
because those teams take it over. AUDI-1145 (own the DDP credit-awarding pipeline) is still in Backlog
and unstarted, and today nobody owns the crediting logic — BAE executes rules handed to them.

**Settle:** who merges the graph leg, who runs the first month, and who signs off when a vendor bill
changes. Note ID-421 already had to stagger the two jobs (10 PM / 11 PM) because a one-day lag breaks
next-day billing.

---

## Out of scope today

BAE (Sherwin, Mike Dolzer) and Andy Everson come in once the rules in items 2 and 3 are decided,
since changing the divisor changes what vendors are paid and that touches contract terms.

MNTN ID crediting (the Graph Crediting workshop assigns AUDI "outputting DDP use signals for MNTN ID")
is a separate follow-on. MNTN ID is GA Q1 2027; DS63 is this quarter.
