# DDP Vendor Billing Review — talk track (AUDI-1089)

**Audience:** DDP billing/reporting owners. **Purpose:** settle one question with reproducible
evidence, then show what fair vendor pricing looks like. **Every number below has a query behind it**
(see `audi_1089_audit_map.md`); the audience will re-run them, so this doc leads with how to check.

Windows (identical everywhere): delivery/uniqueness = 30-day svs `dt 2026-06-02..07-01`; serving =
37-day membership × valuation week `2026-07-02..08` (CIL); bills = June 2026 meter × 12. All read-only.

---

## 0. The one question on the table

> **Claim being tested:** "the meter already avoids paying vendors for signals our free logs
> (guid_log DS23, augmentor DS30) also capture — so there's nothing to recover."

**The data says the opposite.** The meter does **not** preempt. It pays $0.50 CPM on impressions our
own free logs already cover, to the tune of **$273.7K/yr** — and it's visible three independent ways,
each reproducible by you. That's the spine of this review. Everything after it is: *how I know each
vendor's fair value, and what we should actually pay.*

---

## 1. How I valued them (the method)

One substrate, two independent lenses, never merged.

- **One substrate.** All 10 site-visit sources — 8 paid vendors + 2 internal **free** logs (guid DS23,
  augmentor DS30) — land in `site_visit_signal` and are measured **identically**. The free logs are the
  baseline every paid vendor is judged against, because they cost $0 and we keep them either way.
- **Lens A — Dependency ceiling (bottom-up).** The *most* a vendor can rationally be paid = the media
  revenue on impressions that **only that vendor could have enabled** (households no other source, paid
  or free, reported), annualized, times a defensible margin. Above that number, a loss is guaranteed.
  This is a ceiling, not a fair price.
- **Lens B — Coverage / uniqueness (top-down).** How much of the billable data universe does the vendor
  add that we don't *already have for free*? Measured with **holder masks** — a 10-bit signature per
  (IP × domain × date) recording which sources delivered it — so **any** keep-set's exact coverage is
  computable without re-scanning.
- **Billable grain = the won impression.** A vendor is credited once per won impression on an
  MM-targeted serve, at contract CPM — not for ingestion. So "value" is always read against the
  *billed* base, never raw rows delivered.

The two lenses are never added together (the same impression can't be priced at media CPM *and* data
CPM). Where they disagree — 5x5 and Predactiv have tiny dependency but huge unique-domain value — both
are reported side by side.

---

## 2. What I did (the pipeline)

Per source, one pass each, joined to the billing meter:

1. **Delivery** — rows/day, reach (IPs, domains, IP×domain pairs), liveness, junk rate.
2. **Usable survival** — the share that reaches a real consumer: DS13 (domain → vertical) **or** DS19
   (URL → product-category keyword). Only usable rows are creditable.
3. **Uniqueness** — sole vs redundant vs free-co-held, at pair grain and visit-day grain, via the
   holder-mask histogram (exact for all 2⁸ vendor keep-sets).
4. **Serving** — won impressions on each source's IPs in the valuation week (from `cost_impression_log`).
5. **Performance** — visit rate on the vendor's *sole* impressions vs the no-data baseline.
6. **Dollars** — media revenue on sole serves → dependency value; billed usage from the meter
   (`usage_reporting_data`) and the BAE winners table (`ddp_mm_winners_imp`).

**33 queries + 8 self-contained "deck" queries**, all read-only, all with the exact run command in the
header. `MANIFEST.md` = run order + cost; `VALIDATION_GUIDE.md` = glossary + independent anchors (e.g.
meter identity: billed imps × CPM = billed usage, exactly; dropping all metered vendors recovers exactly
$812,397/yr).

---

## 3. What the results show

### 3.1 The free logs alone cover ~60% of the billable universe
Distinct (IP × domain × date), 30 days, usable rows only:

- Total universe: **13.29B** visit-days.
- Covered by the free logs (guid ∪ augmentor), no paid vendor needed: **7.89B = 59.4%.**
- At pair grain: free logs cover **60.4%** of the 5.97B usable pairs.

So a **majority** of every billable signal is already ours at $0. *(deck_d1 / q3c)*

### 3.2 The meter pays vendors for that free-covered signal anyway — $273.7K/yr
Three independent proofs, each runnable by you:

**Proof 1 — the billing table itself (`gold.reporting.ddp_mm_winners_imp_202606`, verified live).** The
`tv_cpm` charged is a pure function of whether *any paid vendor* won the impression — the presence of a
free log is completely ignored:

| Winners on the impression | June impressions | tv_cpm |
|---|--:|--:|
| Free log (23/30) **and** paid vendor both win | **268.9M** | **$0.50 on 100%** |
| Free log wins **alone** | 165.7M | $0 on 100% |
| Paid vendor wins alone | 38.2M | $0.50 on 100% |

On **268.9M impressions/month**, a free log co-won the exact impression and the paid vendor was **still
charged the full $0.50** — a preemptive meter would show $0 there. Co-presence of a free log has **zero**
effect on the charge. *(This proves the mechanism/rate — see the caveat below on why these impression
counts are NOT multiplied into the dollar figure.)*

**Proof 2 — per-vendor dollars (bill × free-co-held share, q3c/q3b masks).** Of the $812.4K/yr metered
bill, **$273.7K (33.7%)** sits on visit-days the free logs also hold:

| Vendor | Bill $/yr | Free-co-held | Recoverable by preemption |
|---|--:|--:|--:|
| 33Across | $422.0K | 52.5% | **$221.7K** |
| 33Across API | $175.9K | 23.8% | **$41.9K** |
| Cybba | $21.5K | 28.2% | **$6.1K** |
| Justuno | $77.1K | 4.9% | **$3.8K** |
| Sovrn | $115.9K | 0.2% | **$0.3K** |
| **Roster** | **$812.4K** | **33.7%** | **$273.7K** |

**Proof 3 — the source table, now self-serve (`bronze.external.targeted_signal`).** The row-level
"used-signal" table is a BQ external table (was believed Athena-only), hive-partitioned on
`source_data_source_id` — so you can directly count vendor-credited (ip × dscid × date) rows that the
free logs (23/30) also delivered on the same day. The mechanism, auditable at the row level.

**Why it happens (mechanism, not a mystery):** credit is first-reporter-wins per (ip × url × date). A
free log only displaces a vendor when the free log reports *first* that day; and each new date on an
already-tracked pair is a fresh billable event. So free logs, in practice, do **not** preempt. Confirmed
in AUDI-1093.

### 3.3 Even after preemption, most residual bills exceed fair value
Preemption keeps every vendor's data — it only stops paying for the free-covered slice. After it:

| Vendor | Bill AFTER preempt | Fair value (most generous) | Worth ÷ bill | Read |
|---|--:|--:|--:|---|
| 33Across | $200.4K | $72–217K | **1.08×** | defensible only at the ceiling |
| 33Across API | $134.0K | $45–134K | **1.00×** | exactly at the fair ceiling |
| Sovrn | $115.6K | $11–34K | **0.29×** | ~3× over even at the ceiling |
| Justuno | $73.3K | $4–11K | **0.15×** | ~7× over |
| Cybba | $15.4K | $1–3K | **0.19×** | ~5× over |

Preemption fixes the double-pay; it does **not** by itself make Sovrn / Justuno / Cybba worth their
bill. Those need repricing or dropping.

---

## 4. What we should pay (recommendations)

Two moves, in order. **Renegotiate before you drop** — dropping a vendor first destroys the BATNA and
can reassign its credits into another paid vendor mid-negotiation.

**Move 1 — implement free-log preemption (stop billing on free-covered signals).** Roster **$812.4K →
$538.7K/yr, −$273.7K (−33.7%), and we keep all the data.** This is the direct answer to the dispute and
needs no vendor cooperation (we own the meter).

**Move 2 — reprice / drop the residual to fair value:**

| Vendor | DS | Current | After preempt | Fair range | Recommendation |
|---|---|--:|--:|--:|---|
| 33Across | 28 | $422.0K | $200.4K | $30–100K | **Renegotiate** — cap ≤ ~$100K or drop; biggest single lever |
| 33Across API | 40 | $175.9K | $134.0K | $10–40K | **Drop / renegotiate** (same vendor as DS28; batch vs real-time) |
| Sovrn | 33 | $115.9K | $115.6K | $0.5–2.4K | **Drop** — not overlap-driven; preemption won't help |
| Justuno | 24 | $77.1K | $73.3K | $14–60K | **Trim** the meter toward the band |
| Cybba | 36 | $21.5K | $15.4K | $1.1–4.7K | **Drop** |
| Klickly | 39 | flat (pending) | — | $0.1–1.5K | **Drop** unless renewal is ~free |
| Predactiv | 26 | flat (pending) | — | high (domain axis) | **Keep / lock price** — hard non-MM (HEM→CRM/identity) dependency |
| 5x5 | 25 | flat (pending) | — | high (domain axis) | **Keep** (TI-1027) |

Metered rate itself ($0.50 CPM) is **not** the problem — it's below the residual break-even. The
overpayment is **volume**: billing on free-covered and non-marginal impressions. Preempt the volume,
then reprice the two 33Across feeds; drop Sovrn and Cybba.

**Sequencing:** lock flat-fee prices first (dropped vendors' credits reassign into flat-fee vendors at
$0 marginal cost today, inflating their measured value before renewal) → preempt → renegotiate 33Across
→ drop Sovrn/Cybba.

---

## 5. Audit this yourself

The fast path is 8 self-contained queries (`runbook/queries/deck_d1..d8`), plain `bq query`, run as-is.
The billing-table and source-table proofs run on `dw-main-gold.reporting.ddp_mm_winners_imp` and
`dw-main-bronze.external.targeted_signal`. Every claim above → its query → its expected number is in
**`audi_1089_audit_map.md`**. `VALIDATION_GUIDE.md` lists the internal consistency anchors (meter
identity, mask consistency, boundary identity) you can check without trusting any of my outputs.

**Caveats disclosed up front:**
- **The $273.7K is meter-anchored, not derived from Proof 1's impression counts.** The winners table
  (`ddp_mm_winners_imp`) has multiple path-rows per impression, so its impression totals over-count the
  final meter (`usage_reporting_data`) — it proves the *rate behavior* (no preemption), not the dollars.
  The dollar figure is each vendor's free-co-held **share** applied to its **actual** June meter bill.
- Valuation week is N=1 (July, a seasonal trough) → dependency figures are a stated scenario envelope,
  not a confidence interval.
- The meter's credit regime changed May 2026 (fractional → integer) — never mix pre/post-May bills.
- `website_crawl_verticals` / `product_categorization` are live snapshots (<0.5% day-to-day drift);
  flat-fee amounts are pending finance.
