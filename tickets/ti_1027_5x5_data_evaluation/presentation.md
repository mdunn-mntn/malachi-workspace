# 5x5 Data Evaluation (DS 25) — Readout

**Question:** 5x5's contract ends end of June. Is its data worth renewing? Is its impact on MNTN Matched outsized
relative to its scale, or in line? *(Estimation exercise — measurable bits first.)*

## Bottom line
**Recommend KEEP (renew).** 5x5 is a **flat-fee** data partner whose impact on MNTN Matched is **outsized — ~3.4×
its share of raw data** — and is **concentrated in B2B**, MNTN's #1 Q2 growth theme. It is the **#2 most-unique** of
all site-visit data partners. Final renew/renegotiate sign-off pending the flat-fee amount (with billing).

---

## What 5x5 is
A data partner that sends us **IP → website-visit** records. These flow into `site_visit_signal`, get classified
into verticals (domain → industry), and feed **MNTN Matched** scoring. 5x5 is **one of ~8 external partners** +
2 internal sources (our own bidstream + pixel) feeding that same pipeline. It is **live today** and billed as a
**flat fee** (not per-use).

## 1. Scale — 5x5 is a modest slice of raw data
- **~3.6% of raw site-visit records** (~93M rows/day), ~20.8M IPs/day, ~93K domains/day.
- It sends **domains, not full URLs** (only 3.8% carry a page path, vs 67–100% for every other partner). *This is
  fine for MNTN Matched* — the pipeline only uses the domain anyway — but means 5x5 adds nothing to any
  page/keyword-level use.

## 2. Impact on MNTN Matched — OUTSIZED (≈3.4×) → `ti_1027_chart_leverage.png`
- **68.5% of 5x5's domains are unique** — provided by no other partner, internal or external.
- After keeping only domains that classify to a real vertical (MM-usable): **47,069 unique domains** = **~12% of the
  entire classified-domain universe**, from a partner that is only **3.6% of raw data → ~3.4× leverage.**
- 5x5's value is **incremental domain coverage, not reach**: 73.8% of its IPs we already see via our own
  bidstream/pixel. It surfaces *different sites* for users we already know.

## 3. vs other data partners — 5x5 is top-tier; the per-use vendors are redundant → `ti_1027_chart_vendor_comparison.png`
| Partner | Billing | Unique MM-usable domains |
|---|---|---:|
| Predactiv | flat fee | 164,627 |
| **5x5** | **flat fee** | **47,069** |
| 33Across | $0.50 CPM | 9,277 |
| 33Across API | $0.50 CPM | 2,802 |
| Cybba | $0.50 CPM | 309 |
| Sovrn | $0.50 CPM | 293 |

- 5x5 is the **#2 unique contributor** and the most-unique high-volume partner.
- The **per-impression ($0.50 CPM) vendors add little unique signal** — 33Across API, Sovrn, and Cybba are largely
  redundant. **They, not 5x5, are the candidates for cost review** (separate follow-up).

## 4. What we'd lose without 5x5 — B2B coverage → `ti_1027_chart_vertical_dependence.png`
The verticals most dependent on 5x5-unique domains are **overwhelmingly B2B** — B2B Hiring (34%), Logistics (32%),
Data & Analytics (31%), Workflow Automation (30%), Sales & Marketing (30%), IT & Engineering (25%) — plus premium
retail (luxury apparel, jewelry, footwear) and industrial/medical. **B2B is MNTN's #1 Q2 growth theme**, so dropping
5x5 would most degrade the exact area we're investing to grow.

## 5. Value & the renew decision
- **MNTN Matched touches ~$210–385M/yr of media** and drives a measured **~10–36% visit-rate lift** (Fangorn). Its
  value (via advertiser retention) is conservatively **tens of $M/yr**.
- 5x5 uniquely supplies **~12% of MM's domain signal** (far more in B2B). Against a typical data-partner flat fee
  (tens-to-low-hundreds of $K/yr), that clears break-even with comfortable margin.
- **Decision rule:** keep unless the flat fee is unusually large (≳ low-$M/yr); if so, **renegotiate** (ask for full
  URLs, or a lower fee).

## Caveats
- Value is **domain→vertical coverage**, not reach, and not page-level signal (5x5 is domain-only).
- The dollar figure is an **estimate/band** — the flat-fee amount is still needed to finalize, and a precise
  causal read would require an add/remove model test (proposed as a follow-up only if needed).

## Next steps
1. Get the 5x5 flat-fee amount (billing) → finalize keep/renegotiate.
2. Decision → pipeline owner keeps `25` enabled (no change needed if KEEP).
3. (Follow-up) Review the redundant $0.50-CPM partners (33Across API, Sovrn, Cybba) for savings.
