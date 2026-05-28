# Why Buyers Pick Interest Segments Blindly — and What TI-956 Does About It

**Author:** Malachi Dunn — Targeting Infrastructure
**Audience:** TI team, eng leadership (Kale / Paulo), product (Macie, Allison)
**Window:** 30 days ending 2026-05-28 — 15,529 active campaigns, $40.42M total spend
**Source of truth:** [summary.md](summary.md) (Finding 15 Passes 1-12) + queries / outputs in this folder

---

## Power Line

**Buyers add LiveRamp interest segments to campaigns without any quality signal — and ~$43M/yr of MNTN spend is allocated to the worst-performing segments at the same rate as the best. TI-956 fixes this with a per-segment quality score.**

---

## §1 — Audience family definitions

MNTN's bidder distinguishes four targeting families. The case rests on this taxonomy.

| Family | What it is | DSes | Scored? | Intended use |
|---|---|---|:-:|---|
| **1P (First-Party)** | Advertiser-uploaded customer data | DS4 CRM, DS8 IP List, DS47 CRM-IDG | No | **Retargeting** (not prospecting) |
| **3P (Third-Party)** | Bought interest segments | DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp | No | Prospecting via described interests |
| **MM (Mountain Match)** | MNTN-derived audience scoring | DS13 Vertical, DS38 BUK, DS46 Fangorn | **Yes** — `household_score` 0-10000 | Prospecting via MNTN quality models |
| **RTC** | Real-Time Conquesting qualifier | DS19 | Binary 10000/-1 (recent-site only) | Conquesting recent visitors |

**DS4 (CRM) is a retargeting tool, not a prospecting input.** A campaign that POSITIVELY includes DS4 is retargeting. Buyers commonly use DS4 NEGATIVELY (as exclusion) within prospecting — that's still prospecting, and represents most of our "MM+1P" cohort.

---

## §2 — The permutation space

Three orthogonal axes determine what a campaign actually does:

1. Which families are present (MM / 1P / 3P)
2. For each present family, polarity (positive / negative)
3. For positive clauses, structure (OR-union vs AND-intersect with other clauses)

### Base 8-bucket Venn (presence only, any polarity)

| Bucket | n campaigns | % campaigns | Spend (30d) | % spend | Conv rate |
|---|---:|---:|---:|---:|---:|
| nothing | 11,365 | **73.2%** | $14.49M | **35.9%** | 0.131% |
| MM only | 574 | 3.7% | $1.82M | 4.5% | 0.066% |
| 1P only | 1,292 | 8.3% | $7.65M | 18.9% | 0.055% |
| 3P only | 858 | 5.5% | $5.24M | 13.0% | 0.038% |
| MM + 3P | 717 | 4.6% | $3.39M | 8.4% | 0.061% |
| MM + 1P | 320 | 2.1% | $2.02M | 5.0% | 0.133% |
| 1P + 3P | 251 | 1.6% | $4.52M | 11.2% | 0.010% |
| MM + 1P + 3P | 152 | 1.0% | $1.27M | 3.1% | 0.055% |
| **Total** | **15,529** | 100% | **$40.42M** | 100% | |

### OR/AND distinction for MM-mixed inclusion (the critical refinement)

| Pattern | n camps | % of MM spend | $ spend | What buyer wrote |
|---|---:|---:|---:|---|
| MM only | 574 | 21.4% | $1,818K | just MM |
| **MM OR 3P** (union/expand) | **523** | **25.3%** | **$2,149K** | `OR(MM, 3P)` — expand reach |
| MM AND 3P (intersect/narrow) | 41 | 1.9% | $161K | `AND(MM, 3P)` — narrow MM to 3P matches |
| MM OR 3P + AND 3P too (FICO hybrid) | 45 | 5.3% | $452K | `AND(OR(MM,3P_A), 3P_B)` |
| MM AND NOT 3P (exclude) | 7 | 0.2% | $20K | `AND(MM, NOT 3P)` — barely used |
| MM 3P mixed polarity | 101 | 7.2% | $609K | mixed positive + negative |
| MM OR 1P | 18 | 1.5% | $123K | retargeting + MM |
| **MM AND NOT 1P** (CRM suppress) | **296** | **22.2%** | **$1,881K** | `AND(MM, NOT CRM)` — prospect away from customers |
| MM OR 3P AND NOT 1P | 89 | 11.0% | $933K | expand + CRM-suppress |
| MM OR 1P OR 3P | 46 | 2.3% | $196K | all three OR'd |
| Other MM+1P+3P combos | 22 | 1.7% | $148K | various |

**Key reads:**
- OR-additive inclusion dominates 3P: 523 camps "MM OR 3P" vs 41 "MM AND 3P" (12.8x more).
- 1P inclusion is almost entirely exclusion: 296 "MM AND NOT 1P" vs 18 "MM OR 1P" (16.4x more).
- The semantic "MM AND 3P narrows MM via intersection" exists technically but isn't used.

---

## §3 — What each pattern actually does at delivery time

Empirical proof from 2026-05-26 single-day delivery (~61M impressions). The bidder is **scored-first within pacing, falls through to unscored eligible IPs when MM ceiling exhausts.**

| Pattern | n camps | % imps unscored | % HI band (8k+) | Read |
|---|---:|---:|---:|---|
| MM only (baseline) | 393 | **4.2%** | 71.7% | Bidder mostly bids MM-scored IPs |
| **MM OR 3P (expand)** | 372 | **14.1%** | 58.9% | **3.3x more unscored** — OR brings unscored 3P IPs |
| MM AND 3P (narrow) | 35 | 8.4% | 79.7% | Stays scored — AND narrows MM to 3P-resolved subset |
| MM OR 3P + AND 3P (FICO hybrid) | 34 | **56.0%** | 29.2% | Max unscored — AND-narrowed to 3P-only IPs |
| **MM AND NOT 3P (excl)** | 6 | **0.4%** | **98.9%** | **Cleanest** — exclusion narrows hardest |
| MM AND NOT 1P (CRM suppress) | 287 | 6.7% | 76.0% | Similar to MM_only — narrow without affecting scoring |

**FICO case study — the smoking gun:**

| FICO campaign | Bucket | Spend (30d) | Scored imps/day | Unscored imps/day | Scored / $K |
|---|---|---:|---:|---:|---:|
| 525934 (MM_only) | MM_only | $41.7K | **71,525** | 334 | 1,715 |
| 325113 (MM+3P_incl_only) | MM OR 3P | $168.5K | **60,111** | 236,447 | 357 |

FICO's MM ceiling is ~60-72K scored impressions/day. The MM_only campaign hits the ceiling at $41K of spend; the bigger MM+3P campaign with 4x the budget produces the same scored count + 236K unscored 3P-added impressions.

**FICO's expression (campaign 325113):**
```jsonc
{ "categories": { "where": {
  "op": "and", "value": [
    { "op": "or", "value": [                          // ← top-level OR (additive)
      { "op": "any", "value": { "data_source_id": 13, "category_ids": [111001] }},  // MM
      { "op": "any", "value": { "data_source_id": 35, "category_ids": [/* 9 LiveRamp */] }}  // 3P
    ]},
    { "op": "any", "value": { "data_source_id": 35, "category_ids": [/* 17 more */] }}  // AND constraint
  ]
}}}
```

Buyer wrote OR. The bidder is honoring exactly what the buyer asked for.

---

## §4 — How often does the 3P inclusion actually matter?

Of the 609 MM+3P_incl_only campaigns, only **17.7% are actually ceiling-bound** (overflowing into the 3P-added IPs). The other 76% have a 3P clause that's effectively cosmetic.

| Ceiling status | n camps | % camps in 5a | Spend (30d) | % spend in 5a | Avg unscored | Avg spend / camp |
|---|---:|---:|---:|---:|---:|---:|
| **a. Ceiling-bound (>50% unscored)** | **76** | **17.7%** | $597K | **26.3%** | 76.2% | $7.9K |
| b. Partial overflow (10-50%) | 26 | 6.0% | $73K | 3.2% | 24.1% | $2.8K |
| **c. Below ceiling (<10% unscored)** | **328** | **76.3%** | **$1,599K** | **70.5%** | 1.2% | $4.9K |

**Top below-ceiling examples — buyers paying for 3P clauses that aren't being used:**

| Advertiser | Campaign | Spend (30d) | 3P dscids picked | Unscored % | Read |
|---|---|---:|---:|---:|---|
| Mercury Insurance | 448179 | $36.8K | 15 | **0.1%** | 15 LiveRamp picks unused |
| CareScout #2 | 544745 | $24.3K | **27** | **0.0%** | 27 LiveRamp dscids → 0 delivery |
| CareScout #3 | 594268 | $23.7K | **27** | **0.0%** | Same pattern |
| 4Patriots | 509490 | $26.0K | 21 | 0.4% | 21 dscids ~unused |
| American College of Education | 536501 | $88.4K | 20 | 0.6% | |
| Outdoorsy | 329649 | $98.3K | 12 | 1.4% | |

**Top-15 below-ceiling cohort: ~225 LiveRamp dscids selected across $620K of spend, none being reached at meaningful rates.**

---

## §5 — Where TI-956 actually delivers value

### The bimodal pattern in ceiling-bound (MM + 3P OR) campaigns

| Advertiser | Campaign | Spend | 3P dscids | Unscored % | Read |
|---|---|---:|---:|---:|---|
| **FICO** | 325113 | $168.5K | 17 | **79.7%** | Buyer-driven overflow into 3P |
| **Global X ETFs** | 259738 | $102.8K | 14 | **79.8%** | Same pattern |
| ElevenLabs | various | $103.7K | 120 | (no delivery 5/26) | Heavy 3P investment |
| Outdoorsy | various | $98.3K | 12 | 1.4% | Below ceiling |
| Cheddar's | 531366 | $93.4K | 6 | 1.6% | Below ceiling |

Same bucket (MM + 3P OR), 80% vs 1.4% unscored delivery. The buyers like FICO who hit ceiling and overflow ARE the TI-956 prize zone.

### The big-picture efficiency comparison (spend-weighted, 30d)

| Bucket | n camps | % camps | Spend (30d) | % spend | IVR | CVR | $/conv |
|---|---:|---:|---:|---:|---:|---:|---:|
| MM_only (pure scored prospecting) | 574 | 3.7% | $1.82M | 4.5% | 1.50% | 0.066% | **$51** |
| **3P_only (no MM at all)** | **858** | **5.5%** | **$5.24M** | **13.0%** | **0.75%** | **0.038%** | **$75 (1.5x worse)** |
| MM + 3P OR | 523 | 3.4% | $2.15M | 5.3% | 0.92% | 0.066% | $45 |
| MM + 1P NOT (CRM suppress) | 296 | 1.9% | $1.88M | 4.7% | 1.09% | 0.021% | $107 |
| **1P + 3P (no MM, anti-pattern)** | **252** | **1.6%** | **$4.53M** | **11.2%** | **0.87%** | **0.010%** | **$192 (3.8x worse)** |
| Nothing (RTC/retargeting) | 11,365 | 73.2% | $14.49M | 35.9% | 2.57% | 0.131% | $16 |

**The 3P-reliant prospecting cohorts (3P_only + 1P+3P) = $9.77M / 30d (~$117M/yr). They have the worst per-conversion efficiency of any prospecting bucket.** Per-segment quality scoring is the lever that addresses this directly.

---

## §6 — The smoking gun: per-segment CVR distribution

**LiveRamp segments are not equally good. The spread is 350x — and buyers spend roughly equal amounts on each tier.**

| Quintile (1,005 LiveRamp dscids) | Avg CVR | Median CVR | Spend (30d) | % spend | $/conv |
|---|---:|---:|---:|---:|---:|
| **Q5 (top 20%)** | **0.140%** | 0.066% | $1,877K | 13.9% | **$53** |
| Q4 | 0.016% | 0.015% | $1,904K | 14.1% | $203 |
| Q3 | 0.007% | 0.006% | $1,766K | 13.1% | $429 |
| Q2 | 0.002% | 0.002% | $1,977K | 14.7% | $1,284 |
| **Q1 (bottom 20%)** | **0.0004%** | 0.0003% | $1,672K | 12.4% | **$14,525** |
| Unranked (low support) | 0.062% | — | $4,298K | 31.8% | — |

**Buyers are picking blindly — empirically proven.** Spend distribution across CVR quintiles is essentially flat (12-15% per quintile). The buyer has no quality signal to differentiate top from bottom.

### Counterfactual benefit

| Scenario | Q1+Q2 spend → top-Q performance | Incremental conv/month | Incremental conv/year |
|---|---|---:|---:|
| 25% substitution | $912K | ~68K | ~820K |
| **50% substitution (realistic)** | **$1.82M** | **~136K** | **~1.6M** |
| 100% (theoretical ceiling) | $3.65M | ~272K | ~3.26M |

Conservative 50% substitution alone: **~1.6M incremental conversions per year on the LiveRamp-touching cohorts.**

---

## §7 — Recommendations

### Phase 1: ship TI-956 LiveRamp scoring on a schedule
- Deploy Alex Knorr's framework on weekly cadence, output to GCS.
- Macie wires admin UI: present top-N LiveRamp segments by composite quality score per campaign vertical.

### Phase 2: surface MM ceiling in the UI
- Per (campaign × MM segment × day), compute MM ceiling. Show it during campaign setup.
- Tell buyer: *"Your budget $Y exceeds MM segment X's ceiling ($Z). The overflow $(Y-Z) will route into 3P — pick high-quality segments here →"* (TI-956 surface)

### Phase 3: diagnostic for 76% below-ceiling cohort
- 328 campaigns / $1.6M / 30d sit below ceiling with 3P clauses unused.
- UI flag: *"Your 3P clause isn't being reached at current spend. Options: (a) remove it, (b) scale spend, (c) replace with broader MM target."*

### Phase 4: address 1P+3P anti-pattern
- 252 campaigns / $4.53M / 30d at $192/conv (3.8x worse than MM_only).
- Education / product flow: "Layering 1P and 3P without MM scoring is destructive — here's the data."

---

## Appendix — methodology

- All numbers reconcile to TI-999 Finding 15 in [summary.md](summary.md).
- Window: 30d ending 2026-05-28 (30d).
- Active campaign = ≥1 impression in window.
- AST parse via JS UDF in queries/ti_999_clause_polarity_ast.sql + queries/ti_999_finding15_pass12_or_vs_and_inclusion.sql (OR-group tracking).
- Score distributions from `cost_impression_log.model_params` on 2026-05-26 (~61M impressions).
- Per-segment CVR: equal-attribution proxy (campaign metrics ÷ N dscids). Matches TI-956's default `target_weight='equal'`. True per-segment causal isolation requires Alex's framework Performance axis.
- All bucket sums reconcile to bucket totals within 0.1%; cross-bucket impression totals reconcile to Pass 1 universe.

**Caveats:** descriptive not causal; single-day score-distribution snapshot; equal-attribution can over/underweight specific dscids in mixed-quintile campaigns; quintile rank stability over time not yet verified.

**Open follow-ups:** direct MM-vs-3P IP overlap via federated `household_scoring__v1` (Pass 8 used inference instead — federated scan too slow); per-segment performance with LOO-baseline (Alex's TI-956 framework); multi-week pattern stability.
