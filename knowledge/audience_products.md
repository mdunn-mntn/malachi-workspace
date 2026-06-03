# MNTN Audience Products — canonical definitions

**Status:** canonical reference (TI-897). Replaces the conflicting interpretations floating around.
**Last updated:** 2026-06-03. Built from TI-999 findings + ds_catalog.md + Ryan Kleck / Sean Yang clarifications.

---

## Bottom line

- **MM (Mountain Match)** is THE audience product. It is MNTN's proprietary scoring system — the umbrella under which buyers prospect.
- **HI / PP / MI / Max Reach** are NOT separate products. They are **scoring tiers within MM**, assigned per-IP based on how strongly that IP matches the buyer's bucket, vertical, and keywords.
- A buyer who picks "Mountain Match" in the UI gets all four tiers. The bidder ranks IPs by tier (highest score first) and pacing determines how far down into lower tiers it goes.
- **1P** (advertiser CRM upload) and **3P** (LiveRamp / ShareThis / Dstillery) are separate INPUT layers — they are not scored by MNTN and not part of MM. They can be layered with MM to narrow the eligible IP set.

## What MM evaluates

MM scores each IP against three signals from two data sources:

```
DS13 (MNTN Vertical Categorization)
  ├── Bucket   = Industry         (e.g. "Automotive")
  └── Vertical = Subindustry      (e.g. "Automotive / Used Cars")

DS19 (MNTN Matched)
  └── Keywords (LLM-derived, per-vertical keyword set)
```

The combination of bucket / vertical / keyword membership determines the tier.

## Tier definitions (MM 2.0 state table — locked)

| Tier | In bucket? | In vertical? | Keywords fire? | Score | Bid-eligible? |
|---|:-:|:-:|:-:|---|:-:|
| **HI** (High Intent) | ✓ | ✓ | ✓ | 10000 | ✓ |
| **PP** (Peak Performance) | ✓ | ✓ | ✗ | 8000 | ✓ |
| **MI** (Mid Intent) | ✓ | ✗ | ✓ | 3333–6665 | ✓ |
| **Max Reach** | ✗ | ✗ | ✓ | 1–3332 (random) | ✓ |
| — | ✓ | ✗ | ✗ | 3333–6665 | ✗ (keyword fails) |
| — | ✗ | ✗ | ✗ | NULL | ✗ |

Read: PP is "vertical match, no keyword" → score 8000. MI is "industry match, no vertical, keywords fire." Max Reach is the random-score fallback tier when nothing strong matches.

## Venn (bucket ⊃ vertical, keywords overlay)

![MM 2.0 scoring tiers Venn](../documentation/architecture/audience_products_venn.png)

- **Vertical is a strict subset of Bucket** (subindustry inside industry).
- **Keywords overlap both** Vertical and the rest of Bucket — and can extend past Bucket entirely.
- **HI** = the Vertical ∩ Keywords lens. **PP** = Vertical without Keywords. **MI** = Bucket ∩ Keywords without Vertical. **Max Reach** = the Keywords-outside-Bucket sliver plus all unscored fallback IPs.

Regenerate via `python3 documentation/architecture/audience_products_venn.py`. Companion diagram (Ryan Kleck's earlier sketch) at `documentation/architecture/audience_intent_scoring.png`. Confluence equivalent: [Audience and Intent Scoring Venn Diagram](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3567452174/Audience+and+Intent+Scoring+Venn+Diagram).

## How the score is produced — Non-Fangorn vs Fangorn

Both paths feed the same field (`household_score`, 0–10000) on every impression. The difference is **how the score is generated**.

### Non-Fangorn (legacy — ~78% of S1 advertisers)

Discrete point masses inside each tier band:
- **HI** = exactly 10000 (point mass).
- **PP** = exactly 8000 (point mass).
- **MI** = graduated 3333–6665.
- **Max Reach** = random 1–3332.

Roughly 6,667 distinct score values total. No continuous gradation within HI or PP.

### Fangorn (~22% of S1 advertisers, rolling out)

Continuous 1–10000 (10,000 distinct values). Mechanics (Ryan Kleck, 2026-06-01):

1. Fangorn (DS46, ML model) produces a single raw score 0–1 per IP.
2. The downstream scoring job maps that raw score onto an HHST band using DS13 vertical ∩ DS19 keyword overlap:

```
   if raw > 0.8 and (vertical ∩ keywords)  → HI band   8000–10000  (graduated)
   elif raw > 0.8 and vertical only         → PP band   6666–8000   (graduated)
   elif raw in 0.6–0.8 (any membership)     → MI band   3333–6665   (graduated)
   else (raw < 0.6 or no Fangorn score)    → Max Reach 1–3332      (random)
```

Key implications:
- **PP and HI are not Fangorn-internal concepts.** Fangorn outputs one number; the tier-mapping step splits HI from PP based on whether keywords fired.
- **Max Reach under Fangorn is still random** — "no Fangorn at all" (Ryan). 1–3332 has no scoring semantics either way.
- **The Fangorn switch is an audience overlay** — `audience.audience_segments` is updated to reference DS46, but the base `audience.audiences` row still references DS13 + DS19. A Fangorn-overlaid MM campaign IS still MM; Fangorn replaces the scoring algorithm, not the audience config.

### Why the picture matters

Non-Fangorn produces big point masses at 8000 and 10000 — most of the volume sits in those two bins. Fangorn smooths that into a continuous distribution, which means within-tier ranking (HI #1 vs HI #1000) becomes meaningful. The product story is "Mountain Matched AI" — replacing static tiers with dynamic, graduated scoring.

## When each tier applies (buyer-facing)

| Tier | When the bidder hits it |
|---|---|
| **HI** | First. Highest-intent inventory. Supply is finite per advertiser — almost no campaign fills on HI alone. |
| **PP** | Vertical match without keyword match. The fallback when HI is exhausted but the buyer still wants vertical-targeted spend. |
| **MI** | Bucket-match without vertical. Lower confidence but materially graduated under Fangorn. Use when expanding reach matters more than precision (awareness campaigns, cold-start advertisers). |
| **Max Reach** | The floor. Bidder hits this when pacing demands breadth and no scored IP is available. No intent signal — performance reflects that. |

## What MM is NOT

- **MM is not 1P.** 1P = advertiser-uploaded customer data (DS4 CRM, DS8 IP List, DS47 CRM-IDG). Not scored by MNTN. Used almost entirely as exclusion clauses in prospecting.
- **MM is not 3P.** 3P = bought interest segments (DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP). Not scored by MNTN.
- **1P and 3P layer with MM via AND-intersection** — they NARROW the MM-scored universe; they don't bring unscored IPs into bidding. With HHST > 0, unscored IPs fail the threshold and don't bid (TI-999 Finding 14).
- **RTC (Real-Time Conquesting) is not a separate scoring system.** RTC is the real-time variant of MM that fires within an hour; the main MM scorer catches up after (Sean Yang, 2026-05-29). RTC delivers binary 10000 / −1 for recent-site visitors only.

## Sources

- MM 2.0 state table — Ryan Kleck + Sean Yang (TI team), 2026-05-29 — `knowledge/ds_catalog.md` §MM 2.0 scoring state table.
- Fangorn continuous-scoring mechanics — Ryan Kleck, 2026-06-01 — `knowledge/data_knowledge.md` §Intent Scoring Architecture.
- TI-999 Finding 14 (bidder score reality, three score fields), Finding 15 (MM × 3P AND-intersection).
- TI-896 (Peak Performance / Mountain Matched relationship at expression level).
- Diagram: `documentation/architecture/audience_intent_scoring.png`.
- Confluence: [Audience and Intent Scoring Venn Diagram](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3567452174/Audience+and+Intent+Scoring+Venn+Diagram).
