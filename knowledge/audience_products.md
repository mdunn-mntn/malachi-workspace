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

## Tier definitions (Fangorn — the steady-state model)

**Fangorn runs twice per IP — once with the HI model, once with the PP model.** Each pass produces a raw score in [0, 1], so every IP gets **two Fangorn raw scores**. The HI / PP / MI / Max Reach tier names and the 0–10000 score ranges are how MNTN maps those raw scores onto the bidder's scale, gated by DS13 / DS19 membership.

Mapping rule:

| Tier | Fangorn pass | Raw qualifies | DS13 / DS19 overlay | Score range we map to |
|---|---|---|---|---|
| **HI** (High Intent) | HI model | raw > 0.8 | vertical ∩ keywords | **8000–10000** |
| **PP** (Peak Performance) | PP model | raw > 0.8 | vertical only (no keyword) | **6666–8000** |
| **MI** (Mid Intent) | either | raw 0.6 – 0.8 | (any DS membership) | **3333–6665** |
| **Max Reach** | (neither qualifies) | both raws < 0.6, or no Fangorn score | (any) | **1–3332** (random) |

Read:
- **Two raws, one tier.** Each IP gets a score from the HI model and a score from the PP model. The IP lands in a single tier per impression based on which raw qualifies and which DS overlay fires.
- **The > 0.8 threshold gates HI and PP.** When the HI-model raw clears 0.8 AND the IP is in vertical ∩ keywords → HI band. When the PP-model raw clears 0.8 AND the IP is in vertical only → PP band. The score within the band is that model's raw, remapped onto the band's range.
- **HI / PP are not Fangorn-internal concepts.** Fangorn outputs raw values; HI vs PP is a downstream mapping decision (which model run, which DS overlay).
- **MI is gated by raw 0.6 – 0.8.** Below the HI/PP threshold but above floor — lands in MI regardless of DS membership.
- **Max Reach is the random-score fallback.** No qualifying Fangorn score. The 1–3332 range has no scoring semantics; it's the broadest tier the bidder has access to when nothing better exists.

(Sources: Ryan Kleck, 2026-06-01; Sean Yang / Ryan, 2026-05-29.)

## Venn (bucket ⊃ vertical, keywords overlay)

![MM 2.0 scoring tiers Venn](../documentation/architecture/audience_products_venn.png)

- **Vertical is a strict subset of Bucket** (subindustry inside industry).
- **Keywords overlap both** Vertical and the rest of Bucket — and can extend past Bucket entirely.
- **HI** = the Vertical ∩ Keywords lens. **PP** = Vertical without Keywords. **MI** = Bucket ∩ Keywords without Vertical. **Max Reach** = the Keywords-outside-Bucket sliver plus all unscored fallback IPs.

Regenerate via `python3 documentation/architecture/audience_products_venn.py`. Companion diagram (Ryan Kleck's earlier sketch) at `documentation/architecture/audience_intent_scoring.png`. Confluence equivalent: [Audience and Intent Scoring Venn Diagram](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3567452174/Audience+and+Intent+Scoring+Venn+Diagram).

## How Fangorn scores an IP

```
   Fangorn (DS46) — two model passes per IP
   ┌─────────────────────────┐    ┌─────────────────────────┐
   │  HI model               │    │  PP model               │
   │  raw_HI ∈ [0, 1]        │    │  raw_PP ∈ [0, 1]        │
   └────────────┬────────────┘    └────────────┬────────────┘
                │                              │
                └──────────────┬───────────────┘
                               ▼
        post-processing — pick the qualifying raw, apply DS13 ∩ DS19 overlay,
                          remap onto the bidder's 0–10000 scale
                               │
   ┌───────────────────────────┼─────────────────────────┐
   │                           │                         │
   raw_HI > 0.8       raw_PP > 0.8       either raw      both raws < 0.6
   + vertical         + vertical          in 0.6–0.8      (or no score)
   + keywords         (no keywords)       (any DS)        │
   │                  │                   │               │
   ▼                  ▼                   ▼               ▼
   HI                 PP                  MI              Max Reach
   8000–10000         6666–8000           3333–6665       1–3332 (random)
```

**Two raws produced per IP, one tier assigned per impression.** The score within a band is the qualifying model's raw, remapped onto the band's range — higher raw → higher mapped score. The HI vs PP split is determined by which model qualifies AND which DS overlay fires.

### Implementation note — Fangorn is an audience overlay

When MNTN switches an advertiser to Fangorn, `audience.audience_segments` is updated to reference DS46, but the base `audience.audiences` row still references DS13 + DS19 (Ryan Kleck, 2026-06-01). A Fangorn-overlaid MM campaign **is still MM** — Fangorn replaces the scoring algorithm, not the audience configuration. The bidder reads the score the same way; what changed is the upstream model.

### Legacy path (Non-Fangorn) — being phased out

For advertisers not yet on Fangorn (~78% of S1 spend as of 2026-05-31), there's no continuous raw score — the system assigns scores by discrete rule:
- **HI** = exactly 10000 (point mass).
- **PP** = exactly 8000 (point mass).
- **MI** = graduated 3333–6665.
- **Max Reach** = random 1–3332.

No within-tier ranking inside HI or PP — every HI IP shares the same 10000. As Fangorn rolls out, this collapses into the continuous distribution above and within-band ranking becomes meaningful.

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
