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

**Fangorn produces one number per IP.** That's the entire model output — a raw score in [0, 1] from DS46. The HI / PP / MI / Max Reach tier names and the 0–10000 score ranges are how MNTN remaps that single number onto the bidder's scale.

Mapping rule:

| Tier | Fangorn raw qualifies | DS13/DS19 overlay | Score range we map to |
|---|---|---|---|
| **HI** (High Intent) | > 0.8 | vertical ∩ keywords | **8000–10000** |
| **PP** (Peak Performance) | > 0.8 | vertical only (no keyword) | **6666–8000** |
| **MI** (Mid Intent) | 0.6 – 0.8 | (any DS membership) | **3333–6665** |
| **Max Reach** | < 0.6 or no Fangorn score | (any) | **1–3332** (random) |

Read:
- **One score per IP, not one per tier.** Fangorn raw is the underlying signal; the band is presentation. Higher Fangorn raw within a tier → higher mapped score (so HI #1 and HI #100 are distinguishable).
- **The > 0.8 threshold is the gate to HI / PP.** Above 0.8 the IP stays in the high-confidence tier; the DS13 / DS19 overlay decides HI vs PP.
- **HI / PP are not Fangorn-internal concepts.** Fangorn doesn't "know" about HI or PP — it just outputs raw. The HI vs PP split happens in the post-processing step.
- **MI is gated by raw 0.6 – 0.8, not by DS membership.** Any IP in that raw range lands in MI regardless of which DSes it sits in.
- **Max Reach is the random-score fallback.** No Fangorn evaluation. The 1–3332 range has no scoring semantics; it's the broadest tier the bidder has access to when nothing better exists.

(Sources: Ryan Kleck, 2026-06-01; Sean Yang / Ryan, 2026-05-29.)

## Venn (bucket ⊃ vertical, keywords overlay)

![MM 2.0 scoring tiers Venn](../documentation/architecture/audience_products_venn.png)

- **Vertical is a strict subset of Bucket** (subindustry inside industry).
- **Keywords overlap both** Vertical and the rest of Bucket — and can extend past Bucket entirely.
- **HI** = the Vertical ∩ Keywords lens. **PP** = Vertical without Keywords. **MI** = Bucket ∩ Keywords without Vertical. **Max Reach** = the Keywords-outside-Bucket sliver plus all unscored fallback IPs.

Regenerate via `python3 documentation/architecture/audience_products_venn.py`. Companion diagram (Ryan Kleck's earlier sketch) at `documentation/architecture/audience_intent_scoring.png`. Confluence equivalent: [Audience and Intent Scoring Venn Diagram](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3567452174/Audience+and+Intent+Scoring+Venn+Diagram).

## How Fangorn scores an IP

```
   Fangorn ML model (DS46)
            │
            ▼
   raw score ∈ [0, 1]          ← one number per IP. This is the only ML output.
            │
            ▼
   post-processing step — apply DS13 vertical ∩ DS19 keywords overlay,
                          remap the raw value onto the bidder's 0–10000 scale
            │
   ┌────────┴────────────────────────────────────────────┐
   │                                                     │
   raw > 0.8         raw > 0.8         raw 0.6–0.8       raw < 0.6
   + vertical        + vertical        (any DS)          (or no score)
   + keywords        (no keywords)                       │
   │                 │                 │                 │
   ▼                 ▼                 ▼                 ▼
   HI                PP                MI                Max Reach
   8000–10000        6666–8000         3333–6665         1–3332 (random)
```

**The score within a band is the Fangorn raw, remapped.** Higher raw → higher mapped score. There aren't separate "HI scores" and "PP scores" — there's one Fangorn raw per IP, and HI vs PP is which range we project it onto.

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
