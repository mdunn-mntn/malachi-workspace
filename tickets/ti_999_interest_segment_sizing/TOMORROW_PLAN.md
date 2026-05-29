# TI-999 Finding 15 — Plan for next session

## Status as of 2026-05-28 PM end-of-day

Deck v6 shipped at https://gist.githack.com/mdunn-mntn/dec381061ea7fab62c0d57962c8b1dcf/raw/ti_999_presentation_deck_standalone.html — with a work-in-progress callout. **Taxonomy is in flux; don't share this deck externally yet.**

## Tomorrow's primary task — full DS taxonomy clarification

Per user direction: "we need to focus on tomorrow what we are calling different things, we might need to go through all DS and start clarifying exactly what different things mean."

### Step 1 — full DS catalog audit

Query `bronze.integrationprod.data_sources` for all 60+ data_source_ids. For each, document:

1. Official `name` (from data_sources table)
2. What it actually is (verify against ipdsc__v1 row counts, sample expressions, or ask Zach)
3. Who maintains it (MNTN-internal vs bought vs advertiser-uploaded)
4. Buyer-selectable in the UI? (yes/no)
5. Which family it belongs to in the corrected taxonomy:
   - **MM** — MNTN-derived audience signals (the buyer-selectable MNTN family)
   - **List Retargeting (1P)** — advertiser-uploaded lists (DS4, DS8, DS47)
   - **3P** — bought interest segments (DS17, DS18, DS35)
   - **MNTN First Party (pixel)** — auto-attached, not buyer-selectable (DS21, DS34, possibly DS2)
   - **Other / out-of-scope** — Oracle (DS1, currently zero IPDSC), MNTN Campaigns (DS9), Ego, etc.

### Step 2 — corrections already made (carry into the audit)

| Correction | Source | Status |
|---|---|---|
| DS19 = MNTN Matched, belongs in MM (not separate as "RTC") | data_sources table | Done in Pass 13 |
| Formal retargeting flag is `objective_id=4` on campaigns | data_sources schema | Done in Pass 14 |
| Prospecting = objective_id ∈ {1, 5, 6} | Memory + Pass 14 verification | Done in Pass 14 |
| MM expanded to include DS14 (Global Data) + DS16 (Taxonomy Data) | Pass 15 + sample expressions | Done in Pass 15 |
| List Retargeting (not "1P") for DS4/8/47 | User correction | Done in deck v5/v6 |
| MNTN First Party = DS21/34 pixel data (not buyer-selectable) | User correction | Done in deck v5/v6 |

### Step 3 — once taxonomy is locked, re-run these passes

All currently use the original MM definition (DS13/38/46 only):
- Pass 3 — score distribution per bucket
- Pass 6 — ceiling-bound distribution in MM+3P_incl_only
- Pass 7 — below-ceiling advertiser examples
- Pass 9 — per-bucket IVR + CVR + cost efficiency
- Pass 10 — per-segment CVR quintile distribution (independent of MM def; only depends on LiveRamp dscids — should still be correct)
- Pass 11 — counterfactual benefit (same — depends on dscid ranking)
- Pass 12 — OR vs AND inclusion classification

### Step 4 — Update the deck with re-run numbers

After all passes re-run, do a sweep through deck slides 10-30 updating bucket sizes, % spend, and chart references.

## Key data already in hand (don't re-derive)

- **15,532 active campaigns / $40.42M / 30d** (universe)
- **Prospecting (obj=1,5,6):** 10,216 + 830 (MT) + 818 (S3) = 11,864 campaigns / $31.96M / 30d (75.6% of spend)
- **Retargeting (obj=4):** 3,123 campaigns / $8.47M / 30d (20.9% of spend)
- **Other (obj=7 ego, etc.):** ~545 campaigns / ~$0
- **Pass 15 prospecting buckets (expanded MM):**
  - MM only: 8,838 / 74.5% / $11.24M / 35.2% / $135M annualized
  - MM + 3P: 1,571 / 13.2% / $8.62M / 27.0% / $103M
  - MM + 1P: 1,052 / 8.9% / $6.29M / 19.7% / $75M
  - MM + 1P + 3P: 402 / 3.4% / $5.80M / 18.1% / $70M

## Key per-segment CVR data (does NOT depend on MM definition — still valid)

- 1,005 LiveRamp dscids ranked by CVR (Pass 10)
- 350x spread top vs bottom quintile
- $1.67M / 30d on bottom-quintile LiveRamp (~$20M/yr)
- Counterfactual: ~1.6M incremental conv/year (50% substitution)

## Sequence

1. DS catalog audit (Step 1 + 2) — ~1-2 hours
2. Re-run passes (Step 3) — ~2 hours (one big batch run)
3. Deck update (Step 4) — ~1 hour
4. Ship v7 deck with locked taxonomy

## Open questions to flag

- DS2 (MNTN First Party) — name overlaps with pixel data. Clarify what this DS specifically is vs DS21/34. Is it buyer-selectable?
- DS14 (Global Data) vs DS16 (Taxonomy Data) — what's the distinction? Sampled expressions show both used together in most prospecting campaigns. Are they functionally the same thing or different layers?
- Should DS9 (MNTN Campaigns) be in MM family? It's MNTN-derived but might serve a different role.
- Are there DSes that span multiple families (e.g., used both as targeting AND as exclusion logic)?
