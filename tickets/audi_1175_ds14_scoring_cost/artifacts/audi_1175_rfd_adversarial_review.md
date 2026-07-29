# AUDI-1176 RFD — adversarial review record (2026-07-28)

Two independent reviewers (fresh context), complementary lenses, told to assume the RFD is wrong. Verdict from both: **fix-first, not publish-ready as drafted.** All findings below were verified and folded into `audi_1175_rfd_draft.md`.

## Safety-critical (both would be caught by a pipeline owner)

| # | Finding | Resolution in hardened RFD |
|---|---|---|
| F1 | Sizing counted DS14 *all categories*; the gate uses `category_id=1`. Auction-only IPs carry an exchange category, not 1 — a `cat=1` gate keeps a smaller set than sized, and "matches the bidder gate" had no code proof. | Added **Open Risk A** = gate on the bidder's exact DS14 predicate + re-size on it; flagged the sizing caveat inline. |
| F2 | "RTC covers intra-day" is mis-scoped — RTC (`realtime_conquest_score`, 1-day TTL) fills only conquest matches, not the batch household/vertical score. Re-entering non-conquest IPs have ≤24h staleness. | Honest-core #3: restated as **≤24h score staleness on a re-entering cohort**; shadow run must check score *quality*, not just counts. |

## Materially temper the $ (headline was overstated)

| # | Finding | Resolution |
|---|---|---|
| Savings model | Serverless is DCU pay-per-use (code-confirmed), but a committed-use discount / minimum-spend floor would mean cut DCU-seconds save $0; shadow run measures DCU-seconds not the invoice. | Honest-core #1 + **Gate B**: pull the billing export before sending; downstream BQ scans are reserved (freed ≠ billed). Dropped "genuine savings, not spend-redirect." |
| F3/F4 | `prospecting_keywords` is write-dominated (33.8B rows) w/ broadcast; a 259M-IP gate adds a new shuffle. `vertical_mid` reads all 31 DS13 partitions regardless (DS14 not a partition key). | Honest-core #2: 69%/39% is an **upper bound**; realized savings sub-linear, unquantified until the shadow run. |
| $130k anchor | Stacks 3 optimistic assumptions ($39k base is a 4.5× band; linear scaling; DS19 lands at prospecting_keywords). | BLUF leads with the **floor** (~$1.3k/mo DS13); $9.6k labeled upper bound. |

## Honesty / precision

- BLUF "never used / zero coverage loss" contradicted the honest core → softened to "*appear* non-biddable by design; shadow run confirms."
- 8-vs-30-day window stated as fact → honest-core #4 caveat.
- Starvation ✅ rested on an access-blocked proxy (65%-at-Max-Reach = current state, not post-gate counterfactual) → disclosed.
- "Code-confirmed ×2" hid two mid-investigation misreads + unconfirmed runtime liveness → reframed to "every candidate auction-scoped, safe regardless; runtime liveness not confirmable from code."
- `cache_hhst_population_filters` is mid-band (`hhst 3334-6665`) scoped, not "full prospecting_intent" → corrected.
- idso is a plain upsert; the COALESCE is upstream in v4 → corrected.
- Dropped "likely Ryan Kleck" (guess) → "confirm the DAG owner." Cut the self-serving freq-cap contrast. Demoted "MembershipDB resilience" to a qualitative footnote. Named the decider + netted the new-DAG-dependency downside.

## What survived scrutiny (both reviewers agreed is code-solid)
- The structural waste thesis (31-day ungated scoring vs short-window DS14 bidding).
- HHST auction-scoping (no coupling to the scored universe) — the concern that could have blocked the whole thing.
