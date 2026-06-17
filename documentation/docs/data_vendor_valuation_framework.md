# Data-Vendor Valuation & Willingness-to-Pay Framework

**How to decide what a third-party data vendor is worth and what we should pay for it.** Built from TI-1027 (5x5);
applies to any site-visit / DDP data vendor that feeds MNTN Matched. Shareable.

## The core principle
A vendor's value is **the unique, usable signal it adds — not its raw volume, not its IP reach, and not metadata we
discard.** Redundant data is worth ~$0 (we already have it, or can generate it from our own bidstream — see TI-647).
So always measure value at the *net-new* margin.

## Step 1 — What's in the data (richness)
Profile the RAW feed schema (not just the processed table): columns, metadata, schema stability.
- Flag **thin** feeds (ip/url/time only — e.g. 5x5) vs **rich** (user_agent, referer, query, consent).
- Flag **schema risk** (positional `_COL_*` with no names = fragile).
- Flag **discard** (metadata we receive but drop at `site_visit_signal` — we may pay for rich, keep thin).

## Step 2 — How much (volume)
Per day/window: bytes, events, distinct IPs, distinct domains, **distinct (IP×domain) pairs**, (IP×url) pairs.
(IP×url ≈ IP×domain ⇒ domain-only feed.) Source: GCS parquet + `gsutil du`.

## Step 3 — Uniqueness, layered (the key reframe)
Measure uniqueness at **three grains**, because they diverge:
1. **Unique IPs** — reach we'd lose (usually small; we see most IPs ourselves).
2. **Unique domains** — fresh domain→vertical coverage.
3. **Unique (IP×domain) events** — the truest "unique data value" (a household→site observation no one else has).
For 5x5: 19.8% / 68.5% / **77.3%** — value is in the events, not the reach.
Also: **unique metadata** (does it provide columns no one else does, that we actually use?).

**Measure over the TARGETING window (not a snapshot), and account for recency.** Targeting uses the last ~30 days
(`site_visit_signal` has no TTL, so filter `dt`). Vendors deliver on irregular cadences, so a 7-day-snapshot
"overlap" *overstates* redundancy — a pair "also seen elsewhere" may be weeks old and about to expire. The
targeting-truthful metric is **sole-or-freshest within the window**: per (ip,domain), does any *other* vendor deliver
it within 30 days, and who is most recent? (5x5: 69.8% sole-in-window, 95.4% sole-or-freshest — vs 77% snapshot.)
"Overlap ≠ covered." This usually *raises* the floor.

## Step 4 — Is the unique slice valuable?
- **Classifiable?** % of unique domains that resolve to a vertical (`website_crawl_verticals`) = MM-usable.
- **High-intent?** Join the vendor's IPs to `cost_impression_log.household_score` → tier mix. (Note: score is a
  household property, ~uniform across vendors — so this checks "not garbage," not differentiation.)
- **Which customer-targeting verticals depend on its unique signal?** (e.g. B2B-audience verticals.) Note: these are
  the audience-targeting taxonomy our *customers'* campaigns use — not a proxy for MNTN's own go-to-market targets.
- **Metadata worth it?** Only if a downstream consumer uses it.

## Step 5 — Willingness to pay (three lenses → a band)
Billing base: **CPM = cost per 1,000 impressions served**; per-impression cost is in `cost_impression_log`.
1. **Market / CPM ceiling (walk-away max):** (impressions the vendor's data touches) × peer CPM ($0.50) / 1000.
   We never pay more than the data costs at market rate. (Co-occurrence, not causal — upper bound.)
2. **Incremental-reach floor:** (impressions to households *only* this vendor sees) × CPM / 1000.
3. **Value-based fair price:** vendor's share of MM's unique usable-signal × value(MM). MM ≈ tens of $M/yr via
   retention; a single-digit-to-low-double-digit % share lands in low-to-mid six figures/yr (also the typical DDP
   flat-fee range).
**Output:** floor / fair band / walk-away ceiling, plus per-unit rates:
- **$/net-new IP** (usually low — reach is rarely the value),
- **$/1,000 net-new (IP×domain) events** (the asset),
- **$/net-new classified domain**.
Place the actual fee on the scale → renew ≤ fair · renegotiate above · walk only near the ceiling.

## Step 6 — Choosing between vendors (tie-break rubric)
When two vendors deliver comparable data, decide in order:
**cost → non-redundancy (unique events) → richness (metadata we use) → freshness/delivery reliability →
latency (lag) → schema stability / contract terms.**

## Worked example
5x5 (TI-1027): thinnest feed, 77% unique (IP×domain) events, B2B-concentrated, 80% of touched impressions
High-Intent. WTP: floor ~$40K/yr, fair ~$150–600K/yr, walk-away ~$6.3M/yr. **Recommendation: KEEP/renew ≤ ~$600K/yr.**
Full report: `tickets/ti_1027_5x5_data_evaluation/artifacts/ti_1027_data_valuation_report.md`.

## Caveats
- Impression→vendor attribution is co-occurrence, not causal — bound it (floor/ceiling), don't over-claim.
- For a causal value, run an add/remove model ablation (re-run MM with vs without the vendor → ΔIVR → ΔRevenue).
- Pair-grain uniqueness is heavy — window/sketch; push to Databricks if a single scan is too large.
