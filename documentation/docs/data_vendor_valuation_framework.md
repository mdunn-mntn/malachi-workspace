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

**Volume ≠ value — three guards against over-estimating:**
1. **Never value raw events** — they're inflated by repeat visits (e.g. a vendor's 93M events collapse to 33M distinct
   (IP×domain); 33Across is the biggest feed at 834M events but the *shallowest* in unique depth). Value distinct
   (IP×domain), not events.
2. **Measure the distribution, not the mean** — averages are crushed by the see-once tail. Use median / p90 / buckets.
   (5x5: mean +1.2 additional domains/IP, but the *median* is +1; ~71% of IPs get exactly +1, only ~14% get 2+.)
3. **Anchor on the UNION, never the sum across vendors** — summing per-vendor counts double-counts the overlap
   (~24% of (IP×domain) pairs are seen by 2+ vendors). Use distinct union.

**Per-IP depth (a second value lens).** Two vendors can share an IP yet one is worth more if it sees that IP visit
more *distinct* sites. Measure **distinct domains per IP** and, for shared IPs, the **additional unique domains** a
vendor contributes (union vs best-single-vendor). Are vendors additive or redundant? (Across MNTN site-visit vendors:
**76% of all (IP×domain) pairs come from one vendor**, and stacking vendors gives ~70% more domains per IP than the
best single — so they are **additive**, not redundant.) Note the two lenses can rank vendors differently: **domain
breadth** (what the domain→vertical classifier consumes) vs **per-IP behavioral depth** (per-IP features). State which
the model actually uses.

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
it within 30 days, and who is most recent? Split it four ways — sole / vendor-freshest / **tied (same-day — a copy
survives, so NOT a clean win)** / other-fresher. (5x5: 69.8% sole + 1.2% freshest = ~71% irreplaceable; 24.4% tied;
4.6% other-fresher — vs a 77% one-day snapshot.) "Overlap ≠ covered, but a same-day tie *is* covered." Usually
*raises* the floor.

## Step 4 — Is the unique slice valuable?
- **Classifiable?** % of unique domains that resolve to a vertical (`website_crawl_verticals`) = MM-usable.
- **High-intent?** Join the vendor's IPs to `cost_impression_log.household_score` → tier mix. (Note: score is a
  household property, ~uniform across vendors — so this checks "not garbage," not differentiation.)
- **Which customer-targeting verticals depend on its unique signal?** (e.g. B2B-audience verticals.) Note: these are
  the audience-targeting taxonomy our *customers'* campaigns use — not a proxy for MNTN's own go-to-market targets.
- **Metadata worth it?** Only if a downstream consumer uses it.

## Step 5 — Willingness to pay (three lenses → a band)
Billing base: **CPM = cost per 1,000 impressions served** (CPM = "cost per mille" = per *thousand*, never per person;
confirmed empirically — CIL data cost ≈ $0.001/impr ≈ $1 CPM, media ≈ $10.74 CPM). **Pin down "per 1,000 of WHAT"**
in any CPM quote: impressions *served* (what CIL measures) vs records *delivered* (a much larger base) — same rate,
very different dollars. Per-impression cost is in `cost_impression_log`.
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

---

## 2026-07 extension (AUDI-1089): dependency ceiling, leave-one-out, roster frontier

Three lenses added on top of this framework — full methodology with worked Klickly example in
`tickets/audi_1089_ddp_vendor_evaluations/runbook/dependency_valuation.md`:
1. **Dependency-ceiling valuation**: stock (sole usable IPs) → flow (weekly sole won bids × 52 = expected
   annual won bids; never annualize unique IPs) → performance (visits, Poisson CI) → dollars (observed
   eCPM × margin ladder, net of other data costs). T1 provable floor ↔ T2 ceiling; scenario envelope, not CI.
2. **Leave-one-out billing reassignment**: under first-reporter-wins, dropping a metered vendor saves
   bill × (share of its pairs whose earliest other holder is nobody/flat-fee/free) — measure destinations
   per pair (pair-mix proxies were 8× wrong for Sovrn); metered↔metered overlap proved negligible.
3. **Exhaustive roster frontier**: per-pair holder-bitmask histogram (≤1,024 rows) makes every keep-subset
   an exact lookup-sum — coverage, exact recovery, revenue-at-risk, and NET per roster size; optima may
   nest (then add-order = marginal-coverage ranking).
Negotiation translation: justified CPM on ALL delivered rows / on USED (credited) imps vs the rate paid /
flat-contract equivalent band.


## 2026-07-15 extension (AUDI-1089): audience-count vs delivery-reality populations

When a vendor's value claim is "audience size", split the population before pricing it:
**ALL members** (everything in the membership table, never-served included — the number UIs
display) vs **SERVED members** (IPs that actually win impressions — the number campaigns feel).
First-party logs see auction-active households by construction (pixel + bid-time collection),
so vendor-unique members concentrate in the never-served tail: in AUDI-1089, keyword audience
COUNTS shrank ~30% without vendors while served members stayed 97.6-99.9% covered per score
tier, and vendor-only members served at 0.48% with 26x-below-par visit rates. Price the
delivery-reality population; treat count-only contribution as denominator inflation, not reach.
Also verify WHAT the vendor-dependent categories are — here the collapsing ones were
ad-infrastructure junk classifications (cookie-sync URLs as "Paid Advertising" keywords).

## 2026-07-16 extension (AUDI-1089): Net Kept Margin — the one-formula statement

The whole framework compresses to a stakeholder-ready formula (Malachi's formulation):

    Net Kept Margin_v = (I_v / 1,000) x CPM x M − D_v

    I_v  — unique (IP x Domain x Date) impressions served only because of vendor v
           (net of free sources; leave-one-out against the rest of the paid roster)
    CPM  — client-facing price per 1,000 served impressions
    M    — gross margin as a % of revenue
    D_v  — data cost paid to vendor v over the same window

Positive → the vendor pays for itself; negative → the data costs more than the margin it
creates. Two implementation notes that make it defensible in a room:
- **(I_v/1000) x CPM is directly measurable** as billed media dollars on the unique cohort —
  no CPM assumption needed (realized CPM lands ~40% ABOVE the touched-cohort rate on unique
  slices; low-overlap IPs get fewer, pricier serves).
- **Incremental-only is empirically forced, not a modeling taste**: counting everything a
  vendor "touches" let the 8 paid vendors claim 6.39x the impressions actually served —
  touched-based values sum to more than the business.

## 2026-07-13 extensions (AUDI-1089): portfolio lenses beyond leave-one-out

- **Net-of-free value ladder:** drop every unit your OWN free sources capture from the universe
  first; then (a) STANDALONE value per vendor = its net-of-free units × its measured revenue
  density (vendor as your only paid source), and (b) MARGINAL ladder = greedy add-order pricing
  each roster seat (verify optima nest). Standalone vs marginal is the negotiation: a vendor can be
  near-fair alone and worth 2% of its bill at seat 7. Label $ as DEPENDENT REVENUE; pay-up-to =
  × margin (15/20/30%).
- **Visit-grain (unit × date) accounting:** the value/billing unit is (ip, domain, DATE) — new date
  on a known pair = recency refresh (real, billed); same-date co-delivery = waste. Run coverage at
  both grains; if they agree (ours: within 6%), conclusions are grain-robust.
- **Free-preemption lens (substitutes for drops):** if billing lets paid vendors earn credit on
  units your free sources also capture, quantify bill × free-cohold share = recoverable WITHOUT
  losing the vendor's unique data. Compare against drop savings before recommending drops.
- **Scale-normalized per-unit value:** netnew-vs-free rate × revenue density, per 1M USABLE units
  and per 1M RAW shipped units (dirty feeds deflate on the raw basis). Finds "good but tiny"
  vendors whose verdict scaling could flip (Cybba) and exposes junk-uniqueness artifacts (Sovrn:
  #1 usable-basis, #4 raw-basis). Flat-fee vendors: scaling is free upside; metered vendors:
  scaling multiplies the loss unless repriced.

- **Kept-margin, not revenue, is the pay number** — show revenue AND a pay-range column
  (x blended-margin range, e.g. 10-30%) side by side with the bill; roster P&L = Σ bills vs
  Σ kept margin is the single strongest renegotiation exhibit.
- **Dependent-revenue undervalues BREADTH vendors — always run both axes:** a vendor whose unique
  contribution is classifier domain coverage (many sole classified domains, high same-day-dup IPs)
  shows near-zero sole-serve revenue while carrying large infrastructure value. If the
  dependent-revenue break-even fee and the domain-axis WTP band disagree by >10x, the vendor is a
  breadth vendor: price it as classifier coverage, not media performance (AUDI-1089: Predactiv
  $3.7-11K revenue basis vs $0.7-3M domain basis).

## 2026-07-17 extensions (AUDI-1115): the CPM-per-billing-unit layer

Once you know the billing STRUCTURE (per credited WON impression — confirm empirically, e.g.
the BAE `ddp_mm_winners_imp` table keyed on `ad_served_id`), the "what CPM should we pay"
question decomposes cleanly. Lessons:

- **A CPM is meaningless without its billing UNIT — always match the numerator population to
  the denominator.** The naïve trap (L0/L0p): divide a *marginal* value (media on the sole/unique
  cohort, ~5.6M imps) by the *full* meter (~70M credited imps) → an artificially low CPM that
  mixes two populations. The clean ratio (L0f): value and units on the SAME cohort. Compute both
  numerator and denominator on identical impressions or don't form the ratio.
- **The per-credited-impression media CPM is ≈ vendor-INDEPENDENT** — it's just the platform's
  media rate (CTV ~$10.7 CPM), because the media is what the *advertiser* pays for the impression,
  not a function of which vendor's signal won it. So break-even CPM = media CPM × margin ≈ the
  same $1–3 for every vendor. **The vendor differentiator is RESIDUAL VOLUME (how much unique,
  non-overlap signal survives preemption), not the per-impression rate.**
- **Preemption vs rate-cut are two ROUTES to the same fair total, not additive.** Preempt the
  free-log overlap (removes ~90% of billed volume at the impression grain) OR cut the rate on the
  full meter — doing both double-discounts. Cleaner: preempt, keep the rate (it's below break-even
  on the residual); the savings are volume, not rate.
- **A per-impression pricing lens is NOT a keep/drop test.** Valuing each credited impression at
  full media (fractionally split across co-winners) over-credits — it counts impressions we'd win
  anyway via other paid vendors or free-log membership on the same IP. Use the SOLO/marginal cohort
  for keep/drop; use the per-impression cohort only to price a rate you've decided to pay.
- **Incrementality is the load-bearing unstated assumption.** "media × margin" as value assumes the
  vendor's signal is WHY the impression was served. If we'd serve that household anyway, the value
  is lower. State it; don't cut a rate below break-even without an incrementality read.
- **The two grains of "vendor-unique"** (IP-membership = marginal ~5.6M/mo; impression-winner =
  credited ~27.5M/mo for 33Across; ~5× apart, same per-impression economics) — see
  data_knowledge § billing. Confirm which grain the crediting system uses before quoting totals.
