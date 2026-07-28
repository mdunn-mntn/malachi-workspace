# AUDI-1175: DS14 scoring-cost spike — Presentation

## Audience
AUDI grooming / anyone evaluating whether to fund AUDI-1176.

## Key Message
MNTN scores a 31-day IP universe but can only bid on the ~8-day DS14-addressable slice; gating scoring to the addressable set saves ~$2–11k/mo and is safe across every consumer checked.

## Narrative Flow

### 1. Context
DS14 ("MNTN Global Data") is a freshness gate ANDed onto ~every audience expression: only IPs MNTN saw recently (~8-day serving TTL) are biddable/countable. But MM/vertical intent scoring runs over the full 31-day IPDSC universe, ungated — so we score IPs that can never be bid on. This is cost-reduction work.

### 2. What We Did
- Traced DS14 (origin, effective window, materialization) and the audience_intent scoring pipeline in code (airflow-ti, sqlmesh, membership-db, airflow-camperbid, DDM, idso).
- Sized the scored-vs-addressable gap in BigQuery (HLL distinct-IP on `ipdsc__v1`).
- Estimated Dataproc-serverless cost of the scoring DAG.
- Audited every consumer of the scored universe; ran shadow queries on the HHST recommender.

### 3. Key Findings
- **Waste:** DS19 (MM Core) 69% / DS13 (verticals) 39% of scored IPs are non-addressable within the 8-day window.
- **$:** whole scoring DAG ≈ $39k/mo; the gate saves ~$1.3k/mo (DS13) to ~$11k/mo (~$130k/yr, if the DS19 cut is applied to `prospecting_keywords`).
- **Safe:** serving/bidding, Fangorn (separate 1%-sampled feature store), LiftLab (served-only), AUD-5221 (score-deciles), and the HHST recommender (population is auction-scoped, not the scored universe) are all unaffected.
- **Starvation baseline:** 65% of 32,550 campaigns are already at Max Reach (threshold=0); the gate can't worsen the majority.

### 4. So What?
The optimization is real and safe. Scoring is decoupled from bidding today; gating the scoring input to DS14-addressable IPs cuts daily compute with zero biddable-coverage loss — returning IPs are scored the day they re-enter DS14, and RTC covers intra-day new IPs.

### 5. Next Steps
- Implement via AUDI-1176 (gate the scoring input; primary $ lever = `prospecting_keywords` / DS19).
- Firm up the $ to a point estimate via the GCP billing export when scheduled.

## Charts & Visualizations
- Scored-vs-addressable bar: DS19 499M scored vs 157M addressable; DS13 270M vs 165M.
- Applied-threshold distribution: 65% at Max Reach (threshold=0), ~31% with a real intent gate.

## Appendix
- Queries in `queries/`. Full analytical record + all caveats in `summary.md`.
- Caveats: HLL ~1.5% error; $ is order-of-magnitude (Dataproc serverless, runtime assumed). PTV writer / population source code-confirmed auction-scoped (`bid_price_log`/`bidder_bid_events`).
