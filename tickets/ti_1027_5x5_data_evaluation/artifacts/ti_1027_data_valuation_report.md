# Data-Vendor Valuation & Willingness-to-Pay — 5x5 (DS 25)

**A reusable framework for "what is a data vendor worth, and what should we pay?", applied to 5x5.**
TI · estimation exercise · windows: scale/pairs 2026-06-15 (1d), uniqueness 2026-06-09→15 (7d). Source data in
`outputs/`; methods in `queries/ti_1027_analysis_queries.sql`.

## Bottom line
- 5x5 is a **flat-fee, ultra-thin feed** (just IP→URL→time) whose value is **not its IPs and not metadata — it's the
  unique household→site observations it alone sees.** 77% of its (IP×domain) events and 69% of its domains are
  unique to it, concentrated in **B2B** (MNTN's #1 Q2 growth theme).
- **Fair price ≈ $150K–$600K/yr.** **Floor ≈ $40K/yr** (pure incremental reach). **Walk-away ≈ $6.3M/yr** (what its
  data would cost at the $0.50 peer CPM on all the impressions it touches — we'd never pay more).
- **Recommend: renew at/below the fair band; renegotiate above it; walk only near the ceiling.** Confirm the actual
  flat fee with billing to place it on this scale.

---

## 1. What's in the data (content & richness)
5x5 sends a **positional, schema-less parquet**: `_COL_0` (ip), `_COL_1` (url), `_COL_2` (epoch seconds). **No column
names, no user-agent, no metadata.** It is the **thinnest and highest-schema-risk** of all 10 site-visit vendors —
a vendor-side column reorder would silently corrupt ingestion (flag to Sean regardless of the renew decision).

Richness ranking (raw feed; `outputs/ti_1027_vendor_richness.csv`):
| Vendor | Raw columns | Metadata beyond ip/url/time | Profile |
|---|---|---|---|
| guid_log (internal) | ip, referer, query, ua, advertiser_id, time | query, advertiser_id | rich |
| augmentor (internal) | ip, ua, page, referrer, placement, time | referrer, placement | rich |
| Justuno/Sovrn/Klickly/33Across API (pixel) | + event_id, mobile, query_str, referer, ua | **all dropped** at site_visit_signal | rich-but-stripped |
| 33Across | ip, url, ua, time | user-agent kept | medium |
| Predactiv | ip, url, ua, time | user-agent **dropped** | thin-after-drop |
| Cybba | ip, url, time | none | thin |
| **5x5** | **`_COL_0/1/2` (ip/url/time)** | **none; unknown schema** | **thinnest** |

**Finding — "pay for rich, keep thin":** the 4 pixel vendors deliver `query_str` (incl. referer, user-agent, GPP
consent), `mobile`, `event_id`, and Predactiv delivers `user_agent` — **all discarded** when written to
`site_visit_signal` (we keep only ip/url/ua/time). We may be paying CPM for richer feeds and throwing the metadata
away. (Separate opportunity; does not affect 5x5, which has no metadata to drop.)

## 2. How much total data
5x5 (per day, `outputs/ti_1027_cardinality_2026-06-15.csv`): **1.48 GiB · 93.3M events · 20.8M distinct IPs ·
92.7K distinct domains · 33.1M distinct (IP×domain) pairs · 35.1M (IP×url) pairs.** (IP×url ≈ IP×domain confirms it's
domain-only — only 3.8% of URLs carry a path.) Mid-pack by volume (3.6% of all site-visit records); 5th of 10 by IPs.

## 3. Uniqueness — layered (not just IPs) — `outputs/ti_1027_layered_uniqueness_5x5.csv`
This is the core reframe: uniqueness rises as you go from IP → domain → (IP×domain) event.
| Grain | 5x5 total | Unique to 5x5 | % unique | Also seen internally |
|---|---:|---:|---:|---:|
| **IP** | 20.8M | 4.1M | **19.8%** | 73.8% |
| **Domain** | 202K (7d) | 138K | **68.5%** | 22.5% |
| **(IP×domain) event** | 33.1M | **25.6M** | **77.3%** | 17.9% |

**Read:** 5x5 mostly sees households we already know (only 20% unique IPs), but the **specific sites those households
visit are overwhelmingly 5x5-only (77% of events)**. So the unique *data value* is far larger than the unique *reach*.

**Recency matters — "overlap" ≠ "covered" (`ti_1027_recency_30d_5x5.csv`).** We only target the **last 30 days**
(`site_visit_signal` itself has no TTL — data back to 2025-08-31 — but targeting uses a 30-day window). Vendors
deliver on **irregular cadences**, so a pair "also seen by another vendor" may have been delivered weeks ago and is
about to expire. Measured over the **30-day targeting window**, of 5x5's 754.8M (IP×domain) pairs: **69.8% are SOLE
(no other vendor delivered them in-window)** and **95.4% are sole-or-freshest** — only ~4.6% does another vendor
deliver more recently. So a 7-day snapshot *overstates* redundancy; within the window that drives targeting, ~70% of
5x5's data has no substitute. This **raises the floor** — the $40K unique-reach floor understates badly.
There is **no unique metadata** (5x5 sends none). Of the unique domains, **47K (34%) classify to a vertical** (MM-usable)
= ~12% of the whole classified-domain universe — concentrated in **B2B** (Hiring 34%, Logistics 32%, Data&Analytics 31%,
Sales&Marketing 30%, IT&Eng 25%) + premium retail.

## 4. Is the unique data valuable? — yes
- **Classifiable:** 45% of 5x5 domains (above the 39% universe avg); the unique slice 34% → 47K MM-usable unique domains.
- **High-intent:** **80.6% of impressions to 5x5-observed households are High-Intent (≥6666)**; 39% of its delivered IPs
  are top-tier (10000) — the highest of any high-volume vendor. 5x5 is not bringing low-value households.
- **Strategically placed:** its unique contribution is B2B-concentrated — the area MNTN is investing to grow.
- **Metadata value: none** (thin feed) — so 100% of 5x5's value rests on the unique (IP×domain)→vertical signal.

## 5. Willingness to pay
**What we're buying:** net-new, classifiable household→site observations (the 25.6M unique (IP×domain) events/day),
*not* reach and *not* metadata.

**Three lenses (`outputs/ti_1027_wtp_anchor_5x5.csv`):**
1. **Market / CPM ceiling.** 5x5's data touches **34.35M impressions/day (12.5B/yr)**. At the **$0.50 peer CPM**
   that is **~$6.3M/yr** *if 5x5 were billed like our CPM vendors on every impression it touches*. Upper bound /
   **walk-away max** — we would never pay more than the data costs at market rate. (Co-occurrence, not causation:
   most of those impressions would happen via other signal too.)
2. **Incremental-reach floor.** Impressions to households **only 5x5 sees** = 213.5K/day (77.9M/yr) → **~$40K/yr** at
   $0.50 CPM. The data we'd lose outright. A true floor (ignores the domain-classification value).
3. **Value-based fair price.** 5x5 supplies **~12% of MM's unique classified-domain signal**, B2B-weighted higher.
   MM's value (via advertiser retention) is tens of $M/yr; a 12% B2B-weighted slice of the *data-layer* value lands
   in the **low-to-mid six figures/yr** — also the typical DDP flat-fee range. **Fair ≈ $150K–$600K/yr.**

**Per-unit rates (at the $0.50 peer CPM, impression-equivalent):**
| Unit | Rate | Note |
|---|---|---|
| **Net-new IP** | **~$0.01–0.50 / IP / yr** | Low — most unique IPs barely deliver; reach is *not* where 5x5's value is. Don't pay much per net-new IP. |
| **Net-new (IP×domain) event** | **~$0.03 / 1,000 events** (at fair midpoint) | The real asset — 9.3B unique events/yr. |
| **Net-new classified domain** | **~$3–13 / domain / yr** | 47K unique MM-usable domains; the B2B coverage. |

**Definitive pricing — in the two structures contracts actually use** (Kale: "most are monthly rates or CPMs"):

*Monthly rate (recommended — flat fee + volume minimum):*
- **Floor ~$3K/mo** (we'd happily pay) · **FAIR $15–50K/mo** (anchor ask ~$25–30K/mo = $300–360K/yr) · **walk-away
  ~$525K/mo** (= the CPM ceiling).
- **Attach a volume minimum:** ≥ **2.5B rows/month** AND ≥ **25M unique (IP×domain) pairs/day** — so they can't
  throttle delivery or pad with junk while keeping the fee.

*CPM (if billed per 1,000 impressions) — the rate depends entirely on the billing base:*
- **On matched/incremental impressions: ≤ $0.50 CPM** (peer parity — fair).
- **On all touched impressions: $0.02–0.05 CPM** (~95% is redundant); **>$0.10 on touched = walk away.**
- **Insist the contract bills on matched impressions, not all touched.**

*Reconciliation (same dollars, three views):* **$25K/mo ≈ $0.024 CPM on all touched ≈ $0.50 CPM on matched-only.**
The recency finding (70% of 5x5's data sole in-window) makes the floor firmer, so the fair band is conservative, not
aggressive. Net: renew at/below the fair band; the fee is almost certainly far below the walk-away ceiling.

## 6. How to choose between vendors (tie-break rubric)
When two vendors deliver comparable data, decide in this order:
1. **Cost** (flat-fee with fixed cost beats per-use CPM on redundant volume).
2. **Non-redundancy** — unique (IP×domain) events vs the rest of the stack (the metric that matters most).
3. **Data richness** — metadata we actually use (5x5 = none; pixel feeds rich but we discard it).
4. **Freshness / delivery reliability** — consistent daily delivery vs bursty.
5. **Latency** — processing lag (5x5 = 5h).
6. **Schema stability / contract terms** — 5x5's positional `_COL_*` is a fragility risk.

Applied to the site-visit DDPs: **keep 5x5 + Predactiv** (flat-fee, most unique); **review the $0.50-CPM feeds**
(33Across API 3% unique, Sovrn 2%, Cybba 6%) — redundant per-use spend is the real savings target.

---

## Caveats
- **Impression→5x5 attribution is co-occurrence, not causal.** The ceiling credits 5x5 with all impressions to
  households it observes; the floor counts only impressions to households *only* it observes. Truth is between; the
  fair band rests on the unique-domain contribution, not impression attribution.
- **Flat fee still unknown** — this report gives the value scale; the fee compares against it when billing provides it.
- **Estimation exercise** — bands, not point estimates. The tightest number would need an add/remove model ablation
  (re-run MM with vs without DS25 → ΔIVR → ΔRevenue), proposed only if leadership wants more rigor.
