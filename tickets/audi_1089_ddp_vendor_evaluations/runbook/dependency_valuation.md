# Dependency-Ceiling Valuation — stock → flow → performance → dollars

The bottom-up model: what is the MOST a vendor can rationally be paid per year, derived only from
delivery that could not have happened without them? Complements (never replaces) the fee-band model —
the band answers "what is a fair price"; this answers "above what number is a loss guaranteed."
Charts: `charts/q9c_dependency_ceiling.png` (all vendors, side by side with the fee band) and
`charts/q9c_klickly_ladder.png` (the worked example). Inputs: q3/q6/q7/q4 CSVs — no new queries.

## 1. The chain, in words (Klickly as the worked example)

**Stock.** Klickly's unique contribution is 324,019 sole usable IPs — households no other source
(paid or free) reported on a usable domain in the 30-day window. This is a stock: it does NOT get
multiplied by 52. (666 delivered IPs/week × 52 = 34.6K "annual people" is fiction — the same households
recur weekly, and the whole pool is only 324K.)

**Flow.** That stock generated 3,674 won bids (impressions served) in the valuation week —
total wins, not unique: ~5.5 wins per delivered IP. Annualized: **×52 ≈ 191K expected additional won
bids/year**, a bid-yield of ~0.59 won bids per sole usable IP per year. Deliverability, bid rate, and
win rate are NOT extra multipliers — the impressions are observed post-auction serves, so all of that
is already embedded.

**Why do sole IPs get served at all if only Klickly knows them?** Soleness is judged among
site-visit-signal sources only. A Klickly-sole IP is still reachable through vendor-independent doors:
the advertiser's own retargeting pixel, MM max-reach (no svs needed), 3P segments (LiveRamp IP /
ShareThis), and plain auction presence (Magnite bids on any browsing IP). Of Klickly's 3,674 sole-IP
wins, only **26 were MM-score-gated serves** that provably required Klickly's signal. Hence the range:

- **T1 floor (26 imps/wk, $13/yr):** provable "could not serve without."
- **T2 ceiling (3,674 imps/wk, $2,205/yr):** every win on their IPs credited to them — generous.
- True dependence lies in [T1, T2], plausibly nearer T1. We never publish a fake midpoint.

**Performance, joined on the same IPs.** 1 visit on the week's 3,674 sole wins → VR 0.0272% vs the
0.0223% no-svs baseline — statistically indistinguishable. Annualized ~52 expected visits/yr, but the
Poisson 95% CI on one observed event is ~1–290/yr. No conversion-value lens is added — the data can't
support one.

**Dollars.** Observed effective media CPM on the sole wins: $42.40 / 3,674 × 1000 = **$11.54**
(the whole roster sits at $11.5–12.0, so this is stable). Annual T2 dependent revenue:
$42.40 × 52 = **$2,204.80/yr**. Scenario envelope — volume ×{0.5, 1.0, 1.5} (N=1 week; July is a
seasonal trough) and CPM ×{0.8, 1.0, 1.2} (Q4 prices higher) — outer bounds ×0.4 to ×1.8:
**$882 – $3,969/yr**. This is a stated envelope, NOT a confidence interval; quarterly reruns turn it
into an empirical CI over time.

**Margin.** Gross margin on media revenue is unknowable here (take rates are private). So:
- **Hard ceiling** = the envelope high at 100% margin: even if every dollar of dependent revenue were
  pure profit and every sole win were truly dependent, paying more than **~$3,970/yr** guarantees a loss.
- **Netting:** the other data costs already incurred on those same sole imps ($4.68/wk = $243/yr for
  Klickly) come out of any margin-based value.
- **Margin ladder (Klickly):**

| Gross margin m | Gross value m×$2,205 | Net WTP (− $243 data cost) |
|---|---:|---:|
| 10% | $220 | **−$23 (negative)** |
| 30% | $661 | $418 |
| 50% | $1,102 | $859 |
| 100% (ceiling) | $2,205 | $1,961 |

- **Break-even margin ≈ 11%**: below that, Klickly's dependent revenue doesn't even cover the other
  data costs riding on the same impressions. (This ~11% is roster-wide — data spend runs ~10.8–11% of
  media spend on sole imps for every vendor.)

**The deliverable sentence:** *Under the most generous possible attribution, Klickly's unique dependent
revenue is at most ~$3,970/yr (base ~$2,200); the provable floor is ~$13/yr; at realistic 30–50% margins,
dependency-based willingness-to-pay is ~$420–$860/yr. Separately and additively, its 126 sole classified
domains are worth $378–$1,638/yr on the fee-band axis. Any flat fee beyond low-four-figures is
indefensible on every lens.*

## 2. Three-zone decision rule

- Fee **above the T2 envelope high** → certainly overpaying (loss even at 100% margin, maximal attribution).
- Fee **below the T1 margin-adjusted floor** → certainly rational.
- **Between** → judgment zone, priced by the margin ladder; the fee-band (domain axis) is the additive
  second lens for what a fair number looks like inside the zone.

Metered vendors (Justuno/33Across/Sovrn/Cybba/33A API) don't pay a flat fee, so their test is per-imp:
**rational iff data CPM paid < m × media eCPM** — at $0.50 vs ~$11.6, break-even margin ≈ 4.3% *on
dependent serves*. The problem is the meter also bills non-dependent (shared, first-reporter-won) serves,
which is why their bills exceed any dependency value.

## 3. All vendors — annual dependency numbers (valuation week × 52)

| Vendor | Sole usable IPs (stock) | Won bids/yr (0.5–1.5×) | Yield /IP/yr | Visits/yr (95% CI) | T2 $/yr (0.4–1.8×) | T1 $/yr | WTP @30–50% (net) | Domain axis $/yr |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 33Across | 30.8M | 23.2M | 0.75 | 6,032 (4.9K–7.1K) | $270K ($108–485K) | $1,716 | $51K–105K | $20.5K–89K |
| 5x5 | 29.3M | 5.2M | 0.18 | 1,300 (0.8K–1.9K) | $61.4K ($24.5–110K) | $678 | $11.8K–24.1K | $258K–1.12M |
| 33A API | 9.1M | 5.0M | 0.55 | 780 (437–1,286) | $58.6K ($23.4–105K) | $291 | $11.2K–23.0K | $8.3K–36.1K |
| Predactiv | 5.3M | 1.4M | 0.27 | 208 (57–533) | $16.6K ($6.6–29.9K) | $123 | $3.2K–6.5K | $680K–2.95M |
| Justuno | 5.7M | 622K | 0.11 | 52 (1–290) | $7.5K ($3.0–13.5K) | $83 | $1.5K–2.9K | $13.8K–59.9K |
| Cybba | 172K | 243K | 1.41 | 0 (0–192) | $2.8K ($1.1–5.0K) | $18 | $539–1,100 | $1.1K–4.7K |
| Sovrn | 15.7K | 228K | 14.5 | 0 (0–192) | $2.7K ($1.1–4.8K) | $21 | $498–1,030 | $543–2.4K |
| Klickly | 324K | 191K | 0.59 | 52 (1–290) | $2.2K ($0.9–4.0K) | $13 | $418–859 | $378–1.6K |

Reading notes: 33Across dominates the dependency axis exactly as it dominates volume; 5x5 and Predactiv
INVERT between the two lenses (small dependency, huge domain value) — which is why the lenses must never
be merged; Sovrn's absurd yield (14.5 wins/IP/yr) is 228K wins concentrated on just 15.7K genuinely-unique
IPs — high-traffic households everyone else also serves through other doors.

## 4. Pitfalls (disclose whenever these numbers travel)

1. **Stocks vs flows** — won bids and dollars annualize; unique IPs never do.
2. **N=1 week** — the envelope is a stated scenario band, not a CI; July is a seasonal trough; quarterly
   reruns are the upgrade path to an empirical interval.
3. **T2 is a ceiling** — 99.3% of Klickly's sole wins ran through vendor-independent targeting paths.
4. **Soleness is portfolio-relative and eroding** — augmentor-style feed growth decays it (~−11%/yr value
   at 2%/mo erosion, −21%/yr at 4%/mo); a replacement vendor could also cover the sole set.
5. **Never double-count with the fee band** — domain term + max(ONE imp-priced term); the same
   impressions can't be priced at media CPM and data CPM simultaneously.
