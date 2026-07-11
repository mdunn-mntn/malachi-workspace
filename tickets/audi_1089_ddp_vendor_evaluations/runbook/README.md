# DDP Runbook — Logic, Rankings, and How to Read the Charts

One folder, two halves: `queries/` (canonical SQL, one per step, re-runnable each quarter/renewal)
and `charts/` (the PNG per step + this doc). Data CSVs land in `../outputs/run_<date>/` (gitignored).
Full step definitions: `documentation/docs/ddp_quality_score_runbook.md`.

## 1. The value ladder

A vendor row is worth more the further it climbs. Each rung maps to a measured step:

| Rung | Meaning | Where measured |
|---|---|---|
| Delivered | it arrived in the drop | q1 liveness, q2 reach |
| Usable | DS13-classifiable OR DS19-categorizable domain | q2c funnel (64–100% of raw) |
| Used / credited | first source to report the (ip, url, day) AND it landed on an MM-targeted serve | q1d billed usage |
| Scored | the IP carries an MM score | q5 tier mix |
| HIGH-scored | HI 10000, PP 8000, or graduated band ≥6666 (Fangorn DS46 continuous lands here) | q5 "HIGH total" |
| Bid on / delivered | impressions actually served to the IP | q5 "Delivered %", q6 media $ |
| Converts | visits per impression on the vendor's sole IPs | q7 sole VR |

**Reading "HIGH total" (q5):** it is the share of THAT vendor's delivered IPs that are high-scored —
a within-vendor quality rate, NOT the vendor's share of the overall HI pool. Pool-composition needs a
union query (vendors overlap ~4x on scored IPs); queued as a follow-up. Both views matter: Cybba is
53% HIGH of a tiny footprint; 33Across is 34% HIGH of a footprint that touches most of the pool.

## 2. Why "media $/wk sole" is so small (the counterfactual)

Sole media answers: *if this vendor vanished, what delivered media could not have been targeted?*
Three multiplicative filters crush it:
1. Sole IPs are a minority of each footprint (IPs seen by nobody else).
2. Sole IPs almost never appear in auctions — delivered % is 0.0–0.2% vs 12–34% for shared IPs.
   An IP only one source in ten has seen is usually low-activity, rotating, or bot.
3. The few delivered are 91–98% unscored (q5 "Sole HIGH" = 1.7–4.5%) → no MM-targeted spend.

**Adverse selection, in one line: unique reach and scoreable reach are nearly disjoint.**
"Touched" media ($1.5–3.5M/wk each) is a ceiling for transparency only — those IPs are covered by
5–10 sources at once; remove the vendor and virtually all of it still serves.

## 3. Verdict logic: fee band vs actual bill

Defensible fee band per vendor = dollarized irreplaceable contribution:
- **V:** sole *classified* domains × $3–13/domain/yr (domain→vertical coverage is what MM consumes)
- **D:** sole imps × $0.50 CPM (peer data rate) + media on sole+scored+non-RTC imps (T1 floor)

| Condition | Verdict | Why |
|---|---|---|
| bill ≤ band | KEEP | at or under fair value |
| bill ≤ 3× band | NEGOTIATE (target = band top) | fixable gap — a 50–65% price move a vendor can survive |
| bill > 3× band | DROP | needs an 80–99% discount nobody accepts; sole value ≈ 0 so walking away costs ≈ nothing |

Override: high quality score + over-band bill → renegotiate, don't drop (good data, wrong price).
Under first-reporter-wins, credit competition (esp. our free augmentor, added to svs 2026-05-12)
erodes redundant vendors' bills on its own — June already fell $19K (33Across) / $9.7K (33A API).

### The dependency-ceiling lens (side-by-side second model — see `dependency_valuation.md`)

Bottom-up hard bound: stock (sole usable IPs) → flow (weekly sole won bids × 52) → performance (visits,
Poisson CI) → dollars (observed eCPM ~$11.5 × margin ladder, net of other data costs). T1 floor (score-gated,
provably dependent) to T2 ceiling (all sole-IP wins). Charts: `q9c_dependency_ceiling.png` +
`q9c_klickly_ladder.png`. Klickly: ceiling ~$4.0K/yr, realistic-margin WTP $420-860/yr, T1 floor $13/yr,
break-even margin 11%. Never sum with the fee band's imp term (double-count rule).

### The WTP arithmetic (from `fee_bands()` in the eval chart script)

`band_low = sole_classified × $3/yr` · `band_high = sole_classified × $13/yr + weekly_sole_imps × 52 × $0.0005`

| Vendor | Sole classified | ×$3 | ×$13 | + sole imps $/yr | Band |
|---|--:|--:|--:|--:|---|
| Predactiv | 226,826 | $680K | $2.95M | $710 | $0.7M–3M |
| 5x5 | 86,084 | $258K | $1.12M | $2.6K | formula; index quotes TI-1027 fair $150–600K |
| 33Across | 6,849 | $20.5K | $89K | $11.6K | $30K–100K |
| Justuno | 4,605 | $13.8K | $59.9K | $311 | $14K–60K |
| 33A API | 2,780 | $8.3K | $36.1K | $2.5K | $10K–40K |
| Cybba | 362 | $1.1K | $4.7K | $121 | $1.1K–4.7K |
| Sovrn | 181 | $543 | $2.4K | $114 | $0.5K–2.4K |
| Klickly | 126 | $378 | $1.6K | $96 | $0.1K–1.5K |

**Anchor caveat:** $3–13/domain-yr are calibration constants ($3 ≈ roster aggregate spend per sole
classified domain today; $13 ≈ generous 4x ceiling for indirect value). Right order of magnitude,
stress-testable: even at $30/domain Sovrn tops out ~$5.5K vs its $116K bill.

**Why bills >> worth:** the meter pays volume × overlap × first-reporter ($0.50 per credited
MM-targeted impression, even when 9 other sources had the identical row); worth is uniqueness ×
classifiability × performance (the counterfactual). No shared variables — nothing forces convergence.
Pre-augmentor (before 2026-05-12) it was worse: vendors won "first" on rows our own Magnite bidstream
already contained.

### What we're negotiating, per vendor
- **33Across** ($422K/yr vs $30–100K band): cap ≤$100K/yr, i.e. CPM $0.50 → ~$0.10–0.15 or a billing cap.
  Leverage: augmentor displacement trend; 29% of feed is blocklisted webmail; 6.4% Googlebot IPs;
  likely resells Magnite auction data we already ingest. Walk-away: $5.2K/wk sole media.
- **33A API** ($176K/yr vs $10–40K): drop, or renegotiate only with cookie-sync/ad-infra URLs removed.
- **Sovrn** ($116K/yr vs $0.5–2.4K): drop — a ~98% discount isn't a negotiation. Bug report available
  (77% doubled-protocol URLs; recoverable by splitting on the 2nd protocol) if they want to re-pitch.
- **Justuno** ($77K/yr vs $14–60K): keep-trim — just over band; meter trim or modest CPM cut.
- **Cybba** ($21.5K/yr vs $1.1–4.7K): drop — clean but too small to matter (needs Sean's ENABLED_DSIDS change).
- **Predactiv** (flat, band $0.7–3M/yr): keep, lock price now; ask to restore dropped metadata. HARD
  non-MM dependency (HEM→CRM/identity) — blast-radius check before any change.
- **5x5** (flat, TI-1027): keep; ask for URL paths + user_agent (domain-only feed today).
- **Klickly** (flat, band $0.1–1.5K/yr): drop unless ~free; if kept, ask for checkout query_params
  (BUK-grade tokens) and non-Shopify coverage.

### Improvement asks that GROW a band (alternative to price cuts)
Fix malformed URLs (Sovrn ~4x usable feed), add URL paths/query params (5x5, Klickly — BUK/DS38 input),
send user_agent (enables pre-credit bot filtering — we currently PAY for 33Across's ~6% bot rows),
strip webmail/ad-infra rows at source (33Across, 33A API).

## 4. Composite quality score (0–100)

`raw = 100 × (0.40·V + 0.15·R + 0.15·Q + 0.10·D + 0.20·P)`, liveness gate ×0/1 (all PASS this run).
`curved = 100 × raw / max(raw)` — graded against the best source in the roster (the best we'll get),
so the top source = 100 and the rest read as % of best. Chart color: green ≥80, amber ≥65, red <65.
- V — sole classified domains, log-normalized (the durable unique value)
- R — % of (ip,domain) pairs sole-or-freshest (non-redundancy under first-reporter-wins)
- Q — ½·(% domains classified) + ½·(1 − sole unscored share) (signal quality)
- D — T1 gated sole imps, log-normalized (hard dependency)
- P — sole-IP VR ÷ no-svs baseline (0.0223%), capped 2×, halved; <5K sole imps → neutral 0.5

**THE INDEX (this run, 2026-07-10 — q3 usable refresh LANDED: R unchanged, scores stand; Sovrn's
sole IPs collapsed to 15,660 under the usable restriction).**
Scores are CURVED to best-in-roster = 100 (the best source is the best we'll get; raw in parens):

| # | Source | Curved (raw) | WTP $/yr (pay up to) | Bill run-rate | vs band | Verdict |
|--:|---|--:|---|---|---|---|
| 1 | 5x5 | 100 (70.4) | $150K–600K (TI-1027 fair) | flat fee, pending | — | KEEP |
| 2 | Predactiv | 87 (61.5) | $0.7M–3M | flat fee, pending | — | KEEP, lock price |
| 3 | 33Across | 82 (57.7) | $30K–100K | $422K/yr | 4.2× top | NEGOTIATE cap ≤$100K |
| 4 | Justuno | 81 (56.9) | $14K–60K | $77K/yr | 1.3× top | KEEP-trim |
| 5 | Klickly | 73 (51.1) | $0.1K–1.5K | flat fee, pending | — | DROP unless ~free |
| 6 | 33A API | 71 (49.8) | $10K–40K | $176K/yr | 4.4× top | DROP / renegotiate |
| 7 | Cybba | 71 (49.7) | $1.1K–4.7K | $21.5K/yr | 4.6× top | DROP |
| 8 | Sovrn | 50 (35.5) | $0.5K–2.4K | $116K/yr | 48× top | DROP |

WTP = the defensible fee band from section 3 (V sole-classified-domain value + D dependency media);
"pay up to" = band top. Bill color rule on the chart: green ≤ band top, amber ≤ 3×, red > 3×.

**LEAVE-ONE-OUT — EXACT (q3b landed): drop savings** — 33Across $385.7K (91%), 33A API $142.9K (81%),
**Sovrn $109.0K (94% — v1's $14.5K was wrong: its overlap is 81% with FLAT-FEE vendors, which absorb
credits free)**, Justuno $77.1K (100%), Cybba $21.2K (98%). Both 33Across feeds together = full $598K.
**Frontier (all subsets exact): 3 vendors (33A-combined + 5x5 + Predactiv) retain 98.1% of pair
coverage; +Justuno = 99.5%; free logs alone = 60.4%.** Charts: `q9d_one_out.png`,
`q9e_roster_frontier.png`. Sequencing: renegotiate-before-drop still holds (BATNA); lock flat-fee
prices before drops (they absorb the reassigned coverage).
Score ranks data quality; the VERDICT = score × cost position (a 57.7 at 4× over band still
negotiates down or drops; a 51.1 at ~$0 flat fee could be fine).

## 5. Open items
1. ~~q3 usable-restricted scan~~ — LANDED: R unchanged, Sovrn sole-IP collapse, density measured.
2. **q7b (queued): performance matrix per score-bucket × source** — imps, VR, media averages for every
   vendor × tier (generalizes q7's Klickly-only membership split); the user-requested
   "performance by bucket and source with an aggregate".
3. HI-pool composition (union query — who covers the unique HI pool).
4. Flat-fee amounts: renewal schedule / Maya Triman → completes Klickly/5x5/Predactiv verdict math.
5. Athena `targeted_signal` access → exact per-vendor used-rows + row-level DS13/DS19 split.

## 6. Chart index (charts/)
- `q9c_dependency_ceiling.png` / `q9c_klickly_ladder.png` — dependency-ceiling valuation (stock/flow/performance/$)
- `q9d_one_out.png` — leave-one-out: what dropping each metered vendor actually saves vs reassigns
- `q0_roster_cost.png` — roster + monthly metered bills (wide table)
- `q1_scale_by_day.png` — liveness: days delivered, partial days, IPv6
- `q1b_schema_fields.png` / `q1b_url_richness.png` — field population + URL path share
- `q1c_content_quality.png` / `q1c_unparsed_examples.png` — junk markers + live unparseable examples
- `q1d_used_vs_delivered.png` / `q1d_billed_domains.png` — billed funnel + junk in billed domains
- `q1e_column_value.png` — columns MM consumes today vs latent value
- `q2_window_reach.png` — ranked raw reach (unique IPs/domains/pairs)
- `q2b_daily_drops.png` — rows/IPs dropped per day by filter, with reasons
- `q2c_funnel.png` — THE pivot: raw → usable → DS13/DS19 → billed, per source
- `q2d_usable_share.png` — who supplies the usable pool (counts + %)
- `q5_score_tiers.png` — score-tier mix, touched vs sole (adverse selection)
- `q9_vendor_scorecard.png` — per-vendor wrap-up: usable, money, worth band, verdict, asks
- `q9b_quality_ranking.png` — composite score components + final ranking
