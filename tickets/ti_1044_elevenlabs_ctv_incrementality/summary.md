# TI-1044: ElevenLabs CTV Incrementality — CVR-lift triangulation + power review

**Jira:** https://mntn.atlassian.net/browse/TI-1044
**Status:** In Progress
**Date Started:** 2026-06-23
**Date Completed:** —
**Assignee:** Malachi

---

## 1. Introduction
ElevenLabs (**advertiser_id 51660**, Audience ID 77883) is a B2B advertiser running US CTV through MNTN.
Their Growth / Data-Science team sent a sophisticated incrementality review deck (June 2026, "Is our
Connected-TV campaign driving incremental conversions?") concluding **no statistically significant
incremental lift** from US CTV across three methods (Synthetic Control, Time-Based Regression,
Diff-in-Diff) on two geo designs (Country: US vs intl; State: 48 states vs go-dark GA/IL/OH holdout),
on two KPIs (Contact Sales Forms; new self-serve subscribers).

Kale cc'd Edgar von Trotha, who asked whether stat-sig lift is even detectable at the current ~$1M
spend regardless of targeting. This ticket independently measures, from MNTN's side, whether ElevenLabs
CTV drove incremental **CVR** lift, frames *why* it is effectively unmeasurable at this spend/baseline,
and addresses the four questions their deck poses.

Prior ElevenLabs work: **TI-928** (interest-segment & keyword quality eval, Done). ElevenLabs is the
**#2 advertiser by stale-3P prospecting spend** ($0.72M/30d, **TI-999**) — audience is largely stale
third-party interest segments (structurally underperforming). Deck PDF + Slack thread in `artifacts/`;
Matt Brorby meeting transcript in `meetings/`.

## 2. The Problem
- ElevenLabs' deck reports **no incremental CTV lift**; the well-powered country-subscriber read is ~0%
  (R²=0.87, p=0.81), the positive state-holdout reads (+3.8% to +7.4%) never clear significance (3-state
  holdout, pre-period R²<0.70). Their hypothesis: CTV re-labels organic/direct demand as "TV-driven"
  (attribution ≠ incrementality).
- **Thesis (we believe the "no detectable CVR lift" claim is true):** ElevenLabs CVR ≈ **0.062%** (B2B,
  extremely low). At ~$1M/mo spend the test is **underpowered by ~2×** — it cannot resolve a realistic
  (2–5%) lift. "No detectable lift" ≠ "no lift."
- Leadership (Edgar) needs a clear answer; ElevenLabs wants 4 questions worked through together.

## 3. Plan of Action
0. **[done]** Ticket + folder scaffolding; move deck PDF + Slack thread to `artifacts/`; transcribe the
   Matt Brorby meeting to `meetings/` and reconcile scope.
1. **Feasibility gates + data pull** — verify AID 51660; enumerate live `funnel_level=1` prospecting
   campaigns across the post window; bidder-coverage gate (MNTN-bidder vs Beeswax → ghost-bid feasibility);
   holdout IP counts; baseline CVR/visit-rate/spend.
2. **HEADLINE — power/MDE** — reuse the prefilled TI-884 calculator (raw CVR MDE 7.4% @ ~$1M/mo;
   post-stack 4.4%; $2.04M to detect 5%). Refresh inputs if materially changed.
3. **Triangulation (3 methods, power permitting)** — holdout ITT (guid/clickpass/conversion) +
   CausalImpact / cluster-bootstrap DiD on IVR/CVR + ghost-bid (if on MNTN-bidder). Report point/CI/p.
4. **The 4 deck questions** — reach overlap (Q1), our-side incrementality (Q2), conversion windows (Q3),
   creative/targeting (Q4) — with Mike Dolt's Q1/Q4 limits stated honestly.
5. **Deliverables** — summary.md, internal one-pager, ElevenLabs-facing response deck; close-out.

## 4. Investigation & Findings
### 4.0 Locked facts
- ElevenLabs = AID **51660**; nationwide CTV scale date **2026-05-17**; ~5-week post window.
- **Power/MDE (prefilled TI-884 calculator** —
  https://gist.githack.com/mdunn-mntn/2d362849df017fa243eef03bb61cdfbb/raw/ti_xxx_mde_calculator_prefill.html):
  baseline **CVR 0.062%**, visit rate 3.07%, CPM $8.58, imps/IP 4.22. At ~$1.01M/mo · 4wk ($932K spend,
  10% holdout → 23.1M treated / 2.6M holdout IPs): **raw CVR MDE 7.4%** (BORDERLINE, ±5.2%), **post-stack
  MDE 4.4%** (WELL POWERED via CUPED 0.934 × ghost-ad 0.75 × stratified 0.85 = 0.595), **$2.04M** to hit
  a 5.0% raw target.
- **Implication:** a realistic 2–5% CVR lift sits below the raw 7.4% detection floor → the test as-run
  cannot resolve it. Post-stack 4.4% is achievable only with ghost-ad infra + a stratified/CUPED
  randomized design (dependency, not free).

### 4.1 Matt Brorby meeting (2026-06-23, `meetings/ti_1044_01_...`)
Reconciles scope. Key points:
- **Campaign history:** ElevenLabs (AI co., B2B) ran geo tests in **Bay Area, then TX/FL** (tech hubs,
  high-intent) → saw meaningful lift → triggered the ~$1M/mo **national** campaign (~June 1, 5 weeks).
  Held out 3 states + international as control (SC + DiD). International also grew → muddies their DiD.
- **Two power regimes (the central framing):**
  - **VISITS / IVR** — baseline **~3.07%**, *well-powered*: MDE ~3%, only ~**$40k** needed (no variance
    reduction). We **can** measure visit lift. A visit lift <1% would be genuinely ~null.
  - **CONVERSIONS / CVR** — baseline **0.062%**, *hopelessly underpowered*: raw lift would need ~22%;
    with variance reduction ~13%; at ~$1M/4wk still ≥5% (high for B2B). Budget-for-5%-MDE ≈ **$2M**.
    The deck's KPIs (CSF, subscribers) are exactly these unmeasurable conversion metrics.
- **Dilution narrative (non-defensive explanation):** geo tests nailed high-intent metros; national
  super-broad scale **dilutes** any lift. Real lever, not "MNTN failed." (Cf. Orange Theory: high-spend
  clients exhaust >8000-scored IPs fast → IVR crashes once they drop below threshold.)
- **Ghost-bid bias (load-bearing caveat):** holdout is 10% on the hash, but ghost bids are **not
  frequency-capped** like real bids → the most active (high-frequency, high-visit-rate, often cellular/
  high-attribution) IPs flow into the holdout → holdout inflates to ~13% and skews high-activity →
  **inflates holdout rate → biases lift NEGATIVE.** Cannot stratify away (post-treatment); only fixable
  in code. New ghost-bidding/bid-events tables have a **10-day TTL**. Runnable (own pipeline) but caveat.
- **Tooling reality:** existing causal-impact/holdout pipeline is **visits-based**; CVR adaptation is hard.
- **Immediate ask (Matt):** quick pulse-check today — post the MDE calc + run the standard causal-impact
  report (visits); tag Mike & Matt. Tone: ElevenLabs is collaborative ("how do we optimize?"), not upset.
  Also asked: did **creative** or the **built audience** change between geo tests and national?

### 4.2 Step 1 — feasibility + delivery panel (`queries/ti_1044_daily_ctv_panel.sql`, `outputs/`)
- **AID 51660 = ElevenLabs**, US, active, B2B. All campaigns are **"Beeswax Television" (channel_id 8 = CTV)**
  + display "Multi-Touch" companions (channel_id 1). Test campaigns (group 126226) excluded.
- **Conversion windows (Q3, from advertisers dim):** **30-day** for conversion, click-through AND
  view-through; 2-day abandon. The 30-day **view-through** is the broad credit window the deck flags (Q3) —
  any household that merely *saw* a CTV ad and converts within 30d is credited (a wide, industry-standard rule).
- **Delivery panel (summarydata.all_facts, channel 8, daily, since 2026-02-15):** national ramp is clear —
  ~75K imps/day (Feb geo) → ~200K (Mar 12) → ~310K (Apr) → **~800K–1.4M from May 7 (national)** → FIFA
  Select boost June 11. CTV spend now **~$1–1.5M/mo** (ctv_spend is advertiser-billed $, ~$45 CPM; spend_log
  media cost ~$900K/mo).
- **Two power regimes confirmed empirically:**
  - **Visit rate** ramped ~1% (spring) → **~2.5–3.8%** recent (matches 3.07% baseline) — clean, well-powered.
  - **CVR (attributed conv ÷ advertised uniques)** = **0.004%–0.21%, avg ~0.03–0.06%** (matches 0.062%) —
    tiny and noisy. **Spend scaled ~4× but CVR-per-IP stayed flat → no conversion acceleration**, exactly
    the deck's "attribution looks great, topline flat" pattern.
- **No queryable holdout/ghost cohort for this advertiser:** `clickpass.is_control_group` = 100% false
  (no holdout configured); `bidder_bid_events` (MNTN-bidder) 404'd and wouldn't cover Beeswax. → The
  ghost-ads/PSA holdout (Q2) must come from the **augmentor_log pipeline** (Matt is running it), which
  carries the **negative-lift bias** (ghost bids not frequency-capped → holdout over-represents
  high-frequency/high-visit IPs). Formal ghost-bid number deferred to that pipeline.

### 4.3 Step 2 — power, two regimes (`artifacts/ti_1044_power_analysis.py`, `outputs/ti_1044_power_table.csv`)
Reproduces the prefilled calculator exactly (CVR raw MDE 7.36% ≈ 7.4%) and adds the visits regime.
At ElevenLabs' actual scale (treated 23.1M / holdout 2.6M IPs, CPM $8.58, 4.22 imps/IP, 80% power):

| Outcome | Baseline | Raw MDE | Post-stack MDE | $ to detect 5% | $ to detect 2% | At ~$1M |
|---|---|---|---|---|---|---|
| **Visit rate (IVR)** | 3.07% | **1.03%** (well-powered) | 0.61% | **$36K** | $224K | ✅ can detect ~1% |
| **Conversion rate (CVR)** | 0.062% | **7.36%** (borderline) | 4.38% | **$1.83M** | $11.5M | ❌ floor ~7% |

**Headline:** same 5% lift costs **$36K to detect on visits vs $1.83M on conversions** — a 50× gap driven
entirely by the 0.062% B2B conversion base rate. At ~$1M spend, CVR lift below ~7% is **invisible**, so
ElevenLabs' "no significant CVR lift" is the *expected output of an underpowered test, not evidence of no
effect.* Their own **well-powered country-subscriber read (~0%, p=0.81) is the credible number**; the
positive-but-underpowered state reads are noise. Charts: `ti_1044_chart_power_contrast.png`,
`ti_1044_chart_mde_curve.png`, `ti_1044_chart_visit_vs_cvr.png`.

### 4.4 Step 3 — triangulation (what we can/can't run our side)
- **Descriptive (panel):** spend scaled ~10× (Feb→June); attributed **visit rate rose to ~3%** (real,
  measurable) while **CVR-per-IP stayed flat at ~0.04–0.06%** — no conversion acceleration. This is the
  "attribution looks great, topline flat" pattern, on our own data.
- **Holdout / ghost-ads / PSA (Q2):** no queryable holdout for this Beeswax advertiser
  (`is_control_group` all false; `bidder_bid_events` MNTN-bidder only/404). The ghost-ad study must run via
  the **augmentor_log pipeline** (Matt), which carries a **negative-lift bias** (ghost bids aren't
  frequency-capped → holdout over-represents high-frequency/high-visit IPs → inflates holdout rate). Even a
  clean holdout would be underpowered for CVR per §4.3. → ghost-ad number deferred; not load-bearing.
- **Conclusion:** every conversion-based method (theirs and ours) converges on "can't detect," and the
  power math explains why. Methods-convergence here = the *informative* result.

### 4.5 Step 3b — ACTUAL ghost-ad lift from the new bidder logs (`queries/ti_1044_ghost_lift*.sql`)
**Data-access breakthrough.** ElevenLabs is Beeswax; their ghost bids are NOT in `bidder_bid_events`
(MNTN-bidder, 404/empty) and the GCS bucket `bidder-price-events-prod-east` is 403-denied to me — **but**
the Beeswax stream lands in BQ at **`dw-main-bronze.raw.bid_price_log`** (`threshold_failure_reasons='ghostBid'`,
`is_ctv`, `advertiser_id`, `ip`; ~189K held-out CTV IPs/day for 51660; 10-day TTL, ip-clustered). Ghost
logging live since **2026-05-27** (Ryan Kleck). No ghost-WIN logging → win-rate estimation as before.

**Served-vs-ghost (TI-837 ATT, cohort Jun 13–22, outcomes Jun 13–23):**

| Group | IPs | Visit rate | Conv rate |
|---|---|---|---|
| Treated (served) | 3,466,997 | 2.449% | 0.0618% |
| Control (ghost-holdout) | 605,031 | 0.651% | 0.0460% |

- **IVR lift +276.2%** (95% CI +264→+288%, p<0.001) — BUT this is the **clickpass-attributed** visit metric,
  which is impression-gated (a held-out household can't register an attributed visit), so a holdout comparison on
  it is mechanically large. It is a *different metric* than incremental demand, not an inflated estimate of it —
  the incrementality question is answered by total traffic (guid_log), which is far smaller (north star pattern).
- **CVR lift +34.4%** (95% CI +18.6→+52.3%, p<0.001) — positive and statistically distinguishable at these
  n's (MDE ≈16% here), BUT **confounded**: treated = auction *winners* (higher-value IPs) vs ghost = a
  pre-auction random holdout (ghostBid logged at would-have-*bid*, not would-have-*won*) → **win-selection
  bias inflates it UP**; Matt's frequency-cap bias pulls it down. Not a clean causal point estimate.
- **Reconciliation:** this does NOT overturn ElevenLabs' geo null. Their geo test (country/state totals, no
  win-selection, no attribution) ≈0% is the *unbiased* incrementality estimate. Our +34% is the same
  households' raw served-vs-holdout gap, dominated by serving their best (winning) households. The clean
  **ITT** (targeted-and-bid vs held-out, both pre-auction) confirms the +34% collapses to the null.

**Clean ITT (targeted-and-bid vs ghost-holdout, both pre-auction → no win-selection):**

| Metric | Treated (bid-placed, 6.04M IPs) | Control (ghost, 603K IPs) | Lift | p |
|---|---|---|---|---|
| Conversions (CVR) | 0.04515% | 0.04591% | **−1.7%** | 0.84 (NOT sig) |
| Visits (clickpass) | 1.585% | 0.652% | +143% | <0.001 |

- **THE NUMBER WE TRUST: clean conversion-rate lift = −1.7%, not significant → incrementality ≈ 0**, matching
  ElevenLabs' geo null. The served-vs-ghost **+34% ATT was win-selection bias** (serving the highest-value
  auction-winning households); removing it (ITT) collapses CVR lift to ~0.
- Clickpass IVR lift stays large (+143% ITT / +276% ATT) because the **attributed-visit metric is impression-gated**
  (a credit metric, not causal). **Total traffic (guid_log) ATT = +36%** (treated 2.83% vs control 2.08%) — a
  different, causal-style metric (~8× smaller than the attributed number), but still positive because the ATT
  cohort (served=winners) is win-selection-biased. The one
  unbiased read we have — the clean conversion ITT (−2%, NS) — is the trustworthy incrementality number.
- **Final lift table** (`outputs/ti_1044_ghost_lift*.json`, charts `ti_1044_chart_conv_att_vs_itt.png` /
  `ti_1044_chart_attribution_vs_true.png`):

| Outcome | ATT (served vs ghost) | ITT (clean, pre-auction) | Read |
|---|---|---|---|
| Attributed visits (clickpass) | +276% | +143% (p<.001) | **attribution**, not incrementality |
| **Total visits (guid_log)** | +36% | **+0% (CI −2→+2, p=0.84)** | **≈0 incremental total traffic** |
| Conversions (CVR) | +35% | **−1% (CI −13→+12, p=0.84)** | **≈0 — matches geo null** |

- **Corrected headline (do not overstate IVR):** the clean ITT shows **small / not-significant incremental lift
  for both total visits and conversions.** The +143% is clickpass **attribution**; the +35–36% ATT is
  **win-selection** (served = winners, higher-value; win rate ≈57%).
- **Visit-lag robustness** (`ti_1044_ghost_lift_itt_robust.json`, cohort Jun 13–16, visits through Jun 23, 7–10d
  lag): total-visit (guid) lift = **+1.7%, CI [−0%, +4%], p=0.10 — n.s.** (vs +0% on the 10-day cohort). So total
  visits are **~0–2%, not significant** (point estimate below the ~2.6% MDE at the holdout sample) — NOT a flat
  zero, but no detectable lift. Conversions robust at −2.3% (n.s.). Attributed (clickpass) +160% (attribution).
- **Ghost-win simulation + IV-TOT (2026-06-23, `ti_1044_ghost_win_sim.py`):** formed the served-counterfactual
  two ways. (1) **IV/LATE** TOT = ITT ÷ compliance (0.57): visits ~0–3% (n.s.), conversions ~−2 to −4% (n.s.).
  (2) **Ghost-win ATT** — simulated ghost *wins* by sampling ghost bids at the per-bid win rate **w=0.27**
  (10.96M imps ÷ 40.79M real bids), frequency-weighting the control: visits +35%→**+33%**, conversions
  +32%→**+26%**. The frequency correction is small (~2–6pp) ⇒ **the ATT bias is value-selection, not frequency**
  (we win impressions for the households we bid highest on, who visit/convert anyway). Uniform win-rate sampling
  can't remove value-selection; only the randomized ITT / IV-TOT do — and both say **≈0**.
- **Honest bottom line (triangulated 4 ways):** attribution (industry-standard last-touch + 30-day view-through)
  and incrementality are **different metrics, not better/worse** — the large attributed number and the ≈0 incremental
  number are both correct for what they measure. True incremental lift on both total visits and conversions is
  **≈0 / small and not significant** (randomized ITT and IV-TOT). The +26–35% ATT/ghost-win numbers are
  **value-selection** (serving the households who'd convert regardless), not media-caused lift.
  Corroborated by ElevenLabs' geo null + the power floor. (A fully clean ghost-win TOT would need a
  bid-price-conditional win model — Ryan's `ghost-win-simulation` service intent; would converge on the ITT.)
- **Validation:** delivery continuous since Feb 15 (national May 7), holdout window Jun 13–22 fully active
  (~1M+ imps/day); sample well-powered (603K control / 6.04M treated IPs; visit MDE ~2.6%); guid = total visits
  to ElevenLabs' site only (advertiser_id 51660, all sources).
- **Cross-device IP-matching limitation (important, 2026-06-24):** ElevenLabs' site is huge — **5.42M distinct
  visitor IPs / 5.82M households / 166M pageviews** over Jun 13–23. We served 3.47M IPs; only **~98K overlap
  (same IP)** = 2.83% of served. The guid join matches the **CTV-impression IP** (TV/home router) to the
  **web-visit IP** (phone/laptop); cross-device / cellular / away visits have a different IP → **missed**, so
  the absolute visit rate (2.83%) is an undercount, and (unlike a symmetric loss) it preferentially drops
  *ad-induced cross-device* visits from the served arm → can bias the measured visit lift **downward**. The
  device-agnostic **geo test is cleaner here and also ≈0**; conversions ≈0 corroborate. **Refinement:**
  rebuild the total-visit holdout with **household/identity-graph matching** (IP→household→all visits) to
  remove the cross-device undercount.

### 4.6 Method validation — ghost-bid design doc + Edgar review (`meetings/ti_1044_02_...`)
- **Ryan's Ghost Bid Design** (Confluence 3600547848) confirms our read: holdout = deterministic
  `advertiser_id + household_id(ip)` hash; ghost filter is the **last gate** (after metadata/fcap/pacing/
  spend-cap); Beeswax logs `ghostBid` in `threshold_failure_reasons` (what we queried). A separate
  `ghost-win-simulation` Argo service is meant to model ghost *wins* via win-rate (`hash(auction_id+hh)`)
  — **not live yet**, so the clean bid-level **ITT is the right estimator now** (Matt confirmed he's building
  the same). Our `incrementality_enabled` advertisers carry the ghost cohort.
- **Frequency bias direction (Edgar review + Matt):** ghost bids are NOT impression-fcap'd, so the holdout
  over-represents high-frequency / cellular / high-attribution IPs → **control made more performative →
  measured lift biased DOWN**. Our ITT removes win-selection (up) but not this (down); the two partly offset,
  net effect on the −2% is small → conclusion (≈0) holds.
- **Independent confirmation:** in the Edgar review the user's own run also lands at **−2% CVR lift for
  ElevenLabs** — matches our ITT exactly. Agreed framing: *"conversions are not the right KPI when the
  baseline CVR isn't powered; visits show media did its job, conversions reflect the product."* ElevenLabs
  drives visits fine (IVR test needs only ~$14k for 5%); the conversion base rate is the wall.
- **Method = same as TI-837 / TI-933 (Hannah Select lift):** randomized holdout vs served, rate-lift + 95%
  CI, clickpass/guid/conversion outcomes. **Upgrade:** holdout read directly from the new bidder ghost log
  (`threshold_failure_reasons='ghostBid'`, live 2026-05-27) instead of reconstructing it via the
  `MD5(advertiser_id:ip) mod 1000` hash on augmentor_log.

## 5. Solution
**We agree with ElevenLabs' conclusion — and can explain it.** The "no incremental CTV lift" finding is
**correct but underpowered, not informative**: at a 0.062% B2B conversion rate, no method (geo or
MNTN-side) can resolve a realistic 2–5% lift without ~$2M+ spend. Their well-powered country read (~0%) is
the credible number; the positive state reads are noise.

**Recommendation (honest, non-defensive):**
1. **Stop trying to measure CTV incrementality on conversions for this account** — it is statistically
   impossible at this spend/CVR. Measure **visits** instead (well-powered, $36K detects 5%) if a clean
   holdout test is wanted; or run a **larger/longer geo test** sized to the $2M+ MDE if conversions are
   non-negotiable.
2. **The dilution story is the real lever:** geo tests in high-intent metros (SF, TX, FL) showed lift;
   the national broad scale dilutes it. Concentrating budget on higher-intent geos/audiences likely
   improves *attributed* performance — but note (Mike Dolt) we have **no incrementality-trained model**,
   so any "this will improve incrementality" claim is speculation.
3. **Frame attribution vs incrementality as two metrics** (Q3): attribution credits CTV by last-touch + a
   30-day view-through window (industry standard); incrementality asks what CTV *caused*. The reported (attributed)
   conversions and the ≈0 incremental result are both correct — they answer different questions, which is *why* a
   real incremental test looks flat next to the attributed numbers. (Not "attribution is wrong/inflated.")

**Deliverables:** this summary; charts in `artifacts/`; ElevenLabs-facing deck
(`artifacts/ti_1044_elevenlabs_response_deck.html`); pulse-check Jira/Slack post.

## 6. Questions Answered
- **Q1 — Reach & overlap (how much CTV reach hits already-deep-in-funnel audiences?):** On the targeting
  side, **~100% of ElevenLabs' CTV reach is prospecting** (objective 1: 27.9M imps / $1.38M / 6.1M IPs over
  30d); **retargeting/Ego ≈ 0** (16 imps). They are *not* deliberately re-serving deep-funnel users.
  **Limit (Mike Dolt):** the only deep-funnel signal we have is **pixel site-visitors** (RTC); that is not
  an accurate representation of true funnel depth. We **can block site-visitors** from the buy if they
  want, but cannot quantify overlap with organic/direct demand.
- **Q2 — Incrementality on our side (ghost-ads / PSA):** No queryable production holdout exists for this
  Beeswax advertiser. A ghost-ad/PSA study can be run via the augmentor_log pipeline, **but it carries a
  known negative-lift bias** (ghost bids aren't frequency-capped) **and is underpowered for CVR** regardless
  (§4.3). Our descriptive read (visits scale, conversions flat) already **triangulates with their geo null**.
- **Q3 — Conversion windows / where credit is over-counted:** ElevenLabs runs **30-day** click-through,
  view-through, AND conversion windows (2-day abandon). The **30-day view-through** window credits CTV for
  any conversion within 30 days of an *impression* (no click) — a broad, industry-standard credit rule. A genuine
  *within-attribution* over-count exists: **multi-touch attribution can double-count** a conversion across
  touchpoints (CVR can exceed 100%). Net: the attributed conversion number is large by construction; it is a
  credit metric, **not** a causal one, so it is naturally far above the incremental (≈0) result — different
  questions, not an inflated estimate of incrementality.
- **Q4 — Creative & targeting (new demand vs existing intent; B2B comparables):** Audience is built on
  **stale third-party interest segments** — ElevenLabs is MNTN's **#2 stale-3P prospecting advertiser**
  ($0.72M/30d, TI-999; prior audience eval TI-928). The geo→national move **diluted** a working high-intent
  campaign. Levers exist (concentrate geo/intent, frequency/reach, MM/keyword audiences) but **(Mike) no
  incrementality-trained model exists**, so incrementality gains from audience changes are speculative;
  attributed-performance gains are plausible. Creative-content review is qualitative → CS/follow-on.

## 7. Data Documentation Updates
- `data_catalog.md` — `summarydata.all_facts`: unique columns (`uniques`, `site_visitors`,
  `new_site_visitors`, `visitors`) are **HLL BYTES sketches** → use `HLL_COUNT.MERGE`; `ctv_spend`/
  `media_spend`/order-value are **whole USD (not micros)**; `channel_id 8` = CTV. (Added.)
- _(Pending)_ `data_knowledge.md` — B2B CVR power floor (0.062% → 5% MDE needs ~$2M); ghost-bid holdout
  negative-lift bias (frequency-cap asymmetry); single-advertiser triangulation pattern (lead with visits).

## 8. Open Items / Follow-ups
- **Ghost-ad/PSA number → run via Matt's augmentor pipeline** (decided 2026-06-23). Matt already has the
  ghost-bid pipeline set up; he refreshes + runs it. Handoff inputs: AID **51660**, CTV **prospecting**
  (channel 8, funnel_level 1, objective 1), national flip **~2026-05-07**, want ghost-ad CVR (+ visit)
  lift. **Caveat to report with it:** the holdout carries a **negative-lift bias** (ghost bids not
  frequency-capped → holdout over-represents high-frequency/high-visit IPs) and is **underpowered for CVR**
  per §4.3 — treat as a lower bound. (Alternative if needed in-hand sooner: reuse the TI-837 augmentor_log
  queries directly for this single advertiser — "the old way" — but it reproduces the same bias.)
- **ElevenLabs-facing deck** drafted + shared for internal review (githack); CVR-focused. Route via
  Edgar/Kale before it reaches the customer.
- If they want a real conversion test: **size a geo test to the $2M+ MDE**, or pivot the KPI to visits.
- Possible follow-on: quantify site-visitor (RTC) overlap with their prospecting reach (Q1 deep-dive).

### 4.7 Exclusion/block config — the likely mechanism behind low incrementality (2026-06-24)
**Finding:** ElevenLabs' prospecting campaigns are NOT suppressing prior converters or pageview visitors.
- `block_conversion` / `block_prospecting` settings live in `integrationprod.advertiser_configurations`
  (cols: `block_conversion`, `block_prospecting`, `block_first_party`, `conversion_lookback_window`,
  `page_view_lookback_window`, `enable_taxonomy_block`). TI-310 = the NTB investigation behind this.
- **DATA-QUALITY CAVEAT:** `advertiser_configurations` in BQ is **STALE — frozen 2026-01-12** (last
  update_time) and only stores `block_prospecting=true` rows (0 false). Do NOT use it for current state.
  Reliable/fresh sources: **`audience_audience_segments`** (operative expressions, fresh to today) and
  **`archives_advertiser_configuration_archives`** (config history, fresh to today; ElevenLabs = 0 rows).
- **Operative truth (audience_audience_segments, live):** campaign **608814** (national prospecting) exclusion
  clause = `UserNumPageViews >= 0` (threshold 0, **no lookback**) → disabled; **629615** (FIFA Select) has no
  exclusion clause at all; no conversion-exclusion clause anywhere. Properly-blocked advertisers look like
  `UserLastVisitTime >= 30,day and UserNumPageViews >= 2/5` (camp 42761). Platform-wide: ~18.7K campaigns at
  `>=5`, 9.3K at `>=2`; ~5,500 at the disabled `>=0` (ElevenLabs is one).
- **Implication:** "prospecting" re-serves prior visitors/converters → serving existing demand = the
  **value-selection** that drove our ≈0 incrementality (§4.5–4.6). **Fix:** enable `block_conversion` +
  `block_prospecting` with a high lookback (90d). Should raise true incrementality + NTB%.
- Holdout confirmed in the expression: `md5("51660:"+ip) bucket 0–99 / 1000` = 10% (matches the ghost-ad analysis).
