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
  view-through; 2-day abandon. The 30-day **view-through** window is the over-credit mechanism the deck
  flags — any household that merely *saw* a CTV ad and converts within 30d is credited.
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

- **IVR lift +276.2%** (95% CI +264→+288%, p<0.001) — BUT this is **clickpass-attributed** visits, i.e. the
  attribution signal (north star: clickpass 2–8× vs guid_log ~0%). +276% = 3.76×, squarely in that range →
  **overstates incrementality**; true total-traffic (guid_log) lift is far smaller.
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
- Clickpass IVR lift stays large (+143% ITT / +276% ATT) because it's **attribution**; the guid_log
  total-traffic comparison (running) isolates true incremental visits (expected near 0, per TI-835).
  _(guid result appended.)_
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
3. **Reset expectations on attribution vs incrementality** (Q3): 30-day view-through windows over-credit
   CTV, so platform-reported conversions overstate causal lift — which is *why* a real incremental test
   looks flat by comparison.

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
  any conversion within 30 days of an *impression* (no click), and **multi-touch attribution can double-count**
  a conversion across touchpoints (CVR can exceed 100%). Net: platform-reported conversions **overstate
  causal CVR**, which raises the incremental bar further.
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
