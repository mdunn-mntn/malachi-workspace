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

_(Findings from Steps 2–4 appended as work proceeds.)_

## 5. Solution
_TBD — recommendation + deliverables._

## 6. Questions Answered
Vendor's four questions (answers populated as analysis completes):
- **Q1 — Reach & overlap:** How much CTV reach hits audiences already deep in funnel? _(Limit: only
  pixel site-visitors/RTC are visible — not true funnel depth.)_
- **Q2 — Incrementality on our side:** Ghost-ads / PSA holdout to triangulate the geo result.
- **Q3 — Conversion windows:** Attribution / view-through rules behind platform-reported conversions;
  where is credit over-counted?
- **Q4 — Creative & targeting:** New-demand vs existing-intent audience; B2B comparables. _(Limit: no
  incrementality-trained model → makeup changes are speculation.)_

## 7. Data Documentation Updates
_TBD — B2B CVR power floor; attribution over-credit; single-advertiser triangulation pattern._

## 8. Open Items / Follow-ups
- Ghost-bid leg pending bidder-coverage gate (expect deferral if Beeswax).
- Deep dives on Q1/Q3/Q4 may spin into follow-on tickets if they exceed 2 SP.
