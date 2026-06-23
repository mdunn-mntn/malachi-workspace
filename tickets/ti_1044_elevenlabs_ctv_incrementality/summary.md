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

_(Findings from Steps 1–4 appended as work proceeds.)_

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
