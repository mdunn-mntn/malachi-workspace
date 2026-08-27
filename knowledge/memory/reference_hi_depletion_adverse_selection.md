---
name: reference_hi_depletion_adverse_selection
description: "The converter/visitor-depletion adverse-selection hypothesis — repeatedly-served non-responders accumulate and the residual pool converts worse than fresh; what each premise actually requires, why it fails on conversions and for WGU, and the discriminating test nobody has run"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [adverse selection, converter depletion, pool burndown, saturation, IVR by impression number, prior exposure, Audience Exclusions, Converter Exclusions, Site Visitor Exclusions, Audience Isolation, DS21, DS34, DS16, bid_count collider, ghost_frac]
domain: [audience-scoring, experimentation, bidding]
lifecycle: active
last_verified: 2026-08-27
---
Raised by Malachi in #targeting-squad 2026-08-27: serve a high-intent pool, remove whoever converts, repeat, and what remains is enriched for non-converters, so a 20x-served non-responder should perform worse than a fresh untargeted IP. Alex Knorr's first question was replacement; Matt Brorby countered that WGU sits on interest segments because MM performance tanked. Reusable framing for any "we burn through the audience" argument.

## What each premise actually requires
1. **Removal has to be switched on.** Converter and Site Visitor Exclusions are an **opt-in per-advertiser toggle, prospecting-scoped, off by default** — only 245 change events across 157 advertisers lifetime. **WGU (31357) has all three toggles off**, which falsifies the premise for the advertiser the argument was built around. Detail + the third toggle (Audience Isolation): `data_knowledge.md` § "Audience Exclusions is an OPT-IN, prospecting-only UI surface".
2. **The drain has to have mass.** Per-exposure **conversion** hazard is 0.038% pooled / 0.021% WGU, so twenty exposures strip 0.41–0.77% of the pool. **Run the argument on visits, not conversions** (~0.5–0.65% per exposure, ~9–12% over twenty).
3. **The pool has to behave like a stock.** It does not: ~68–70% of each month's distinct HI reach was unserved in the prior 30 days (HexClad, AUDI-1070), scores are recomputed daily on a rolling window, and DS16 excludes served households only when net-new gating is explicitly on. See `data_knowledge.md` § "HI pool is a FLOW, not a stock".
4. **Frequency has to actually reach the quoted levels.** Prospecting per-IP frequency is median 1–2; Caraway's HI frequency held flat at 1.45–2.07/household/month for twelve months while spend rose 191%. Headline imps/IP figures are shared-IP artifacts.
5. **Visit likelihood has to vary inside the intent label.** If every IP in HI is equally likely, removal selects nothing and there is no effect. That variation is **assumed, never measured.**

## Where it stands
Cross-sectional evidence is **flat** — no decline in conversion or visit hazard across frequency deciles 1.1 to 35.6 imps/IP. Two colliders (post-treatment `bid_count`, and `ghost_frac` climbing 0.10→0.47 with bid count) would manufacture the expected answer, so the obvious instrument is unusable. The **within-advertiser, within-intent-band** test has never been run; spec in `experimentation.md` § "Testing does performance decay with prior exposure". Directionally against the hypothesis: Kindred's DS16 net-new-gated variants (only never-hit households) returned ROAS 1.18–1.35x versus the ungated saturated base's 2.39x.

**How to apply:** do not argue pool burndown for a specific advertiser without first checking its exclusion toggles, and argue supply arithmetic instead when the real concern is a very-high-spend account ([[reference_mm_wgu_suitability_argument]]). Explainer artifact for the mechanism (generic, spread exaggerated, no advertiser named): https://claude.ai/code/artifact/134edb1f-3163-47f6-8c48-d473092bd3d1

Related: [[reference_frequency_capping]], [[project_intent_tier_pacing]], [[reference_hhst_efficiency_sizing]], [[reference_exclusions_invisible_to_scoring]].
