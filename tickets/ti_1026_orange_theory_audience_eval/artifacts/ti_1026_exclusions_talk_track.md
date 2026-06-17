# Orange Theory — Demographic Exclusions: Recommendation & Talk Track (for CS)

**Recommendation:** Don't stack demographic (income/age) exclusions. Lean on MNTN's intent scoring — it's the
performance lever. If income screening is a hard brand requirement, use **one** provider, not several.

It's effectively **performance OR exclusions, not both**: the scoring already finds the best-converting households;
broad demographic exclusions mostly remove good households on top of it, using unreliable data.

---

## Talk track — the questions the client will ask

**Q: What does MNTN's targeting actually use to find good prospects?**
Behavioral *intent*, not demographics. We score every household on what it actually does online — the content and
sites it browses, how that maps to fitness/health interest, how many relevant page views, and how recently — and turn
that into an intent score. A "High Intent" household is one whose behavior strongly signals fitness interest. The
score already encodes who's likely to convert; it's earned from behavior, not assumed from a demographic profile.

**Q: How much does the income/age exclusion remove?**
About **1.3M households — ~29% of the intent-qualified audience.** These are people whose behavior already signals
fitness interest, but a third-party income estimate tags them low-income, so they're dropped before we ever bid.

**Q: We know people earning under ~$40K aren't buying OTF memberships — so why is excluding them detrimental?**
Two reasons:
1. **The income data is an unreliable estimate.** It's inferred at the IP level (an IP is a household/network, not a
   tax return). When we compared the three providers used here, they agreed on **only 0.36%** of who's "low-income"
   (65,571 households out of 18.3M flagged by at least one). Equifax flagged 2.9M, TransUnion 4.5M, Experian 12.6M —
   for the *same* concept. If income were a reliable attribute, they'd largely agree. They don't.
2. **Behavior beats a demographic guess.** When a household's browsing already signals fitness intent but a noisy
   income estimate says "low-income," the behavior is the better predictor. Excluding it discards a household our
   model has already validated as a prospect — on a guess that's wrong most of the time. (And the income tag being
   wrong is common: shared/business IPs, households where the earner differs from the estimate, etc.)

**Q: Then why not stack multiple providers for the same thing (e.g. HHI <$30K) to be safe?**
It backfires. A household is excluded if **any** provider flags it, so stacking providers excludes the *union*
— **18.3M households** — and you inherit **every** provider's errors, even though they agree on only 0.36% of who's
low-income. Stacking maximizes over-exclusion, not accuracy. (One configured provider, Oracle, delivers no data at
all right now — it's excluding no one.)

**Q: Why does Experian flag ~4× more low-income than Equifax — is Experian padding?**
No — it's the opposite of what you'd guess. Looking at each provider's *full* income distribution: **Experian's is
realistic** — ~10% of households <$25K (slightly below the true US ~18–20%), peaking at $50–75K. **Equifax/IXI is the
skewed one** — only **3.6%** labeled <$30K and **41% labeled $150K+**, which is implausibly affluent. That's
consistent with Equifax's "Income 360 (IXI)" being an *asset / financial-capacity* estimate rather than earned income,
so it under-counts low-income. So Experian flags 4× more simply because **Equifax barely labels anyone low-income**,
not because Experian pads.

**Q: If we must screen income, which provider?**
For *low-income screening specifically*, **Experian is the more realistic signal** — Equifax/IXI would exclude almost
no one (3.6%) and miss genuinely low-income households (it skews affluent). But **at the individual-household level all
providers are still unreliable** (0.36% three-way agreement), so whichever you pick, apply **one** provider, treat it
as a coarse directional filter, and don't stack.

**Q: So what should we do instead?**
Trust the score. The intent model already concentrates spend on the households most likely to convert (the High-Intent
tier performs best). Removing the demographic exclusions recovers ~1.3M intent-qualified households at little quality
cost, because the scoring — not the demographic filter — is what's driving performance.

---

## The numbers (for reference)
- Income/age exclusions remove ~**1.31M / 28.7%** of the intent-qualified (keyword-matched) audience.
- 3 income providers, "low-income" flag, 14-day window: Equifax 2.89M · TransUnion 4.45M · Experian 12.60M · union 18.34M.
  Pairwise agreement: Eq∩Ex 517K · Eq∩Tu 282K · Ex∩Tu 860K · **all three 65,571 (0.36% of the union).**
- Full income distributions (`outputs/ti_1026_income_distribution.csv`): **Equifax/IXI** — only **3.6%** <$30K, **41%** $150K+
  (affluent-skewed; IXI is asset/capacity-based). **Experian** — **10.2%** <$25K, peak $50–75K (realistic). → Experian
  flags 4× more low-income because Equifax under-labels it, not because Experian pads.
- Source: `outputs/ti_1026_income_provider_agreement.*`, `ti_1026_exclusion_bite_on_mm.sql`. Income data is IP-level
  third-party (Equifax/IXI, Experian, TransUnion) — an estimate, not verified income.

*Caveat: third-party delivery is bursty, so provider totals are window-based; the disagreement signal (0.36% three-way
agreement) is the robust takeaway. Performance comparisons are descriptive — the cleanest proof of "exclusions don't
help" would be a holdout test.*

---

## The bigger gap (Kelly, 2026-06-17): how do we guide customers on which demo segments to use?

The real problem isn't OTF — it's that **we offer many redundant 3P segments for the same attribute** (e.g. HHI
$20–29.9K from Equifax, Experian, TransUnion, Oracle…) with **no quality ranking and no recommendation**, so
customers (and CS) guess. For goals MNTN Matched can't do alone (pure demographic targeting), 3P is the only option —
so we need to tell them *which* segment.

**Interim guidance rubric (per demographic attribute):**
1. **Never stack** providers for the same attribute. Exclusions/includes are OR'd, so stacking = the union = you
   inherit every provider's errors (income example: 3 providers → 18.3M excluded vs 2.9M for the most conservative).
   **Pick one.**
2. **Rank the candidates** by: **Coverage** (IPs reached), **Freshness** (recency), **Uniqueness** (not just a dupe of
   another), and — the real differentiator — **Performance** (do the segment's IPs actually behave as labeled?).
3. **For exclusions specifically,** pick the provider whose *distribution is realistic*, not the one that flags the
   fewest. (Counterexample: for "low income," Equifax/IXI labels only 3.6% of households <$30K — it skews affluent
   (asset-based) and would barely screen / would miss real low-income; Experian's ~10% <$25K is the more realistic
   signal. "Conservative" ≠ "accurate.")
4. **Set expectations:** demographic 3P is a *coarse* filter — the providers disagree (0.36% three-way agreement on
   income), so it's directional, not precise. Use it only when a real targeting goal requires it; otherwise lean on intent.

**The honest limitation:** by Coverage + Freshness alone there's often **no clear winner** (the OTF income segments are
all fresh, none deprecated, yet disagree and span 4× in size). The missing ingredient is a **per-segment quality/
performance score** — which is exactly **[TI-956] interest-segment quality scoring** (sized by **[TI-999]**). The
durable answer to "what segment should I use?" is MNTN **surfacing a recommended segment per attribute** (ranked by
that score) in the UI / via the **[TI-1037]** diagnostic — so customers stop choosing blind among duplicates.
