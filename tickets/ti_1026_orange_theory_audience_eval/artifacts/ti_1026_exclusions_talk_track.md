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
It backfires. A household is excluded if **any** provider flags it, so stacking three providers excludes the *union*
— **18.3M households** — and you inherit **every** provider's false positives. The most aggressive provider (Experian,
12.6M) flags ~4× as many as the most conservative (Equifax, 2.9M). Stacking maximizes over-exclusion, not accuracy.
(One of the configured providers also delivers no data at all right now — it's excluding no one.)

**Q: If we must screen income, which provider?**
None is authoritative — they barely agree. If income screening is a hard requirement, pick **one** provider with a
sensible band, apply it once, and accept it's a coarse filter that costs reach. Don't layer several.

**Q: So what should we do instead?**
Trust the score. The intent model already concentrates spend on the households most likely to convert (the High-Intent
tier performs best). Removing the demographic exclusions recovers ~1.3M intent-qualified households at little quality
cost, because the scoring — not the demographic filter — is what's driving performance.

---

## The numbers (for reference)
- Income/age exclusions remove ~**1.31M / 28.7%** of the intent-qualified (keyword-matched) audience.
- 3 income providers, "low-income" flag, 14-day window: Equifax 2.89M · TransUnion 4.45M · Experian 12.60M · union 18.34M.
  Pairwise agreement: Eq∩Ex 517K · Eq∩Tu 282K · Ex∩Tu 860K · **all three 65,571 (0.36% of the union).**
- Source: `outputs/ti_1026_income_provider_agreement.*`, `ti_1026_exclusion_bite_on_mm.sql`. Income data is IP-level
  third-party (Equifax/IXI, Experian, TransUnion) — an estimate, not verified income.

*Caveat: third-party delivery is bursty, so provider totals are window-based; the disagreement signal (0.36% three-way
agreement) is the robust takeaway. Performance comparisons are descriptive — the cleanest proof of "exclusions don't
help" would be a holdout test.*
