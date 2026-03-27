# Slack Message Draft — Chris Addy (Media Plan Algorithm Questions)

**Channel:** DM to Chris Addy (cc Kirsa)
**Context:** Kirsa suggested reaching out. Working on causal impact analysis for media plan experimentation (TI-748).

---

Hey Chris! Kirsa pointed me your way — I'm working on a causal impact analysis measuring whether the recommended media plan improves prospecting IVR (TI-748). I've gone through the release brief and requirements doc, but had a few questions about how the algorithm works that would help me interpret the results.

**Quick context on what we found:** The overall IVR effect is near zero across 8 advertisers, BUT we found a strong pattern — advertisers whose plans concentrated budget on fewer publishers (16 networks) saw +10-17% IVR improvement, while those spread across more publishers (26 networks) saw -26 to -31% decline. The degree of concentration appears to predict who benefits.

**Questions:**

1. **What determines how many publishers the algorithm recommends?** We saw some advertisers get 16 publishers and others get 26 for their recommended plan. Is this driven by budget size, vertical, audience size, deliverability constraints, or some combination? Understanding this would help us explain why concentration varies.

2. **Is there a table/log of the scoring per publisher?** We know the algorithm uses spendability, historical performance (VVR), and semantic relevance. If there's data on the individual scores (e.g., a spendability score per network per advertiser), we could test whether the algorithm's publisher selection correlates more with inventory availability vs performance.

3. **What does `deliverability_classification` on `media_plan` mean?** We see values like "medium" — is this the algorithm's confidence in being able to deliver the full budget? And does it relate to how concentrated the plan is?

4. **Is there a way to tune concentration?** Our data (small N caveat) suggests fewer, higher-conviction picks outperform a broader spread. Is there a parameter or threshold that controls how many publishers get recommended? Could it be adjusted to test more concentrated plans?

5. **How does Flex Targeting interact with the recommended plan in delivery?** We see ~5-6% of impressions going to un-recommended publishers (e.g., Tubi Entertainment). Is this entirely from the Flex budget, or can the bidder deviate from the plan allocation for other reasons?

No rush on any of these — happy to dig through tables/code if you can point me in the right direction. And happy to share what we've found so far if it's useful for the team.

Thanks!
Malachi
