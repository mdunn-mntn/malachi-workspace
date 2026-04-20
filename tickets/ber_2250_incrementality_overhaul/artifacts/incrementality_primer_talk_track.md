# Incrementality Primer — Talk Track

**Format:** Loom, ~20 minutes. Narrate conversationally — teaching moment, not a boardroom pitch.

**How to read this:**
- Everything in `> blockquotes` is what you say out loud.
- `[NEXT → Slide X: description]` = hit the right arrow. The description tells you what's about to appear so you're never surprised.
- `[PAUSE]` = 2-3 seconds of silence. Let the slide breathe.
- `[SLOW]` = drop your speaking pace for emphasis.
- `[STOP RECORDING]` = you're done.

---

## You're looking at: SLIDE 1 — Title
*"Incrementality Measurement: A Primer for the TI Team"*

> Hey everyone. This is a primer on incrementality measurement — what it is, why it matters, and what we're building. It's about twenty minutes. If you're working on anything related to targeting, experiments, or how we measure whether our ads actually work, this is for you.

[NEXT → Slide 2: Big red "4,100%" and "-63%" numbers. eBay sign-flip.]

---

## You're looking at: SLIDE 2 — The eBay Sign-Flip

> I want to start with a number.

[PAUSE]

> Four thousand, one hundred percent. That was eBay's reported return on their paid search spend. Fifty million dollars a year in ads, returning forty-one hundred percent. By every dashboard they had, this was the best investment in the company.

[PAUSE]

> And then the true number. Negative sixty-three percent. The ads were *losing money*. Not underperforming. Losing. Every dollar they spent on paid search came back as sixty-three cents of loss.

[NEXT → Slide 3: "Same company. Same data. Different method." with Tadelis story text.]

---

## You're looking at: SLIDE 3 — The Tadelis Story

> Same company. Same data. Different method.

> Here's what happened. In 2013, eBay hired an economist from Berkeley named Steve Tadelis. And Tadelis asked the simplest possible question: "What happens if we turn it off?"

> Nobody had asked that before. Fifty million a year, and nobody had asked what happens if we stop.

> So they did. They turned off paid search in a third of the country. And sales — [SLOW] barely — moved. The people clicking eBay's paid search ads were going to buy from eBay anyway. The entire fifty million was going to people the ads didn't influence.

> Tadelis published the paper in Econometrica — one of the top economics journals in the world. eBay's CMO left the company.

[PAUSE]

[NEXT → Slide 4: Power Line in large navy text — "Without a counterfactual, ROAS is fiction."]

---

## You're looking at: SLIDE 4 — Power Line

[PAUSE — let the slide appear, hold 1 second, then speak]

> [SLOW] Without a counterfactual... ROAS is fiction.

[PAUSE — 3 full seconds of silence]

> That's the core idea of this entire talk. If you don't know what *would have happened without the ad*, you cannot know what the ad caused. Everything else I'm going to show you is a strategy for answering that counterfactual question.

[NEXT → Slide 5: Y(1), Y(0) equation box. "The One Idea Everything Is Built On."]

---

## You're looking at: SLIDE 5 — Potential Outcomes

> Let me make this concrete. Every household has two potential outcomes. Y-one — what happens if they see our ad. Y-zero — what happens if they don't. The causal effect of our ad on that household is the difference between those two numbers.

> Simple, right? Except there's a catch — and it's the catch that makes this an entire field of study.

[NEXT → Slide 6: "You only ever observe one." in large text.]

---

## You're looking at: SLIDE 6 — The Fundamental Problem

> You only ever observe one.

> The household either saw the ad or they didn't. You can't rewind the universe and play it both ways. The other outcome — the counterfactual — is permanently gone. You will never observe it.

> This is called the fundamental problem of causal inference. And every method I'm about to walk through — randomization, geo experiments, synthetic controls, all of it — is a different strategy for *estimating* that missing half.

[NEXT → Slide 7: Three numbered items — selection bias, activity bias, confounding.]

---

## You're looking at: SLIDE 7 — Why Ads Are Especially Hard

> Now, this problem exists everywhere in science. Clinical trials face it. Economists face it. But advertising has a specific twist that makes it worse.

> Three forces are working against us.

> First — selection bias. We don't show ads randomly. Our targeting systems specifically find people likely to buy. That means the people who saw the ad were *already different* from people who didn't. They were better prospects before any ad ran.

> Second — activity bias. People who are online more, who watch more CTV, who browse more — they're easier to reach with ads AND they're more likely to buy. So exposure and conversion are correlated even if the ad does nothing.

> Third — confounding. Holiday season boosts ad spending AND sales simultaneously. The correlation between spend and sales partly reflects advertising effects, but it also reflects shared causes like seasonality.

> All three of these forces push reported ROAS upward.

[NEXT → Slide 8: "View-through ROAS looks high even if the ad did nothing." in bold.]

---

## You're looking at: SLIDE 8 — The Result

> And this is the result. View-through ROAS looks high even if the ad did nothing.

> [SLOW] The targeting *works* — and that's actually the problem. It works so well that when we look at people who saw our ads, they convert at a high rate. But they would have converted at a high rate anyway, because we selected them *because* they were likely to convert. The ad gets credit for behavior it didn't cause.

> This is why eBay's dashboards said four thousand percent and the truth was negative sixty-three. The dashboards were measuring selection, not causation.

[NEXT → Slide 9: Numbered list of five strategies.]

---

## You're looking at: SLIDE 9 — Five Strategies Overview

> So how do we actually solve this? How do we estimate the missing half — the counterfactual?

> There are five families of methods, and I'm going to walk through each one. Some are better than others. Some we can use today, some we're building toward. But they all attack the same fundamental problem from different angles.

[NEXT → Slide 10: "Strategy 1 — Randomization." Coin-flip logic.]

---

## You're looking at: SLIDE 10 — Randomization

> Strategy one. Randomization. This is the most powerful idea in the field, and once it clicks you'll see why practitioners get almost religious about it.

> The logic is simple. If you flip a coin to decide who sees the ad, then the exposed and unexposed groups are — on average — identical in every way except exposure. Same income distribution, same purchase intent, same browsing behavior, same everything. The coin doesn't care about any of that.

> So any difference you see in outcomes afterward? It must have been caused by the ad. Because the coin is the only thing that differed.

> That's the magic of a randomized controlled trial. It doesn't eliminate selection bias by measuring it and adjusting for it. It eliminates it *by construction*.

[NEXT → Slide 11: Treatment vs Control side-by-side boxes. Ghost ads.]

---

## You're looking at: SLIDE 11 — Ghost Ads

> Now, the cleverest version of this in advertising is called ghost ads — or ghost bidding.

> Here's how it works. You still run the auction. Your targeting system still selects which households to bid on. But for the control group, instead of showing the real ad, you show nothing — or a public service announcement. The key insight is on the slide: *both groups were selected by targeting. Only one was exposed.*

> This is huge. It means your control group has the same intent profile, the same behavioral characteristics, the same everything as your treatment group. The only difference is whether they actually saw the ad. So selection bias is gone.

> Google runs over a hundred million ghost ads per day. The Trade Desk markets this. Viant markets this. And right now, our bidder can't do it natively. That's one of the things we're building.

[NEXT → Slide 12: ITT vs TOT side-by-side boxes.]

---

## You're looking at: SLIDE 12 — ITT vs TOT

> Quick but important distinction. When you run one of these experiments, you have two ways to analyze it.

> Intent-to-treat — ITT — compares the groups as assigned. Everyone in the treatment group versus everyone in the control group, regardless of whether they actually saw the ad. This is conservative. It's clean. It's what you should report externally.

> Treatment-on-treated — TOT — adjusts for compliance. It tries to isolate the effect on people who actually saw the ad. This is closer to what leadership wants to know — "what's the effect of someone *actually seeing* our ad?"

> The difference matters in practice. We saw this on TI-835 — our ITT analysis showed zero lift because only fourteen to sixteen percent of the treatment group actually received impressions. When eighty-six percent of your "treated" group got no treatment, the comparison collapses. Ghost ads and the ATT estimator we're building address exactly this problem.

[NEXT → Slide 13: Three CTV challenges — no user ID, no ghost infra, need huge samples.]

---

## You're looking at: SLIDE 13 — Why RCTs Are Hard in CTV

> So if randomization is so great, why doesn't everyone just do it? Three reasons, specific to CTV.

> First, there's no persistent user ID. In CTV, the unit is the household — an IP address that maps to a living room. Three people watch the same screen. You flip one coin for the household, but three people see the result. This isn't fatal, but it complicates the math.

> Second, ghost-ad infrastructure doesn't exist in our bidder today. The bidder needs to support "log the auction result but don't serve the ad." That's a product change. We're working on a stopgap — using win rates from the augmentor log to approximate ghost impressions — but the real thing requires engineering work.

> Third — and this is the Lewis and Rao problem — CTV conversion rates are low, and the effects we're trying to detect are modest. Two to eight percent lift. To reliably detect that, you need *enormous* sample sizes. Most ad experiments in the industry are underpowered, which means they produce null results that get interpreted as "the ad doesn't work" when really the experiment just didn't have enough data.

[NEXT → Slide 14: "Strategy 2 — Geo Experiments." "Can't randomize households? Randomize geographies."]

---

## You're looking at: SLIDE 14 — Geo Experiments

> Which brings us to strategy two — and this is the pragmatic answer for right now.

> If you can't randomize individual households — randomize geographies. Take the two hundred and ten DMAs in the US. Flip coins at the DMA level. Some markets get your CTV campaign, others don't. Then compare total sales between the two groups.

> The logic is the same as an RCT. Randomization at the geo level means the groups are, on average, balanced. Outcome differences are causal.

[NEXT → Slide 15: Four green checkmarks — no IDs needed, walled gardens don't matter, co-viewing doesn't matter, MNTN already partners with Haus.]

---

## You're looking at: SLIDE 15 — Why Geo Works for CTV

> And here's why this is such a natural fit for CTV specifically.

> You don't need user IDs. You're comparing aggregate sales by geography, not tracking individual households. Walled gardens — Disney, Netflix, Amazon — don't matter, because you're measuring *total sales*, not pixel fires from your own ad server. Co-viewing doesn't matter because a DMA is a DMA regardless of how many people watch.

> And critically — we already partner with Haus for this. Haus is in our Integrations Marketplace right now, offering geo-based lift testing to our advertisers. So this isn't hypothetical. It's live. The question is whether we also build our own platform-native capability or rely entirely on partners.

[NEXT → Slide 16: Three red X items — 41% effect after campaign ends, DMAs not interchangeable, limited power.]

---

## You're looking at: SLIDE 16 — Geo Challenges

> But geo isn't free. Three things to know.

> First — and this surprised me — forty-one percent of CTV's incremental effect shows up *after* the campaign ends. If you stop measuring when the campaign turns off, you're missing almost half the value. The measurement window is critical.

> Second, DMAs aren't interchangeable. New York City and Omaha are not twin experimental units. You can't just compare them directly. Modern methods — GeoLift, synthetic control — solve this by constructing weighted combinations of control markets that historically match the treated market. But it adds complexity.

> Third, you only have about two hundred and ten DMAs to work with. That limits statistical power, especially for smaller advertisers.

[NEXT → Slide 17: "Strategy 3 — Quasi-Experiments." "When you can't randomize, exploit something that looks like randomization."]

---

## You're looking at: SLIDE 17 — Quasi-Experiments

> Strategy three. Sometimes you can't randomize at all — not users, not geos. But sometimes something happened in the world that *looks like* a random assignment if you squint. We call these quasi-experiments.

[NEXT → Slide 18: DiD equation — (CA after - CA before) - (OR after - OR before). "Big assumption: parallel trends."]

---

## You're looking at: SLIDE 18 — DiD

> The workhorse here is difference-in-differences — DiD. The idea is simple.

> Say we launched a campaign in California on March 1st. We didn't launch in Oregon. We take the change in California's sales — March versus February — and subtract the change in Oregon's sales over the same period. That double difference cancels out the California-Oregon baseline gap *and* any nationwide March seasonality. What's left — under certain assumptions — is the causal effect of the California campaign.

> The big assumption is parallel trends. Without the campaign, California and Oregon would have moved in sync. If that's true, this works beautifully. If California was already accelerating for other reasons, you're in trouble.

[NEXT → Slide 19: CausalImpact. Synthetic CA = 30% AZ + 25% NV + 20% TX + 25% CO. "We already use this at MNTN."]

---

## You're looking at: SLIDE 19 — CausalImpact / BSTS

> And here's the souped-up version — synthetic control. Instead of comparing California to one other state, you build a *synthetic California* out of a weighted combination of all the other states. Thirty percent Arizona, twenty-five percent Nevada, twenty percent Texas, twenty-five percent Colorado — chosen so that pre-campaign, this synthetic California matches the real one almost exactly.

> Then after the campaign launches, you compare real California to synthetic California. The gap is your causal effect.

> This is what Google's CausalImpact tool does — the Bayesian structural time series approach. And we already use it at MNTN. We used it for the TI-748 media plan analysis. So if you've seen those results, you've already seen this method in action. Now you know the formal name and — more importantly — the assumption it rests on.

[NEXT → Slide 20: "Strategy 4 — Marketing Mix Modeling." "Completely different animal. Top-down, not bottom-up."]

---

## You're looking at: SLIDE 20 — MMM

> Strategy four is a completely different animal. Everything I've shown you so far is bottom-up — individual exposures, individual or geo-level comparisons. Marketing Mix Modeling is top-down.

> You take weekly aggregated data — total spend by channel, total sales, plus controls for seasonality, promotions, competitor actions — and you regress sales on everything. The coefficient on CTV spend, properly modeled, gives you the marginal sales response. From that you derive incremental ROAS.

> This is the only method that works across *all* channels simultaneously. CTV, search, social, display, linear TV, print — all in one model. And it gives you saturation curves, budget allocation recommendations, the works.

> The weakness: it's observational. You didn't randomize anything. The causal interpretation depends on controlling for every confounder, which you never do perfectly.

[NEXT → Slide 21: Triangulation diagram — Geo Lift Tests → Bayesian MMM → iROAS. "46% of leading marketers use this."]

---

## You're looking at: SLIDE 21 — Triangulation

> Which is why the modern pattern is triangulation. You don't rely on MMM alone. You run experiments — geo lift tests — to get causal estimates. Then you feed those estimates into the MMM as Bayesian priors. The model is now anchored to experimental truth instead of floating on correlations.

> Experiments tell you "CTV has this much causal effect." The MMM tells you "given that, here's how to allocate across all channels." The combination is stronger than either alone.

> This is the architecture Google, Meta, and nearly half of leading marketers have converged on. We're not there yet, but this is the end state we're building toward.

[NEXT → Slide 22: "Strategy 5 — Causal ML." "Mathematically beautiful. Operationally dangerous."]

---

## You're looking at: SLIDE 22 — Causal ML

*Move through this one with conviction. You're dismissing, not teaching.*

> Strategy five. And I'm going to be direct about this one.

> Causal ML — propensity matching, doubly-robust estimators, uplift models, causal forests — these tools are mathematically beautiful. And in advertising, they are operationally dangerous.

> They all rest on one assumption: no unmeasured confounders. If you've controlled for everything that affects both exposure and outcome, they work perfectly. The problem is — in ads, you *always* have unmeasured confounders. Unobserved intent. Off-platform exposure. Audience overlap with other channels. Things the model can't see.

[NEXT → Slide 23: Big red "62-115 pp" number. "The Evidence Is Brutal." Gordon 2023.]

---

## You're looking at: SLIDE 23 — The Gordon Result

> And here's the evidence. This is from a 2023 paper in Marketing Science. The researchers took 663 actual randomized experiments from Meta — real RCTs with known true effects — and asked: how well do observational methods recover the truth?

> They threw everything at it. Five thousand user features. Deep learning. State-of-the-art double machine learning.

> [SLOW] Median absolute error: sixty to one hundred and fifteen percentage points.

[PAUSE]

> On effects that were truly six to twenty-eight percent, the observational methods were off by sixty to a hundred and fifteen points. Not close. Not "a little biased." Fundamentally wrong. And this was with Facebook's *own user data* — the best features any platform could provide.

> No amount of ML closes the gap. If you don't have a counterfactual, you don't have causation — no matter how fancy the model.

[NEXT → Slide 24: Attribution vs Incrementality side-by-side boxes. "Both useful. Only one is causal."]

---

## You're looking at: SLIDE 24 — Attribution vs Incrementality

> Now I want to draw a sharp line between two things that get confused constantly.

> Attribution asks: "Among the paths that converted, how do we divide credit among the touchpoints?" That's a journey question. It's about dividing a pie that already exists.

> Incrementality asks: "Would they have converted *without* the ad?" That's a causal question. It's about whether the ad created new pie.

> Both are useful. Both have their place. But they answer fundamentally different questions, and only one of them is causal. The trap the industry falls into — and that we fall into — is using attribution numbers as if they were incrementality numbers.

[NEXT → Slide 25: ROAS table — Last-click $3.50, Multi-touch $2.80, View-through $2.40, Shapley $1.90. "None tell you if CTV caused any sales."]

---

## You're looking at: SLIDE 25 — The Attribution Trap

> Here's the trap in concrete terms. Take a single CTV campaign. Same data, same conversions, same spend. Run it through four different attribution models.

> Last-click says three fifty. Multi-touch says two eighty. View-through says two forty. Shapley says one ninety. Four different numbers from the same data. And not one of them tells you whether CTV *caused* any sales. They're all just different ways of dividing credit for conversions that may have happened regardless.

> Attribution is the journey. Incrementality is the causation. We need both, but we need to stop confusing them.

[NEXT → Slide 26: "How Power Works." Three drivers: effect size, sample size, variance. MDE warning.]

---

## You're looking at: SLIDE 26 — Power

> One more concept before we bring it home. Statistical power. This is the thing everyone hand-waves, and it quietly kills more experiments than bad methodology.

> Power is the probability that your experiment correctly detects an effect that actually exists. Three things drive it: how big the effect is, how much data you have, and how noisy your outcomes are.

> Here's the practical implication. Before you run any experiment, you need to compute the minimum detectable effect — the MDE. That's the smallest lift your experiment can reliably see given its sample size. If your MDE is fifteen percent but the realistic lift is five percent, you've designed an experiment that *cannot succeed*. You'll run it, get a null result, and conclude "the ad doesn't work" — when really you just didn't have enough data to see it.

[NEXT → Slide 27: "Most ad experiments are underpowered." Lewis-Rao QJE 2015.]

---

## You're looking at: SLIDE 27 — Lewis-Rao

> And this is the uncomfortable reality. A 2015 paper in the Quarterly Journal of Economics — one of the top journals in all of economics — showed that most ad experiments are underpowered.

> A null result in an ad experiment usually means "not enough data." Not "no effect."

> CTV makes this structurally hard. Low conversion rates mean high variance. Modest true lifts — two to eight percent — mean small signal. And household-level measurement means your "sample size" is millions of households, not billions of impressions. Even with enormous campaigns, detecting a realistic lift is right at the edge of what's statistically possible.

> This isn't pessimism. It's the scientific reality. And it means we need to be honest about uncertainty — with ourselves and with our advertisers.

[NEXT → Slide 28: "Why This Matters Now." Haus in our Marketplace. LiftLab running 5 experiments on our campaigns.]

---

## You're looking at: SLIDE 28 — Why Now

*Shift your energy here. You're no longer the teacher. You're the advocate.*

> So — why does this matter *right now*?

> [SLOW] Haus is already in our Integrations Marketplace, selling lift testing to our advertisers. LiftLab is running five experiments this quarter on *our* campaigns. Partners are answering the question "does MNTN work?" and we're not in the room.

[PAUSE]

> If we can't validate their numbers — or build our own — we're outsourcing our most important measurement question. We're letting someone else tell the story of whether MNTN works.

> That's not acceptable. Not for a company that wants to own its measurement narrative.

[NEXT → Slide 29: "What We're Building." Geo holdouts → Ghost ads → Calibrated MMM diagram. "Ghost ads are the goal. Geo experiments get us there."]

---

## You're looking at: SLIDE 29 — What We're Building

> Here's what we're doing about it.

> Ghost ads are the goal. Per-advertiser, household-level causal measurement — the scientific gold standard. We're building toward that with the ghost bidding framework under BER-2250.

> Geo experiments get us there. The infrastructure exists today. We can run geo holdouts now while we build ghost-ad capability. This is our bridge.

> And the end state is calibrated MMM — where our experimental results feed into a Bayesian model that produces per-advertiser incremental ROAS with honest uncertainty bounds.

> Geo holdouts, ready today. Ghost ads, building now. Calibrated MMM, the end state. Everything else is support.

[NEXT → Slide 30: Power Line in large text — "Without a counterfactual, ROAS is fiction." and "We're building ours now."]

---

## You're looking at: SLIDE 30 — Close

[PAUSE — let the slide appear. 2 seconds of silence before you speak.]

> [SLOW] Without a counterfactual... ROAS is fiction.

[PAUSE — 3 full seconds]

> Every method I just walked through — randomization, geo experiments, synthetic control, MMM — is a strategy for building that counterfactual. For estimating the universe where the ad didn't run, so we can measure what the ad actually caused.

> [SLOW] We're building ours now.

[PAUSE — hold 3 seconds]

[STOP RECORDING]

---

## Recording Tips

- **Record in one take if you can.** The conversational energy of a single take is better than a polished multi-take. Mistakes make it feel human.
- **Power Pause on Slide 4 and Slide 30.** These are your two biggest moments. Three seconds of silence before "Without a counterfactual, ROAS is fiction." Every second of silence makes the line hit harder.
- **Slow down on the numbers.** Four thousand one hundred percent. Negative sixty-three percent. Sixty to a hundred and fifteen percentage points. Numbers need space to land.
- **The Tadelis story is your emotional peak.** This is where the audience decides whether to care. Lean into it. Don't rush. "They turned it off in a third of the country. Sales barely moved." Let that sit.
- **Slides 22-23 (Causal ML) should feel fast and decisive.** You're not teaching — you're dismissing. "Mathematically beautiful. Operationally dangerous." Then the Gordon result. Then move on. Thirty to forty seconds total.
- **Slide 28 (Why Now) should feel urgent.** Shift from teacher to advocate. "Partners are answering the question 'does MNTN work?' and we're not in the room." That's a punch. Deliver it like one.
- **End cleanly.** Don't say "that's all I have" or "any questions." The Power Line is the last thing in the air. Say it, pause, stop recording.
