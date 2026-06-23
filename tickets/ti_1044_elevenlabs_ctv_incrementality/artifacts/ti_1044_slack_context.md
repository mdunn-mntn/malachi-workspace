# TI-1044 — Slack context (ElevenLabs reporting no lift)

Thread title: **"ElevenLabs reporting no lift"** — @Mike Dolt @Matt Brorby
Date: 2026-06-23. Participants: Malachi Dunn, Kale McNaney, Edgar von Trotha, Mike Dolt.

---

**Malachi — 11:28 AM** _(thread opener + screenshot of the June-6 MDE calc)_

**Malachi — 11:31 AM**
> These are the calculated numbers from June 6th. Compared to average CVR rates, ElevenLab's CVR
> rates are incredibly low (0.062%) which is expected for B2B sales. As a result, the number of
> impressions needed to even measure a 5% MDE is quite high, at an estimated **$2.04m**! It makes
> perfect sense they were not able to find significance.
>
> Also, its entirely likely the lift on CVR would not be expected to exceed 5%. In fact, many large
> clients of ours targeting individuals see lift in the range of **2%**.
>
> All factors considered, being a B2B brand with incredibly low CVR makes a true incremental test
> nearly impossible to measure without significant spend. In this case, >$2m at a minimum.
>
> Calculator: https://gist.githack.com/mdunn-mntn/2d362849df017fa243eef03bb61cdfbb/raw/ti_xxx_mde_calculator_prefill.html

**Kale McNaney — 11:36 AM**
> cc: @Edgar von Trotha

**Edgar von Trotha — 11:39 AM**
> @malachi are you saying that with the current spend of $1M, it will be nearly impossible to see any
> stat-sig lift regardless of targeting mix or any other campaign factor?

**Malachi — 11:42 AM**
> Unless their CVR lift was >8%, a pipe dream reality, then yes, impossible. But I will run the numbers
> myself on just these campaigns and see what I can find. This is based off of my estimates when I
> created this power calculator ~2 weeks ago.
>
> It doesn't mean there's no lift. It just means we can't detect it 🤷‍♂️
>
> I also hear they ran a big campaign in the past on the best regions, namely SF and Texas so its
> possible these CVR numbers are even more inflated and we'd need even more spend.

**Edgar von Trotha — 11:52 AM**
> Did you see the deck from ElevenLabs? They presented 4 questions they want us to work through together:
> 1. **Reach overlap** — How much of CTV reach is hitting audiences already deep in the funnel? Do we
>    have any way of seeing this? Would it help to adjust the bidding to a different intent bucket?
> 2. **Incrementality on MNTN side** — Can we look at ghost ads/PSA to triangulate with their geo
>    results? Do we have a view of this yet?
> 3. **Conversion windows** — What attribution windows and view-through rules sit behind the
>    platform-reported conversions? (7-day rtg, 14-day prospecting, 30-day conversion window seen in
>    platform.) Are there any other rules affecting performance?
> 4. **Creative & Targeting** — Is the current creative / audience built to drive new demand? What have
>    similar B2B advertisers changed? (Creative looks fine; keen to understand audience makeup.)

**Mike Dolt — 12:06 PM** _(reality-checks — load-bearing for Q1 & Q4)_
> We don't have visibility into which audiences are already deep in the funnel besides **site visitors
> we get from pixel**. We can **block those for them** if they want, but that is not an accurate
> representation of being deep in funnel.
> What Malachi is talking about above is our internal incrementality calculations. Not sure what the
> question is.
> We currently **don't have a model trained to improve incrementality** so any audience makeup
> adjustments will be **nothing more than speculation**.

**Malachi — 12:09 PM**
> #2 is just an estimate of what would need to be spent in order to detect a 5% CVR lift, **not the
> actual incrementality results**. I'll have to run those in a bit.

---

## Takeaways that shape TI-1044
- The **$2.04M / 5%-MDE** figure is a *power estimate*, not the incrementality result. This ticket runs
  the **actual** MNTN-side incrementality numbers.
- Edgar's question (leadership): is stat-sig lift detectable at ~$1M spend regardless of targeting? →
  Headline answer: no (raw CVR MDE ~7.4%; realistic 2–5% lift is below the floor).
- **Q1 limit (Mike):** only deep-funnel signal = pixel site-visitors (RTC). Not true funnel depth.
- **Q4 limit (Mike):** no incrementality-trained model → audience-makeup "fixes" are speculation.
