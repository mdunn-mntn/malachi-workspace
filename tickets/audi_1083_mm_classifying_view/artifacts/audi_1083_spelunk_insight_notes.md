# /insight spelunking submissions — 2026-08-21

**Reframed after transcribing the session** (`meetings/audi_1083_01_insight_spelunking_2026_08_21.txt`).
My pre-session framing was wrong on the deliverable and wrong on the direction of the recommendation.

## What Mike actually asked for

- The deliverable is an **optimization recommendation for the advertiser**, reached from the reporting
  UI. Getting the squad close to what clients see is the primary goal; the insights are a bonus. His
  words: "if we generate some insights here, that's great, but obviously this is not a sustainable way
  to optimize things" — the real target is the **audience optimization tool** that will send or apply
  these recommendations automatically. Over 80% of clients launch a campaign and never touch it again.
- **Use your own expertise.** Do not roleplay ignorance of intent scoring. The one limit is surface:
  no raw data, no Ask Atlas, only what the client can see. "We're not trying to come up with the
  solution advertisers would come up with, we're trying to come up with the best solution, because we
  might actually go ahead and do that later."
- **Scope to the campaign named on the sheet**, not the advertiser. Advertisers have many campaigns and
  the others may be fine.
- UI bugs, counterintuitive reporting, and wrong data are worth noting, but they are a **side channel to
  the reporting team**, not the ask.
- Mark `YES!` when you start on a row. Mark `not a good candidate` when there is nothing to optimize:
  campaign no longer running, conversions that happen off-site (a phone number in the creative), or an
  industry that structurally never converts (Johnny's list — dentists, foundation repair).

## The recommendation direction I had backwards

Peak Performance **broadens** the audience past high intent. Two people in the session made the opposite
call to mine: advertisers assume a bigger audience is better, but many of these campaigns should sit on
high intent only, and turning Peak Performance **off** is the optimization. Mike confirmed it live on a
campaign where "peak performance is actually the highest spending [segment] and it brought zero
conversions — that is 100% a legitimate recommendation."

**So the test to run per campaign is the audience-segment report:** how much did the Peak Performance
segment spend, and how many conversions did it return? If it is spending with no conversions, recommend
turning it off. That report is **not a default** — Create Report → the blank template dropdown → audience
segment, or Performance Metrics → "Pivot by". You do not have to save it. Two different reporting-page
versions are live, so the path differs by advertiser.

**None of the four notes below have had that check run.** Run it before submitting.

---

## 1. FICO — aid 37056 · cgid 81053

**Campaign:** FY26_Croud_myFICO_US_Direct_MNTN_CTV_CTV_Mixed_3P_PP

**Observed in the UI:** Audience `myFICO - Prepared Planners (Feb '26)`. The MNTN Matched block lists
Peak Performance at 8,102,268, OR'd with nine Equifax / TransUnion / Experian / Epsilon profiles, for a
Total Audience of 42,573,372. Two segments in the AND block are marked DEPRECATED, one sized 0. July
spend $220,762.98; CPA $41.65 against a $25.00 eCPA goal. Budget & Goal rail: Peak Performance
"Enabled", Confidence "High".

**Recommendation (pending the segment check):** the audience is 5x wider than its Peak Performance pool
because of the OR'd third-party block. Pull the audience-segment report and split spend and conversions
across Peak Performance vs the third-party profiles. Whichever side is spending without converting is
the cut — most likely the 3P OR block, possibly Peak Performance itself. Separately, remove the two
DEPRECATED segments; one is sized 0 and can only be dead weight.

**Also worth passing to reporting:** the campaign ID appears nowhere in the UI, six campaigns share a
truncated name on the dashboard, and the audience list shows a campaign count with no campaign names, so
there is no way to get from the sheet's cgid to the right screen without guessing.

---

## 2. Join Found — aid 38652 · cgid 106676

**Campaign:** Found MNTN Matched Prospecting

$162,262.84 spend, CPA $508.66 against a $300 goal (attainment 0.589). Largest unclaimed Peak
Performance miss on the list. Carries the same shape as FICO: Peak Performance with an OR-additive
third-party include. 83 of the 148 unclaimed PP campaigns share it.

**Do:** open the audience-segment report, compare Peak Performance spend and conversions against the 3P
segments, recommend cutting whichever converts nothing.

---

## 3. Ancient Nutrition — aid 31455 · cgid 117662

**Campaign:** AN CTV Prospecting - Peak Performance

$149,658.14 spend, CPA $213.49 against a $140 goal (attainment 0.655). Peak Performance v2 with no
keyword layer, so it cannot reach the high-intent band at all — its ceiling is the Peak Performance
band. Different mechanism from FICO: here PP is not diluting a tighter audience, it **is** the whole
audience, and the tighter tier is unreachable.

**Do:** check whether the segment report shows Peak Performance spending without converting. If so the
recommendation is to add the keyword layer (which unlocks high intent) rather than to turn PP off, since
turning it off leaves nothing.

---

## 4. The Bouqs, eCommerce Unit — aid 32147 · cgid 119362

**Campaign:** CTV Subscriptions Prospecting

$67,622.13 spend, CPA $52.94 against a $20 goal (attainment 0.691) — the worst goal ratio of the three.
Peak Performance with an OR-additive third-party include.

**Do:** same segment-report check as Join Found.

---

## Cross-cutting, if one submission is wanted instead of four

Of the 148 unclaimed Peak Performance campaigns missing CPA on this list, **83 have a third-party
segment OR'd alongside Peak Performance** and **36 have no household score threshold set**, which makes
the intent score inert on those campaigns. Both are audience-configuration causes that the optimization
tool could detect and act on automatically, and neither is visible to the advertiser today.

## Other notes raised in the session, for the reporting team

- The audience-segment and keyword reports are not default reports; a client has to build one to see
  which segment their money went to. Mike: "it is a bit counterintuitive."
- Two reporting-page versions are live simultaneously; the newer one is less intuitive.
- A campaign showed delivery in a state that is not in its geo filter. Narrowing the date window made it
  disappear, so the audience was edited mid-flight and reporting spans both configurations.
- Several goal values are unrealistic (a $6 CPA goal on a foundation-repair advertiser). Goal
  recommendations are in progress; the suggestion raised was to show the advertiser their vertical's
  average as a reference point rather than only guardrails.
- Only site visits and conversions are attributed. A creative whose call to action is a phone number has
  performance that cannot be tracked, so the campaign is not a candidate.
