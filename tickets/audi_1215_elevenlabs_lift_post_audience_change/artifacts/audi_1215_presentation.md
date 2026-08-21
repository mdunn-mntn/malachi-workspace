# AUDI-1215 ElevenLabs Response Deck: source content

**Audience:** ElevenLabs (their DS team + growth leadership), after internal review.
**Power Line:** Your CTV causes visits. Measured the way you asked.
**Structure:** Bookend open on their own deck's question 2 (ghost-ads holdout study) → randomized results → why dashboards look different → conversions are a power problem → frequency lever → three next steps → Power Line close.

---

## 1. Title
ElevenLabs CTV Incrementality: The Study You Asked For
MNTN Data Science & Measurement · August 2026

## 2. Open (their question, our answer)
June, your measurement review, question 2: "Can we run a MNTN-side conversion-lift study (ghost ads / PSA holdout) to triangulate with our geo results?"
We ran it. It has been running the whole time.

## 3. How it works
Ghost bidding: 10% of your audience is always held out at random. The bidder values every auction identically for both groups and logs the bid it would have placed for held-out households, then never serves them. Comparing the two groups isolates what the ads caused. Clinical-trial standard, no media spent on the control, conservative by design (7-day visit windows, coverage and statistical gates).
One-pager attached for your team.

## 4. Hero
+16.5%
Visit lift versus the holdout since your July changes. p < 0.000003.
(Anchor above: "Since July 11, ads reached 6.6M households; 672K were held out.")

## 5. Lift held through the changes (chart: prepost bars)
Visits: +11.1% before the changes, +16.5% after. Significant in both periods.
Conversions: +11% before, +35% after. Wide intervals.

## 6. Day by day (chart: daily lift with markers)
Lift dipped while the changes settled, then climbed. Every change date is marked; the 7/1-7/10 transition window is excluded from both averages.

## 7. Why your dashboards look worse
The new precision audience visits your site 6x less on its own. That was the point: less spend on people who were coming anyway.
So credited (attributed) visits fell, while the share of visits actually caused by ads held and rose.
Attribution counts touches. Incrementality counts causes. The campaign trades the first for the second.

## 8. Your null and our lift are the same finding
At a 0.06% B2B conversion rate, this spend cannot resolve even a 5% conversion lift. Their largest country read still landed at ~0% (p=0.81). Ours is directionally up, not yet significant. All three are the same statement.
Detecting a 5% conversion lift: about $2M/month. The same lift on visits: $36K.
A power problem, not a performance problem. Conversions lag; visits are the readable KPI.

## 9. The next lever: frequency (chart: frequency lift)
Households reached 2-10 times lift +18-20%. Reached 11+ times: -18%.
70% of your reached households see 3 or fewer impressions while the 11+ tail absorbs spend at negative lift.
Frequency targets move spend from the red bar to the navy bars.

## 10. Three next steps
1. Resume the campaign. The holdout only measures while it runs: no spend, no experiment, and the conversion answer never arrives.
2. Set frequency targets on the campaign (the playbook exists; feasibility confirmed).
3. Read visits as the primary KPI and enroll in the incrementality measurement beta so this view is always on.

## 11. Close
Your CTV causes visits. Measured the way you asked.
+11.1% before your changes, +16.5% after, each significant on its own, on a randomized always-on holdout.

## Appendix
A1. What changed, when (6/30 audience swap to precision segments; conversion window to 43 days; visitor/converter blocks 30 to 90 days; custom segments 7/16; further audience adds 7/24 and 7/29). Transition window 7/1-7/10 excluded from pre/post averages.
A2. Method: intent-to-treat on the randomized ghost-bid holdout, one count per household at first bid, outcomes within 7 days, campaign group 122748, pre = 6/23-6/30, post = 7/11-8/13. Holdout share 9.2-9.5% in every period.
A3. Full numbers: pre visits 0.916% reached vs 0.824% holdout (+11.14%, p=1.7e-10); post 0.158% vs 0.136% (+16.46%, p=2.6e-06); conversions pre +11.25% (p=0.37), post +34.65% (p=0.15).
