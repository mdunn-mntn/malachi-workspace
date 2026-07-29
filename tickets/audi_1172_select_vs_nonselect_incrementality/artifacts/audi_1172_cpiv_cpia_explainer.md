# How MNTN Computes CPIV and CPIA — AUDI-1172

**Bottom line:** Select produces extra website visits about 1.6x cheaper, and extra conversions about 3x cheaper, than Performance TV. Below is what those two numbers mean and how we got them.

Two products are compared, both running on connected TV. **Select** is MNTN Select, the media marketplace where buyers choose premium programs and deals. **Non-Select** is **Performance TV (PTV)**, MNTN's automated performance product. *(Prospecting = campaigns aimed at reaching brand-new potential customers, not re-reaching people who already engaged.)* The figures pool the prospecting advertisers on each product that had usable experiment data.

## 1. What the two numbers answer

- **CPIV — Cost Per Incremental Visit.** For every *extra* website visit the advertising actually caused, how many dollars did we spend?
- **CPIA — Cost Per Incremental Acquisition** (an acquisition is a conversion: a purchase or sign-up). Same idea, per extra conversion.

"Incremental" just means **extra**: not total visits, only the ones that happened *because of* the ad. Lower is better — each extra outcome cost less to produce.

## 2. The headline result

Prospecting campaigns, June 22 – July 27, 2026:

| | Select | Performance TV | Verdict |
|---|---|---|---|
| **CPIV** (cost per extra visit) | **$5.23** | **$8.23** | Select ~1.6x cheaper |
| **CPIA** (cost per extra conversion) | **$84** | **$256** | Select ~3.0x cheaper |

One thing to flag up front: an earlier, rougher cut made Select look ~5x cheaper on visits. That 5x was a false result caused by a visit-counting problem (section 3), not reality. The correct figure is ~1.6x. Select is still cheaper, just not 5x.

## 3. Why it is not simply "spend ÷ visits"

Two things break the simple spend-divided-by-visits math.

**(a) There are two different definitions of a "visit."** Our lift experiment and the client's dashboard do not count visits the same way.

- The **experiment** counts a visit when a small piece of tracking code on the advertiser's website registers a person, within 7 days of us trying to show that person an ad. It is a fast, clean signal, ideal for *measuring lift*, but it deliberately undercounts — and it undercounts **much more for Performance TV** (it captures only about a third of the visits the client sees) than for Select (which it captures almost fully).
- The **MNTN Reporting dashboard** counts a **Verified Visit**: a visit credited to the ad when someone who saw or clicked it later shows up on the site. This is the number the client sees.

So the experiment's raw visit count is much smaller than what the advertiser sees in Reporting, and smaller by a *different amount* for each product.

**(b) The experiment never recorded spend.** It tracks visits, not dollars. So we pulled the actual dollars spent, from our billing records, for those exact prospecting campaigns.

Put those together and the trap is clear: divide real, full spending by the experiment's undercounted visits and you get a cost that is far too high — and worst for Performance TV, whose undercount is largest. That is what produced the misleading 5x.

## 4. How we find the "extra," and how we fix the mismatch

**Finding the extra part: the ghost-bid holdout.** For prospecting, about **10% of users are secretly held out**. MNTN works out the bid (a bid = our offer to show one ad to one person) it *would* have made for them, but shows no ad. (We call it a *ghost* bid because we compute the bid we would have made but never actually run it.) We then compare the group that saw ads (**treated**) against the held-out group. The gap in their visit rates is the **lift**: the slice of visits the advertising caused. Picture two near-identical store aisles where only one gets a promotional sign — the extra foot traffic in the signed aisle is what the sign caused.

This gives us a **relative** lift, a percentage: "treated visited X% more than the held-out group."

**The fix: use the percentage, not the raw count.** A percentage stays the same no matter how you count the visits underneath it. "22% more" is 22% more either way. So we take the percentage lift from the experiment and apply it to the **Verified Visits that Reporting actually reports**, which expresses the extra visits in the same currency the advertiser sees:

> **Incremental Verified Visits = Reporting Verified Visits × lift ÷ (1 + lift)**
>
> **CPIV = spend ÷ incremental Verified Visits**  (CPIA is the same, using conversions.)

Why divide by (1 + lift)? Verified Visits include both the organic visits and the extra ones; dividing by (1 + lift) pulls out just the extra slice. *This is the same method MNTN uses for its customer-facing incrementality dashboard — confirmed by Matt Brorby, who built the lift pipeline.*

*Technical note for reconciliation: the lift plugged in here is the volume-weighted pooled lift (Select +102.7%, Performance TV +14.5%; bigger campaigns count more), the right choice for a total-cost metric. It differs from the +22% / +4.3% average-campaign lift in the lift report, which weights every campaign equally.*

## 5. The one honest caveat

The experiment measures lift over a **7-day window** (how long after the ad we keep counting visits); Reporting counts over a longer, full window. We measure the percentage in the 7-day window and apply it to the full-window Verified Visits, assuming the extra-visit percentage is the same in both. This is unavoidable: the held-out users are never shown an ad, so they generate no Reporting visits we could measure directly — the experiment is the only place the "what if there had been no ad" comparison exists.

## 6. Bottom line

Measured correctly, on the same basis the client sees in Reporting: **Select costs ~$5.23 per extra visit vs ~$8.23 for Performance TV (~1.6x cheaper), and ~$84 vs ~$256 per extra conversion (~3.0x cheaper).** Select delivers extra outcomes more efficiently.

Do not confuse this with the ~5x relative visit-*lift* finding (+22% vs +4.3%) in the lift report — that is a real, separate result and is unchanged. And the raw-experiment ~5x *cost* ratio was inflated by the visit undercount, so ignore it. The cost figures above are the ones to trust and to share.
