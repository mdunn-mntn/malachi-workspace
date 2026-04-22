# War-room channel (C0ATVHK2EDV) — 60-day scrape, 2026-04-22

Total messages: 62; substantive: 35

[2026-04-21 07:44] **richard**: <!channel> revenue has been slowing down at a pretty alarming rate. There is strong consensus that we are experiencing sales and support issues, but I am not convinced that product/eng is in the clear. I'm going to share a summary of three or four parallel conversations I was having yesterday w/ Ray, Johnny, and various other ppl:

*Summary of last night's conversations:*
Conversions and order value metrics dropped materially in late 2025. Top line metrics (CPV, CPA, visits) look healthy. Median conversions by CGID bottomed out in September 2025. Additionally, Ray has shared data with me that demonstrates a *10% drop in orders with any amount, starting around Dec 2025*.

We always assumed our CPMs were the leading cause for performance loss and churn. But we corrected our CPMs for all new customers that registered after March 10th. Despite that, Johnny has data that proves that advertisers who launched from 3/11 - 3/25 are seeing healthy CPV, CPA, but unusually low ROAS.

All signs point to something changing in the latter part of 2025 that hurt performance, and we conveniently blamed CPMs when in reality a bigger issue was at play. Some of the leading theories are:
1. We launched Conversion Pixel Opt Out in November 2025
2. The customer base is tilting toward SMB or brand advertisers. Smaller sites have lower raw conversions and smaller order values
*What I want this group to do:*
I want to answer a series of questions that can help us contextualize what's really going on. I'll send in a follow up message

[2026-04-21 07:50] **richard**: <!channel>

How did our customer behaviors change over time? (dating back at least 6 months)
• Tracking pixel opt out rate
• Conversion pixel opt out rate
• Pixel installation rate
• Campaign types launched (prospecting/RT)
• Budget sizes
• Audience types used
How did our customer profiles change?
• Size of customers
• Industry
• E-com yes/no
What are our customers saying?
• Sentiment over past 18 months (or however far back we can go)
• Themes in customer conversations 
• Special callout: How many customers have raised CPM concerns?
Churn (MoM over past 18 months)
• Logo churn
• Net spend churn
• Analysis of customer churn behavior for advertisers created after March 11th
<@U044VUTURJN> I want you to project manage this and assign owners.

<@U0KFLLV1R> will be technical lead, and <@U0A25BW26CA> to assign a TPM to serve as co-lead for this initiative. I need all findings consolidated in a single doc, with an up-to-date executive summary at all times. This needs to be treated with extreme urgency.

<@U09NMHWE2AF> please add additional analysts to this channel if you need help doing churn analysis.

[2026-04-21 07:50] **richard**: Paulo and I don't have time to war room this with y'all. You guys can meet whenever you want. We'll be checking in regularly and answering questions as they come up

[2026-04-21 07:51] **richard**: Lastly, <@U09NMHWE2AF> I believe you were working w/ Paulo on a churn analysis, so perhaps that work can be ported into this program

[2026-04-21 08:04] **Johnny**: <@U044VUTURJN> could we schedule an initial meeting today to share the info pieces from everyone?

[2026-04-21 08:09] **Alex Bloore**: Thanks for the background. Pulling <@U09DF4RLT97> for TPM support.

[2026-04-21 08:13] **Alex Bloore**: I need to get a good understanding of what we've observed so far, and then I would add that I need to understand major product changes in targeting, buying, and measurement in the measurement period we're discussing (November 2025 at least).

I think there's a case to make that we need to look longer than 6 months, because we need to understand seasonality impacts.

[2026-04-21 08:17] **richard**: I think we should have a default lookback of 18 months on everything we pull. I mentioned 6 months for customer behavior because the primary focus was on pixel changes, which had the most material changes in the past 6 months. But maybe best to keep it simple and stick to 18 months for all exercises

[2026-04-21 08:25] **Jason Huertas**: <@U0KFLLV1R> <@U0695RKH939> looking at cals, can you both flex 9am pt or 930am pt this morning to meet on this?

  > [04-21 08:30] ray: 930am is not great for me. can we do like 910am right after mission control?

[2026-04-21 08:26] **Johnny**: 9:30am is fine

[2026-04-21 08:30] **ray**: 930am is not great for me. can we do like 910am right after mission control?

[2026-04-21 08:32] **Jason Huertas**: Done

  > [04-21 08:48] Johnny: i will add <@UK4MAJD7Y> to the meeting, he's more familiar with the ad buying side changes

  > [04-21 08:53] Kaila: thanks Jason for scheduling!

  > [04-21 08:54] Benny: Add meeeeee please :smiley: ?

  > [04-21 08:54] Johnny: i did

  > [04-21 08:58] Kaila: i have a conflict with the time. <@U09DF4RLT97> can you post AIs here? lmk how i can help

[2026-04-21 09:51] **Dave**: <!here> From my data warehouse egg hunt:
Conversion volume is stable to up, not down — 165M → 188M rows from Sep to Jan, and tracked customer order value is up ~24% over the same window ($9.5B → $11.8B). Distinct advertisers sending order amounts also grew (1,988 → 2,055). If pixel had broken anything, all three would fall.
What's actually happening is a clear customer-mix shift:
• 68% of our new advertisers are lead-gen (sending order IDs but no amounts) vs 50% of veterans. That's a structural difference — lead-gen customers don't have order amounts to send.
• Some customers are actively opting out of conversion tracking — they don't want it.
• Average order value per conversion is down (~$600 → ~$430) — likely a mix of smaller customers and softer macro.
*All three of those actually support MNTN Express being the right move — we're meeting customers where they are.*
_Worth noting per <@U529MLZTL>_: the Conversion Blocking Keyword filter wasn't ported during the GCP migration and was re-added to TRPX in mid/late Oct, so Sep–Oct counts were likely slightly inflated by spam/test conversions leaking through. Using Nov as the true baseline, conversions are still up ~11% and tracked value up ~33% by Jan — so the growth story holds even after correcting for that.

  > [04-21 09:54] Jason Huertas: <@U0KFLLV1R> FYI on my hunch around order amt = 0 and not using orderids. I’ve seen lead gen customers pass thru the order ids from pixel if this number tracks to what you’re seeing too

  > [04-21 09:56] Dave: yeah a lot of customers who don't use ROAS will just map Order ID only

  > [04-21 09:57] ray: <@U09DF4RLT97> this is what i was trying to account for with the Order ID Cardinality, recognizing that it may still be coming through in limited volume relative total number of conversions, but otherwise something we dedupe against.

  > [04-21 10:04] Dave: <@U0KFLLV1R> in Jan 2026, 67.7% of new advertisers (&lt;90d) sent only Order IDs without amounts (lead-gen pattern), vs 49.9% of veterans. So the Order ID-only cohort is materially overrepresented in our newer signups - exactly the dynamic you guys are describing.

[2026-04-21 09:52] **ray**: Action Items
• Will: data by spend tier over time
• Will: data by vertical over time (ecommerce or not - current distribution is 50/50)
• Ray: Revenue per customer per month over last 18 months
• Performance metric for AIDs considering Conversion Pixels/Order IDs
    ◦ Is overall performance getting worse or not when considering something like CPV
    ◦ Confirm whether performance is dropping for AIDs that do have conversion data
    ◦ Differentiate between customers who do have Order IDs (this is the dupe problem) and customers who don't have order amounts (this is the ROAS problem)
• Check 3rd party measurement relevance
• Will: 3-6 month LTV - is it declining
• Churned AIDs
    ◦ Is it the dupe order ID thing
    ◦ spend tier
    ◦ mix of business types
    ◦ engagement (retention/nurturing) is lagging - we let people walk out the door
        ▪︎ aggregate sentiment analysis
        ▪︎ gong calls
        ▪︎ lack of data is the driver
    ◦ Support Tier
• Jason: we've been looking at observational metrics - what are the things that may have caused this to happen (prod/eng impacts)
    ◦ what got shipped over time that could have caused this

  > [04-21 10:58] ray: <@U09DF4RLT97> <@U0A25BW26CA> <@U09NMHWE2AF> <@U03026FP6CR> <@U0695RKH939> I moved the action items into this GDoc: <https://docs.google.com/document/d/1zxLvBjd1EldNyKE1DHTGpbLHQIxFv9DPK11UjQ_eWvE/edit?tab=t.0>

Not too sure where to go from here but i've marked the items I'll be working on with a `[P1]` flag for now.

We also have EJ on hand to help take some tasks as well.

  > [04-21 11:17] Jason Huertas: <@U044VUTURJN> can you help here with the PM work and then setting up a war room huddle cadence. My first one of these, I believe there’s a playbook on these war rooms so we can stay close and progress to the deliverable richard is expecting

  > [04-21 11:35] Kaila: my mornings are cooked availability wise. going to see if we can do a daily 30 min in the early afternoon

  > [04-21 11:51] ray: <@U0A25BW26CA> is this what you were thinking for
&gt; Revenue per customer per month over last 18 months
<https://docs.google.com/spreadsheets/d/1vpvh0AGwULVDYt6iGEneqcMbPlEHa5j41NsMbAnNDdU/edit?gid=1539412638#gid=1539412638>

  > [04-21 11:54] Alex Bloore: Yup - So there's a long running downward trend on rev / AID.

The April dip is just due to an incomplete month, right? Meaning it isn't forecasted.

  > [04-21 11:55] ray: correct, not forecasted for April

  > [04-21 11:57] Alex Bloore: this should (almost by definition) correlate with the analysis I asked for from <@U09NMHWE2AF> to show the change in distribution of spend tiers.

  > [04-21 17:08] ray: &gt;  Differentiate between customers who do have Order IDs (this is the dupe problem) and customers who don't have order amounts (this is the ROAS problem) 
I made `ray.aid_order_info` in CoreDW.
• it's a view so I can add to it or change some definitions
• currently backed by data (static) for customers that launched from Oct 2024 to now
• `has_order_amt` is whether or not they have order_amts coming in at all. There's a pretty big split
```select has_order_amt, count(1) from ray.aid_order_info group by 1;
 has_order_amt | count 
---------------+-------
 t             |  1308
 f             |  1716
(2 rows)```
• `has_order_ids` is if the "order ID cardinality" is &gt;= 0.5 based on `distinct order IDs / total raw conversions`. I picked 0.5 almost arbitrarily. If it's less then we have a lot to gain from the dedupe no longer deuping 
• `has_conversions` is if they had a raw conversion at all
*sample record:*
``` select * from ray.aid_order_info limit 1;
-[ RECORD 1 ]--------+-----------------------
advertiser_id        | 50646
start_date           | 2026-03-05
raw_conversions      | 32750
order_amts           | 0
order_ids            | 24959
order_id_cardinality | 0.76210687022900763359
has_order_amt        | f
has_unique_order_ids | t
has_conversions      | t```
So i think we can then look at performance using this flag against the advertisers to see if performance is dropping. Groupings might be something like
• ROAS: advertisers where `has_order_amts = true`
• CPV: advertisers where `has_conversions = false`
• etc

  > [04-21 17:14] ray: quick table showing the makeup over time

  > [04-22 07:42] Alex Bloore: So, we're seeing a gradual decline in use of conversions, but not drastic. The real decline seems to be on the use of Order Amount. Since we're heavily focused on ROAS in all our reporting, that's going to be a problem.

  > [04-22 07:47] Johnny: yes, my writing in the canvas is pointing to the same direction but in a diff angle. to extend that, with our business strategy and customer base shifting (towards SMB and MidMkt), is the existing performance standard (using CPV/CPA/ROAS) still proper and correctly reflecting the performance for those customers?

[2026-04-21 09:52] **Daniella Kubiak**: Can we post all the call links in this chat moving forward, or invite everyone here so we can be caught up please?

[2026-04-21 09:54] **Jason Huertas**: Will share recording and notes shortly

  > [04-22 07:56] Jason Huertas: <@U044VUTURJN> here is recording link from yesterday’s call for our notes:

<https://mountain.zoom.us/rec/share/7_tft7DC_6k7iMZeIIdTzyIher7AjpRhXK6VxjbzZMxLzbKyhque2NvSlGRwQEMf.bw1qjxAt8kBbr2cQ?startTime=1776787917000>
Passcode: 6TUj2A+E

[2026-04-21 10:13] **Dave**: "how-am-i-alive-what-is-life-i-wanna-die" you got that right. :daniella-clap::daniella-clap::daniella-clap:

[2026-04-21 10:51] **ray**: <@U0695RKH939> can you link (either dashboards or images) what you were sharing into this Canvas please: <https://mntn.enterprise.slack.com/docs/T0EAULQ10/F0AUCJM4NBC>

[2026-04-21 11:39] **Johnny**: <@U09NMHWE2AF> for a separated question - how do you identify SMB vs largeCorp customers? is it from the 'CS segment' data? like a query below? thanks! cc <@U03026FP6CR>
```select
    a.advertiser_id as __value,
    concat(a.advertiser_id, ' : ', a.company_name) as __text
from
    advertisers a join salesforce.accounts s using (advertiser_id)
where
    s.cs_segment__c in ($CS_SEG)
    and ( $AID_LIVE_ONLY = 0 or status_id = 3 )
order by 1;```

  > [04-21 11:40] Will Cavey: I've been using revenue cuts, anything under 25k / month when active is SMB

  > [04-21 11:42] Johnny: Oh

  > [04-21 11:43] Johnny: how to define revenue? and how to define largeCorp?
maybe you have an AID list for SMB and largeCorp to share with us?

  > [04-21 11:51] Will Cavey: I can grab one

[2026-04-21 11:40] **Kaila**: <!here> - daily war room call scheduled starting tomorrow at 1pm PT. everyone in this channel is invited

  > [04-21 14:02] Kaila: *meeting time update:* tomorrow’s war room is now at 1:30pm PT but rest of the series will continue to be at 1pm PT

[2026-04-21 13:04] **Dako Bogdanov**: <@U0ACFCGHT52>

[2026-04-21 14:02] **Kaila**: *meeting time update:* tomorrow’s war room is now at 1:30pm PT but rest of the series will continue to be at 1pm PT

[2026-04-21 18:59] **richard**: Hey all, how's everything going? Do we have a doc setup yet? I'm signing off for the day but LMK if there's anything I can help with in the morning

  > [04-21 19:06] Alex Bloore: The team has been adding updates in the Canvas for this channel. There are a few outstanding analyses, but early hypotheses seem to point to lower value customers taking up a larger share of our AIDs and driving down average revenue per customer. 

We will consolidate into a doc over the next 24 hours with more findings and insights. 
F0AUCJM4NBC

  > [04-21 19:32] ray: Action item doc: <https://docs.google.com/document/d/1zxLvBjd1EldNyKE1DHTGpbLHQIxFv9DPK11UjQ_eWvE/edit?tab=t.0>

[2026-04-22 07:46] **Alex Bloore**: <@U09NMHWE2AF> I had a thought - can we quantify what I'm thinking of as the velocity of budget contraction over time? In other words - we're seeing average spend/AID trending down, and IIRC from your analysis, we're also seeing the share of customers reducing budget to &lt;90% of their beginning amount increase.

I'd like to understand if that budget contraction is happening faster over time. It's probably useful to understand in aggregate and on a cohort basis.

If we can figure out a good way to represent this, it can become a health metric for the business.

[2026-04-22 07:54] **Jason Huertas**: <@U04HYGQSKQA> <@U06M0E8KWF3> - when exactly in Nov did we ship pixel conversion opt out. Also pasting Dave’s analysis from the DM here for everyone to see. Looks like conversion pixel opt out is growing since november but should not be materially impacting our analysis here.

• 112 advertisers total opted out — 0.4% of active accounts
• Growing ~20× since launch: 3 (Nov) → 60 (April)
• 50× concentrated in new accounts (1.95% vs 0.04% for veterans)
• Opted-out = 0% coverage by design — every one drags the numerator down
• Today: ~1–2 pts of Ray’s drop. In two quarters at current growth: 5–10 pts and climbing.
• Fix: exclude `conv_pixel_opt_out = TRUE` from the denominator. We shouldn’t be factoring these numbers are for order amount values since these customers don’t want them and won’t be using ROAS

[2026-04-22 07:54] **Will Cavey**: I was working on this - basically yes at least for new customers, our 3mo and 6 month CLV from new acqusitions are on the decline

  > [04-22 08:26] Alex Bloore: Each point on the line chart is a monthly cohort, correct? Lined up with the months on the bar chart?

  > [04-22 08:30] Alex Bloore: A couple of comments:
• We have slightly declining # of new AIDs per month AND they are spending progressively less on average. That's not a good recipe. 
• So we're seeing the cohorts 6-mo LTV decline. I guess what I'm also curious about is if MoM budget reductions are accelerating. 

  > [04-22 10:05] Will Cavey: yes each is a cohort - overall rates of decleration are stable

but the number of AIDs decreasing each month in both count and as a % is growing - so its a volume problem not a magnitude problem

  > [04-22 10:15] Alex Bloore: got it.

[2026-04-22 07:56] **Jason Huertas**: <@U044VUTURJN> here is recording link from yesterday’s call for our notes:

<https://mountain.zoom.us/rec/share/7_tft7DC_6k7iMZeIIdTzyIher7AjpRhXK6VxjbzZMxLzbKyhque2NvSlGRwQEMf.bw1qjxAt8kBbr2cQ?startTime=1776787917000>
Passcode: 6TUj2A+E

[2026-04-22 08:30] **Alex Bloore**: A couple of comments:
• We have slightly declining # of new AIDs per month AND they are spending progressively less on average. That's not a good recipe. 
• So we're seeing the cohorts 6-mo LTV decline. I guess what I'm also curious about is if MoM budget reductions are accelerating. 

[2026-04-22 08:32] **Alex Bloore**: I'll also add that based on what Jason shared (via Dave) - I don't feel like the hypothesis about conversion pixel opt-out driving this is validated. It's too small of a share be the main driver for the trends we are seeing (and those trends aren't new).

[2026-04-22 08:35] **Alex Bloore**: I need someone to provide an analysis of share of audience types (MM, 3P, etc.) over time. There's lots of ways to slice this, but let's start by seeing if there's any shifts in the distribution of audience types being used by customers in our measurement period. Who can take that?

[2026-04-22 08:42] **Jason Huertas**: If I get a list of AIDs of customers, I can take that on and get the campaign audience definitions pulled. <@U09NMHWE2AF> can you share list of AIDs?

  > [04-22 10:57] Will Cavey: <@U09DF4RLT97> this work? its all new AIDs w/ spend after 2025

<https://docs.google.com/spreadsheets/d/1ghdvbDla2uvG5iAaGBPjVI3Y3_N7FUiY_Io5bSAaIVs/edit?usp=sharing>

  > [04-22 12:04] Jason Huertas: Plus <@U090LRULRMG> helping us track down the audience comp shifts <@U0A25BW26CA> <@U09NMHWE2AF>

[2026-04-22 10:05] **Will Cavey**: yes each is a cohort - overall rates of decleration are stable

but the number of AIDs decreasing each month in both count and as a % is growing - so its a volume problem not a magnitude problem

[2026-04-22 10:36] **richard**: <!here> I want to make sure its clear that the questions I posed and the format I requested at the top of the channel are still going to be needed. It's because I want to eliminate a ton of the grapevine comms around the types of customers we're closing, etc. The analysis you guys have done so far is VERY helpful FWIW, I'm already using some of the learnings in Jedi mtgs.

But just want to make sure we still deliver the doc format + exec summary as requested at the end

  > [04-22 10:39] Kaila: <@U09DF4RLT97> <@U0A25BW26CA> definitely want to talk through creation/formatting of the document during the call today

[2026-04-22 12:04] **Jason Huertas**: Plus <@U090LRULRMG> helping us track down the audience comp shifts <@U0A25BW26CA> <@U09NMHWE2AF>

[2026-04-22 12:52] **Mike Dolt**: Do we know when this drop in performance has started? Was it just beginning of November?

  > [04-22 13:01] Alex Bloore: It's been steadily declining - but seems to have gotten a bit more pronounced in Oct/Nov.

  > [04-22 13:03] Mike Dolt: Peak Performance was added in beginning of October. 

  > [04-22 13:04] Mike Dolt: It doesn't explain why just the conversions and ROAS went down though

