/*
Purpose:
This query compares the performance of Real-Time Conquesting (RTC) vs Non-RTC campaigns
for impressions served yesterday (24h window).
It calculates:
 - Impressions (ads served)
 - Visits (from verified impressions within 1 day)
 - Impression-to-Visit Rate (IVR)
 - Total Spend
 - Cost per Thousand Impressions (CPM)
 - Cost per Visit (CPV)

Results are grouped into two rows:
 1 = RTC, 0 = Non-RTC
*/

select
    -- Flag impressions that belong to Real-Time Conquesting (RTC)
    case when cil.model_params ~ 'realtime_conquest_score=10000' then 1 else 0 end as is_rtc,

    -- Total unique ad impressions shown
    count(distinct cil.impression_id) as impressions,

    -- Total unique visits generated from those impressions
    count(distinct v.impression_id) as visits,

    -- Impression-to-Visit Rate: visits ÷ impressions
    (count(distinct v.impression_id)::float / count(distinct cil.impression_id)) as ivr,

    -- Total ad spend (media + data + platform)
    sum(cil.media_spend + cil.data_spend + cil.platform_spend) as total_spend,

    -- Cost per 1,000 impressions (CPM)
    (sum(cil.media_spend + cil.data_spend + cil.platform_spend) / count(distinct cil.impression_id)) * 1000.0 as cpm,

    -- Cost per Visit (CPV). If no visits, return NULL to avoid division by zero.
    case
        when count(distinct v.impression_id) > 0
        then sum(cil.media_spend + cil.data_spend + cil.platform_spend) / count(distinct v.impression_id)
        else null
    end as cpv

from logdata.cost_impression_log cil
-- Link impressions to their campaigns
join public.campaigns c on c.campaign_id = cil.campaign_id

-- Link impressions to visits (only valid, verified visits within 1 day of impression)
left join summarydata.ui_visits v on cil.impression_id = v.impression_id
    and v.time >= (current_date - interval '2 days')
    and v.time < current_date
    and v.elapsed_time::interval <= interval '1 day'
    and v.from_verified_impression = true

where
  -- Only impressions that happened "yesterday" (24h midnight-to-midnight window)
  cil.time >= (current_date - interval '2 days')
  and cil.time < (current_date - interval '1 day')

  -- Exclude unlinked/bad impressions
  and cil.unlinked = false

  -- Only take campaigns that are funnel_level 1 and from channel 8
  and c.funnel_level = 1
  and c.channel_id = 8

-- Group results into RTC vs Non-RTC
group by is_rtc;