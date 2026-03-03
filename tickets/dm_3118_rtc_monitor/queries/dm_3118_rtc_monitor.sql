
-- advertiser_id	campaign_group_id	campaign_id
-- 32205	42173	195395

-- logdata
-- guid log page views
-- conversion log

--grab impressions main ctv campaign only
-- drop table if exists test.sno_campaign_rtc_impressions;
-- create table test.sno_campaign_rtc_impressions as

-- create a temp table first and then query the temp table you create
/*
select
    impression_id
, case when cil.model_params ~ 'realtime_conquest_score=10000' then 1 else 0 end as is_rtc -- only lives in cil
, count(1) as impressions
, sum(media_spend + data_spend + platform_spend) as spend
, sum(media_spend + data_spend + platform_spend) / count(1) * 1000.0 as cpm
from logdata.cost_impression_log cil
join public.campaigns c on c.campaign_id = cil.campaign_id
where 1 = 1
and cil.time = current_date::timestamp --(current_date - '1 day' interval)::timestamp
  --and cil.time < current_date::timestamp
and cil.unlinked is false
and c.funnel_level = 1 -- filters campaigns by the pure prospecting
and c.channel_id = 8 -- ctv ads
group by 1, 2
;

select
    *
from summarydata.ui_visits v
where 1 = 1
and v.time >= (current_date - '1 day' interval)::timestamp
  and v.time < current_date::timestamp
and impression_id is not nullhgh
and elapsed_time::interval <= '1 day'::interval
and from_verified_impressions is true -- verified visits and what we filter in the UI
limit 10
;
*/

/*
select column_name, data_type
from information_schema.columns
where table_schema = 'logdata' and table_name = 'cost_impression_log'
*/

select
    case when cil.model_params ~ 'realtime_conquest_score=10000' then 1 else 0 end as is_rtc,
    count(distinct cil.impression_id) as impressions,
    count(distinct v.impression_id) as visits,
    (count(distinct v.impression_id)::float / count(distinct cil.impression_id)) as ivr,
    (count(distinct v.impression_id)::float / count(distinct cil.impression_id)) * 1000 as visits_per_1k_impressions,
    sum(cil.media_spend + cil.data_spend + cil.platform_spend) as total_spend,
    (sum(cil.media_spend + cil.data_spend + cil.platform_spend) / count(distinct cil.impression_id)) * 1000.0 as cpm,
    case
        when count(distinct v.impression_id) > 0
        then sum(cil.media_spend + cil.data_spend + cil.platform_spend) / count(distinct v.impression_id)
        else null
    end as cpv
from logdata.cost_impression_log cil
join public.campaigns c on c.campaign_id = cil.campaign_id
left join summarydata.ui_visits v on cil.impression_id = v.impression_id
    and v.time >= (current_date - interval '2 days')
    and v.time < current_date
    and v.elapsed_time::interval <= interval '1 day'
    and v.from_verified_impression = true
where cil.time >= (current_date - interval '2 days')
  and cil.time < (current_date - interval '1 day')
  and cil.unlinked = false
  and c.funnel_level = 1
  and c.channel_id = 8
group by is_rtc;


with impressions as (
    select
        cil.impression_id,
        case when cil.model_params ~ 'realtime_conquest_score=10000' then 1 else 0 end as is_rtc,
        (cil.media_spend + cil.data_spend + cil.platform_spend) as spend
    from logdata.cost_impression_log cil
    join public.campaigns c on c.campaign_id = cil.campaign_id
    where cil.time >= now() - interval '24 hours' -- get the last 24 hours of data from the moment the query is ran
      and cil.time < now()
      and cil.unlinked is false --
      and c.funnel_level = 1 -- only pure prospecting campaigns
      and c.channel_id = 8 -- only ctv
),
visits as (
    select
        v.impression_id
    from summarydata.ui_visits v
    where v.time >= now() - interval '24 hours'
      and v.time < now()
      and v.impression_id is not null
      and v.elapsed_time::interval <= interval '1 day' -- time delta between the visit and its matched impression is less than 24 hours
      and v.from_verified_impression = true -- is considered a verified visit based on in-house attribution
)
select
    i.is_rtc,
    count(distinct i.impression_id) as impressions,
    count(distinct v.impression_id) as visits,
    (count(distinct v.impression_id)::float / count(distinct i.impression_id)) as ivr,
    sum(i.spend) as total_spend,
    (sum(i.spend) / count(distinct i.impression_id)) * 1000.0 as cpm
from impressions i
left join visits v on i.impression_id = v.impression_id
group by i.is_rtc;


--

-- logdata.cost_impression_logs --impressions
-- summarydata.ui_visits -- visits, gets a combo of both // summarydata.visits -- 1st touch / last touch -- visits table is a combination of both, ui is only do we have the "first touch" // summarydata.last_tvtouch_visits
-- sumarydata.ui_conversions -- conversions



-- have to join on impression_id

-- guid is session-specific and is page-specific
-- summarydata.visits is supposed to be "pre-cleansed", verified visits is the internal secrets

-- in the visit record, there is a field for impression time (last known / associated impression) to that verified visit. time - impression_time, making sure that's within a 24 hour period
-- days_elapsed

-- select *
-- from tpa.membership_updates_logs -- tell you whether it belongs to the audience or not on a given day
-- where
--
-- -- has an ip it has