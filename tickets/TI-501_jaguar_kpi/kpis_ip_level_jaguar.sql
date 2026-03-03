/* ========================================================================
   IP-LEVEL KPI AGGREGATION (CORRECTED)

   Purpose: Get performance metrics aggregated by IP for joining to
   external IP classification table (is_model_added flag)

   Filters:
   - Vertical: 113002 (or all if NULL)
   - Funnel 1 campaigns only (objective_id = 1)
   - Only IPs with impressions in the time period
   - Only visits/conversions attributed to impressions in the time period

   Fixes applied:
   - Use host(ip) to strip /32 CIDR notation from INET columns
   - Use order_amt instead of order_amt_usd (which is null)

   Output: One row per IP with impressions, spend, verified_visits,
   conversions, order_value
   ======================================================================== */


/* ------------------------------------------------------------------------
   INPUT PARAMETERS
   ------------------------------------------------------------------------ */

drop table if exists input_params;
create temp table input_params as
select
    date '2025-11-30' as target_date

    -- Vertical filter options (uncomment ONE):
  , array[113002] as vertical_ids    -- specific verticals
    -- , null::int[] as vertical_ids  -- all verticals
;

-- Validation
-- select * from input_params;


/* ------------------------------------------------------------------------
   STEP 1: IDENTIFY QUALIFYING ADVERTISERS

   Get advertisers in the specified vertical(s)
   ------------------------------------------------------------------------ */

drop table if exists temp_advertisers;
create temp table temp_advertisers as
select distinct
    av.advertiser_id
  , av.vertical_id
  , av.vertical_name
from fpa.advertiser_verticals av
cross join input_params ip
where 1 = 1
    and av.type = 1

    -- Vertical filter (NULL = all verticals)
    and (
        ip.vertical_ids is null
        or av.vertical_id = any(ip.vertical_ids)
    )
;

-- Validation: Check advertiser count
-- select count(distinct advertiser_id) as advertiser_count, count(distinct vertical_id) as vertical_count from temp_advertisers;


/* ------------------------------------------------------------------------
   STEP 2: IDENTIFY QUALIFYING CAMPAIGN GROUPS (FUNNEL 1 ONLY)

   Get campaign groups with objective_id = 1 for qualifying advertisers
   ------------------------------------------------------------------------ */

drop table if exists temp_campaign_groups;
create temp table temp_campaign_groups as
select distinct
    ta.advertiser_id
  , ta.vertical_id
  , ta.vertical_name
  , cgr.campaign_group_id
from temp_advertisers ta
inner join campaign_groups_raw cgr
    on cgr.advertiser_id = ta.advertiser_id
where 1 = 1
    and cgr.objective_id = 1    -- Funnel 1 only
;

-- Validation: Check campaign group count
-- select count(distinct campaign_group_id) as campaign_group_count, count(distinct advertiser_id) as advertiser_count from temp_campaign_groups;


/* ------------------------------------------------------------------------
   STEP 3: MAP CAMPAIGN GROUPS TO CAMPAIGNS

   Get individual campaign_ids from qualifying campaign_groups
   ------------------------------------------------------------------------ */

drop table if exists temp_campaigns;
create temp table temp_campaigns as
select distinct
    tcg.advertiser_id
  , tcg.vertical_id
  , tcg.vertical_name
  , tcg.campaign_group_id
  , c.campaign_id
from temp_campaign_groups tcg
inner join public.campaigns c
    on c.campaign_group_id = tcg.campaign_group_id
where 1 = 1
    and (c.deleted is null or c.deleted = false)    -- exclude deleted campaigns
;

-- Validation: Check campaign count
-- select count(distinct campaign_id) as campaign_count, count(distinct campaign_group_id) as campaign_group_count from temp_campaigns;


/* ------------------------------------------------------------------------
   STEP 4: IMPRESSIONS + SPEND (cost_impression_log)

   Join on campaign_id
   Note: ip column is TEXT in this table (no conversion needed)
   ------------------------------------------------------------------------ */

drop table if exists temp_impressions;
create temp table temp_impressions as
select
    cil.ip

    -- Volume metrics
  , count(*) as impressions
  , sum(
        coalesce(cil.media_spend, 0)
      + coalesce(cil.data_spend, 0)
      + coalesce(cil.platform_spend, 0)
    ) as spend
from logdata.cost_impression_log cil
inner join temp_campaigns tc
    on tc.campaign_id = cil.campaign_id
cross join input_params ip
where 1 = 1
    and cil.time >= ip.target_date
    and cil.time < ip.target_date + interval '1 day'
    and cil.ip is not null
group by cil.ip
;

-- Validation: Check impression totals
-- select count(*) as unique_ips, sum(impressions) as total_impressions, sum(spend) as total_spend from temp_impressions;


/* ------------------------------------------------------------------------
   STEP 5: VERIFIED VISITS (summarydata.ui_visits)

   Filter by impression_time to only count visits attributed to
   impressions that occurred during our time period

   FIX: Use host(ip) to strip /32 CIDR notation from INET type
   ------------------------------------------------------------------------ */

drop table if exists temp_visits;
create temp table temp_visits as
select
    host(uv.ip) as ip    -- host() strips /32 suffix from INET

    -- Volume metrics
  , count(*) as verified_visits
from summarydata.ui_visits uv
inner join temp_campaigns tc
    on tc.campaign_id = uv.campaign_id
cross join input_params ip
where 1 = 1
    -- Filter by when the IMPRESSION happened, not when the visit happened
    and uv.impression_time >= ip.target_date
    and uv.impression_time < ip.target_date + interval '1 day'
    and uv.ip is not null
group by host(uv.ip)
;

-- Validation: Check visit totals
-- select count(*) as unique_ips, sum(verified_visits) as total_vv from temp_visits;


/* ------------------------------------------------------------------------
   STEP 6: CONVERSIONS + ORDER VALUE (summarydata.ui_conversions)

   Filter by impression_time to only count conversions attributed to
   impressions that occurred during our time period

   FIX: Use order_amt instead of order_amt_usd (which is null)
   Note: ip column is TEXT in this table (no conversion needed)
   ------------------------------------------------------------------------ */

drop table if exists temp_conversions;
create temp table temp_conversions as
select
    uc.ip

    -- Volume metrics
  , count(*) as conversions
  , sum(coalesce(uc.order_amt, 0)) as order_value    -- order_amt, not order_amt_usd
from summarydata.ui_conversions uc
inner join temp_campaigns tc
    on tc.campaign_id = uc.campaign_id
cross join input_params ip
where 1 = 1
    -- Filter by when the IMPRESSION happened, not when the conversion happened
    and uc.impression_time >= ip.target_date
    and uc.impression_time < ip.target_date + interval '1 day'
    and uc.ip is not null
group by uc.ip
;

-- Validation: Check conversion totals
-- select count(*) as unique_ips, sum(conversions) as total_conversions, sum(order_value) as total_order_value from temp_conversions;


/* ------------------------------------------------------------------------
   STEP 7: COMBINE INTO IP-LEVEL KPIs

   Start from impressions (inner join) - only IPs with impressions included
   Left join visits and conversions to capture all impression IPs
   ------------------------------------------------------------------------ */

drop table if exists temp_ip_kpis;
create temp table temp_ip_kpis as
select
    ti.ip

    -- Volume metrics
  , ti.impressions
  , ti.spend
  , coalesce(tv.verified_visits, 0) as verified_visits
  , coalesce(tc.conversions, 0) as conversions
  , coalesce(tc.order_value, 0) as order_value
from temp_impressions ti
left join temp_visits tv
    on tv.ip = ti.ip
left join temp_conversions tc
    on tc.ip = ti.ip
where 1 = 1
    and ti.impressions > 0    -- should always be true, but explicit
;

-- Validation: Check combined totals
-- select count(*) as total_ips, sum(impressions) as impressions, sum(spend) as spend, sum(verified_visits) as vv, sum(conversions) as conversions, sum(order_value) as order_value from temp_ip_kpis;


/* ========================================================================
   FINAL OUTPUT: IP-Level KPIs

   Export this result for joining to Spark ip_vert_df (is_model_added)
   ======================================================================== */

select
    ip

    -- Volume metrics
  , impressions
  , spend
  , verified_visits
  , conversions
  , order_value
from temp_ip_kpis
order by spend desc
;


/* ========================================================================
   AGGREGATE VALIDATION: Compare to expected totals

   Use this to validate against summary tables or known benchmarks
   ======================================================================== */

select
    -- Volume totals
    count(distinct ip) as households_reached
  , sum(impressions) as total_impressions
  , sum(spend) as total_spend
  , sum(verified_visits) as total_vv
  , sum(conversions) as total_conversions
  , sum(order_value) as total_order_value

    -- Calculated KPIs (at aggregate level)
  , case when sum(conversions) > 0 then sum(spend) / sum(conversions) else null end as cpa
  , case when sum(verified_visits) > 0 then sum(spend) / sum(verified_visits) else null end as cpv
  , case when sum(spend) > 0 then sum(order_value) / sum(spend) else null end as roas
  , case when sum(conversions) > 0 then sum(order_value) / sum(conversions) else null end as aov
  , case when sum(verified_visits) > 0 then sum(conversions)::numeric / sum(verified_visits) else null end as cvr
  , case when count(distinct ip) > 0 then sum(verified_visits)::numeric / count(distinct ip) else null end as vvr
  , case when sum(impressions) > 0 then sum(verified_visits)::numeric / sum(impressions) else null end as ivr
from temp_ip_kpis
;

commit;

-- Cleanup (uncomment when needed)
-- drop table if exists input_params;
-- drop table if exists temp_advertisers;
-- drop table if exists temp_campaign_groups;
-- drop table if exists temp_campaigns;
-- drop table if exists temp_impressions;
-- drop table if exists temp_visits;
-- drop table if exists temp_conversions;
-- drop table if exists temp_ip_kpis;