-- NTB (New to Brand) Email Analysis
-- Current window: Nov 16, 2025 → Nov 30, 2025 (2 weeks)
-- Lookback window: Nov 30, 2024 → Nov 16, 2025 (historical)

-- Step 1: Get all advertisers with conversions in full year, flag those with 1+ year history
drop table if exists temp_advertiser_history;
create temp table temp_advertiser_history as
select
    cl.advertiser_id
  , min(cl.time) as min_conversion_date
  , max(cl.time) as max_conversion_date
  , count(*) as total_conversions
  , case
        when min(cl.time) <= '2024-12-30 00:00:00' then true  -- conversions within first week = 1+ year history
        else false
    end as has_year_history
from logdata.conversion_log cl
where 1 = 1
    and cl.time >= '2024-11-30 00:00:00'
    and cl.time < '2025-11-30 00:00:00'
group by
    cl.advertiser_id
;

-- Validation: Check advertiser counts by history flag
-- select has_year_history, count(*) as advertiser_count from temp_advertiser_history group by has_year_history;

-- Step 2: Extract emails from current window (Nov 16-30, 2025)
drop table if exists temp_current_window_raw;
create temp table temp_current_window_raw as
with classified as (
    select
        cl.advertiser_id
      , cl.email as raw_email
      , cl.query
      , case
            when cl.query like '{%' and cl.query not like '%{%22%' then 'json'
            when cl.query like '%=%' then 'querystring'
            else 'other'
        end as query_format
    from logdata.conversion_log cl
    where 1 = 1
        and cl.time >= '2025-11-16 00:00:00'
        and cl.time < '2025-11-30 00:00:00'
)
select
    c.advertiser_id
  , case
        when c.raw_email = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' then null
        when c.raw_email = '' then null
        else c.raw_email
    end as email
  , case
        when c.query_format = 'json'
            and (c.query::json->>'email_data') is not null
            and (c.query::json->>'email_data') != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
            and (c.query::json->>'email_data') != ''
            then (c.query::json->>'email_data')
        when c.query_format = 'querystring'
            and c.query like '%email_data=%'
            and split_part(split_part(c.query, 'email_data=', 2), '&', 1) != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
            and split_part(split_part(c.query, 'email_data=', 2), '&', 1) != ''
            then split_part(split_part(c.query, 'email_data=', 2), '&', 1)
    end as email_data
from classified c
;

-- Validation: Check current window counts
-- select count(*) as total_rows, count(email) as email_count, count(email_data) as email_data_count from temp_current_window_raw;

-- Step 3: Calculate email prevalence and filter qualifying advertisers (90%+ email prevalence + 1 year history)
drop table if exists temp_qualifying_advertisers;
create temp table temp_qualifying_advertisers as
select
    tah.advertiser_id
  , tah.min_conversion_date
  , tah.max_conversion_date
  , tah.total_conversions
  , tah.has_year_history
  , tcwr.current_conversions
  , tcwr.email_count
  , tcwr.email_data_count
  , tcwr.distinct_emails
  , tcwr.distinct_email_data
  , tcwr.distinct_combined
  , tcwr.email_prev
  , tcwr.email_data_prev
  , tcwr.combined_prev
from temp_advertiser_history tah
    inner join (
        select
            tcw.advertiser_id
          , count(*) as current_conversions
          , count(tcw.email) as email_count
          , count(tcw.email_data) as email_data_count
          , count(distinct tcw.email) as distinct_emails
          , count(distinct tcw.email_data) as distinct_email_data
          , count(distinct coalesce(tcw.email, tcw.email_data)) as distinct_combined
          , count(tcw.email)::float / nullif(count(*), 0) as email_prev
          , count(tcw.email_data)::float / nullif(count(*), 0) as email_data_prev
          , count(case when tcw.email is not null or tcw.email_data is not null then 1 end)::float / nullif(count(*), 0) as combined_prev
        from temp_current_window_raw tcw
        group by
            tcw.advertiser_id
    ) tcwr
        on tah.advertiser_id = tcwr.advertiser_id
where 1 = 1
    and tah.has_year_history = true                           -- must have 1+ year history
    and (
        tcwr.email_prev >= 0.5                                -- 90%+ email prevalence
        or tcwr.email_data_prev >= 0.5                        -- OR 90%+ email_data prevalence
        or tcwr.combined_prev >= 0.5                          -- OR 90%+ combined prevalence
    )
;

-- Validation: Check qualifying advertiser count
-- select count(*) as qualifying_count from temp_qualifying_advertisers;
-- select * from temp_qualifying_advertisers order by combined_prev desc limit 20;

-- Step 4: Get distinct emails per advertiser from current window (deduped)
drop table if exists temp_current_emails;
create temp table temp_current_emails as
select distinct
    tcwr.advertiser_id
  , tcwr.email
  , tcwr.email_data
from temp_current_window_raw tcwr
    inner join temp_qualifying_advertisers tqa
        on tcwr.advertiser_id = tqa.advertiser_id
where 1 = 1
    and (tcwr.email is not null or tcwr.email_data is not null)
;

-- Validation: Check deduped email counts
-- select advertiser_id, count(*) as row_count, count(distinct email) as distinct_email, count(distinct email_data) as distinct_email_data from temp_current_emails group by advertiser_id;

-- Step 5: Get historical emails from lookback window (only for qualifying advertisers)
drop table if exists temp_historical_emails;
create temp table temp_historical_emails as
with classified as (
    select
        cl.advertiser_id
      , cl.email as raw_email
      , cl.query
      , case
            when cl.query like '{%' and cl.query not like '%{%22%' then 'json'
            when cl.query like '%=%' then 'querystring'
            else 'other'
        end as query_format
    from logdata.conversion_log cl
        inner join temp_qualifying_advertisers tqa
            on cl.advertiser_id = tqa.advertiser_id
    where 1 = 1
        and cl.time >= '2024-11-30 00:00:00'
        and cl.time < '2025-11-16 00:00:00'
)
select
    c.advertiser_id
  , case
        when c.raw_email = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' then null
        when c.raw_email = '' then null
        else c.raw_email
    end as email
  , case
        when c.query_format = 'json'
            and (c.query::json->>'email_data') is not null
            and (c.query::json->>'email_data') != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
            and (c.query::json->>'email_data') != ''
            then (c.query::json->>'email_data')
        when c.query_format = 'querystring'
            and c.query like '%email_data=%'
            and split_part(split_part(c.query, 'email_data=', 2), '&', 1) != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
            and split_part(split_part(c.query, 'email_data=', 2), '&', 1) != ''
            then split_part(split_part(c.query, 'email_data=', 2), '&', 1)
    end as email_data
from classified c
;

-- Validation: Check historical counts
-- select count(*) as total_historical from temp_historical_emails;

-- Step 6: Aggregate historical email appearances (email column)
drop table if exists temp_historical_email_counts;
create temp table temp_historical_email_counts as
select
    the.advertiser_id
  , the.email
  , count(*) as email_appearances
from temp_historical_emails the
where 1 = 1
    and the.email is not null
group by
    the.advertiser_id
  , the.email
;

-- Step 7: Aggregate historical email appearances (email_data column)
drop table if exists temp_historical_email_data_counts;
create temp table temp_historical_email_data_counts as
select
    the.advertiser_id
  , the.email_data
  , count(*) as email_data_appearances
from temp_historical_emails the
where 1 = 1
    and the.email_data is not null
group by
    the.advertiser_id
  , the.email_data
;

-- Step 8: Aggregate historical email appearances (combined - union both sources)
drop table if exists temp_historical_combined_counts;
create temp table temp_historical_combined_counts as
select
    advertiser_id
  , email_value
  , sum(appearances) as combined_appearances
from (
    select
        the.advertiser_id
      , the.email as email_value
      , count(*) as appearances
    from temp_historical_emails the
    where 1 = 1
        and the.email is not null
    group by
        the.advertiser_id
      , the.email

    union all

    select
        the.advertiser_id
      , the.email_data as email_value
      , count(*) as appearances
    from temp_historical_emails the
    where 1 = 1
        and the.email_data is not null
    group by
        the.advertiser_id
      , the.email_data
) unioned
group by
    advertiser_id
  , email_value
;

-- Validation: Check historical aggregation
-- select count(*) from temp_historical_email_counts;
-- select count(*) from temp_historical_email_data_counts;
-- select count(*) from temp_historical_combined_counts;

-- Step 9: Calculate NTB rates per advertiser
drop table if exists temp_ntb_rates;
create temp table temp_ntb_rates as
with email_ntb as (
    select
        tce.advertiser_id
      , count(distinct tce.email) as current_distinct
      , count(distinct case when thec.email is null then tce.email end) as ntb_distinct
      , count(distinct case when thec.email is not null then tce.email end) as returning_distinct
      , sum(coalesce(thec.email_appearances, 0)) as returning_historical_appearances
    from temp_current_emails tce
        left join temp_historical_email_counts thec
            on tce.advertiser_id = thec.advertiser_id
            and tce.email = thec.email
    where 1 = 1
        and tce.email is not null
    group by
        tce.advertiser_id
)
, email_data_ntb as (
    select
        tce.advertiser_id
      , count(distinct tce.email_data) as current_distinct
      , count(distinct case when thedc.email_data is null then tce.email_data end) as ntb_distinct
      , count(distinct case when thedc.email_data is not null then tce.email_data end) as returning_distinct
      , sum(coalesce(thedc.email_data_appearances, 0)) as returning_historical_appearances
    from temp_current_emails tce
        left join temp_historical_email_data_counts thedc
            on tce.advertiser_id = thedc.advertiser_id
            and tce.email_data = thedc.email_data
    where 1 = 1
        and tce.email_data is not null
    group by
        tce.advertiser_id
)
, combined_ntb as (
    select
        cc.advertiser_id
      , count(distinct cc.email_value) as current_distinct
      , count(distinct case when thcc.combined_appearances is null then cc.email_value end) as ntb_distinct
      , count(distinct case when thcc.combined_appearances is not null then cc.email_value end) as returning_distinct
      , sum(coalesce(thcc.combined_appearances, 0)) as returning_historical_appearances
    from (
        select distinct
            tce.advertiser_id
          , coalesce(tce.email, tce.email_data) as email_value
        from temp_current_emails tce
    ) cc
        left join temp_historical_combined_counts thcc
            on cc.advertiser_id = thcc.advertiser_id
            and cc.email_value = thcc.email_value
    group by
        cc.advertiser_id
)
select
    tqa.advertiser_id
  , tqa.min_conversion_date
  , tqa.max_conversion_date
  , tqa.total_conversions
  , tqa.has_year_history
  , tqa.current_conversions

    -- Email column metrics
  , tqa.email_prev
  , coalesce(en.current_distinct, 0) as email_current_distinct
  , coalesce(en.ntb_distinct, 0) as email_ntb_distinct
  , coalesce(en.returning_distinct, 0) as email_returning_distinct
  , coalesce(en.ntb_distinct::float / nullif(en.current_distinct, 0), 0) as email_ntb_rate
  , coalesce(en.returning_historical_appearances, 0) as email_returning_historical_appearances

    -- Email_data column metrics
  , tqa.email_data_prev
  , coalesce(edn.current_distinct, 0) as email_data_current_distinct
  , coalesce(edn.ntb_distinct, 0) as email_data_ntb_distinct
  , coalesce(edn.returning_distinct, 0) as email_data_returning_distinct
  , coalesce(edn.ntb_distinct::float / nullif(edn.current_distinct, 0), 0) as email_data_ntb_rate
  , coalesce(edn.returning_historical_appearances, 0) as email_data_returning_historical_appearances

    -- Combined metrics
  , tqa.combined_prev
  , coalesce(cn.current_distinct, 0) as combined_current_distinct
  , coalesce(cn.ntb_distinct, 0) as combined_ntb_distinct
  , coalesce(cn.returning_distinct, 0) as combined_returning_distinct
  , coalesce(cn.ntb_distinct::float / nullif(cn.current_distinct, 0), 0) as combined_ntb_rate
  , coalesce(cn.returning_historical_appearances, 0) as combined_returning_historical_appearances
from temp_qualifying_advertisers tqa
    left join email_ntb en
        on tqa.advertiser_id = en.advertiser_id
    left join email_data_ntb edn
        on tqa.advertiser_id = edn.advertiser_id
    left join combined_ntb cn
        on tqa.advertiser_id = cn.advertiser_id
;

-- Validation: Check NTB rates
-- select * from temp_ntb_rates order by combined_ntb_rate desc limit 20;

-- Step 10: Detail log - emails that were seen before with metadata
drop table if exists temp_returning_email_detail;
create temp table temp_returning_email_detail as
select
    tce.advertiser_id
  , tce.email
  , tce.email_data
  , 'email' as source
  , thec.email_appearances as historical_count
from temp_current_emails tce
    inner join temp_historical_email_counts thec
        on tce.advertiser_id = thec.advertiser_id
        and tce.email = thec.email
where 1 = 1
    and tce.email is not null

union all

select
    tce.advertiser_id
  , tce.email
  , tce.email_data
  , 'email_data' as source
  , thedc.email_data_appearances as historical_count
from temp_current_emails tce
    inner join temp_historical_email_data_counts thedc
        on tce.advertiser_id = thedc.advertiser_id
        and tce.email_data = thedc.email_data
where 1 = 1
    and tce.email_data is not null
;

-- Validation: Check returning email detail
-- select advertiser_id, source, count(*) as returning_count, sum(historical_count) as total_historical from temp_returning_email_detail group by advertiser_id, source;

-- Final output 1: Qualifying advertisers with NTB rates
select * from temp_ntb_rates order by combined_ntb_rate desc;

-- Final output 2: Returning email detail log
select * from temp_returning_email_detail order by advertiser_id, source, historical_count desc;

-- Cleanup (uncomment when done)
-- drop table if exists temp_advertiser_history;
-- drop table if exists temp_current_window_raw;
-- drop table if exists temp_qualifying_advertisers;
-- drop table if exists temp_current_emails;
-- drop table if exists temp_historical_emails;
-- drop table if exists temp_historical_email_counts;
-- drop table if exists temp_historical_email_data_counts;
-- drop table if exists temp_historical_combined_counts;
-- drop table if exists temp_ntb_rates;
-- drop table if exists temp_returning_email_detail;