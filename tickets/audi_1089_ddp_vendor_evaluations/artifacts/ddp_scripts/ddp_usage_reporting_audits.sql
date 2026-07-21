-- ============================================================
-- DATE VARIABLES (BigQuery Syntax)
-- ============================================================

-- Declare date variables
drop table if exists reporting.temp_date_params;
create table reporting.temp_date_params AS
    SELECT
        DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS reporting_month,
        DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH) AS prior_month,
        DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS reporting_month_start,
        DATE_TRUNC(CURRENT_DATE(), MONTH) AS reporting_month_end

-- ============================================================
-- STEP 1: Grab usage data
-- ============================================================

        drop table if exists reporting.sno_montly_usage;
        create table reporting.sno_montly_usage as
        select reporting_month
              ,case when data_source_id in (28,40) then 28
                    when data_source_id in (11,35) then 35
                    else data_source_id
               end as data_source_id
             , sum(impressions) as impressions
             , sum(usage) as usage
        from dw-main-bronze.coredw.usage_reporting_data
        WHERE reporting_month >= (SELECT prior_month FROM reporting.temp_date_params)
        group by 1,2
        ;

-- ============================================================
-- STEP 2: Grab CIL monthly counts
-- ============================================================
        drop table if exists reporting.sno_montly_cil_counts;
        create table reporting.sno_montly_cil_counts as
        select
             (select prior_month FROM reporting.temp_date_params) AS month
             , count(1) as impressions
        from dw-main-silver.logdata.cost_impression_log cil
        join dw-main-gold.public.campaigns  c ON c.campaign_id = cil.campaign_id
        where date(cil.time) >= (SELECT prior_month FROM reporting.temp_date_params)
        and date(cil.time) < (SELECT reporting_month FROM reporting.temp_date_params)
            and cil.unlinked is false
          --and c.campaign_template_id = 10
            and c.channel_id = 8 and c.funnel_level = 1
            and c.objective_id = 1

        union all

        select
             (select reporting_month FROM reporting.temp_date_params) AS month
             , count(1) as impressions
        from dw-main-silver.logdata.cost_impression_log cil
        join dw-main-gold.public.campaigns  c ON c.campaign_id = cil.campaign_id
        where date(cil.time) >= (SELECT reporting_month_start FROM reporting.temp_date_params)
            and date(cil.time) < (SELECT reporting_month_end FROM reporting.temp_date_params)
            and cil.unlinked is false
          --and c.campaign_template_id = 10
            and c.channel_id = 8 and c.funnel_level = 1
            and c.objective_id = 1
        ;

-- ============================================================
-- STEP 3: Calculate usage diffs
-- ============================================================
        drop table if exists reporting.temp_sno_usage;
        create table reporting.temp_sno_usage as
        select *
             , (usage-prior_usage)*1.0/nullif(prior_usage,0) as usage_diff_pct
             , usage-prior_usage as usage_diff
        from ( select reporting_month
                     , data_source_id
                     , impressions
                     , usage
                     , lag(usage) over(partition by data_source_id order by reporting_month) as prior_usage
                from reporting.sno_montly_usage
             )x
        ;

-- ============================================================
-- STEP 4: Calculate impression diffs
-- ============================================================
        drop table if exists reporting.temp_sno_cil;
        create table reporting.temp_sno_cil as
        select *
             , (impressions-prior_impressions)*1.0/nullif(prior_impressions,0) as impressions_diff_pct
        from ( select month
                    ,  impressions
                     , lag(impressions) over(order by month) as prior_impressions
                from reporting.sno_montly_cil_counts
             )x
        ;

-- ============================================================
-- STEP 5: Run against gate logic
-- ============================================================
        drop table if exists reporting.temp_sno_gate;
        create table reporting.temp_sno_gate as

        with thresholds as
         (select 0.15 as pct, 2000 as amt )  -- Thresholds: 15% and $2000

        , gate_checks as
          (
                select *
                     , case when usage_diff_pct between -1*t.pct and t.pct then 0 else 1
                       end as gate1 --usage_diff_pct
                     , case when usage_diff_pct between impressions_diff_pct - t.pct and impressions_diff_pct + t.pct then 0 else 1
                       end as gate2 --usage_impression_delta
                     , case when usage_diff between 0 and t.amt then 0 else 1
                       end as gate3 --usage_delta
                from (
                        select u.reporting_month
                             , u.data_source_id
                             , ds.name
                             , u.usage
                             , u.prior_usage
                             , cil.impressions
                             , cil.prior_impressions
                             , u.usage_diff_pct
                             , cil.impressions_diff_pct
                             , u.usage_diff
                        from reporting.temp_sno_usage u
                        join reporting.temp_sno_cil cil on cil.month = u.reporting_month
                        join `dw-main-bronze`.integrationprod.data_sources ds on ds.data_source_id = u.data_source_id
                        where u.reporting_month >= (select prior_month from reporting.temp_date_params)
                     )x
                cross join thresholds t
           )

        select *
             , case when (gate1 + gate2 + gate3) < 3 then 'pass' else 'fail' end as final
        from gate_checks
        ;

-- ============================================================
-- STEP 6: Insert output into audit table
-- ============================================================
        insert into `dw-main-bronze`.coredw.usage_reporting_audits
        (
         reporting_month,
         data_source_id,
         name,
         usage,
         prior_usage,
         impressions,
         prior_impressions,
         usage_diff_pct,
         impressions_diff_pct,
         usage_diff,
         pct,
         amt,
         gate1_usage_diff_pct,
         gate2_usage_diff_pct_impression_delta,
         gate3_increase_in_dollar,
         final,
         created_at
        )
        select
           (SELECT reporting_month FROM reporting.temp_date_params) as reporting_month,
            data_source_id,
            name,
            usage,
            prior_usage,
            impressions,
            prior_impressions,
            CAST(ROUND(usage_diff_pct * 100, 2) AS NUMERIC),
            CAST(ROUND(impressions_diff_pct * 100, 2) AS NUMERIC),
            usage_diff,
            CAST(ROUND(pct*100, 2) AS NUMERIC),
            amt,
            gate1,
            gate2,
            gate3,
            final,
            current_timestamp as created_at
        from reporting.temp_sno_gate
        where data_source_id not in (25,26) and reporting_month = (SELECT reporting_month FROM reporting.temp_date_params)
        order by 1, case when data_source_id = 35 then 1
                         when data_source_id = 17 then 2
                         when data_source_id = 29 then 3
                         else 4
                    end
               , data_source_id;

-- ============================================================
-- END
-- ============================================================