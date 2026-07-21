

----------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------  PRE-CHECKS ----------------------------------------------------------
------------------------------------------------------ start ---------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
--- This table is cost impression logs linked to IPDSC 
select dt, data_source_id, count(1)
from mntn-analytics-prod-01.analytics_curated.enriched_impressions 
where dt >= date('2026-05-01')
group by 1,2
order by 1 desc, 2;

select max(created_date), max(updated_date)
--from dw-main-bronze.tpa.categories
from dw-main-bronze.tpa.liveramp_categories;

select dt, source_data_source_id, count(1)
from dw-main-bronze.external.targeted_signal
where dt >= '2026-05-01'
group by 1,2
order by 1 desc;

select dt, count(1)
from dw-main-bronze.external.targeted_signal_domain
where dt >= '2026-05-01'
group by 1
order by 1 desc;

select count(1) from dw-main-bronze.integrationprod.campaigns; -- compare to RUN IN COREDW - select count(1) from public.campaigns -- no need to match just close enough is good
select * from dw-main-bronze.integrationprod.direct_data_partners; -- DDP external reporting, CPM and type (crm, mm, tpa) check

----------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------  PRE-CHECKS  -----------------------------------------------------------
------------------------------------------------------  end   ---------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- GENERATE USAGE REPORTS DATA ---------------------------------------------------
------------------------------------------------------ start ---------------------------------------------------------------
---------------------------------------------- RUN IN dw-main-gold project -------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- Step 0: tpa.direct_data_partners in intprod and adjust data based on reporting requirements
-- select * from integrationprod.direct_data_partners;
        -- TPA
                drop table if exists dw-main-gold.reporting.ddp_tpa_direct_data_partners_tpa;
                create table dw-main-gold.reporting.ddp_tpa_direct_data_partners_tpa as
                select cast(coalesce(report_under_data_source_id,data_source_id) as int64) as report_data_source_id
                     , cast(coalesce(primary_data_source_id, report_under_data_source_id, data_source_id) as int64) as credit_data_source_id
                     , cast(data_source_id as int64) as data_source_id
                     , fixed_cpm
                     , is_current
                from dw-main-bronze.integrationprod.direct_data_partners
                where type = 'interests'
                  and coalesce(date(valid_from), go_live_date) <= date('2026-06-01') --before end of month
                  and coalesce(date(valid_to), date('9999-12-31')) > date('2026-05-01') --after start of month
                  and is_current
                  and external_reporting_required
                ; -- select * from dw-main-bronze.integrationprod.direct_data_partners;

        -- CRM
                drop table if exists dw-main-gold.reporting.ddp_tpa_direct_data_partners_crm;
                create table dw-main-gold.reporting.ddp_tpa_direct_data_partners_crm as
                select cast(coalesce(report_under_data_source_id,data_source_id) as int64) as report_data_source_id
                     , cast(coalesce(primary_data_source_id, report_under_data_source_id, data_source_id) as int64) as credit_data_source_id
                     , cast(data_source_id as int64) as data_source_id
                     , fixed_cpm
                     , is_current
                from dw-main-bronze.integrationprod.direct_data_partners
                where type = 'crm'
                  and coalesce(date(valid_from), go_live_date) <= date('2026-06-01') --before end of month
                  and coalesce(date(valid_to), date('9999-12-31')) > date('2026-05-01') --after start of month
                  and is_current
                  and external_reporting_required
                ;-- select * from reporting.ddp_tpa_direct_data_partners_crm;

        -- MM
                drop table if exists dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm;
                create table dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm as
                select cast(coalesce(report_under_data_source_id,data_source_id) as int64) as report_data_source_id
                     , cast(coalesce(primary_data_source_id, report_under_data_source_id, data_source_id) as int64) as credit_data_source_id
                     , cast(data_source_id as int64) as data_source_id
                     , fixed_cpm
                     , is_current
                from dw-main-bronze.integrationprod.direct_data_partners
                where type = 'mntn_matched'
                  and coalesce(date(valid_from), go_live_date) <= date('2026-06-01') --before end of month
                  and coalesce(date(valid_to), date('9999-12-31')) > date('2026-05-01') --after start of month
                  and is_current
                  and external_reporting_required
                ; -- select * from reporting.ddp_tpa_direct_data_partners_mm;

                -- to account for 2 dsids with reporting credit as 1
                drop table if exists dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit
                create table dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit as
                select credit_data_source_id, array_agg(report_data_source_id) as dsids, count(distinct report_data_source_id) as credit_divisor
                from dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm
                group by 1
                order by 1
                ;-- select * from reporting.ddp_tpa_direct_data_partners_mm_credit;
----------------------------------------------------------------------------------------------------------------------------



-- Step 1: Grab impression category matches from enriched_impressions (RPLAT category_facts job intermediate table)

        --TPA
                drop table if exists dw-main-gold.reporting.ddp_tpa_matches;
                create table dw-main-gold.reporting.ddp_tpa_matches as
                select distinct cil.advertiser_id
                     , cil.campaign_id
                     , cil.time
                     , cil.ip
                     , cil.ad_served_id
--                     , case when cil.data_source_id = 11 then 35 else cil.data_source_id end as data_source_id
                     , i.report_data_source_id as data_source_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64) AS data_source_category_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.and_seq') AS INT64) AS and_seq
                    , SAFE_CAST(JSON_VALUE(elem, '$.or_seq')  AS INT64) AS or_seq
                    , cil.dt
                from mntn-analytics-prod-01.analytics_curated.enriched_impressions cil -- cost impression logs linked to IPDSC 
                CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(category_info)) AS elem
                --we need public.campaigns table campaign_template_id = 10
                join dw-main-bronze.integrationprod.campaigns c on c.campaign_id = cil.campaign_id
                                                                and c.channel_id = 8 and c.funnel_level = 1
                                                                and c.objective_id = 1
                join dw-main-gold.reporting.ddp_tpa_direct_data_partners_tpa i on i.data_source_id = cil.data_source_id
                where cil.dt >= date('2026-05-01') and cil.dt < date('2026-06-01')
                AND EXISTS (
                        SELECT 1
                        FROM UNNEST(cil.data_source_category_id.list) AS dsci
                        WHERE dsci.element = SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64)
                      )
                ;

        --CRM
                drop table if exists dw-main-gold.reporting.ddp_crm_matches;
                create table dw-main-gold.reporting.ddp_crm_matches as
                select distinct cil.advertiser_id
                     , cil.campaign_id
                     , cil.time
                     , cil.ip
                     , cil.ad_served_id
                     , cil.data_source_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64) AS data_source_category_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.and_seq') AS INT64) AS and_seq
                    , SAFE_CAST(JSON_VALUE(elem, '$.or_seq')  AS INT64) AS or_seq
                    , cil.dt
                from mntn-analytics-prod-01.analytics_curated.enriched_impressions cil -- cost impression logs linked to IPDSC 
                CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(category_info)) AS elem
                --we need public.campaigns table campaign_template_id = 10
                join dw-main-bronze.integrationprod.campaigns c on c.campaign_id = cil.campaign_id
                                                                and c.channel_id = 8 and c.funnel_level = 1
                                                                and c.objective_id = 1
                where cil.dt >= date('2026-05-01') and cil.dt < date('2026-06-01')
                AND EXISTS (
                        SELECT 1
                        FROM UNNEST(cil.data_source_category_id.list) AS dsci
                        WHERE dsci.element = SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64)
                      )
                  and cil.data_source_id = 4 --CRM 


        --MM
               drop table if exists dw-main-gold.reporting.ddp_mm_matches;
               create table dw-main-gold.reporting.ddp_mm_matches as
               select distinct cil.advertiser_id
                    , cil.campaign_id
                    , cil.time
                    , cil.ip
                    , cil.ad_served_id
                    , cil.data_source_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64) AS data_source_category_id
                    , SAFE_CAST(JSON_VALUE(elem, '$.and_seq') AS INT64) AS and_seq
                    , SAFE_CAST(JSON_VALUE(elem, '$.or_seq')  AS INT64) AS or_seq
                    , cil.dt
                from mntn-analytics-prod-01.analytics_curated.enriched_impressions cil -- cost impression logs linked to IPDSC 
                CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(category_info)) AS elem
                --we need public.campaigns table campaign_template_id = 10
                join dw-main-bronze.integrationprod.campaigns c on c.campaign_id = cil.campaign_id
                                                                and c.channel_id = 8 and c.funnel_level = 1
                                                                and c.objective_id = 1
                where cil.dt >= date('2026-05-01') and cil.dt < date('2026-06-01')
                AND EXISTS (
                        SELECT 1
                        FROM UNNEST(cil.data_source_category_id.list) AS dsci
                        WHERE dsci.element = SAFE_CAST(JSON_VALUE(elem, '$.data_source_category_id') AS INT64)
                      )
                and cil.data_source_id in (13,19) --MM -- Include MMV3
                ;

-- Step 2: determine CPMs to use

        --TPA
                drop table if exists dw-main-gold.reporting.ddp_liveramp_categories;
                create table dw-main-gold.reporting.ddp_liveramp_categories as
                select *
                from (select data_source_category_id
                           , tv_cpm
                           , path as segment_name
                           , row_number() over (partition by data_source_category_id
                        order by case when deprecated = false then 0 else 1 end, created_date desc, updated_date desc) as seq
                      from dw-main-bronze.tpa.liveramp_categories --- copied data from GCP
                     ) l
                where l.seq = 1;

                drop table if exists dw-main-gold.reporting.ddp_sharethis_categories;
                create table dw-main-gold.reporting.ddp_sharethis_categories as
                select data_source_category_id
                      , sharethis_id
                      , st.fixed_cpm as tv_cpm
                      , segment_name
                      , seq
                from (select data_source_category_id
                           , sharethis_id
                           , path as segment_name
                           , row_number() over (partition by data_source_category_id
                                                order by case when deprecated = false then 0 else 1 end, created_date desc, updated_date desc) as seq
                      from dw-main-bronze.external.sharethis_categories -- since there was no change in the data, we don't need to copy the data from GCP
                      ) l
                join dw-main-gold.reporting.ddp_tpa_direct_data_partners_tpa st on 1=1
                                                                          and st.report_data_source_id = 17
                where l.seq = 1;



                drop table if exists dw-main-gold.reporting.ddp_tpa_categories;
                create table dw-main-gold.reporting.ddp_tpa_categories as
                select distinct 35 as data_source_id, data_source_category_id, tv_cpm, segment_name
                from dw-main-gold.reporting.ddp_liveramp_categories
                union all
                select distinct 17, data_source_category_id, tv_cpm, segment_name
                from dw-main-gold.reporting.ddp_sharethis_categories
                ;

                drop table if exists dw-main-gold.reporting.ddp_tpa_matches_cpm;
                create table dw-main-gold.reporting.ddp_tpa_matches_cpm as
                select m.*
                       , coalesce(tc.tv_cpm,0) as tv_cpm
                       , tc.segment_name
                from dw-main-gold.reporting.ddp_tpa_matches m
                join dw-main-gold.reporting.ddp_tpa_categories tc on tc.data_source_id = m.data_source_id
                                                           and m.data_source_category_id = tc.data_source_category_id


        --CRM
                drop table if exists dw-main-gold.reporting.ddp_crm_matches_cpm;
                create table dw-main-gold.reporting.ddp_crm_matches_cpm as
                select cil.advertiser_id
                     , cil.campaign_id
                     , cil.time
                     , cil.ip
                     , cil.ad_served_id
                     , cil.data_source_id
                     , cil.data_source_category_id
                     , cil.and_seq
                     , cil.or_seq
                     , cil.dt
                     , min(coalesce(crm.fixed_cpm,0)) as cpm
                     , TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(tp.names, r'"([^"]+)"\]\}'), r'^"|"$', '')) as segment_name
                from dw-main-bronze.external.targeted_signal ts
                join dw-main-gold.reporting.ddp_crm_matches cil
                           on ts.ip = cil.ip
                          and ts.data_source_id = cil.data_source_id
                          and ts.data_source_category_id = cil.data_source_category_id
                join dw-main-bronze.tpa.categories tp on tp.data_source_id = cil.data_source_id
                                                        and tp.data_source_category_id = cil.data_source_category_id
                left join dw-main-gold.reporting.ddp_tpa_direct_data_partners_crm crm on crm.report_data_source_id = ts.source_data_source_id
                where date(ts.dt) >= date('2026-05-01') - interval 30 day and date(ts.dt) < date('2026-06-01')
                  and ts.data_source_id = 4
                  and ts.ip_to_dscid_link_number = 1
                  and date(ts.dt) >= cil.dt - interval 30 day
                  and date(ts.dt) <= cil.dt
                group by 1,2,3,4,5,6,7,8,9,10,12;

               ---- this is to account for any CRM matches that don't have a corresponding targeted signal record (and thus no CPM)
               ---- we will assign them a CPM of 0 and null segment name so they are included in the winners selection with 0 CPM
               ---- this is needed to ensure we are not over crediting to TPA/MM Matches for CRM matches that don't have a valid targeted signal record in the lookback window since those should still be eligible for winning impressions with 0 CPM
               insert into dw-main-gold.reporting.ddp_crm_matches_cpm
               select cil.advertiser_id
                     , cil.campaign_id
                     , cil.time
                     , cil.ip
                     , cil.ad_served_id
                     , cil.data_source_id
                     , cil.data_source_category_id
                     , cil.and_seq
                     , cil.or_seq
                     , cil.dt
                     , 0 as cpm
                     , cast(null as string) as segment_name
                from dw-main-gold.reporting.ddp_crm_matches cil
                where not exists (select 1
                                  from dw-main-gold.reporting.ddp_crm_matches_cpm b
                                  where b.ad_served_id = cil.ad_served_id
                                   and b.data_source_id = cil.data_source_id
                                   and b.data_source_category_id = cil.data_source_category_id)
                group by 1,2,3,4,5,6,7,8,9,10,11,12
                ;

    --MM
                drop table if exists dw-main-gold.reporting.ddp_mm_matches_uids;
                create table dw-main-gold.reporting.ddp_mm_matches_uids as
                select cil.*
                     , ts.source_data_source_id as mm_dsid
                     , ts.uid
                     , ts.dt as ts_dt
                from dw-main-bronze.external.targeted_signal ts
                join dw-main-gold.reporting.ddp_mm_matches cil
                           on ts.ip = cil.ip
                          and ts.data_source_category_id = cil.data_source_category_id
                left join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm mm on mm.report_data_source_id = ts.source_data_source_id
                where date(ts.dt) >= date('2026-05-01') - interval 30 day and date(ts.dt) < date('2026-06-01')
                  and ts.data_source_id in (13,19)
                  and date(ts.dt) >= cil.dt - interval '30' day
                  and date(ts.dt) <= cil.dt
                ;


                drop table if exists dw-main-gold.reporting.ddp_mm_matches_cpm;
                create table dw-main-gold.reporting.ddp_mm_matches_cpm as
                                select advertiser_id
                                     , campaign_id
                                     , time
                                     , ip
                                     , cil.ad_served_id
                                     , cil.data_source_id
                                     , 999 as data_source_category_id
                                     , and_seq
                                     , or_seq
                                     , dt
                                     , max(coalesce(mm.fixed_cpm,0)) as cpm
                                --     REMOVED from ddp_mm_matches_cpm and add it under ddp_mm_winners_imp - MMV3 changes
                                --   , count(distinct coalesce(mm.credit_data_source_id, cil.mm_dsid)) as mm_dsid_count
                                     , array_agg(distinct coalesce(mm.report_data_source_id, cil.mm_dsid)) as mm_dsids
                                from dw-main-gold.reporting.ddp_mm_matches_uids cil
                                left join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm mm on mm.report_data_source_id = cil.mm_dsid
                                group by 1,2,3,4,5,6,7,8,9,10


    --combine all
                drop table if exists dw-main-gold.reporting.ddp_all_matches_cpm;
                create table dw-main-gold.reporting.ddp_all_matches_cpm as
                --tpa
                select *
                        , null as mm_dsids
                from dw-main-gold.reporting.ddp_tpa_matches_cpm
                union all

                --crm
                select *
                    , null as mm_dsids
                from dw-main-gold.reporting.ddp_crm_matches_cpm

                --mm
                union all
                select advertiser_id
                     , campaign_id
                     , time
                     , ip
                     , ad_served_id
                     , data_source_id
                     , data_source_category_id
                     , and_seq
                     , or_seq
                     , dt
                     , cpm
                     , null as segment_name
                     , mm_dsids
                from dw-main-gold.reporting.ddp_mm_matches_cpm
                ;

-- Step 3 - determine impression winners and credits and reporting data requirements

    --generate winners
                drop table if exists dw-main-gold.reporting.ddp_winners;
                create table dw-main-gold.reporting.ddp_winners as
                select *
                from (select *
                           --this is to pick the highest CPM per AND grouping (may have 1 or more if there's a tie across different DS)
                           , rank() over(partition by advertiser_id, campaign_id, ip, "time", ad_served_id,  data_source_id order by tv_cpm desc) as and_cpm_seq
                      from (  select m.advertiser_id
                                   , m.campaign_id
                                   , m.ad_served_id
                                   , m.and_seq
                                   , m.or_seq
                                   , m.data_source_id
                                   , m.data_source_category_id
                                   , m.tv_cpm
                                   , m.time
                                   , m.ip
                                   , m.segment_name
                                   --this is to pick the lowest CPM per OR grouping (may have 1 or more if there's a tie across different DS)
                                   , rank() over(partition by advertiser_id, campaign_id, ip, "time", ad_served_id, and_seq order by tv_cpm) as or_cpm_seq
                              from ( select *
                                         ,  row_number() over(partition by m.advertiser_id, m.campaign_id, m.ip, m.time, m.ad_served_id, m.and_seq, m.data_source_id order by tv_cpm, data_source_category_id) as cpm_seq
                                     from dw-main-gold.reporting.ddp_all_matches_cpm m
                                  ) m
                              where cpm_seq = 1
                            ) a
                      where or_cpm_seq = 1
                     ) b
                 where and_cpm_seq = 1
                 ;


    --determine the impression % in case there are multiple winning dsids
                drop table if exists dw-main-gold.reporting.ddp_winners_imp;
                create table dw-main-gold.reporting.ddp_winners_imp as
                select *
                     , 1.0/count(*) over(partition by advertiser_id, campaign_id, ad_served_id) as impression_cnt
                from (
                    select advertiser_id
                         , campaign_id
                         , ad_served_id
                         , and_seq
                         , or_seq
                         , data_source_id
                         , data_source_category_id
                         , tv_cpm
                    from dw-main-gold.reporting.ddp_winners
                ) a
                ;

   --for DS 35, generate winners aggregated with aggregated cpms and segments
                drop table if exists dw-main-gold.reporting.ddp_winners_agg;
                create table dw-main-gold.reporting.ddp_winners_agg as
                select advertiser_id
                     , campaign_id
                     , ad_served_id
                     , time
                     , ip
                     , data_source_id
                     , STRING_AGG(DISTINCT CAST(data_source_category_id AS STRING), ',' ORDER BY CAST(data_source_category_id AS STRING)) AS data_source_category_ids
                     , STRING_AGG(DISTINCT FORMAT('%.2f', tv_cpm), ',' ORDER BY FORMAT('%.2f', tv_cpm)) AS tv_cpms
                     , STRING_AGG(DISTINCT segment_name, ',' ORDER BY segment_name) AS segment_names
                     , max(tv_cpm) as tv_cpm
                from (select *
                           --this is to pick the highest CPM per AND grouping (may have 1 or more if there's a tie across different DS)
                           , rank() over(partition by advertiser_id, campaign_id, ip, "time", ad_served_id,  data_source_id order by tv_cpm desc) as and_cpm_seq
                      from (  select m.advertiser_id
                                   , m.campaign_id
                                   , m.ad_served_id
                                   , m.and_seq
                                   , m.or_seq
                                   , m.data_source_id
                                   , m.data_source_category_id
                                   , m.tv_cpm
                                   , m.time
                                   , m.ip
                                   , m.segment_name
                                   --this is to pick the lowest CPM per OR grouping (may have 1 or more if there's a tie across different DS)
                                   , rank() over(partition by advertiser_id, campaign_id, ip, "time", ad_served_id, and_seq order by tv_cpm) as or_cpm_seq
                              from ( select *
                                         ,  row_number() over(partition by m.advertiser_id, m.campaign_id, m.ip, m.time, m.ad_served_id, m.and_seq, m.data_source_id order by tv_cpm, data_source_category_id) as cpm_seq
                                     from dw-main-gold.reporting.ddp_all_matches_cpm  m
                                   ) m
                              where cpm_seq = 1
                            ) a
                      where or_cpm_seq = 1
                     ) b
                group by 1,2,3,4,5,6
                ;


    --for MM
        --grab MM winners domains
                drop table if exists dw-main-gold.reporting.ddp_mm_winners_domains;
                create table dw-main-gold.reporting.ddp_mm_winners_domains as
                select w.ad_served_id
                     , u.mm_dsid
                     , STRING_AGG(DISTINCT d.domain, ',' ORDER BY d.domain) AS domains
                from dw-main-bronze.external.targeted_signal_domain d
                join dw-main-gold.reporting.ddp_mm_matches_uids u on d.uid = u.uid
                join dw-main-gold.reporting.ddp_winners_imp w on u.ad_served_id = w.ad_served_id
                join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm mm on mm.report_data_source_id = u.mm_dsid
                where date(d.dt) >= date('2026-05-01') - interval '30' day and date(d.dt) < date('2026-06-01')
                  and d.dt = u.ts_dt
                  and w.data_source_id in (19,13)
                group by 1,2;


        --grab MM winners dsids
                drop table if exists dw-main-gold.reporting.ddp_mm_winners_imp;
                create table dw-main-gold.reporting.ddp_mm_winners_imp as
                with grouped_mntn_match_dsids AS (  --- we need to group all dsids by ad_served_id for both mntn v2 and v3 after winners are determined
                    SELECT
                        w.ad_served_id,
                        count(distinct coalesce(mm.credit_data_source_id, u.mm_dsid)) as mm_dsid_count,  -- NEW: Total count of winners 13 and/or 19
                        array_agg(distinct coalesce(mm.report_data_source_id, u.mm_dsid)) as mm_dsids_winner --- List of mm_dsids winners - all_matches_cpm should have raw mm_dsids
                    from dw-main-gold.reporting.ddp_winners_imp w
                    join dw-main-gold.reporting.ddp_mm_matches_uids u on u.ad_served_id = w.ad_served_id and u.data_source_id = w.data_source_id
                    left join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm mm on mm.report_data_source_id = u.mm_dsid
                    where w.data_source_id  in (19,13)
                    group by w.ad_served_id
                )
                select w.*, g.mm_dsid_count, g.mm_dsids_winner
                from dw-main-gold.reporting.ddp_winners_imp w
                join dw-main-gold.reporting.ddp_mm_matches_cpm c on c.ad_served_id = w.ad_served_id and c.data_source_id = w.data_source_id
                join grouped_mntn_match_dsids g on g.ad_served_id = w.ad_served_id
                where w.data_source_id  in (19,13)

----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- GENERATE USAGE REPORTS DATA ---------------------------------------------------
------------------------------------------------------ end -----------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------------
--------------------------------------------   BACK-UP DATA FOR AUDIT    ---------------------------------------------------
------------------------------------------------------ start ---------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------

       --create tables for the month (one-time for the month)
        create table reporting.ddp_all_matches_cpm_202605 as select * from reporting.ddp_all_matches_cpm ;
        create table reporting.ddp_winners_202605 as select * from reporting.ddp_winners;
        create table reporting.ddp_winners_imp_202605 as select * from reporting.ddp_winners_imp ;
        create table reporting.ddp_winners_agg_202605 as select * from reporting.ddp_winners_agg;
        create table reporting.ddp_mm_winners_domains_202605 as select * from reporting.ddp_mm_winners_domains ;
        create table reporting.ddp_mm_winners_imp_202605 as select * from reporting.ddp_mm_winners_imp ;

        --run this for every run of the month -- run only if we have multiple runs in the month
        insert into reporting.ddp_all_matches_cpm_202605
        select * from reporting.ddp_all_matches_cpm;

        insert into reporting.ddp_winners_202605
        select * from reporting.ddp_winners;

        insert into reporting.ddp_winners_imp_202605
        select * from reporting.ddp_winners_imp;

        insert into reporting.ddp_winners_agg_202605
        select * from reporting.ddp_winners_agg;

        insert into reporting.ddp_mm_winners_domains_202605
        select * from reporting.ddp_mm_winners_domains;

        insert into reporting.ddp_mm_winners_imp_202605
        select * from reporting.ddp_mm_winners_imp;

----------------------------------------------------------------------------------------------------------------------------
--------------------------------------------   BACK-UP DATA FOR AUDIT    ---------------------------------------------------
------------------------------------------------------ end -----------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- USAGE REPORTS OUTPUT ----------------------------------------------------------
------------------------------------------------------ start ---------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
--Step 4 generate usage report tables and output report data

    --TPA
            --DS35
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds35;
                create table dw-main-gold.reporting.ddp_usage_report_ds35 as
                select data_source_category_ids
                    , segment_names
                    , tv_cpms
                    , tv_cpm
                    , impression_cnt as impression_raw
                    , ceil(impression_cnt) as impression
                    , round((ceil(impression_cnt) / 1000.0) * tv_cpm, 4) as usage
                from (
                select a.data_source_category_ids, a.segment_names, a.tv_cpms, w.tv_cpm, sum(w.impression_cnt) as impression_cnt
                from dw-main-gold.reporting.ddp_winners_imp w
                join dw-main-gold.reporting.ddp_winners_agg a on a.ad_served_id = w.ad_served_id
                                                        and a.data_source_id = w.data_source_id
                where w.data_source_id = 35
                 group by 1,2,3,4) a
                ;


            --DS17
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds17;
                create table dw-main-gold.reporting.ddp_usage_report_ds17 as
                select data_source_category_id
                    , sharethis_id
                    , segment_name
                    , tv_cpm
                    , impression_cnt as impression_raw
                    , ceil(impression_cnt) as impression
                    , round((ceil(impression_cnt) / 1000.0) * tv_cpm, 4) as usage
                from (
                select w.data_source_category_id, st.sharethis_id, st.segment_name, w.tv_cpm, sum(w.impression_cnt) as impression_cnt
                from dw-main-gold.reporting.ddp_winners_imp w
                join dw-main-gold.reporting.ddp_sharethis_categories st on st.data_source_category_id = w.data_source_category_id
                where w.data_source_id = 17
                 group by 1,2,3,4) a
                ;

    --CRM
            --D29
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds29;
                create table dw-main-gold.reporting.ddp_usage_report_ds29 as
                select  'All' as segment_name
                    , tv_cpm
                    , impression_cnt as impression_raw
                    , ceil(impression_cnt) as impression
                    , round((ceil(impression_cnt) / 1000.0) * tv_cpm, 4) as usage
                from (
                select tv_cpm, sum(w.impression_cnt) as impression_cnt
                from dw-main-gold.reporting.ddp_winners_imp w
                where w.data_source_id = 4
                  and w.tv_cpm > 0
                 group by 1 ) a
                ;

    --MM

            --DS28
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds28;
                create table dw-main-gold.reporting.ddp_usage_report_ds28 as
                select
                case when impression_cnt < 1000 then cast(null as string) else domains end as domains -- <<-- change threshold here depending on time range
                     , sum(impression_cnt) as impressions_raw
                     , ceil(sum(impression_cnt)) as impressions
                from (
                    select
                        domains,
                        sum(impression_cnt) as impression_cnt
                    from (
                        select
                            w.ad_served_id,
                            -- Use SAFE_DIVIDE to handle division by zero
                             SAFE_DIVIDE(
                                w.impression_cnt / w.mm_dsid_count,
                                (
                                    select count(*)
                                    from unnest(w.mm_dsids_winner) AS mm_dsid
                                    where mm_dsid in unnest(c.dsids)
                                )
                            ) AS impression_cnt
                             , m.domains

                        from dw-main-gold.reporting.ddp_mm_winners_imp w
                        join dw-main-gold.reporting.ddp_mm_winners_domains m
                            on m.ad_served_id = w.ad_served_id
                        join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit c
                            on true
                            and m.mm_dsid IN unnest(c.dsids)
                         where  m.mm_dsid = 28
                    )
                    where impression_cnt is not null  -- Filter out null results from SAFE_DIVIDE
                    group by 1
                )
                group by 1;
                ;

                --DS40
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds40;
                create table dw-main-gold.reporting.ddp_usage_report_ds40 as
                 select
                case when impression_cnt < 1000 then cast(null as string) else domains end as domains -- <<-- change threshold here depending on time range
                     , sum(impression_cnt) as impressions_raw
                     , ceil(sum(impression_cnt)) as impressions
                from (
                    select
                        domains,
                        sum(impression_cnt) as impression_cnt
                    from (
                        select
                            w.ad_served_id,
                            -- Use SAFE_DIVIDE to handle division by zero
                             SAFE_DIVIDE(
                                w.impression_cnt / w.mm_dsid_count,
                                (
                                    select count(*)
                                    from unnest(w.mm_dsids_winner) AS mm_dsid
                                    where mm_dsid in unnest(c.dsids)
                                )
                            ) AS impression_cnt
                             , m.domains
                        from dw-main-gold.reporting.ddp_mm_winners_imp w
                        join dw-main-gold.reporting.ddp_mm_winners_domains m
                            on m.ad_served_id = w.ad_served_id
                        join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit c
                            on true
                            and m.mm_dsid IN unnest(c.dsids)
                         where  m.mm_dsid = 40
                    )
                    where impression_cnt is not null  -- Filter out null results from SAFE_DIVIDE
                    group by 1
                )
                group by 1;


            --DS24
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds24;
                create table dw-main-gold.reporting.ddp_usage_report_ds24 as
                select
                case when impression_cnt < 1000 then cast(null as string) else domains end as domains -- <<-- change threshold here depending on time range
                     , sum(impression_cnt) as impressions_raw
                     , ceil(sum(impression_cnt)) as impressions
                from (
                    select
                        domains,
                        sum(impression_cnt) as impression_cnt
                    from (
                        select
                            w.ad_served_id,
                            -- Use SAFE_DIVIDE to handle division by zero
                             SAFE_DIVIDE(
                                w.impression_cnt / w.mm_dsid_count,
                                (
                                    select count(*)
                                    from unnest(w.mm_dsids_winner) AS mm_dsid
                                    where mm_dsid in unnest(c.dsids)
                                )
                            ) AS impression_cnt
                             , m.domains
                        from dw-main-gold.reporting.ddp_mm_winners_imp w
                        join dw-main-gold.reporting.ddp_mm_winners_domains m
                            on m.ad_served_id = w.ad_served_id
                        join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit c
                            on true
                            and m.mm_dsid IN unnest(c.dsids)
                         where  m.mm_dsid = 24
                    )
                    where impression_cnt is not null  -- Filter out null results from SAFE_DIVIDE
                    group by 1
                )
                group by 1;
                ;


            --DS33
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds33;
                create table dw-main-gold.reporting.ddp_usage_report_ds33 as
                select
                case when impression_cnt < 1000 then cast(null as string) else domains end as domains -- <<-- change threshold here depending on time range
                     , sum(impression_cnt) as impressions_raw
                     , ceil(sum(impression_cnt)) as impressions
                from (
                    select
                        domains,
                        sum(impression_cnt) as impression_cnt
                    from (
                        select
                            w.ad_served_id,
                            -- Use SAFE_DIVIDE to handle division by zero
                             SAFE_DIVIDE(
                                w.impression_cnt / w.mm_dsid_count,
                                (
                                    select count(*)
                                    from unnest(w.mm_dsids_winner) AS mm_dsid
                                    where mm_dsid in unnest(c.dsids)
                                )
                            ) AS impression_cnt
                             , m.domains

                        from dw-main-gold.reporting.ddp_mm_winners_imp w
                        join dw-main-gold.reporting.ddp_mm_winners_domains m
                            on m.ad_served_id = w.ad_served_id
                        join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit c
                            on true
                            and m.mm_dsid IN unnest(c.dsids)
                         where  m.mm_dsid = 33
                    )
                    where impression_cnt is not null  -- Filter out null results from SAFE_DIVIDE
                    group by 1
                )
                group by 1;
                ;


            --DS36
                drop table if exists dw-main-gold.reporting.ddp_usage_report_ds36;
                create table dw-main-gold.reporting.ddp_usage_report_ds36 as
                select
                case when impression_cnt < 1000 then cast(null as string) else domains end as domains -- <<-- change threshold here depending on time range
                     , sum(impression_cnt) as impressions_raw
                     , ceil(sum(impression_cnt)) as impressions
                from (
                    select
                        domains,
                        sum(impression_cnt) as impression_cnt
                    from (
                        select
                            w.ad_served_id,
                            -- Use SAFE_DIVIDE to handle division by zero
                             SAFE_DIVIDE(
                                w.impression_cnt / w.mm_dsid_count,
                                (
                                    select count(*)
                                    from unnest(w.mm_dsids_winner) AS mm_dsid
                                    where mm_dsid in unnest(c.dsids)
                                )
                            ) AS impression_cnt
                             , m.domains

                        from dw-main-gold.reporting.ddp_mm_winners_imp w
                        join dw-main-gold.reporting.ddp_mm_winners_domains m
                            on m.ad_served_id = w.ad_served_id
                        join dw-main-gold.reporting.ddp_tpa_direct_data_partners_mm_credit c
                            on true
                            and m.mm_dsid IN unnest(c.dsids)
                         where  m.mm_dsid = 36
                    )
                    where impression_cnt is not null  -- Filter out null results from SAFE_DIVIDE
                    group by 1
                )
                group by 1;
                ;


----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- USAGE REPORTS OUTPUT ----------------------------------------------------------
------------------------------------------------------ end -----------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- FINAL RESULTS TABLE -----------------------------------------------------------
------------------------------------------------------ start----------------------------------------------------------------


--- Athena section


--- create table structure to combine all results - no need to change anything on this
    --drop table dw-main-gold.reporting.mt_temp_ddp_reports_2026_05;
    CREATE TABLE dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 as
    select a.*, b.domains
    from
        (select
              cast('2025-06-30' as date) as dt
            , 35 as data_source_id
            , data_source_category_ids as data_source_category_id
            , segment_names  as segment_name
            , tv_cpms
            , round(cast(tv_cpm as numeric), 2) AS tv_cpm
            , round(cast(impression as numeric), 4) AS impressions
            , round(cast(usage as numeric), 4) AS usage
            , data_source_category_ids as sharethis_id
        from dw-main-gold.reporting.ddp_usage_report_ds35 limit 1) a
        full outer join
        (
        select string_agg(domains, ', ') AS domains
        from dw-main-gold.reporting.ddp_usage_report_ds28 limit 1) b on 1 = 1
        where dt = date('2020-03-01')

    --insert individual results (change the date to the max(dt) processed for the month

        -- MM with usage MM ds28 -- 33 Across
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , domains
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 28
              , domains
              , round(cast(impressions as numeric), 4)
              , round(cast((impressions/1000)* 0.50 as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds28;

        -- MM with usage MM ds40 -- 33 Across API
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , domains
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 40
              , domains
              , round(cast(impressions as numeric), 4)
              , round(cast((impressions/1000)* 0.50 as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds40;


        -- MM with usage MM ds24 -- Justuno
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , domains
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 24
              , domains
              , round(cast(impressions as numeric), 4)
              , round(cast((impressions/1000)* 0.50 as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds24;


        -- MM with usage MM ds33 -- Sovrn
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , domains
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 33
              , domains
              , round(cast(impressions as numeric), 4)
              , round(cast((impressions/1000)* 0.50 as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds33;

        -- MM with usage MM ds36 -- Cybba
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , domains
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 36
              , domains
              , round(cast(impressions as numeric), 4)
              , round(cast((impressions/1000)* 0.50 as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds36;


        -- CRM with usage ds29 -- deepsync
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , segment_name
        , impressions
        , usage
        )
        select cast('2026-05-31' as date)
              , 29
              , 'All' as segment_name
              , round(cast(sum(impression) as numeric), 4)
              , round(cast(sum(usage) as numeric), 4)
        from dw-main-gold.reporting.ddp_usage_report_ds29;

        -- Liveramp
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , data_source_category_id
        , segment_name
        , tv_cpms
        , tv_cpm
        , impressions
        , usage)
        select cast('2026-05-31' as date)
            , 35
            , data_source_category_ids
            , segment_names
            , tv_cpms
            , round(cast(tv_cpm as numeric), 2) AS tv_cpm
            , round(cast(impression as numeric), 4) AS impressions
            , round(cast(usage as numeric), 4) AS usage
        from dw-main-gold.reporting.ddp_usage_report_ds35;


        -- ShareThis
        INSERT INTO dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 (
          dt
        , data_source_id
        , data_source_category_id
        , segment_name
        , tv_cpm
        , impressions
        , usage
        , sharethis_id)
        select cast('2026-05-31' as date)
            , 17
            , cast(data_source_category_id as string)
            , segment_name
            , round(cast(tv_cpm as numeric), 2) AS tv_cpm
            , round(cast(impression as numeric), 4) AS impressions
            , round(cast(usage as numeric), 4) AS usage
            , sharethis_id
        from dw-main-gold.reporting.ddp_usage_report_ds17

 ----   Run to check if data looks correct before exporting
select dt, data_source_id, sum(impressions), sum(usage) from dw-main-gold.reporting.mt_temp_ddp_reports_2026_05 group by 1,2

    insert into dw-main-bronze.coredw.usage_reporting_data
    select
        dt,
        data_source_id,
        replace(data_source_category_id, ',', '|') as data_source_category_id,
        segment_name,
        tv_cpms,
        tv_cpm,
        impressions,
        usage,
        sharethis_id,
        STRUCT(
              ARRAY(
                SELECT STRUCT(element AS element)
                FROM UNNEST(SPLIT(domains, ',')) AS element
              ) AS list
            ) AS domains,
        date_trunc(dt, month) as reporting_month,
        'In Progress' as status
    from dw-main-gold.reporting.mt_temp_ddp_reports_2026_05


----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- FINAL RESULTS TABLE -----------------------------------------------------------
------------------------------------------------------ end -----------------------------------------------------------------
