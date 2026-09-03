# BOS Daily Pipelines
DAG Owner: Forrest Bajbek (fbajbek@mountain.com)
Downstream Owner: Yutong Chen (tony@mountain.com)
Inverval:
- `bos__hourly` runs every hour
- `bos__spend` runs every 15 minutes

In addition to this documentation, there is a [video overview](https://drive.google.com/file/d/1LKppiSz-nVBLsuxF1kDvA3xg1IgYmqk1/view?usp=drive_link) you can watch!

## History

These tables originally ran as LDS pipelines in CoreDW before the [BigQuery Migration](https://docs.google.com/spreadsheets/d/1VIp5PPkh6IelyvMs5TmUgXQ730wS8GglQQ3keOSiPjo/edit?gid=0#gid=0), and their ownership was dubious. Now they are owned explicitly by Performance Pacing.

## Design

These DAGs populate the following tables in CoreDB:
- `performance.campaign_audience_size`
    - Part of `bos__hourly` DAG
    - One row per campaign, providing a count of IPs from Bidder Events for the last 24 hours
- `performance.campaign_avg_cpm`
    - Part of `bos__hourly` DAG
    - One row per campaign, providing average media_cost from `dw-main-silver.logdata.cost_impression_log` for the last 5 days.
- `performance.campaign_performance`
    - Part of `bos__spend` DAG
    - One row per campaign, providing cost, spend, and impressions over all time and for end-of-day UTC.
    - Also includes budget optimization metrics, such as conversion_rate, visit_rate, roas, cpa, cpv, and cpcv.
- `performance.campaign_group_performance`
    - Part of `bos__spend` DAG
    - Same as `campaign_performance`, but at the campaign_group level. Doesn't include budget optimization metrics.
- `performance.campaign_flight_end_cost`
    - Part of `bos__spend` DAG
    - One row per campaign/flight, providing today's media_cost for flights that are not currently active, but ended today.
- `performance.campaign_group_flight_end_cost`
    - Part of `bos__spend` DAG
    - Same as `campaign_flight_end_cost`, but aggregated to campaign_group.
- `performance.campaign_utc_yesterday_costs_impressions_by_hour`
    - Part of `bos__spend` DAG
    - One row per campaign per hour, providing media_cost and impressions from `dw-main-silver.logdata.cost_impression_log` starting from beginning of yesterday.
- `performance.sum_by_private_marketplace_by_hour`
    - Part of `bos__spend` DAG
    - One row per private_marketplace_deal_id per hour, providing media cost from `dw-main-silver.logdata.cost_impression_log` for the last 31 days.

In general, each table follows this pattern:
1. A Spark job runs in Dataproc Serverless.
    - pulls the appropriate data from BigQuery and CoreDB.
    - results are written to `camperbid` schema (an ephemeral staging area) in CoreDB.
2. A SQL script syncs table in `camperbid` schema to table in `performance` schema.

### Exceptions
The only exceptions to this design are `campaign_performance` and `campaign_group_performance`:
- `campaign_performance` depends on `campaign_summary_hourly` and `flight_metrics_per2388`
- `campaign_group_performance` depends on `campaign_summary_hourly`

`campaign_summary_hourly` and `flight_metrics_per2388` are two BigQuery tables that are constructed for efficiency purposes:
- `campaign_summary_hourly` is a time-partitioned table that pulls the following and overwrites the time partitions
    - the last 24 hours worth of data from `dw-main-bronze.logdata.spend_pacing`
    - the last 5 days worth of data from `dw-main-bronze.logdata.cost_impression_log`
- `flight_metrics_per2388` aggregates data from `dw-main-silver.summarydata.all_facts` to create budget optimization metrics.

## Oncall

The most frequent failures for these DAGs are the tests:
- staleness test: fails if table in `performance` schema is older thnn 30 minutes.
- regression test: fails if data in table in `camperbid` schema is not at least 95% as great as data in `performance` schema (run only for `campaign_group_performance`)

What to do in event of failure:
- If the staleness tests are failing, data is not being updated in the upstream sources, and you should contact Data Platform.
- If the regression tests are failing, the upstream data itself is not correct, and yo should either double check the ETL's logic or contact Data Platform.
