# Databricks notebook source
# SparkSession Module to start a module
from pyspark.sql import SparkSession


# Windowing functions
from pyspark.sql.window import Window


# Row and Column Functions to manipulate Spark dataframes
from pyspark.sql import Row, Column


# Sql Types for objects
from pyspark.sql.types import *

from pyspark.sql import functions as F


# I tend to import Functions as F, this is good practice because if you
# import pyspark.sql.functions that have the same name as the python functions
# (eg. mean, sum, etc) it might overwrite the native python. however, I also
# import some functions that are not native to python with just their names
# for not having to type "F." all the time
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, from_unixtime, unix_timestamp


# COMMAND ----------

import boto3
from typing import Dict
from botocore.exceptions import ClientError
from typing import Dict
from datetime import datetime,timedelta
import random
import json
from pyspark.sql import DataFrame


def get_secret(secret_name: str) -> Dict:
    region_name = "us-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secrets = get_secret("redshift-prod")
secrets['jdbcUrl'] = f"""jdbc:{secrets.get("engine")}://{secrets.get("host")}:{secrets.get("port")}/coredw"""

redshiftOptions = {
    "url": secrets.get("jdbcUrl"),
    "tempdir": "s3://aws-glue-assets-077854988703-us-west-2/temporary/",
    "unload_s3_format": "PARQUET",
    "aws_iam_role": "arn:aws:iam::077854988703:role/service-role/prod-redshift_commands_access_role",
    "user": secrets.get("username"),
    "password": secrets.get("password"),
}

def loadRedshiftQuery(query: str) -> DataFrame:
    redshiftOptions['query'] = query
    return spark.read.format("redshift") \
    .options(**redshiftOptions) \
    .load()

# COMMAND ----------

from datetime import date, datetime, timedelta

log_lookback_days = 90
end = date.today() 
# end = date(2025, 2, 27)

date_list = [end - timedelta(days=x) for x in range(log_lookback_days)]

guid_paths = list(f"s3://mntn-data-archive-prod/guid_log/dt={date}" for date in date_list)
guid_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/guid_log").format("parquet").load(guid_paths).withColumn("mntn_id_type", F.lit(2))

conv_paths = list(f"s3://mntn-data-archive-prod/conversion_log/dt={date}" for date in date_list)
conv_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/conversion_log").format("parquet").load(conv_paths).withColumn("mntn_id_type", F.lit(3))

# COMMAND ----------

from datetime import date, datetime, timedelta

log_lookback_days = 30
end = date.today() 
# - timedelta(days=1)

date_list = [end - timedelta(days=x) for x in range(log_lookback_days)]

impression_paths = list(f"s3://mntn-data-archive-prod/impression_log/dt={date}" for date in date_list)
impression_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/impression_log").format("parquet").load(impression_paths).withColumn("mntn_id_type", F.lit(10))

vast_event_paths = list(f"s3://mntn-data-archive-prod/event_log/dt={date}" for date in date_list)
vast_event_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/event_log").format("parquet").load(vast_event_paths).withColumn("mntn_id_type", F.lit(10))

clickpass_paths = list(f"s3://mntn-data-archive-prod/clickpass_log/dt={date}" for date in date_list)
clickpass_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/clickpass_log").format("parquet").load(clickpass_paths).withColumn("mntn_id_type", F.lit(10))

# COMMAND ----------

impression_df = (impression_file_df
        .withColumn('path', F.col('_info.path'))
        .withColumn('file_parts', F.split(F.col('path'), '/'))
        .withColumn('event_type', F.col('file_parts').getItem(F.lit(4)))
        .filter(F.col('event_type') == 'impression'))

# COMMAND ----------

campaign_info_df = loadRedshiftQuery("""
    select 
    c.campaign_id,
    cg.campaign_group_id,
    cg.parent_campaign_group_id,
    cg.objective_id campaign_group_objective_id,
    c.objective_id campaign_objective_id,
    c.channel_id
from public.campaigns c
left join public.campaign_groups cg on c.campaign_group_id = cg.campaign_group_id
order by cg.campaign_group_id
""")

# COMMAND ----------

guid_taxonomy_df = loadRedshiftQuery("""
    select advertiser_id, data_source_id, path_from_root
    from fpa.categories
    where mntn_id_type = 2
""")

conv_taxonomy_df = loadRedshiftQuery("""
    select advertiser_id, data_source_id, path_from_root
    from fpa.categories
    where mntn_id_type = 3
""")

impression_taxonomy_df = loadRedshiftQuery("""
    select advertiser_id, mntn_id, data_source_id, path_from_root
    from fpa.categories
    where mntn_id_type = 10
    and path_from_root_types like '%0,1,4,%'
    order by advertiser_id, mntn_id
""")

vv_taxonomy_df = loadRedshiftQuery("""
    select advertiser_id, mntn_id, data_source_id, path_from_root
    from fpa.categories
    where mntn_id_type = 10
    and path_from_root_types like '%0,1,5,%'
    order by advertiser_id, mntn_id
""")



# COMMAND ----------

from pyspark.sql.functions import last

guid_data_df = guid_file_df\
    .select('ip', 'advertiser_id', 'epoch')\
    .groupBy('advertiser_id', 'ip')\
    .agg(F.max(F.col('epoch')).alias("epoch"))\
    .join(guid_taxonomy_df, guid_file_df.advertiser_id == guid_taxonomy_df.advertiser_id, 'left')\
    .select('ip', guid_taxonomy_df.advertiser_id, 'epoch', 'data_source_id', 'path_from_root')\
    .filter(F.col('data_source_id').isNotNull())

guid_data_df = guid_data_df.withColumn('epoch', F.round(F.col('epoch') / 1000000).cast('integer'))

conv_data_df = conv_file_df\
    .select('ip', 'advertiser_id', 'epoch_time')\
    .groupBy('advertiser_id', 'ip')\
    .agg(F.max(F.col('epoch_time')).alias("epoch"))\
    .join(conv_taxonomy_df, conv_file_df.advertiser_id == conv_taxonomy_df.advertiser_id, 'left')\
    .select('ip', conv_taxonomy_df.advertiser_id, 'epoch', 'data_source_id', 'path_from_root')\
    .filter(F.col('data_source_id').isNotNull())

conv_data_df = conv_data_df.withColumn('epoch', F.round(F.col('epoch') / 1000000).cast('integer'))


all_data_df = (guid_data_df.union(conv_data_df)
    .union(
        conv_data_df.withColumn(
            'data_source_id',
            F.lit(21)
        ).withColumn('path_from_root', F.concat(F.lit('{"pathFromRoot":['), F.col('advertiser_id'), F.lit(']}')))
    )
    .union(
        guid_data_df.withColumn(
            'data_source_id',
            F.lit(34)
        ).withColumn('path_from_root', F.concat(F.lit('{"pathFromRoot":['), F.col('advertiser_id'), F.lit(']}')))
    )
)

# COMMAND ----------

impression_data_df = (
    impression_df
        .select('ip', 'advertiser_id', 'campaign_id', 'epoch')
        .groupBy('advertiser_id', 'campaign_id', 'ip')
        .agg(F.max(F.col('epoch')).alias("epoch"))
        .join(impression_taxonomy_df, (impression_df.advertiser_id == impression_taxonomy_df.advertiser_id) & (impression_df.campaign_id == impression_taxonomy_df.mntn_id), 'left')
        .select('ip', impression_taxonomy_df.advertiser_id, 'epoch', 'data_source_id', 'path_from_root')
        .filter(F.col('data_source_id').isNotNull())
    )

impression_data_df = impression_data_df.withColumn('epoch', F.round(F.col('epoch') / 1000000).cast('integer'))
# display(impression_data_df.groupBy(['advertiser_id', 'campaign_id']).agg(F.count(F.col('ip'))))

vast_event_data_df = (
    vast_event_file_df
        .select('ip', 'advertiser_id', 'campaign_id', 'epoch')
        .groupBy('advertiser_id', 'campaign_id', 'ip')
        .agg(F.max(F.col('epoch')).alias("epoch"))
        .join(impression_taxonomy_df, (vast_event_file_df.advertiser_id == impression_taxonomy_df.advertiser_id) & (vast_event_file_df.campaign_id == impression_taxonomy_df.mntn_id), 'left')
        .select('ip', impression_taxonomy_df.advertiser_id, 'epoch', 'data_source_id', 'path_from_root')
        .filter(F.col('data_source_id').isNotNull())
    )

vast_event_data_df = vast_event_data_df.withColumn('epoch', F.round(F.col('epoch') / 1000).cast('integer'))
# display(vast_event_data_df.limit(1000))

# COMMAND ----------

vv_data_df = (
    clickpass_file_df
        .withColumn('epoch', F.unix_timestamp('time').cast('integer'))
        .select('ip', 'advertiser_id', 'campaign_id', 'epoch')
        .groupBy('advertiser_id', 'campaign_id', 'ip')
        .agg(F.max(F.col('epoch')).alias("epoch"))
        .join(vv_taxonomy_df, (clickpass_file_df.advertiser_id == vv_taxonomy_df.advertiser_id) & (clickpass_file_df.campaign_id == vv_taxonomy_df.mntn_id), 'left')
        .select('ip', vv_taxonomy_df.advertiser_id, 'epoch', 'data_source_id', 'path_from_root')
        .filter(F.col('data_source_id').isNotNull())
    )

# display(vv_data_df.limit(1000))
# display(vv_data_df.groupBy(['advertiser_id', 'campaign_id']).agg(F.count(F.col('ip'))))

# advertiser_id,campaign_id,count
# 38998,302671,801
# 37619,258998,207
# 35731,353292,8530


# COMMAND ----------

all_data_df = (
    all_data_df.union(impression_data_df)
    .union(vast_event_data_df)
    .union(vv_data_df)
)

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf

export_data_df = all_data_df.withColumn('categories', F.from_json(F.col('path_from_root'), "MAP<STRING,ARRAY<INT>>")['pathFromRoot'])
export_data_df = export_data_df.withColumn('category_id', F.explode(F.col('categories')))
export_data_df = (export_data_df.groupBy('ip', 'data_source_id', 'category_id')
                    .agg(F.max(F.col('epoch')).alias('epoch'))
                  )
export_data_df = (export_data_df
                  .groupBy('ip', 'data_source_id')
                  .agg(F.max('epoch').alias('epoch'), 
                       F.collect_list(F.struct(F.col('category_id'), F.col('epoch'))).alias('categories')
                       )
                  )

export_data_df = export_data_df.withColumn(
    'data_sources', 
    F.struct(
        F.create_map(
            F.col('data_source_id'), 
            F.struct(
                F.col('epoch'), 
                F.col('categories').alias('category_info')
            )
        ).alias('data_sources')
    )
)

export_data_df = export_data_df.withColumn('updateRequest', 
    F.struct( 
        F.col('ip').alias('id'), 
        F.col('data_sources').alias('add_data_sources'), 
        F.col('epoch'),
        F.lit(None).alias('remove_data_sources'),
        F.lit(None).alias('location_info'),
        F.lit(None).alias('household_score'),
        F.lit(None).alias('geo_version'),
        F.lit(None).alias('delta'),
        F.lit(None).alias('geo_loc_info'),
        F.lit(None).alias('metadata_info'),
    )
)

display(export_data_df.limit(1000))

# all_data_df = all_data_df.withColumn('categories', F.transform(F.col('categories'), lambda x: F.struct(F.lit(x).alias('category_id'),  F.col('epoch'))))
# all_data_df = (all_data_df.withColumn('category', F.explode(F.col('categories')))
#                .groupBy('ip', 'data_source_id', 'category.category_id')
#                .agg(F.max(F.col('category.epoch')).alias('epoch'))
# )

# display(all_data_df.limit(100))

# (all_data_df
#  .groupBy('ip', 'data_source_id')
#  .agg(F.collect_list))

# all_data_df = all_data_df.select('ip', 'epoch', 'data_source_id', 'categories')\
#     .sort('ip')\
#     .groupBy('ip', 'data_source_id')\
#     .agg(F.flatten(F.collect_list(F.col('categories'))).alias('categories'), F.max(F.col('epoch')).alias('epoch'), F.first('data_source_id'))

# all_data_df = all_data_df.withColumn(
#     'data_sources', 
#     F.struct(
#         F.create_map(
#             F.col('data_source_id'), 
#             F.struct(
#                 all_data_df.epoch, 
#                 all_data_df.categories.alias('category_info')
#             )
#         ).alias('data_sources')
#     )
# )

# all_data_df = all_data_df.withColumn('updateRequest', 
#     F.struct( 
#         all_data_df.ip.alias('id'), 
#         all_data_df.data_sources.alias('add_data_sources'), 
#         all_data_df.epoch,
#         F.lit(None).alias('remove_data_sources'),
#         F.lit(None).alias('location_info'),
#         F.lit(None).alias('household_score'),
#         F.lit(None).alias('geo_version'),
#         F.lit(None).alias('delta'),
#         F.lit(None).alias('geo_loc_info'),
#         F.lit(None).alias('metadata_info'),
#     )
# )


# COMMAND ----------

# MAGIC %md
# MAGIC # OLD

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf

all_data_df = all_data_df.withColumn('categories', F.from_json(F.col('path_from_root'), "MAP<STRING,ARRAY<INT>>")['pathFromRoot'])

all_data_df = all_data_df.withColumn('categories', F.transform(F.col('categories'), lambda x: F.struct(F.lit(x).alias('category_id'),  F.col('epoch'))))

all_data_df = all_data_df.select('ip', 'epoch', 'data_source_id', 'categories')\
    .sort('ip')\
    .groupBy('ip', 'data_source_id')\
    .agg(F.flatten(F.collect_list(F.col('categories'))).alias('categories'), F.max(F.col('epoch')).alias('epoch'), F.first('data_source_id'))

all_data_df = all_data_df.withColumn(
    'data_sources', 
    F.struct(
        F.create_map(
            F.col('data_source_id'), 
            F.struct(
                all_data_df.epoch, 
                all_data_df.categories.alias('category_info')
            )
        ).alias('data_sources')
    )
)

all_data_df = all_data_df.withColumn('updateRequest', 
    F.struct( 
        all_data_df.ip.alias('id'), 
        all_data_df.data_sources.alias('add_data_sources'), 
        all_data_df.epoch,
        F.lit(None).alias('remove_data_sources'),
        F.lit(None).alias('location_info'),
        F.lit(None).alias('household_score'),
        F.lit(None).alias('geo_version'),
        F.lit(None).alias('delta'),
        F.lit(None).alias('geo_loc_info'),
        F.lit(None).alias('metadata_info'),
    )
)


# COMMAND ----------

display(all_data_df.limit(1000))

# COMMAND ----------

all_data_df.select('updateRequest.*')\
    .write.format("json").mode("overwrite")\
        .save("s3://sh-dw-external-tables-dev/ip_data_updates/2025/02/24/")
        # .save("s3://mntn-data-archive-dev/mu_json_2/")

# COMMAND ----------

# all_data_df.select("ip", "epoch", "data_source_id", "categories").write.parquet("s3://sh-dw-external-tables-dev/ip_data_updates/2025/02/24/all_data_df.parquet")
# cached_all_data_df = spark.read.parquet("s3://sh-dw-external-tables-dev/ip_data_updates/2025/02/24/all_data_df.parquet")

# display(cached_all_data_df.groupBy("data_source_id").agg(F.countDistinct("ip").alias("distinct_ips_count")))

t = (cached_all_data_df
        .filter(F.col('data_source_id') == 34)
        .withColumn('category_id', F.explode('categories.category_id'))
        .groupBy("data_source_id", 'category_id')
        .agg(F.countDistinct("ip").alias("distinct_ips_count")))

display(t)

# COMMAND ----------

# MAGIC %md
# MAGIC # Just Old Style Impression Data

# COMMAND ----------

from datetime import date, datetime, timedelta

log_lookback_days = 30
end = date.today() - timedelta(days=1)

date_list = [end - timedelta(days=x) for x in range(log_lookback_days)]

impression_paths = list(f"s3://mntn-data-archive-prod/impression_log/dt={date}" for date in date_list)
impression_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/impression_log").format("parquet").load(impression_paths).withColumn("mntn_id_type", F.lit(10))

event_paths = list(f"s3://mntn-data-archive-prod/event_log/dt={date}" for date in date_list)
event_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/event_log").format("parquet").load(event_paths).withColumn("mntn_id_type", F.lit(10))

clickpass_paths = list(f"s3://mntn-data-archive-prod/clickpass_log/dt={date}" for date in date_list)
clickpass_file_df = spark.read.option("basePath", "s3://mntn-data-archive-prod/clickpass_log").format("parquet").load(clickpass_paths).withColumn("mntn_id_type", F.lit(10))

# COMMAND ----------

import pandas as pd

file_path = "/Workspace/Users/zach@mountain.com/membership_replays/select_distinct_ds_data_source_id__ads_a.csv"
df_pd = pd.read_csv(file_path)
ds_df = spark.createDataFrame(df_pd)

df_pd = pd.read_csv("/Workspace/Users/zach@mountain.com/membership_replays/select_ccg_parent_campaign_group_id_as_c.csv")
cmap_df = spark.createDataFrame(df_pd)

# COMMAND ----------

# "{\"AID\":${this.advertiserId},\"IP\":\"${this.ip}\",\"IS_NEW\":false,\"IS_CONTROL_GROUP\":false,\"REFERER\":\"http://www.steelhouse.com/__sh_vv_1/channelId/$channelId/cid/${this.campaignId}/cgid/${this.creativeGroupId}\",\"IS_COOKIED\":false,\"QUERY_STRING\":\"\",\"CACHE_BUSTER\":\"${Random.nextLong()}\",\"MOBILE\":false,\"OTHER\":\"\",\"PRODUCT\":{\"CURRENCY\":\"USD\",\"REFERRER\":\"http://www.steelhouse.com/__sh_vv_1/channelId/$channelId/cid/${this.campaignId}/cgid/${this.creativeGroupId}\"},\"CART\":{\"ITEMS\":[],\"PRODUCTS\":[]},\"GUID\":\"${this.pageViewGuid}\",\"EPOCH\":${this.epoch + 1_000_000}}"


cp_pv_df = (clickpass_file_df
 .join(ds_df, ['campaign_id', 'advertiser_id'], how='left')
 .join(cmap_df, F.col('campaign_id') == cmap_df.old_mtouch, how='left')
 .filter(F.col('new_mtouch').isNotNull())
 .withColumn('epoch', F.unix_millis(F.col('time')) * 1000)
 .withColumn('mt_referer', F.concat(F.lit('http://www.steelhouse.com/__sh_vv_1/channelId/'), F.col('new_mtouch_channel_id'), F.lit('/cid/'), F.col('new_mtouch'), F.lit('/cgid/'), F.col('creative_group_id')))
 .withColumn('pv',
             F.struct([
                 F.col('advertiser_id').alias('AID'),
                 F.col('ip').alias('IP'),
                 F.lit(False).alias('IS_NEW'),
                 F.lit(False).alias('IS_CONTROL_GROUP'),
                 F.col('mt_referer').alias('REFERER'),
                 F.lit(False).alias('IS_COOKIED'),
                 F.lit('').alias('QUERY_STRING'),
                 F.col('epoch').cast(StringType()).alias('CACHE_BUSTER'),
                 F.lit(False).alias('MOBILE'),
                 F.lit('').alias('OTHER'),
                 F.struct(
                     F.lit('USD').alias('CURRENCY'),
                     F.col('mt_referer').alias('REFERER'),
                     ).alias('PRODUCT'),
                 F.struct(F.lit([]).alias("ITEMS"), F.lit([]).alias("PRODUCTS")).alias('CART'),
                 F.col('guid').alias('GUID'),
                 (F.col('epoch') + 1_000_000).alias('EPOCH')
                 ])
))

display(cp_pv_df.limit(1000))

# COMMAND ----------

cp_ua_df = (cp_pv_df.withColumn('ua', F.struct(
                F.col('epoch'),
                F.col('advertiser_id'),
                F.col('ip'),
                F.col('guid'),
                F.crc32(F.col('guid')).alias('guid_hash'),
                F.crc32(F.col('ip')).alias('ip_hash'),
                F.concat(F.lit('__sh_vv_1/channelId/'), F.col('channel_id'),F.lit('/cid/'), F.col('campaign_id'), F.lit('/cgid/'), F.col('creative_group_id')).alias('url_path'),
                F.lit(False).alias('mobile'),
                #  F.lit(None).alias('location'),
                F.col('pv.CACHE_BUSTER').alias('cache_buster'),
                # F.col('ua_raw').alias('user_agent'),
                # F.col('is_new').alias('from_new_user'),
                # F.col('device_type').alias('device_type'),
                F.lit("").alias('custom_tag'),
                F.struct(
                    # F.col('product_name').alias('prod_name'),
                    # F.col('product_category').alias('prod_category'),
                    # F.col('product_brand').alias('prod_brand'),
                    F.lit(0).alias('prod_price'),
                    F.col('pv.PRODUCT.REFERER').alias('prod_url'),
                    # F.col('product_img_url').alias('image_url'),
                    # F.col('product_sku').alias('prod_sku'),
                ).alias('product'),
                F.struct(
                    F.lit(0).alias('quantity'),
                    F.lit(0).alias('value'),
                    F.lit([]).alias('product_ids'),
                ).alias('cart'),
                F.struct(
                    F.lit('direct').alias('ref_type')
                ).alias('referrer')
            )))

display(cp_ua_df.limit(1000))

# COMMAND ----------

ua_df = (cp_ua_df.groupBy('advertiser_id', 'ip')
        .agg(F.collect_list(F.col('ua')).alias('user_activity'))
        .select('advertiser_id', 'ip', 'user_activity')
        #  .withColumn('conversion_activity', F.lit([]))
        .withColumn('id', F.col('ip')))

(ua_df.withColumn('conversion_activity', F.lit([]))
    .write.format("json")
    .mode("overwrite")
    .save(f"s3://sh-dw-external-tables-dev/topics/batch-user-activity/type=cp/dt=2025-01-22"))

# COMMAND ----------

display(ua_df.limit(1000))

# COMMAND ----------

idu_df = (event_file_df
    .filter((F.col('event_type_raw') == 'vast_start') | (F.col('event_type_raw') == 'vast_impression'))
    .join(ds_df, ['campaign_id', 'advertiser_id'], how='left')
    .filter(F.col('ip').isNotNull() & F.col('campaign_id').isNotNull() & F.col('data_source_id').isNotNull())
    .withColumn('data_source_ids', F.array(F.col('data_source_id'), F.lit(9)))
    .withColumn('epoch', F.unix_seconds(F.col('time')))
    .withColumn('data_source_id', F.explode('data_source_ids'))
    .withColumn('category_info', 
                F.struct(F.col('campaign_id').alias('category_id'), 
                F.col('epoch').alias('epoch'))
    )
    .groupBy('ip', 'data_source_id').agg(
        F.max(F.col('epoch')).alias('epoch'), 
        F.struct(
            F.col('data_source_id').alias('data_source'),
            F.struct(
                F.max(F.col('epoch')).alias('epoch'),
                F.array_agg('category_info').alias('category_info')
            ).alias('data_source')
        ).alias('data_source_info')
    )
    .withColumnRenamed('ip', 'id')
    .groupBy('id')
    .agg(
        F.max(F.col('epoch')).alias('epoch'), 
        F.map_from_entries(F.collect_list(F.col('data_source_info'))).alias('data_sources')
    )
    .withColumn('updateRequest', 
        F.struct( 
            F.col('id'), 
            F.col('epoch'),
            F.struct(F.col('data_sources')).alias('add_data_sources'), 
            F.lit(None).alias('remove_data_sources'),
            F.lit(None).alias('location_info'),
            F.lit(None).alias('household_score'),
            F.lit(None).alias('geo_version'),
            F.lit(None).alias('delta'),
            F.lit(None).alias('geo_loc_info'),
            F.lit(None).alias('metadata_info'),
        )
    )
 )

display(idu_df.limit(1000))

# (idu_df.select('updateRequest.*')
#             .write.format("json").mode("overwrite")
#             .save(f"s3://sh-dw-external-tables-dev/ip_data_updates/dt=2025-01-22"))