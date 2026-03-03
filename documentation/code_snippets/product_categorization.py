from datetime import datetime
from datetime import timedelta

from databricks.vector_search.client import VectorSearchClient


def delete_vector_search_index(endpoint_name: str, index_fqn: str) -> None:
    client = VectorSearchClient(disable_notice=True)
    indexes = client.list_indexes(endpoint_name)['vector_indexes']
    current_index_info = [i for i in indexes if i['name'] == index_fqn]
    if current_index_info:
        print(f"Dropping the index {index_fqn} from endpoint {endpoint_name}")
        client.delete_index(endpoint_name=endpoint_name, index_name=index_fqn)


def model(dbt, session):
    run_date = dbt.config.get("run_date")
    dbt.config(file_format="parquet")
    dbt.config(partition_by="dt")
    dbt.config.get("location_root")
    dbt.config(materialized="incremental")
    dbt.config(incremental_strategy="append")
    dbt.config(unique_key=["composite_key"])

    cfg_endpoints = dbt.config.get("llm_mm_taxonomy_lookup")
    cfg_vector_endpoint = cfg_endpoints["vector_endpoint"]
    cfg_index_fqn = cfg_endpoints["vector_index"]

    # Deleting index as we don't need it anymore after insertion of new
    # data source category ids into mntn_matched_taxonomy
    delete_vector_search_index(
        endpoint_name=cfg_vector_endpoint,
        index_fqn=cfg_index_fqn
    )

    yesterday = datetime.strftime(
        datetime.strptime(run_date, "%Y-%m-%d") - timedelta(1), "%Y-%m-%d"
    )

    pct_df = dbt.ref("product_categorization_temp").filter(f"dt = '{yesterday}'")
    pct_df.createOrReplaceTempView("pct")

    mmt_df = dbt.ref("mntn_matched_taxonomy")
    mmt_df.createOrReplaceTempView("mmt")

    all_rows = (
        session.sql(
            """
                SELECT
                p.composite_key
                , p.data_source_id
                , p.domain
                , p.product_industry
                , p.product_subindustry
                , p.product_category
                , p.product_subcategory
                , p.data_source_category_id
                , p.dt
                 FROM pct p
                 left semi join mmt m
                    on p.data_source_category_id[0] = m.data_source_category_id
                """
        )
        .repartition(50)
    )

    if dbt.is_incremental:
        this_df = session.table(f"{dbt.this}").filter(f"dt = '{yesterday}'")
        if not this_df.isEmpty():
            msg = (f"Table {dbt.this} has data in partition dt='{yesterday}'. "
                   f"Consider deleting files in the partition before re-running")
            raise FileExistsError(msg)
        all_rows.filter(f"dt = '{yesterday}'")

    return all_rows
