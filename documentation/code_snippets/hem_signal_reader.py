import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession


class HEMSignalReader:
    def __init__(
        self,
        spark: SparkSession,
        env: str,
        max_partition_dt: str,
    ):
        delta_load_days = 30
        snapshot_lookback_days = 36

        self._env = env
        self._spark = spark

        dt_base = datetime.date.fromisoformat(max_partition_dt) + datetime.timedelta(days=1)
        self._utc_end_dt = dt_base.isoformat()
        self._utc_lookback_start_dt = (
            dt_base - datetime.timedelta(days=snapshot_lookback_days)
        ).isoformat()
        self._utc_delta_start_dt = (dt_base - datetime.timedelta(days=delta_load_days)).isoformat()
        self.__filter_expression: Optional[str] = None

        self._describe_reader()

    @property
    def hem_signal_location(self) -> str:
        return "gs://mntn-data-archive-prod/signals/hashed_email_signal/"

    def _describe_reader(self) -> None:
        print("-----------hashed_email_signal read summary--------------------")
        print("utc_end_dt", self._utc_end_dt)
        print("utc_lookback_start_dt", self._utc_lookback_start_dt)
        print("utc_delta_start_dt", self._utc_delta_start_dt)
        print("hem signal location", self.hem_signal_location)
        print("---------------------------------------------------------------")

    def _read_saved_ds_mode_decisions(self) -> DataFrame:
        ds_inventory = Row("data_source_id", "ds_save_mode_override")

        data_source_inventory = [
            ds_inventory(21, "delta"),
            ds_inventory(22, "snapshot"),
            ds_inventory(23, "delta"),
            ds_inventory(26, "delta"),
            ds_inventory(29, "snapshot"),
        ]

        df = (
            self._spark.createDataFrame(data_source_inventory)
            .select(F.col("data_source_id").cast("int"), "ds_save_mode_override")
            .groupBy("data_source_id")
            .agg(F.max("ds_save_mode_override").alias("ds_save_mode_override"))
        )

        # show for troubleshooting purposes if issue arises with any particular data source
        df.show(truncate=False)

        return df

    def _read_hem_signal_summary(self) -> DataFrame:
        look_back_dt_condition = (
            f"dt >= '{self._utc_lookback_start_dt}' and dt < '{self._utc_end_dt}'"
        )
        print(f"Reading {self.hem_signal_location} files summary for", look_back_dt_condition)
        base_df = (
            self._spark.read.option("basePath", self.hem_signal_location)
            .format("binaryFile")
            .load(f"{self.hem_signal_location}/dt=????-??-??/hh=[0-2][0-9]/data_source_id=??/hash_type=sha256/")
            .drop("content")
            .where(look_back_dt_condition)
            .where("hash_type = 'sha256'")
        )

        dt_hh_df = base_df.select(
            "data_source_id",
            F.col("dt").cast("string").alias("dt"),
            "hh",
        ).withColumn("dt_hh", F.struct("dt", "hh"))

        summary_df = dt_hh_df.groupBy("data_source_id").agg(
            F.max("dt").alias("dt_max"),
            F.max("dt_hh").alias("dt_hh_max"),
        )
        return summary_df

    def __extract_filter_expression(self, partition_state: list) -> str:
        ds_condition = {}
        for h in partition_state:
            if h.dt_max != h.dt_hh_max.dt:
                msg = (
                    "There is issue in deriving latest partition "
                    f"date for data source {h.data_source_id}. "
                    f"Should be {h.dt_max}, but got {h.dt_hh_max.dt}"
                )
                raise ValueError(msg)
            if h.ds_type.ds_type.lower() == "snapshot":
                ds_condition[h.data_source_id] = (
                    f"(data_source_id = '{h.data_source_id}' "
                    f"and dt = '{h.dt_hh_max.dt}' "
                    f"and hh = '{h.dt_hh_max.hh}')"
                )
            else:
                ds_condition[h.data_source_id] = (
                    f"(data_source_id = '{h.data_source_id}' "
                    f"and dt >= '{self._utc_delta_start_dt}' "
                    f"and dt < '{self._utc_end_dt}')"
                )

        ds_condition_arr = list(ds_condition.values())
        res = " or ".join(ds_condition_arr)
        return res

    @property
    def filter_expression(self) -> str:
        if self.__filter_expression is not None:
            print("Reusing filter expression ", self.__filter_expression)
            return self.__filter_expression

        saved_df = self._read_saved_ds_mode_decisions()
        summary_df = self._read_hem_signal_summary()

        ds_type_df = (
            summary_df.alias("dm")
            .join(F.broadcast(saved_df.alias("sm")), on=["data_source_id"], how="inner")
            .withColumn(
                "ds_type",
                F.expr(
                    """
                CASE
                    WHEN sm.ds_save_mode_override = 'snapshot' THEN struct("snapshot" as ds_type, 1 as final_decision, 0 as decision_id)
                    ELSE struct("delta" as ds_type, 1 as final_decision, 0 as decision_id)
                    END
                """
                ),
            )
            .repartition(1)
        )
        ds_type_df.cache()
        ds_type_df.show(truncate=False)
        summary_data = ds_type_df.collect()
        self.__filter_expression = self.__extract_filter_expression(summary_data)
        return self.__filter_expression

    def hem_signal_df(self) -> DataFrame:
        fltr_expr = self.filter_expression
        print("hem_signal filter expression:", fltr_expr)
        hem_df = (
            self._spark.read.option("basePath", self.hem_signal_location)
            .parquet(f"{self.hem_signal_location}/dt=????-??-??/hh=[0-2][0-9]/data_source_id=??/hash_type=sha256/")
            .where("hash_type = 'sha256'")
            .where(fltr_expr)
            .select(
                "uid",
                "ip",
                F.col("hashed_email").alias("hem_sha256"),
                "time",
                "dt",
                "data_source_id",
            )
        )
        return hem_df


class HashedPhoneSignalReader:
    def __init__(
        self,
        spark: SparkSession,
        env: str,
        max_partition_dt: str,
    ):
        delta_load_days = 30
        snapshot_lookback_days = 360

        self._env = env
        self._spark = spark

        dt_base = datetime.date.fromisoformat(max_partition_dt) + datetime.timedelta(days=1)
        self._utc_end_dt = dt_base.isoformat()
        self._utc_lookback_start_dt = (
            dt_base - datetime.timedelta(days=snapshot_lookback_days)
        ).isoformat()
        self._utc_delta_start_dt = (dt_base - datetime.timedelta(days=delta_load_days)).isoformat()
        self.__filter_expression: Optional[str] = None

        self._describe_reader()

    @property
    def signal_location(self) -> str:
        return "gs://mntn-data-archive-prod/signals/hashed_phone_signal/"

    def _describe_reader(self) -> None:
        print("")
        print("-----------hashed_phone_signal read summary--------------------")
        print("utc_end_dt", self._utc_end_dt)
        print("utc_delta_start_dt", self._utc_delta_start_dt)
        print("hashed signal location", self.signal_location)
        print("---------------------------------------------------------------")

    def _read_saved_ds_mode_decisions(self) -> dict:
        data_source_inventory = {
            22: "snapshot",
            29: "snapshot",
        }

        print("Data sources processing type:")
        print("-----------------------------")
        for data_source_id, ds_type in data_source_inventory.items():
            print(f"{data_source_id}={ds_type}")
        print("-----------------------------")
        return data_source_inventory

    def __get_dirs_in_location(self, s3_path: str) -> list:
        print(f"Listing {s3_path}")
        hadoop = self._spark.sparkContext._gateway.jvm.org.apache.hadoop  # type: ignore[union-attr]
        URI = self._spark.sparkContext._gateway.jvm.java.net.URI  # type: ignore[union-attr]
        fs = hadoop.fs.FileSystem.get(URI(s3_path), hadoop.conf.Configuration())
        status = fs.listStatus(hadoop.fs.Path(s3_path))

        result = []
        for fileStatus in status:
            if fileStatus.isDirectory():
                result.append(fileStatus.getPath().toString())
        return result

    @classmethod
    def __extract_hive_partition(cls, expected_name: str, path_str: str) -> list:
        clean_path = path_str.rstrip().rstrip("/")
        fldr = clean_path.split("/")[-1]
        hive_partition = fldr.split("=")
        if len(hive_partition) != 2:
            raise ValueError(
                f"Invalid hive partition folder: '{fldr}'. Expected format '{expected_name}=<number>'"
            )
        if hive_partition[0] != expected_name:
            raise ValueError(
                f"Invalid hive partition column name: '{hive_partition[0]}'. Expected name: '{expected_name}'"
            )

        return hive_partition

    def __get_data_source_locations(self) -> dict:
        res = {}
        ds_dirs = self.__get_dirs_in_location(self.signal_location)
        for d in ds_dirs:
            hive_partition = self.__extract_hive_partition(
                expected_name="data_source_id", path_str=d
            )
            res[hive_partition[1]] = d

        return res

    def __get_data_source_dates(self, path_str: str) -> list:
        dt_dirs = self.__get_dirs_in_location(path_str)
        dt_res = []
        for d in dt_dirs:
            hive_partition = self.__extract_hive_partition(expected_name="dt", path_str=d)
            dt = hive_partition[1]
            datetime.date.fromisoformat(dt)
            dt_res.append(dt)

        print(f"dates in location {path_str}: {dt_res}")
        return dt_res

    def __extract_filter_expression(self) -> str:
        ds_save_mode = self._read_saved_ds_mode_decisions()
        data_sources = self.__get_data_source_locations()
        ds_condition = {}

        for ds_id_str, loc in data_sources.items():
            ds_id = int(ds_id_str)
            if ds_id not in ds_save_mode:
                print(
                    f"Skipping data_source_id {ds_id} as data source save mode (delta or snapshot) is not defined for it {ds_save_mode}"
                )
            elif ds_save_mode[ds_id] == "snapshot":
                max_dt = max(self.__get_data_source_dates(loc))
                if max_dt >= self._utc_lookback_start_dt:
                    ds_condition[ds_id] = f"(data_source_id = '{ds_id}' and dt = '{max_dt}')"
                else:
                    print(
                        f"Skipping data_source_id {ds_id} as max_dt {max_dt} is older than snapshot lookback start date {self._utc_lookback_start_dt}"
                    )
            else:
                ds_condition[ds_id] = (
                    f"(data_source_id = '{ds_id}' "
                    f"and dt >= '{self._utc_delta_start_dt}' "
                    f"and dt < '{self._utc_end_dt}')"
                )

        ds_condition_arr = list(ds_condition.values())
        if len(ds_condition_arr) < 1:
            raise ValueError("There should be at least 1 data source to read")
        res = " or ".join(ds_condition_arr)
        return res

    @property
    def filter_expression(self) -> str:
        if self.__filter_expression is not None:
            print("Reusing filter expression ", self.__filter_expression)
            return self.__filter_expression
        else:
            self.__filter_expression = self.__extract_filter_expression()
            return self.__filter_expression

    def signal_df(self) -> DataFrame:
        fltr_expr = self.filter_expression
        print("Hashed phone signal filter expression:", fltr_expr)
        hem_df = (
            self._spark.read.option("basePath", self.signal_location)
            .parquet(self.signal_location)
            .where(fltr_expr)
            .select(
                "uid",
                "ip",
                "phone_sha256",
                "time",
                "dt",
                "data_source_id",
            )
        )
        return hem_df