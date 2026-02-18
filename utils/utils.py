from pyspark import SparkConf
from pyspark.sql import SparkSession, Column, DataFrame
from typing import Optional
from utils.logger import log
import os


def start_session(app_name: str, **defaults):
    env_variable = os.environ.get("RUN_MODE", "local[1]")
    log.warn(f"Starting session... Run mode set to {env_variable}")
    match env_variable:
        case "TEST":
            master = "local[1]"
        case "CLUSTER":
            master = "yarn"
        case _:
            master = "local[1]"
    config = (
        SparkConf().setMaster(master)
        .setAppName(app_name)
        .set("spark.executor.memory", "2g")
        .set("spark.executor.cores", "2")
        .setAll(defaults)
    )
    session = SparkSession.builder.config(conf=config).enableHiveSupport().getOrCreate()
    log.warn("Session started")
    return session


def extract_table(spark: SparkSession,
                  table_name: str,
                  condition: Optional[Column] = None,
                  columns: Optional[Column] = "*"):
    log.warn(f"Extracted table {table_name}")
    return spark.table(table_name).select(columns) if not condition \
        else spark.table(table_name).select(columns).filter(condition)


def write_result(spark: SparkSession,
                 result_df: DataFrame,
                 output_table: str,
                 partitions_num: int,
                 partition_column: str = None,
                 write_mode: str = "overwrite",
                 compression_type: str = "snappy",
                 backup: bool = True):
    if spark.catalog.tableExists(output_table) and write_mode == "overwrite" and backup:
        log.warning(f"Table {output_table} existing. Backup set true, starting to create backup...")
        spark.table(output_table) \
            .write \
            .option("compression", "gzip") \
            .mode("overwrite") \
            .saveAsTable(output_table+"_bkp")
        log.warn(f"Backup saved at {output_table}_bkp")

    log.warn("Started saving table")
    if partitions_num:
        result_df = result_df.repartition(partitions_num)

    writer = result_df.write.mode(write_mode)

    if partition_column:
        writer = writer.partitionBy(partition_column)

    if compression_type:
        writer = writer.option("compression", compression_type)

    writer.saveAsTable(output_table)
