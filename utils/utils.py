from functools import wraps
from pyspark import SparkConf
from pyspark.sql import SparkSession, Column, DataFrame
from typing import Optional
from utils.logger import log
import utils.logger
import os
import inspect


def start_session(app_name: str, **defaults):
    env_variable = os.environ.get("RUN_MODE", "local[1]")
    log.info(f"Starting session... Run mode set to {env_variable}")
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
    log.info("Session started")
    return session


def with_spark(app_name: str, **defaults):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            session_provided = False
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())

            if 'session' in kwargs and kwargs['session'] is not None:
                session_provided = True
                spark = kwargs['session']
            elif params and len(args) > 0 and params[0] == 'session' and args[0] is not None:
                session_provided = True
                spark = args[0]
            else:
                spark = start_session(app_name, **defaults)
                utils.logger.log = utils.logger.get_spark_logger(spark)
                kwargs['session'] = spark

            try:
                return func(*args, **kwargs)
            finally:
                if not session_provided:
                    log.info("Stopping Spark session")
                    spark.stop()

        return wrapper
    return decorator


def extract_table(spark: SparkSession,
                  table_name: str,
                  condition: Optional[Column] = None,
                  columns: Optional[Column] = "*"):
    log.info(f"Extracted table {table_name}")
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
        log.info(f"Backup saved at {output_table}_bkp")

    log.info("Started saving table")
    if partitions_num:
        result_df = result_df.repartition(partitions_num)

    writer = result_df.write.mode(write_mode)

    if partition_column:
        writer = writer.partitionBy(partition_column)

    if compression_type:
        writer = writer.option("compression", compression_type)

    writer.saveAsTable(output_table)
