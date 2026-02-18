from typing import Optional
from utils.variables import Tables as tb
from utils.utils import start_session, extract_table, write_result
from pyspark.sql.session import SparkSession
import utils.logger
import pyspark.sql.functions as f


def run(session: Optional[SparkSession] = None):
    spark = start_session("hello_world") if session is None else session
    utils.logger.log = utils.logger.get_spark_logger(spark)

    some_df = extract_table(spark, tb.test_table)

    result_df = some_df.select(
        f.col("some_value").alias("vl"),
        f.col("some_date").alias("dt"),
        f.col("some_partition_key").alias("pk")
    ).filter(f.col("pk") == "2024-01-01")

    write_result(spark, result_df, output_table=tb.output_test_table, partitions_num=10)


if __name__ == '__main__':
    run()
