from typing import Optional
from utils.variables import Tables as tb
from utils.variables import OutputTables as otb
from utils.utils import extract_table, write_result, with_spark
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f


@with_spark("hello_world_app")
def run(session: Optional[SparkSession] = None):
    some_df = extract_table(session, tb.test_table)

    result_df = some_df.select(
        f.col("some_value").alias("vl"),
        f.col("some_date").alias("dt"),
        f.col("some_partition_key").alias("pk")
    ).filter(f.col("pk") == "2024-01-01")

    write_result(session, result_df, output_table=otb.test_table, partitions_num=10)


if __name__ == '__main__':
    run()
