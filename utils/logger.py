from pyspark.sql import SparkSession
import logging


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s]: %(message)s")

def get_spark_logger(spark: SparkSession):
    return spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
