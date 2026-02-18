from pyspark.sql import SparkSession
from utils.variables import Tables
from utils.logger import log
import os


def execute_hql(spark: SparkSession, table: str):
    splitted_tbl = table.split(".")
    cur_path = os.path.curdir
    log.warn(f"Current path is {cur_path}")
    db, tbl = splitted_tbl[0], splitted_tbl[1]
    if not spark.catalog.databaseExists(db):
        log.warn(f"Creating database {db}")
        spark.sql(f"CREATE DATABASE {db}")
    if not spark.catalog.tableExists(table):
        with open(cur_path+f"/resources/hql/{db}/{tbl}.hql") as hql_file:
            log.warn(f"Reading DDL at {hql_file.name}")
            query = hql_file.read()
            log.warn(f"Creating table {table}")
            spark.sql(query)


def create_all_resoures(spark: SparkSession):
    log.warn("Getting tables from variables")
    tables = Tables()
    for table in tables:
        execute_hql(spark, table)


def drop_test_tables(spark: SparkSession):
    for db in spark.catalog.listDatabases():
        if db.name == "default":
            continue
        for table in spark.catalog.listTables(db.name):
            spark.sql(f"DROP TABLE {db.name+'.'+table.name}")
        spark.sql(f"DROP DATABASE {db.name}")

