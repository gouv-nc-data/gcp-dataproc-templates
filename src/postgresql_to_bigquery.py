from pyspark.sql import SparkSession
import argparse
from typing import Optional
from logging import Logger


def get_logger(spark: SparkSession) -> Logger:
    """
    Convenience method to get the Spark logger from a SparkSession

    Args:
        spark (SparkSession): The initialized SparkSession object

    Returns:
        Logger: The Spark logger
    """

    log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
    return log_4j_logger.LogManager.getLogger(__name__)


def upload_table(spark: SparkSession, table_name: str, url: str, dataset: str, mode: str):
    get_logger(spark).info("migration table %s" % table_name['table_name'])
    df = spark.read.jdbc(url, table_name['table_name'], properties={"driver": "org.postgresql.Driver"})
    get_logger(spark).info("###############################################")
    get_logger(spark).info(df.head())
    get_logger(spark).info("###############################################")
    get_logger(spark).info(df.dtypes)
    get_logger(spark).info("###############################################")
    import pyspark.sql.functions as F
    for c_name, c_type in df.dtypes:
        if c_type in ('double', 'float', 'decimal'):
            df = df.withColumn(c_name, F.round(c_name, 4))
    get_logger(spark).info("upload de la table %s" % table_name['table_name'])
    get_logger(spark).info(df.dtypes)
    get_logger(spark).info("###############################################")
    df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .mode(mode) \
        .save("%s.%s" % (dataset, table_name['table_name']))


def query_factory(schema: str, exclude: str = None) -> str:
    if exclude != "":
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema
    return query


def run(spark: SparkSession, app_name: Optional[str], schema: str, url: str, dataset: str, mode: str, exclude: str):
    query = query_factory(schema, exclude)

    table_names = spark.read \
                       .format("jdbc") \
                       .option("url", url) \
                       .option("driver", "org.postgresql.Driver") \
                       .option("query", query) \
                       .option("TimeStampFormat", "dd-MM-yyyy HH:mm:ss") \
                       .option("TreatEmptyValuesAsNulls", True) \
                       .option("IgnoreLeadingWhiteSpace", True) \
                       .option("IgnoreTrailingWhiteSpace", True) \
                       .load()

    get_logger(spark).info("migration de %s tables" % table_names.count())

    for table_name in table_names.collect():
        upload_table(spark, table_name, url, dataset, mode)

    get_logger(spark).info("fin migration")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--jdbc-url',
        type=str,
        dest='jdbc_url',
        required=True,
        help='URL JDBC vers la bdd source')

    parser.add_argument(
        '--schema',
        type=str,
        dest='schema',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--dataset',
        type=str,
        dest='dataset',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--mode',
        type=str,
        dest='mode',
        required=False,
        default="overwrite",
        help='schéma à migrer')

    parser.add_argument(
        '--exclude',
        type=str,
        dest='exclude',
        required=False,
        default="",
        help='tables à exclure de la migration')

    known_args, pipeline_args = parser.parse_known_args()

    spark = SparkSession.builder \
        .appName("PostgreSQL Migration with PySpark") \
        .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
        
    run(app_name="database transfert",
        spark=spark,
        schema=known_args.schema,
        url="jdbc:%s" % known_args.jdbc_url,
        dataset=known_args.dataset,
        mode=known_args.mode,
        exclude=known_args.exclude)
