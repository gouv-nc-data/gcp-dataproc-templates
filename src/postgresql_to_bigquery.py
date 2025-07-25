from pyspark.sql import SparkSession
import argparse
from typing import Optional
from logging import Logger
import time


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


def upload_table(spark: SparkSession, table_name: str, url: str, dataset: str, mode: str, bucket: str):
    get_logger(spark).info("migration table %s" % table_name['table_name'])
    start_time = time.time()
    df = spark.read.jdbc(url, table_name['table_name'], properties={"driver": "org.postgresql.Driver"})
    elapsed_time = time.time() - start_time
    get_logger(spark).info(f"Table {table_name['table_name']} chargée en {elapsed_time:.2f} secondes.")
    
    # get_logger(spark).info(df.head())
    for c_name, c_type in df.dtypes:
        if c_type.startswith('decimal'):
            get_logger(spark).info("conversion de decimal vers float de la colonne %s" % c_name)
            df = df.withColumn(c_name, df[c_name].cast("float"))
    get_logger(spark).info("upload de la table %s" % table_name['table_name'])

    start_time = time.time()
    if len(bucket) > 0:
        df.write \
            .format("bigquery") \
            .option("temporaryGcsBucket", bucket) \
            .mode(mode) \
            .save("%s.%s" % (dataset, table_name['table_name']))
    else:
        df.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .mode(mode) \
            .save("%s.%s" % (dataset, table_name['table_name']))
    elapsed_time = time.time() - start_time
    get_logger(spark).info(f"Table {table_name['table_name']} uploadée en {elapsed_time:.2f} secondes.")

def query_factory(schema: str, exclude: str = None) -> str:
    if exclude != "":
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema
    return query


def run(spark: SparkSession, app_name: Optional[str], schema: str, url: str, dataset: str, mode: str, exclude: str, bucket: str):
    query = query_factory(schema, exclude)
    get_logger(spark).info("liste des tables : %s" % query)
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
        upload_table(spark, table_name, url, dataset, mode, bucket)

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

    parser.add_argument(
        '--bucket',
        type=str,
        dest='bucket',
        required=False,
        default="",
        help='bucket temporaire')

    known_args, pipeline_args = parser.parse_known_args()

    spark = SparkSession.builder \
        .appName("PostgreSQL Migration with PySpark") \
        .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    input_url = known_args.jdbc_url
    if input_url[:5] != "jdbc:":
        input_url = "jdbc:%s" % known_args.jdbc_url

    run(app_name="database transfert",
        spark=spark,
        schema=known_args.schema,
        url=input_url,
        dataset=known_args.dataset,
        mode=known_args.mode,
        exclude=known_args.exclude,
        bucket=known_args.bucket)
