from utils.readers import read_htmls_from_minio
from utils.transformer import transform_html_to_tables
from utils.writers import write_to_parquet, write_to_postgres, write_to_json
from utils.pages_checker import check_processed_pages
from clients.postgres_client import get_pg_props_spark
from utils.logger import log_to_table, get_logger
from config import MINIO_TMP_PATH, PAGES_PROCESSING_TABLE
from pyspark.sql import SparkSession
import sys


logger = get_logger(__name__)


@log_to_table()
def processing_raw_data(spark, bucket_raw, bucket_processed, db_name, schema_name):

    logger.info(f"processing_raw_data start\n")

    postgres_props = get_pg_props_spark(db_name=db_name)

    html_pairs = read_htmls_from_minio(bucket_raw)
    clean_pairs = check_processed_pages(spark, html_pairs, schema_name, postgres_props)
    events_df, distances_df, results_df, log_df = transform_html_to_tables(clean_pairs, spark)

    write_to_postgres(log_df, db_name=db_name, schema_name=schema_name, table=PAGES_PROCESSING_TABLE)

    paths = {}
    paths["events_raw"] = f"s3a://{bucket_processed}/events/"
    paths["distances_raw"] = f"s3a://{bucket_processed}/distances/"
    paths["results_raw"] = f"s3a://{bucket_processed}/results/"

    write_to_parquet(events_df, paths["events_raw"])
    write_to_parquet(distances_df, paths["distances_raw"])
    write_to_parquet(results_df, paths["results_raw"])

    paths_list = [paths]
    write_to_json(spark, paths_list, MINIO_TMP_PATH)

    logger.info(f"processing_raw_data done\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("processing_raw_data").getOrCreate()
    bucket_raw = sys.argv[1]
    bucket_processed = sys.argv[2]
    db_name = sys.argv[3]
    schema_name = sys.argv[4]
    processing_raw_data(spark, bucket_raw, bucket_processed, db_name, schema_name)
    spark.stop()