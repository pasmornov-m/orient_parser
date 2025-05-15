from etl.reader import read_htmls_from_minio
from etl.transformer import transform_html_to_tables
from etl.writer import write_to_parquet, write_to_postgres, write_to_json
from utils.pages_checker import check_processed_pages
from clients.postgres_client import get_postgres_properties
from config import MINIO_TMP_PATH
from pyspark.sql import SparkSession
import sys


def stage2(spark, postgres_props, bucket_raw, bucket_processed):

    print(f"Stage2 start\n")

    html_pairs = read_htmls_from_minio(bucket_raw)
    clean_pairs = check_processed_pages(spark, html_pairs, postgres_props)
    events_df, distances_df, results_df, log_df = transform_html_to_tables(clean_pairs, spark)
    write_to_postgres(log_df, "pages_processing_log", postgres_props)

    paths = {}
    paths["events_raw"] = f"s3a://{bucket_processed}/events/"
    paths["distances_raw"] = f"s3a://{bucket_processed}/distances/"
    paths["results_raw"] = f"s3a://{bucket_processed}/results/"

    write_to_parquet(events_df, paths["events_raw"])
    write_to_parquet(distances_df, paths["distances_raw"])
    write_to_parquet(results_df, paths["results_raw"])

    paths_list = [paths]
    write_to_json(spark, paths_list, MINIO_TMP_PATH)

    print(f"Stage2 done\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("stage2").getOrCreate()
    bucket_raw = sys.argv[1]
    bucket_processed = sys.argv[2]
    postgres_props = get_postgres_properties()
    stage2(spark, postgres_props, bucket_raw, bucket_processed)
    spark.stop()