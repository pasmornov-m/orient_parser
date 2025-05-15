from etl.reader import read_from_parquet, read_from_json
from etl.transformer import transform_tables
from etl.writer import write_to_parquet, write_to_json
from config import MINIO_TMP_PATH
from clients import spark_client
from pyspark.sql import SparkSession
import sys


def stage3(spark, bucket_processed):

    print(f"-- stage3 start\n")

    raw_paths = read_from_json(spark, MINIO_TMP_PATH)

    raw_events_df = read_from_parquet(spark, raw_paths["events_raw"])
    raw_distances_df = read_from_parquet(spark, raw_paths["distances_raw"])
    raw_results_df = read_from_parquet(spark, raw_paths["results_raw"])

    transformed_tables = transform_tables(raw_events_df, raw_distances_df, raw_results_df)
    
    paths = {}
    paths["transformed_events"] = f"s3a://{bucket_processed}/transformed_events/"
    paths["transformed_groups"] = f"s3a://{bucket_processed}/transformed_groups/"
    paths["transformed_participants"] = f"s3a://{bucket_processed}/transformed_participants/"
    paths["transformed_results"] = f"s3a://{bucket_processed}/transformed_results/"

    write_to_parquet(transformed_tables["events"], paths["transformed_events"])
    write_to_parquet(transformed_tables["groups"], paths["transformed_groups"])
    write_to_parquet(transformed_tables["participants"], paths["transformed_participants"])
    write_to_parquet(transformed_tables["results"], paths["transformed_results"])

    paths_list = [paths]
    write_to_json(spark, paths_list, MINIO_TMP_PATH)

    print(f"-- stage3 done\n")


if __name__ == "__main__":
    # spark = spark_client("stage3")
    spark = SparkSession.builder.appName("stage3").getOrCreate()
    bucket_processed = sys.argv[1]
    stage3(spark, bucket_processed)
    spark.stop()