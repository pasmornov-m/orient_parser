from utils.readers import read_from_parquet, read_from_json
from utils.transformer import transform_tables
from utils.writers import write_to_parquet, write_to_json
from config import MINIO_TMP_PATH
from pyspark.sql import SparkSession
import sys


def transform_raw_data(spark, bucket_processed):

    print(f"-- transform_raw_data start\n")

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

    print(f"-- transform_raw_data done\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("transform_raw_data").getOrCreate()
    bucket_processed = sys.argv[1]
    transform_raw_data(spark, bucket_processed)
    spark.stop()