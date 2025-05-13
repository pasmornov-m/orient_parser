from etl.reader import read_htmls_from_minio, read_from_parquet, read_from_postgres
from etl.transformer import transform_html_to_tables, transform_tables
from etl.writer import write_to_parquet, write_to_postgres
from utils.pages_checker import check_processed_pages
from db_utils.check_postges import create_database, create_tables
from clients.minio_client import create_minio_client, ensure_bucket_exists
import pyspark.sql.functions as F
from datetime import datetime


NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def stage1(raw_bucket, processed_bucket):
    print(f"{NOW} -- stage1 start\n")

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, raw_bucket, processed_bucket)

    create_database()
    create_tables()

    print(f"{NOW} -- stage1 done\n")

def stage2(spark, postgres_props, bucket_raw, bucket_processed):

    print(f"{NOW} -- stage2 start\n")

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

    print(f"{NOW} -- stage2 done\n")

    return paths

def stage3(spark, raw_paths, bucket_processed):

    print(f"{NOW} -- stage3 start\n")

    raw_events_df = read_from_parquet(spark, raw_paths["events_raw"])
    # raw_events_df.count()
    print("raw_events_df")
    raw_distances_df = read_from_parquet(spark, raw_paths["distances_raw"])
    # raw_distances_df.count()
    print("raw_distances_df")
    raw_results_df = read_from_parquet(spark, raw_paths["results_raw"])
    # raw_results_df.count()
    print("raw_results_df")

    transformed_tables = transform_tables(raw_events_df, raw_distances_df, raw_results_df)
    print("transform_tables")
    
    paths = {}
    paths["transformed_events"] = f"s3a://{bucket_processed}/transformed_events/"
    paths["transformed_groups"] = f"s3a://{bucket_processed}/transformed_groups/"
    paths["transformed_participants"] = f"s3a://{bucket_processed}/transformed_participants/"
    paths["transformed_results"] = f"s3a://{bucket_processed}/transformed_results/"
    print("paths")

    write_to_parquet(transformed_tables["events"], paths["transformed_events"])
    write_to_parquet(transformed_tables["groups"], paths["transformed_groups"])
    write_to_parquet(transformed_tables["participants"], paths["transformed_participants"])
    write_to_parquet(transformed_tables["results"], paths["transformed_results"])
    print("write_to_parquet")

    print(f"{NOW} -- stage3 done\n")

    return paths

def stage4(spark, paths, postgres_props):

    print(f"{NOW} -- stage4 start\n")

    transformed_events = read_from_parquet(spark, paths["transformed_events"])
    transformed_groups = read_from_parquet(spark, paths["transformed_groups"])
    transformed_participants = read_from_parquet(spark, paths["transformed_participants"])
    transformed_results = read_from_parquet(spark, paths["transformed_results"])

    write_to_postgres(transformed_events, "events", postgres_props)

    events_lookup = read_from_postgres(spark, "events", postgres_props).select("event_id", "event_date").cache()

    processed_groups = transformed_groups.join(
        events_lookup,
        on="event_date",
        how="inner"
    ).select("group_name", "cp", "length_km", "event_id")

    write_to_postgres(processed_groups, "group_params", postgres_props)

    groups_lookup = read_from_postgres(spark, "group_params", postgres_props).select("group_id", "event_id", "group_name").cache()

    processed_participants = transformed_participants.groupBy("full_name", "birth_year").agg(F.collect_set("team").alias("team"))

    write_to_postgres(processed_participants, "participants", postgres_props)
    
    participants_lookup = read_from_postgres(spark, "participants", postgres_props).select("participant_id", "full_name", "team", "birth_year").cache()

    processed_results = transformed_results \
        .join(events_lookup, on="event_date", how="inner") \
        .join(groups_lookup, on=["event_id","group_name"], how="inner") \
        .join(participants_lookup, on=["full_name","birth_year"], how="inner") \
        .select(
            "event_id",
            "group_id",
            "participant_id",
            "position_number",
            "qualification",
            "bib_number",
            "finish_position",
            "result_time",
            "time_gap"
        )

    write_to_postgres(processed_results, "results", postgres_props)

    print(f"{NOW} -- stage4 done\n")