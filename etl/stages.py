from etl.reader import read_htmls_from_minio, read_from_parquet, read_from_postgres
from etl.transformer import transform_html_to_tables, transform_tables
from etl.writer import write_to_parquet, write_to_postgres
import pyspark.sql.functions as F


def stage1(spark, bucket_raw, bucket_processed):

    html_pairs = read_htmls_from_minio(bucket_raw)
    events_df, distances_df, results_df = transform_html_to_tables(html_pairs, spark)

    paths = {}
    paths["events_raw"] = f"s3a://{bucket_processed}/events/"
    paths["distances_raw"] = f"s3a://{bucket_processed}/distances/"
    paths["results_raw"] = f"s3a://{bucket_processed}/results/"

    write_to_parquet(events_df, paths["events_raw"])
    write_to_parquet(distances_df, paths["distances_raw"])
    write_to_parquet(results_df, paths["results_raw"])

    return paths

def stage2(spark, raw_paths, bucket_processed):

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

    return {
        "events":       transformed_tables["events"],
        "groups":       transformed_tables["groups"],
        "participants": transformed_tables["participants"],
        "results":      transformed_tables["results"]
    }

def stage3(spark, dfs, postgres_props):

    write_to_postgres(dfs["events"], "events", postgres_props)

    events_lookup = read_from_postgres(spark, "events", postgres_props).select("event_id", "event_date").cache()

    processed_groups = dfs["groups"].join(
        events_lookup,
        on="event_date",
        how="inner"
    ).select("group_name", "cp", "length_km", "event_id")

    write_to_postgres(processed_groups, "group_params", postgres_props)

    groups_lookup = read_from_postgres(spark, "group_params", postgres_props).select("group_id", "event_id", "group_name").cache()

    processed_participants = dfs["participants"].groupBy("full_name", "birth_year").agg(F.collect_set("team").alias("team"))

    write_to_postgres(processed_participants, "participants", postgres_props)
    
    participants_lookup = read_from_postgres(spark, "participants", postgres_props).select("participant_id", "full_name", "team", "birth_year").cache()

    processed_results = dfs["results"] \
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