from etl.reader import read_from_parquet, read_from_json, read_from_postgres
from etl.writer import write_to_postgres
from config import MINIO_TMP_PATH
from clients.postgres_client import get_postgres_properties
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def stage4(spark, postgres_props):

    print(f"-- stage4 start\n")

    paths = read_from_json(spark, MINIO_TMP_PATH)

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

    print(f"-- stage4 done\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("stage4").getOrCreate()
    postgres_props = get_postgres_properties()
    stage4(spark, postgres_props)
    spark.stop()