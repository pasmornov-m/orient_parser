from utils.readers import read_from_parquet, read_from_json, read_from_postgres
from utils.writers import write_to_postgres
from config import MINIO_TMP_PATH
from clients.postgres_client import get_pg_props_spark
from utils.logger import log_to_table, get_logger
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import sys


logger = get_logger(__name__)

@log_to_table()
def write_pg_final_tables(spark, db_name, schema_name):

    logger.info(f"-- write_pg_final_tables start\n")

    postgres_props = get_pg_props_spark(db_name)

    paths = read_from_json(spark, MINIO_TMP_PATH)

    transformed_events = read_from_parquet(spark, paths["transformed_events"])
    transformed_groups = read_from_parquet(spark, paths["transformed_groups"])
    transformed_participants = read_from_parquet(spark, paths["transformed_participants"])
    transformed_results = read_from_parquet(spark, paths["transformed_results"])

    write_to_postgres(df=transformed_events, db_name=db_name, schema_name=schema_name, table="events")

    events_lookup = read_from_postgres(spark, f"{schema_name}.events", postgres_props).select("event_id", "event_date").cache()

    processed_groups = transformed_groups.join(
        events_lookup,
        on="event_date",
        how="inner"
    ).select("group_name", "cp", "length_km", "event_id")

    write_to_postgres(df=processed_groups, db_name=db_name, schema_name=schema_name, table="group_params")

    groups_lookup = read_from_postgres(spark, f"{schema_name}.group_params", postgres_props).select("group_id", "event_id", "group_name").cache()

    processed_participants = transformed_participants.groupBy("full_name", "birth_year").agg(F.collect_set("team").alias("team"))

    write_to_postgres(df=processed_participants, db_name=db_name, schema_name=schema_name, table="participants")
    
    participants_lookup = read_from_postgres(spark, f"{schema_name}.participants", postgres_props).select("participant_id", "full_name", "team", "birth_year").cache()

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

    write_to_postgres(df=processed_results, db_name=db_name, schema_name=schema_name, table="results")

    logger.info(f"-- write_pg_final_tables done\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("write_pg_final_tables").getOrCreate()
    db_name = sys.argv[1]
    schema_name = sys.argv[2]
    write_pg_final_tables(spark, db_name, schema_name)
    spark.stop()