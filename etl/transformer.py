from parsers.html_processor import VRNFSO_html_processor_spark
from utils.spark_helper import union_all
from schemas.spark_schemas import EVENT_SCHEMA, DIST_SCHEMA, RESULTS_SCHEMA
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    

def transform_html_to_tables(pairs, spark):

    events_dfs, dist_dfs, res_dfs = [], [], []

    log_schema = StructType([StructField("page_name", StringType(), False),
                         StructField("date", TimestampType(), False)
                         ])
    pages_log = []

    for url_date, html in pairs:
        parser = VRNFSO_html_processor_spark(url_date, html, spark)

        df_ev = parser.parse_events()
        df_ds = parser.parse_distances()
        df_rs = parser.parse_results()

        if df_ev is not None:
            events_dfs.append(df_ev)
        if df_ds is not None:
            dist_dfs.append(df_ds)
        if df_rs is not None:
            res_dfs.append(df_rs)
        pages_log.append((url_date, datetime.now()))
    
    pages_log = spark.createDataFrame(pages_log, schema=log_schema)

    return (
        union_all(spark, events_dfs, EVENT_SCHEMA),
        union_all(spark, dist_dfs, DIST_SCHEMA),
        union_all(spark, res_dfs, RESULTS_SCHEMA),
        pages_log
    )

def transform_tables(df_events, df_distances, df_results):

    transformed_events = df_events.select(F.substring(F.col("event_name"), 1, 100).alias("event_name"), 
                                      F.col("event_date"), 
                                      F.substring(F.col("city"), 1, 50).alias("city")).distinct()

    transformed_groups = df_distances.select(F.col("event_date"), 
                                            F.substring(F.col("group_name"), 1, 20).alias("group_name"), 
                                            F.col("cp"), 
                                            F.col("length_km")).distinct()

    transformed_participants = df_results.select(F.col("full_name"), 
                                                F.substring(F.col("team"), 1, 50).alias("team"), 
                                                F.col("birth_year")).distinct()

    transformed_results = df_results.select(F.col("event_date"), 
                                            F.col("group_name"),
                                            F.col("position_number"),
                                            F.col("full_name"),
                                            F.col("team"),
                                            F.substring(F.col("qualification"), 1, 10).alias("qualification"),
                                            F.col("bib_number"),
                                            F.col("birth_year"),
                                            F.col("finish_position"),
                                            F.col("result_time"),
                                            F.col("time_gap"))

    return {
        "events": transformed_events, 
        "groups": transformed_groups, 
        "participants": transformed_participants,
        "results": transformed_results
    }