from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

EVENT_SCHEMA = StructType([
    StructField("event_name", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("city", StringType(), True),
])
DIST_SCHEMA = StructType([
    StructField("event_date", DateType(), True),
    StructField("group_name", StringType(), True),
    StructField("cp", IntegerType(), True),
    StructField("length_km", FloatType(), True),
])
RESULTS_SCHEMA = StructType([
    StructField("event_date", DateType(), True),
    StructField("group_name", StringType(), True),
    StructField("position_number", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("team", StringType(), True),
    StructField("qualification", StringType(), True),
    StructField("bib_number", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("result_time", StringType(), True),
    StructField("time_gap", StringType(), True),
    StructField("finish_position", IntegerType(), True),
])