from pyspark.sql import SparkSession
import json


def write_to_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def write_to_postgres(df, table, properties):
    df.write.jdbc(
        url=properties['url'],
        table=table,
        mode='append',
        properties={
            "user": properties['user'],
            "password": properties['password'],
            "driver": properties['driver']
        }
    )

def write_to_json(spark, data, path):
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(path)