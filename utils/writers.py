from clients.postgres_client import get_pg_props_spark

def write_to_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def write_to_postgres(df, db_name, schema_name, table, mode='append'):
    db_props = get_pg_props_spark(db_name)
    df.write.jdbc(
        url=db_props['url'],
        table=f"{schema_name}.{table}",
        mode=mode,
        properties={
            "user": db_props['user'],
            "password": db_props['password'],
            "driver": db_props['driver']
        }
    )

def write_to_json(spark, data, path):
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(path)