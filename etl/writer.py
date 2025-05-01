def write_to_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)

def write_to_postgres(df, table: str, properties: dict):
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
