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
