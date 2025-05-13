from clients.minio_client import create_minio_client, list_objects


def read_htmls_from_minio(bucket_name):
    client = create_minio_client()
    objects = list_objects(client, bucket_name)

    html_pairs = []
    for object_name in objects:
        html = client.get_object(bucket_name, object_name).read().decode('windows-1251')
        html_pairs.append([object_name, html])

    return html_pairs

def read_from_parquet(spark, path):
    return spark.read.parquet(path)

def read_from_postgres(spark, table, properties):
    return spark.read.jdbc(
        url=properties['url'],
        table=table,
        properties={
            "user": properties['user'],
            "password": properties['password'],
            "driver": properties['driver']
        }
    )
