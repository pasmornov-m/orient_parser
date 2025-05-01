from clients.minio_client import create_minio_client, list_objects
from pyspark.sql import SparkSession

def read_files_from_minio(spark, bucket_name):
    client = create_minio_client()

    objects = list_objects(client, bucket_name)

    def load_and_decode(path):
        # path: s3a://bucket/html/...
        # выделим object_name:
        object_name = path.split(f"{bucket_name}/", 1)[1]
        data = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark._jsc.hadoopConfiguration()) \
            .open(spark._jvm.org.apache.hadoop.fs.Path(path)) \
            .readAllBytes()
        html = bytearray(data).decode('windows-1251')
        return object_name, html

    paths = [f"s3a://{bucket_name}/{obj}" for obj in objects]
    rdd = spark.sparkContext.parallelize(paths).map(load_and_decode)
    return rdd
