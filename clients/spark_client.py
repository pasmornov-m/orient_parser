from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_MASTER, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT


def create_spark_session():
    jars = [
        "/opt/spark/jars/postgresql-42.7.5.jar",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/jars/hadoop-common-3.3.4.jar"
    ]
    jars_str = ",".join(jars)
    return SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
    # .config("spark.jars", jars_str) \
