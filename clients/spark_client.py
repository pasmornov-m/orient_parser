from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_MASTER, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT


def create_spark_session(app_name=SPARK_APP_NAME):
    return SparkSession.builder \
    .appName(app_name) \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars", ",".join([
        "/opt/spark/spark_jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/spark_jars/jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/spark_jars/postgresql-42.7.5.jar",
        "/opt/spark/spark_jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/spark_jars/checker-qual-3.48.3.jar"
    ])) \
    .getOrCreate()
