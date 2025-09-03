from dotenv import load_dotenv
import os

load_dotenv()


# MINIO

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = "minio:9000"
MINIO_ENDPOINT_LOCAL = "localhost:9000"
MINIO_BUCKET_RAW = "raw-html"
MINIO_BUCKET_PROCESSED = "processed-data"
MINIO_BUCKET_TMP = "temp"
MINIO_TMP_PATH = f"s3a://{MINIO_BUCKET_TMP}/json_path"


# POSTGRES

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = "db"
POSTGRES_PORT = 5432
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/"
ORIENT_DB_NAME = 'orient_data'
ORIENT_SCHEMA_NAME = 'orient_data'
LOG_TABLE = 'etl_log'
PAGES_PROCESSING_TABLE = "pages_processing_log"

MAIN_SQL_FILENAME = '/opt/airflow/db_utils/sql_tables.sql'


# SPARK

SPARK_APP_NAME = "VRNFSO_ETL"
SPARK_MASTER = "spark-master://spark:7077"

SPARK_CONFIG = {
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.jars": ",".join([
        "/opt/spark/spark_jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/spark_jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/spark_jars/postgresql-42.7.5.jar",
        "/opt/spark/spark_jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/spark_jars/checker-qual-3.48.3.jar"
        ])
    }


