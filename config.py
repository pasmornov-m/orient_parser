from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = "minio:9000"
MINIO_BUCKET_RAW = "raw-html"
MINIO_BUCKET_PROCESSED = "processed-data"
MINIO_BUCKET_TMP = "temp"
MINIO_TMP_PATH = f"s3a://{MINIO_BUCKET_TMP}/json_path"

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_URL = "jdbc:postgresql://db:5432/orient_data"

SPARK_APP_NAME = "VRNFSO_ETL"
SPARK_MASTER = "spark://spark:7077"
# SPARK_MASTER = "local[*]"