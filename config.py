from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = "localhost:9000"
MINIO_BUCKET_RAW = "raw-html"
MINIO_BUCKET_PROCESSED = "processed-data"

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_URL = "jdbc:postgresql://127.0.0.1:5433/postgres"

SPARK_APP_NAME = "VRNFSO_ETL"
SPARK_MASTER = "local[*]"