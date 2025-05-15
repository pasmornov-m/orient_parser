from clients.minio_client import create_minio_client, ensure_bucket_exists
from db_utils.check_postges import create_database, create_tables
from config import MINIO_BUCKET_TMP
import sys


def stage1(raw_bucket, processed_bucket):
    print(f"Stage1 start\n")

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, raw_bucket, processed_bucket, MINIO_BUCKET_TMP)

    create_database()
    create_tables()

    print(f"Stage1 done\n")

if __name__ == "__main__":
    raw_bucket = sys.argv[1]
    processed_bucket = sys.argv[2]
    stage1(raw_bucket, processed_bucket)