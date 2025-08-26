from clients.minio_client import create_minio_client, ensure_bucket_exists
from db_utils.check_postges import prepare_db
from utils.logger import log_to_table
from config import MINIO_BUCKET_TMP
import sys


def prepare_db_and_storage(raw_bucket, processed_bucket, db_name, schema_name, sql_filename):
    print(f"prepare_db_and_storage start\n")

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, raw_bucket, processed_bucket, MINIO_BUCKET_TMP)

    prepare_db(db_name, schema_name, sql_filename)

    print(f"prepare_db_and_storage done\n")

# if __name__ == "__main__":
#     raw_bucket = sys.argv[1]
#     processed_bucket = sys.argv[2]
#     db_name = sys.argv[3]
#     schema_name = sys.argv[4]
#     sql_filename = sys.argv[5]
#     prepare_db_and_storage(raw_bucket, processed_bucket, db_name, schema_name, sql_filename)