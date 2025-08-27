from clients.minio_client import create_minio_client, ensure_bucket_exists
from db_utils.check_postges import prepare_db
from utils.logger import get_logger
from config import MINIO_BUCKET_TMP


logger = get_logger(__name__)


def prepare_db_and_storage(raw_bucket, processed_bucket, db_name, schema_name, sql_filename):
    logger.info(f"prepare_db_and_storage start\n")

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, raw_bucket, processed_bucket, MINIO_BUCKET_TMP)

    prepare_db(db_name, schema_name, sql_filename)

    logger.info(f"prepare_db_and_storage done\n")

