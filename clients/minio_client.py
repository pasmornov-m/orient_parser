from minio import Minio
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def list_objects(client, bucket_name):
    return [obj.object_name for obj in client.list_objects(bucket_name)]

def get_object(client, bucket_name, object_name):
    return client.get_object(bucket_name, object_name)

def ensure_bucket_exists(client, raw_bucket, processed_bucket, temp_bucket):
    if not client.bucket_exists(raw_bucket):
        print(f"raw_bucket didn't exists")
        raise RuntimeError(f"Critical error: required bucket '{raw_bucket}' does not exist.")
    print(f"raw_bucket '{raw_bucket}' found.")
    if not client.bucket_exists(processed_bucket):
        print(f"processed_bucket '{processed_bucket}' not found, creating it...")
        client.make_bucket(processed_bucket)
    else:
        print(f"processed_bucket '{processed_bucket}' found.")

    if not client.bucket_exists(temp_bucket):
        print(f"temp_bucket '{temp_bucket}' not found, creating it...")
        client.make_bucket(temp_bucket)
    else:
        print(f"temp_bucket '{temp_bucket}' found.")