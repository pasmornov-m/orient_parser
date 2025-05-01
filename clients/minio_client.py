from minio import Minio
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def create_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def list_objects(client: Minio, bucket_name):
    return [obj.object_name for obj in client.list_objects(bucket_name)]

def get_object(client: Minio, bucket_name: str, object_name: str):
    return client.get_object(bucket_name, object_name)
