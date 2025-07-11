import os
import sys
from minio.error import S3Error
from io import BytesIO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import MINIO_BUCKET_RAW, MINIO_ENDPOINT_LOCAL, EXAMPLE_RESULTS_PAGES
from clients.minio_client import create_minio_client


client = create_minio_client(endpoint=MINIO_ENDPOINT_LOCAL)

if not client.bucket_exists(MINIO_BUCKET_RAW):
    client.make_bucket(MINIO_BUCKET_RAW)
    print(f"Создан бакет: {MINIO_BUCKET_RAW}")
else:
    print(f"Бакет {MINIO_BUCKET_RAW} уже существует")


for filename in os.listdir(EXAMPLE_RESULTS_PAGES):
    if filename.endswith(".html"):
        file_path = os.path.join(EXAMPLE_RESULTS_PAGES, filename)
        with open(file_path, "rb") as f:
            data = f.read()
            object_name = filename.replace("_rez.html", "")
            try:
                client.put_object(
                    bucket_name=MINIO_BUCKET_RAW,
                    object_name=object_name,
                    data=BytesIO(data),
                    length=len(data),
                    content_type="text/html"
                )
                print(f"Загружен: {filename} как {object_name}")
            except S3Error as e:
                print(f"Ошибка при загрузке {filename}:", e)