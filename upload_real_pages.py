import sys
from datetime import datetime, timedelta
import time
import requests
from minio.error import S3Error
from io import BytesIO
from config import MINIO_BUCKET_RAW
from clients.minio_client import create_minio_client


client = create_minio_client()

if not client.bucket_exists(MINIO_BUCKET_RAW):
    client.make_bucket(MINIO_BUCKET_RAW)
    print(f"Создан бакет: {MINIO_BUCKET_RAW}")
else:
    print(f"Бакет {MINIO_BUCKET_RAW} уже существует")

def minio_put_object(client, bucket_name, object_name, url, html_content):
    try:
        client.put_object(
            bucket_name,
            object_name,
            data=BytesIO(html_content),
            length=len(html_content),
            content_type="text/html"
        )
        print(f"Страница {url} успешно загружена как '{object_name}'")
    except S3Error as err:
        print("Ошибка загрузки:", err)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python upload_real_pages.py <start_year> <end_year>")
        sys.exit(1)

    try:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
    except ValueError:
        print("Годы должны быть целыми числами")
        sys.exit(1)

    if start_year < end_year:
        year_range = range(start_year, end_year + 1)
    else:
        year_range = range(start_year, end_year - 1, -1)

    for year in year_range:
        start_date = datetime(year, 12, 5)
        end_date = datetime(year, 4, 1)
        current_date = start_date

        while current_date >= end_date:
            date_str = current_date.strftime("%Y%m%d")  # Формат YYYYMMDD
            print(f"Обработка даты: {date_str}")

            url = f"https://vrnfso.ru/download/{year}/{date_str}_rez.htm"

            try:
                response = requests.head(url)
            except requests.RequestException as e:
                print(f"Ошибка запроса HEAD к {url}: {e}")
                break

            if response.status_code == 200:
                print(f"Доступен: {url}")
                try:
                    page = requests.get(url)
                    html_content = page.content
                    minio_put_object(client, MINIO_BUCKET_RAW, date_str, url, html_content)
                except requests.RequestException as e:
                    print(f"Ошибка загрузки страницы {url}: {e}")
            else:
                print(f"Страница {url} недоступна (код {response.status_code})")

            time.sleep(5)
            current_date -= timedelta(days=1)
