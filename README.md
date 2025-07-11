# Проект обработки и загрузки данных с результатами соревнований

## Описание

Данный проект предназначен для автоматизированной генерации, обработки и загрузки HTML-страниц с результатами спортивных соревнований по спортивному ориентированию, а также последующего ETL-процесса с использованием Apache Spark и загрузки данных в PostgreSQL и MinIO.

---

## Структура проекта

```

.
├── clients
│   ├── minio_client.py                     # Клиент для взаимодействия с MinIO
│   ├── postgres_client.py                  # Клиент для взаимодействия с PostgreSQL
│   └── spark_client.py                     # Клиент для управления Spark сессиями
├── create_example_pages
│   ├── generate_results.sh                 # Bash-скрипт для генерации множества страниц
│   ├── generate_single_page.py             # Скрипт генерации одной HTML-страницы с результатами по дате
│   └── upload_results_to_minio.py          # Скрипт загрузки сгенерированных страниц в MinIO
├── dags
│   ├── dag_etl.py                          # DAG Apache Airflow для ETL процесса (нерабочее состояние)
│   └── dag_etl_spark_submit.py             # DAG для запуска Spark submit заданий
├── db_utils
│   ├── check_postges.py                    # Утилиты для проверки состояния PostgreSQL
│   ├── spark_schemas.py                    # Схемы данных для Spark
│   └── sql_schemas.sql                     # SQL-схемы для создания таблиц в базе
├── etl
│   ├── stages                              # Стадии ETL процесса (stage1, stage2, ...)
│   ├── reader.py                           # Модуль чтения данных
│   ├── transformer.py                      # Модуль преобразования данных
│   └── writer.py                           # Модуль записи данных в целевые хранилища
├── parsers
│   └── html_processor.py                   # Парсер и обработка HTML-страниц с результатами
├── utils
│   ├── pages_checker.py                    # Проверка и валидация страниц
│   ├── spark_helper.py                     # Вспомогательные функции для работы со Spark
│   └── text_cleaner.py                     # Очистка текстовых данных
├── config.py                               # Конфигурация проекта и константы
├── docker-compose.yml                      # Docker Compose для локального запуска инфраструктуры
├── download_spark_jars.sh                  # Скрипт для скачивания JAR файлов Spark
├── generate_and_upload_example_pages.sh    # Bash-скрипт генерации и загрузки страниц
├── upload_real_pages.py                    # Скрипт загрузки реальных страниц из внешних источников в MinIO
└── requirements.txt                        # Библиотеки, необходимые локально

````

---

## Установка и подготовка

Установите зависимости:

   ```bash
   pip install -r requirements.txt
   ```

Настройте переменные окружения для MinIO и других сервисов (можно использовать `.env`):

   ```
   MINIO_ACCESS_KEY=your_access_key
   MINIO_SECRET_KEY=your_secret_key
   MINIO_ENDPOINT_LOCAL=localhost:9000
   MINIO_BUCKET_RAW=raw-html
   POSTGRES_CONNECTION=your_postgres_connection_string
   ```

При необходимости скачайте нужные JAR файлы для Spark:

   ```bash
   ./download_spark_jars.sh
   ```

Запустите инфраструктуру через Docker Compose (MinIO, PostgreSQL, Spark и т.п.):

   ```bash
   docker compose up -d
   ```

---

## Генерация и загрузка тестовых HTML страниц

Для генерации страниц и их загрузки в MinIO используйте скрипт:

```bash
./generate_and_upload_example_pages.sh <num_pages>
```

* `<num_pages>` — количество страниц, которые нужно сгенерировать (последовательно по датам с 01.04.2024 по 01.12.2024 или другой диапазон, настроенный в скриптах).

---

## Загрузка реальных страниц

Для загрузки реальных страниц из внешнего источника (по URL) в MinIO запустите:

```bash
python3 upload_real_pages.py <start_year> <end_year>
```

* Аргументы задают диапазон годов для загрузки страниц.

---

## ETL процесс

Используйте Airflow DAG'и из папки `dags/` для автоматизации ETL процессов:

`dag_etl_spark_submit.py` — основной DAG для запуска задач с использованием Spark submit.

После установки и развёртывания всех необходимых зависимостей, загрузки реальных или example страниц в MinIO хранилище, необходимо перейти в веб-интерфейс Airflow:

[http://localhost:8080](http://localhost:8080)

Там вы увидите DAG, реализующий ETL-пайплайн. Его можно запустить вручную и отслеживать выполнение задач.

---

## Использование модулей

* `clients/` — работа с внешними системами: MinIO, PostgreSQL, Spark.
* `etl/` — логика чтения, обработки и записи данных.
* `parsers/html_processor.py` — парсинг HTML-страниц.
* `utils/` — вспомогательные инструменты и проверки.

---

## Важные моменты

* Генерация страниц происходит с формированием имени файлов вида `YYYYMMDD_rez.html`.
* Загрузка в MinIO использует ключ объекта вида `YYYYMMDD`, т.е. без суффикса `_rez.html`.
* Скрипты используют конфигурацию из `config.py` и переменные окружения из `.env` (необходимо указать свои ключи для MinIO).
* Все скрипты запускаются рамках автоматизированного Airflow пайплайна.
