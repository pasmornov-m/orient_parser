from clients.postgres_client import get_pg_props_psycopg2
from utils.readers import read_sql_file
from utils.logger import get_logger


logger = get_logger(__name__)


def create_database(db_name):
    conn = get_pg_props_psycopg2(db_name='postgres')

    conn.autocommit = True
    cursor = conn.cursor()

    logger.info(f"Проверка существования базы данных '{db_name}'")
    cursor.execute("SELECT 1 FROM pg_database WHERE datname =  %s", (db_name,))
    exists = cursor.fetchone()

    if not exists:
        logger.info(f"База данных '{db_name}' не найдена. Создаю")
        cursor.execute(f"CREATE DATABASE {db_name}")
        logger.info(f"База данных '{db_name}' успешно создана.")
    else:
        logger.info(f"База данных '{db_name}' уже существует.")

    cursor.close()
    conn.autocommit = False
    conn.close()


def create_schema(db_name, schema_name):
    conn = get_pg_props_psycopg2(db_name)
    cursor = conn.cursor()

    logger.info(f"Создаю схему '{schema_name}' в базе '{db_name}' (если она отсутствует)")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    logger.info(f"Схема '{schema_name}' готова.")

    cursor.close()
    conn.commit()
    conn.close()


def create_table(db_name, sql_filename):
    conn = get_pg_props_psycopg2(db_name)
    cursor = conn.cursor()

    logger.info(f"Читаю SQL из файла '{sql_filename}'")
    sql_code = read_sql_file(sql_filename)
    logger.debug(f"SQL-код для создания таблиц:\n{sql_code}")

    logger.info("Выполняю SQL для создания таблиц")
    try:
        cursor.execute(sql_code)
        logger.info("Таблицы успешно созданы.")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {e}")
        raise

    cursor.close()
    conn.commit()
    conn.close()

def prepare_db(db_name, schema_name, sql_filename):
    logger.info(f"Проверка/создание БД '{db_name}', схемы '{schema_name}', и таблиц из '{sql_filename}'")
    create_database(db_name)
    create_schema(db_name, schema_name)
    create_table(db_name, sql_filename)

