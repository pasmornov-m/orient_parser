from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_URL
import psycopg2


def get_pg_props_psycopg2(db_name):
    return psycopg2.connect(
            host=POSTGRES_HOST,
            database=db_name,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )

def get_pg_props_spark(db_name):
    return {
        "url": f"{POSTGRES_URL}{db_name}",
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }