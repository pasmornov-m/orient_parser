from config import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD

def get_postgres_properties():
    return {
        "url": POSTGRES_URL,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
