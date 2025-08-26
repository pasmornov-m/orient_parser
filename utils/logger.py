import logging
import sys
import functools
import time
from utils.writers import write_to_postgres
from config import ORIENT_DB_NAME, ORIENT_SCHEMA_NAME, LOG_TABLE
from db_utils.spark_schemas import LOG_SCHEMA

def get_logger(name=__name__, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def log_to_table(db_name=ORIENT_DB_NAME, schema_name=ORIENT_SCHEMA_NAME, table_name=LOG_TABLE):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            spark = args[0]
            operation_name = func.__name__
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.time()
                duration = int((end_time - start_time).total_seconds())
                log_data = [{
                    'operation_name': operation_name,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration_sec': duration
                }]
                log_df = spark.createDataFrame(log_data, schema=LOG_SCHEMA)
                write_to_postgres(df=log_df, db_name=db_name, schema_name=schema_name, table=table_name)
        return wrapper
    return decorator