from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED, SPARK_CONFIG, ORIENT_DB_NAME, ORIENT_SCHEMA_NAME, MAIN_SQL_FILENAME
from etl.prepare_db_and_storage import prepare_db_and_storage

spark_conn = "spark_conn"
env_vars = {"PYTHONPATH": "/opt/airflow/"}

default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=11)
}

dag = DAG(
    dag_id = "spark_submit_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['spark', 'etl']
)

prepare_db_and_storage_task = PythonOperator(
    task_id='prepare_db_and_storage_task',
    python_callable=prepare_db_and_storage,
    op_args=[MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED, ORIENT_DB_NAME, ORIENT_SCHEMA_NAME, MAIN_SQL_FILENAME],
    dag=dag
)

processing_raw_data_task = SparkSubmitOperator(
    task_id='processing_raw_data_spark_task',
    application='/opt/airflow/etl/processing_raw_data.py',
    name='processing_raw_data_job',
    application_args=[MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED, ORIENT_DB_NAME, ORIENT_SCHEMA_NAME],
    conn_id=spark_conn,
    verbose=True,
    conf=SPARK_CONFIG,
    env_vars=env_vars,
    dag=dag
)

transform_raw_data_task = SparkSubmitOperator(
    task_id='transform_raw_data_spark_task',
    application='/opt/airflow/etl/transform_raw_data.py',
    name='transform_raw_data_job',
    application_args=[MINIO_BUCKET_PROCESSED],
    conn_id=spark_conn,
    verbose=True,
    conf=SPARK_CONFIG,
    env_vars=env_vars,
    dag=dag
)

write_pg_final_tables_task = SparkSubmitOperator(
    task_id='write_pg_final_tables_spark_task',
    application='/opt/airflow/etl/write_pg_final_tables.py',
    name='write_pg_final_tables_job',
    application_args=[ORIENT_DB_NAME, ORIENT_SCHEMA_NAME],
    conn_id=spark_conn,
    verbose=True,
    conf=SPARK_CONFIG,
    env_vars=env_vars,
    dag=dag
)

prepare_db_and_storage_task >> processing_raw_data_task >> transform_raw_data_task >> write_pg_final_tables_task