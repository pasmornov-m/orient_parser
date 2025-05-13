from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED
from etl.stages import stage1, stage2, stage3
from clients.postgres_client import get_postgres_properties
from clients.spark_client import create_spark_session

default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=10)
}

dag = DAG(
    dag_id = "full_etl_pipeline",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=['spark', 'etl']
)

def init_context():
    spark = create_spark_session()
    postgres_props = get_postgres_properties()
    return spark, postgres_props

def run_stage1(**context):
    spark, postgres_props = init_context()
    paths = stage1(spark, postgres_props, MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)
    spark.stop()
    context['ti'].xcom_push(key='paths', value=paths)

def run_stage2(**context):
    spark = create_spark_session()
    paths = context['ti'].xcom_pull(task_ids='stage1_task', key='paths')
    dfs = stage2(spark, paths, MINIO_BUCKET_PROCESSED)
    spark.stop()
    context['ti'].xcom_push(key='dfs', value=dfs)

def run_stage3(**context):
    spark, postgres_props = init_context()
    dfs = context['ti'].xcom_pull(task_id='stage2_task', key='dfs')
    stage3(spark, dfs, postgres_props)
    spark.stop()

stage1_task = PythonOperator(
    task_id='stage1_task',
    python_callable=run_stage1,
    dag=dag
)

stage2_task = PythonOperator(
    task_id='stage2_task',
    python_callable=run_stage2,
    dag=dag
)

stage3_task = PythonOperator(
    task_id='stage3_task',
    python_callable=run_stage3,
    dag=dag
)

stage1_task >> stage2_task >> stage3_task