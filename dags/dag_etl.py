from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED
from etl.stages import stage1, stage2, stage3, stage4
from clients.postgres_client import get_postgres_properties
from clients.spark_client import create_spark_session


default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=11)
}

dag = DAG(
    dag_id = "full_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['spark', 'etl']
)


def init_context():
    spark = create_spark_session()
    postgres_props = get_postgres_properties()
    return spark, postgres_props

def run_stage1(**context):
    stage1(MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)

def run_stage2(**context):
    spark, postgres_props = init_context()
    print("Spark context stopped?", spark._jsc.sc().isStopped())
    raw_paths = stage2(spark, postgres_props, MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)
    context['ti'].xcom_push(key='raw_paths', value=raw_paths)
    spark.stop()

def run_stage3(**context):
    spark = create_spark_session()
    raw_paths = context['ti'].xcom_pull(task_ids='stage2_task', key='raw_paths')
    transformed_paths = stage3(spark, raw_paths, MINIO_BUCKET_PROCESSED)
    context['ti'].xcom_push(key='transformed_paths', value=transformed_paths)
    spark.stop()

def run_stage4(**context):
    spark, postgres_props = init_context()
    transformed_paths = context['ti'].xcom_pull(task_ids='stage3_task', key='transformed_paths')
    stage4(spark, transformed_paths, postgres_props)
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

stage4_task = PythonOperator(
    task_id='stage4_task',
    python_callable=run_stage4,
    dag=dag
)

stage1_task >> stage2_task >> stage3_task >> stage4_task