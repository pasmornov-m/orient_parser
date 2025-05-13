from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED
from etl.stages import stage1, stage2, stage3, stage4
from clients.postgres_client import get_postgres_properties
from clients.spark_client import create_spark_session


NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    # "retry_delay": timedelta(minutes=10)
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
    stage1(MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)

def run_stage2(**context):
    spark, postgres_props = init_context()
    raw_paths = stage2(spark, postgres_props, MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)
    spark.stop()
    context['ti'].xcom_push(key='raw_paths', value=raw_paths)

def run_stage3(**context):
    spark = create_spark_session()
    raw_paths = context['ti'].xcom_pull(task_ids='stage2_task', key='raw_paths')
    transformed_paths = stage3(spark, raw_paths, MINIO_BUCKET_PROCESSED)
    spark.stop()
    context['ti'].xcom_push(key='transformed_paths', value=transformed_paths)

def run_stage4(**context):
    spark, postgres_props = init_context()
    transformed_paths = context['ti'].xcom_pull(task_ids='stage3_task', key='transformed_paths')
    stage4(spark, transformed_paths, postgres_props)
    spark.stop()

# def test_s3a_connection(**context):
#     spark = create_spark_session()
#     print("create_spark_session")
#     print(f"s3a://{MINIO_BUCKET_PROCESSED}/transformed_events/")
#     df = spark.read.parquet(f"s3a://{MINIO_BUCKET_PROCESSED}/transformed_events/")
#     df.show()
#     spark.stop()

def test_s3a_connection(**context):
    spark = create_spark_session()
    print("Spark URI:", spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).getUri())

    print(">>> Листинг папки s3a://processed-data/")
    path = spark._jvm.org.apache.hadoop.fs.Path("s3a://processed-data/")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    files = fs.listStatus(path)
    for f in files:
        print(f.getPath())

    print(">>> Листинг s3a://processed-data/events")
    events_path = spark._jvm.org.apache.hadoop.fs.Path("s3a://processed-data/events")
    event_files = fs.listStatus(events_path)
    for f in event_files:
        print(f.getPath(), f.isFile(), f.getLen())

    print(">>> Начинаем чтение Parquet...")
    try:
        print(spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).getUri())
        print("Попытка чтения:", "s3a://processed-data/events")

        df = spark.read.parquet("s3a://processed-data/events")
        df.show()
    except Exception as e:
        import traceback
        print("Ошибка при чтении Parquet:")
        traceback.print_exc()
    spark.stop()


stage_test_task = PythonOperator(
    task_id="test_task",
    python_callable=test_s3a_connection,
    dag=dag
)

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

stage_test_task >> stage1_task >> stage2_task >> stage3_task >> stage4_task