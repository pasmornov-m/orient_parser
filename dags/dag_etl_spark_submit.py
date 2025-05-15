from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT
from etl.stages.stage1 import stage1

spark_conn = "spark_conn"
env_vars = {"PYTHONPATH": "/opt/airflow/"}
spark_conf = {
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.jars": ",".join([
        "/opt/spark/spark_jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/spark_jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/spark_jars/postgresql-42.7.5.jar",
        "/opt/spark/spark_jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/spark_jars/checker-qual-3.48.3.jar"
        ])
    }

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

stage1_task = PythonOperator(
    task_id='stage1_task',
    python_callable=stage1,
    op_args=[MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED],
    dag=dag
)

stage2_task = SparkSubmitOperator(
    task_id='stage2_spark_task',
    application='/opt/airflow/etl/stages/stage2.py',
    name='stage2_job',
    application_args=[MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED],
    conn_id=spark_conn,
    verbose=True,
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag
)

stage3_task = SparkSubmitOperator(
    task_id='stage3_spark_task',
    application='/opt/airflow/etl/stages/stage3.py',
    name='stage3_job',
    application_args=[MINIO_BUCKET_PROCESSED],
    conn_id=spark_conn,
    verbose=True,
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag
)

stage4_task = SparkSubmitOperator(
    task_id='stage4_spark_task',
    application='/opt/airflow/etl/stages/stage4.py',
    name='stage4_job',
    conn_id=spark_conn,
    verbose=True,
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag
)

stage1_task >> stage2_task >> stage3_task >> stage4_task