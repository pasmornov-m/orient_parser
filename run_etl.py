from config import MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED
from etl.all_in_one_stages import stage1, stage2, stage3
from clients.postgres_client import get_postgres_properties
from clients.spark_client import create_spark_session

def main():
    spark = create_spark_session()
    postgres_props = get_postgres_properties()
    raw_paths = stage1(spark, postgres_props, MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED)
    processed_dfs = stage2(spark, raw_paths, MINIO_BUCKET_PROCESSED)
    stage3(spark, processed_dfs, postgres_props)
    spark.stop()

if __name__ == "__main__":
    main()