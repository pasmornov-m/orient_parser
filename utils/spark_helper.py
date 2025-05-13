from pyspark.sql import DataFrame
from functools import reduce


def union_all(spark, dfs, schema):
    if not dfs:
        return spark.createDataFrame([], schema)
    return reduce(DataFrame.unionByName, dfs)

def repartition_df(df: DataFrame, num_partitions: int) -> DataFrame:
    return df.repartition(num_partitions)

def cache_df(df: DataFrame) -> DataFrame:
    return df.cache()
