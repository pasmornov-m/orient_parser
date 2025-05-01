from pyspark.sql import DataFrame


def union_all(*dfs: DataFrame) -> DataFrame:
    return dfs[0].unionByName(*dfs[1:])

def repartition_df(df: DataFrame, num_partitions: int) -> DataFrame:
    return df.repartition(num_partitions)

def cache_df(df: DataFrame) -> DataFrame:
    return df.cache()
