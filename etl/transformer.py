from parsers.html_processor import VRNFSO_html_processor_spark
from utils.spark_helper import union_all

def transform_html_to_tables(rdd, spark):
    events_dfs, dist_dfs, res_dfs = [], [], []

    for url_date, html in rdd.collect():
        parser = VRNFSO_html_processor_spark(url_date, html, spark)

        df_ev = parser.parse_events()
        df_ds = parser.parse_distances()
        df_rs = parser.parse_results()

        if df_ev is not None:
            events_dfs.append(df_ev)
        if df_ds is not None:
            dist_dfs.append(df_ds)
        if df_rs is not None:
            res_dfs.append(df_rs)

    # объединяем списки DataFrame
    all_events    = union_all(*events_dfs)  if events_dfs else spark.createDataFrame([], parser.parse_events().schema)
    all_distances = union_all(*dist_dfs)   if dist_dfs  else spark.createDataFrame([], parser.parse_distances().schema)
    all_results   = union_all(*res_dfs)    if res_dfs   else spark.createDataFrame([], parser.parse_results().schema)

    return all_events, all_distances, all_results
