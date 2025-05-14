from etl.reader import read_from_postgres

def check_processed_pages(spark, date_html_pairs, postgres_props):
    
    input_pages_df = spark.createDataFrame([(url_date,) for url_date, _ in date_html_pairs],["page_name"])
    processed_pages_df = read_from_postgres(spark, "pages_processing_log", postgres_props)
    
    new_pages_df = input_pages_df.join(
        processed_pages_df,
        on="page_name",
        how="left_anti"
    )
    
    new_page_names = [row.page_name for row in new_pages_df.collect()]
    new_page_names_set = set(new_page_names)
    
    result_pairs = [(url_date, html) for url_date, html in date_html_pairs if url_date in new_page_names_set]
    
    return result_pairs