def get_person_nearby_postcodes_lookup(spark):

    df_point_postcode = spark.read.parquet(
        "scrape_wikidata/processed_data/step_2_person_postcode_lookups/"
    )
    df_point_postcode.createOrReplaceTempView("df_point_postcode")

    sql = """
    select person, collect_list(nearby_postcodes) as nearby_postcodes
    from df_point_postcode
    group by person

    """
    df_point_postcode = spark.sql(sql)
    df_point_postcode.createOrReplaceTempView("df_point_postcode")
    return df_point_postcode
