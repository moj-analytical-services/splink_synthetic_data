def get_person_nearby_postcodes_lookup(spark):

    df_point_postcode = spark.read.parquet(
        "scrape_wikidata/processed_data/step_2_person_postcode_lookups/"
    )
    # .filter("person = 'Q42'")
    df_point_postcode.createOrReplaceTempView("df_point_postcode")

    sql = """
    select person, point_type, collect_list(nearby_postcodes) as nearby_postcodes
    from df_point_postcode
    group by person, point_type
    """
    df_point_postcode = spark.sql(sql)
    df_point_postcode.createOrReplaceTempView("df_point_postcode")

    # Pivot

    sql = """
    select * from df_point_postcode

    PIVOT (
        first(nearby_postcodes)
        for point_type in ('birth_place' as birthplace_nearby_locs, 'residence' as residence_nearby_locs, 'random' as random_nearby_locs)
        )

    """
    df_point_postcode = spark.sql(sql)
    df_point_postcode.createOrReplaceTempView("df_point_postcode")

    return df_point_postcode
