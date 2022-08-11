from pyspark.sql import Row


def read_df_names(spark):
    df_names = spark.read.parquet("scrape_wikidata/raw_data/names/")
    df_names.createOrReplaceTempView("df_names")
    return df_names


def get_df_given_names_with_freqs(spark):
    df_names = spark.read.parquet(
        "scrape_wikidata/processed_data/step_3_alt_name_lookups/given_name.parquet"
    )
    df_names.createOrReplaceTempView("dfn")
    sql = """
    select original_name, collect_list(struct(alt_name, weight)) as alt_names
    from dfn
    group by original_name
    """
    df_names = spark.sql(sql)
    return df_names


def get_df_family_names_with_freqs(spark):
    df_names = spark.read.parquet(
        "scrape_wikidata/processed_data/step_3_alt_name_lookups/family_name.parquet"
    )
    df_names.createOrReplaceTempView("dfn")
    sql = """
    select original_name, collect_list(struct(alt_name, weight)) as alt_names
    from dfn
    group by original_name
    """
    df_names = spark.sql(sql)
    return df_names


def get_df_filter(spark):

    data_list = [
        {
            "to_filter_out": [
                "rt.",
                "hon.",
                "sir",
                "rev.",
                "lady",
                "duke",
                "of",
                "dr.",
                "dr",
                "baron",
                "the",
                "and",
                "last",
                "baronet",
                "barron",
            ]
        },
    ]

    df_filter = spark.createDataFrame(Row(**x) for x in data_list)
    df_filter.createOrReplaceTempView("df_filter")
    return df_filter


# Functions that split the humanLabel, humanAltLabel, Given, family names into array
def split_field(col_name, out_name, num_cols=3):
    parts = [f"{col_name}[{i-1}] as {out_name}_{i}" for i in range(1, num_cols + 1)]
    return ", ".join(parts)


def name_split(df_person, spark):
    df_filter = get_df_filter(spark)
    df_filter.createOrReplaceTempView("df_filter")
    df_person.createOrReplaceTempView("df_person")

    # Given names

    split_given_name = "ifnull(split(given_nameLabel,' \\\\| '), array())"

    hl_rr = "regexp_replace(humanLabel, ',(.+)', '')"  # Remove titles in form John Smith, 3rd Baron of Fife
    label_parts_expr = f"split({hl_rr}, ' \\\\| ')"
    split_label = f"ifnull(flatten(transform({label_parts_expr}, x -> slice(split(x, ' '), 1, size(split(x, ' ')) - 1))), array())"

    rr_x = "regexp_replace(x, ',(.+)', '')"
    alt_label_parts_expr = "split(humanAltlabel, ', ')"
    split_alt_label = f"ifnull(flatten(transform({alt_label_parts_expr}, x -> slice(split({rr_x}, ' '), 1, size(split({rr_x}, ' ')) - 1))), array())"

    union_all_names = (
        f"array_union({split_given_name}, array_union({split_label},{split_alt_label}))"
    )
    filter_union_all_names = f"array_except({union_all_names}, to_filter_out)"

    filter_union_all_given_names = (
        f"filter({filter_union_all_names}, x -> x not rlike '[0-9]')"
    )

    # Family names
    split_family_name = "ifnull(split(family_nameLabel,' \\\\| '), array())"

    hl_rr = "regexp_replace(humanLabel, ',(.+)', '')"
    label_parts_expr = f"split({hl_rr}, ' \\\\| ')"
    split_label = f"ifnull(flatten(transform({label_parts_expr}, x -> slice(split(x, ' '), -1,1))), array())"

    rr_x = "regexp_replace(x, ',(.+)', '')"
    alt_label_parts_expr = "split(humanAltlabel, ', ')"
    split_alt_label = f"ifnull(flatten(transform({alt_label_parts_expr}, x -> slice(split({rr_x}, ' '), -1, 1))), array())"

    union_all_names = f"array_union({split_family_name}, array_union({split_label},{split_alt_label}))"
    filter_union_all_family_names = f"array_except({union_all_names}, to_filter_out)"

    sql = f"""
    select *,

    {filter_union_all_given_names} as given_names_array,
    {filter_union_all_family_names} as family_names_array

    from df_person
    cross join df_filter

    """
    df_person = spark.sql(sql)

    df_person.createOrReplaceTempView("df_person")

    sql = f"""
    select
    *,
    {split_field('given_names_array','given_name', 4)},
    {split_field('family_names_array','family_name',2)}
    from df_person
    """
    df_person = spark.sql(sql)
    df_person.createOrReplaceTempView("df_person")

    return df_person
