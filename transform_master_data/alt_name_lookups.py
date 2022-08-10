from .pipeline import SQLPipeline


def get_name_weighted_lookup(
    con, raw_name_col, tablename_alt_names, tablename_scraped_one_row_per_person
):
    """
    Get a table that is a lookup between original names and weighted alternatives:
    | original_name   | alt_name_arr                      | alt_name_weight_arr      |
    |:----------------|:----------------------------------|:-------------------------|
    | jody            | ['joseph', 'joe', 'judith', 'jo'] | [0.43, 0.23, 0.16, 0.16] |

    """

    # The table 'name_frequency_counts' contains a count of
    # name frequencies from the scraped data

    pipeline = SQLPipeline(con)

    sql = f"""
    select unnest({raw_name_col}) as name
    from {tablename_scraped_one_row_per_person}
    """
    pipeline.enqueue_sql(sql, "all_names")

    sql = """
    select lower(name) as name, count(*) as count
    from all_names
    group by name
    order by count desc
    """

    pipeline.enqueue_sql(sql, "name_frequency_counts")

    # Concatenate all of our name variants of different types
    sql = f"""
    select
        lower(original_name) as original_name, lower(alt_name) as alt_name, name_variant_type
    from {tablename_alt_names}

    union all

    select
        lower(alt_name) as original_name, lower(original_name) as alt_name, name_variant_type
    from {tablename_alt_names}
    """

    pipeline.enqueue_sql(sql, "name_variants_concat")

    sql = """
    select distinct original_name, alt_name, name_variant_type
        from name_variants_concat
    """

    pipeline.enqueue_sql(sql, "distinct_names_concat")

    # Weight the name variants, using the frequency of the name variant as the weight
    # but adding arbitrary greater emphasis
    # to alt names that appear in nicknames, diminutive and hypocorism
    sql = """
      select n.*,

        case
            when name_variant_type = 'nickname' then count+5000
            when name_variant_type = 'diminutive' then count+2000
            when name_variant_type = 'hypocorism' then count+2000
            else  count
            end as weighted_count

        from distinct_names_concat as n
        inner join
        name_frequency_counts as c
        on n.alt_name = c.name
        where
        count >= 10
        and original_name != alt_name
    """

    pipeline.enqueue_sql(sql, "names_with_weights_as_counts")

    # Group by original_name and alt_name, summing the counts
    sql = """
    select
        original_name,
        alt_name,
        sum(weighted_count) as weight
    from names_with_weights_as_counts
    group by original_name, alt_name
    order by original_name, weight desc
    """
    pipeline.enqueue_sql(sql, "names_with_weights_as_floats")

    # Get proportions within groups by counts
    sql = """
    select
        original_name,
        alt_name,
        cast(weight as double)/(sum(weight) over (partition by original_name)) as weight
    from names_with_weights_as_floats
    """

    pipeline.enqueue_sql(sql, "weighted_proportions")

    # One row per original name, with alt names and weights as lists

    sql = """
    select
        original_name,
        list(alt_name) as alt_name_arr,
        list(weight)  as alt_name_weight_arr
    from weighted_proportions
    group by original_name
    """

    pipeline.enqueue_sql(sql, "final")

    # Run this to see intermediate outputs
    # return pipeline.execute_pipeline_in_parts()
    return pipeline.execute_pipeline()
