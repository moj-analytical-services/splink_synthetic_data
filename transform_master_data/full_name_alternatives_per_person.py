def add_full_name_alternatives_per_person(
    pipeline,
    one_row_per_person_tablename="'out_data/wikidata/processed/one_row_per_person/raw_scraped_one_row_per_person.parquet'",
):

    # Ensure humanAltLabel is a (potentially empty) array
    sql = f"""
    select
        *,
        case when
            humanAltLabel[1] is null then []
            else str_split(humanAltLabel[1], ', ')
        end as humanAltLabel_fix,

    from {one_row_per_person_tablename}
    where humanAltLabel is not null
    """

    pipeline.enqueue_sql(sql, "rel_human_alt_label_array_fixed")

    # Create a list of all the human labels and human alt labels
    sql = """
    select
        * EXCLUDE (humanAltLabel_fix),
        list_concat(humanLabel,  humanAltLabel_fix) as full_name_arr
    from rel_human_alt_label_array_fixed
    """

    pipeline.enqueue_sql(sql, "rel_human_labels_as_array")

    # Remove the occasional entry which is actually the persons ID e.g. Q85910376
    sql = """
    select
        * EXCLUDE (full_name_arr),
        array_filter(full_name_arr, x -> (not regexp_matches(x, '^Q\d+$'))) as full_name_arr,
    from rel_human_labels_as_array
    """

    pipeline.enqueue_sql(sql, "rel_human_labels_as_array_filtered_qnumbers")

    # Use given name and family name to crate a name string e.g. given_name [John, James]
    # and family name [Smith]
    # become a single string "John James Smith"
    sql = """
    select
        *,
        replace(list_string_agg(list_concat(given_nameLabel, family_nameLabel)), ',', ' ')
            as family_give_name_concat,

    from rel_human_labels_as_array_filtered_qnumbers
    """

    pipeline.enqueue_sql(sql, "concatenated_given_family_names")

    # Add this to our list of full names
    sql = """
    select
        * EXCLUDE (full_name_arr, family_give_name_concat),
        list_concat(full_name_arr,  list_value(family_give_name_concat)) as full_name_arr,
    from concatenated_given_family_names
    """

    pipeline.enqueue_sql(sql, "all_names_in_list")

    # Deduplicate the list of full names
    sql = """
    select
        * EXCLUDE (full_name_arr),
        list_distinct(full_name_arr) as full_name_arr,
    from all_names_in_list
    """

    pipeline.enqueue_sql(sql, "distinct_human_label_list")

    return pipeline
