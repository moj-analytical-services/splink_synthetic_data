import duckdb
from transform_master_data.pipeline import SQLPipeline


def tokens_in_full_name_not_in_given_family_name():

    path = "out_data/wikidata/processed/one_row_per_person/raw_scraped_one_row_per_person.parquet"

    con = duckdb.connect()

    pipeline = SQLPipeline(con)

    sql = f"""
    select
        humanLabel,
        case when
            humanAltLabel[1] is null then []
            else str_split(humanAltLabel[1], ', ')
        end as humanAltLabel,

        given_nameLabel,
        family_nameLabel,
        humanDescription
    from '{path}'
    """

    pipeline.enqueue_sql(sql, "rel_human_alt_label_array_fixed")

    sql = """
    select list_concat(humanLabel,  humanAltLabel) as humanLabel
    from rel_human_alt_label_array_fixed
    """

    pipeline.enqueue_sql(sql, "rel_human_labels_as_array")

    sql = """
    select str_split(unnest(humanLabel), ' ') as hl
    from rel_human_labels_as_array
    """
    pipeline.enqueue_sql(sql, "unnested_human_labels")

    sql = """

    select lower(unnest(hl)) as name_token
    from unnested_human_labels
    """

    pipeline.enqueue_sql(sql, "all_tokens_in_human_label")

    sql = """
    select name_token, count(*) as token_count
    from all_tokens_in_human_label
    group by name_token
    """

    pipeline.enqueue_sql(sql, "all_tokens_in_human_label_counts")

    sql = f"""
    select list_concat(given_nameLabel, family_nameLabel) as names_list
    from  '{path}'
    """

    pipeline.enqueue_sql(sql, "raw_given_family_concat")

    sql = """
    select distinct lower(unnest(names_list)) as name_token
    from raw_given_family_concat
    """
    pipeline.enqueue_sql(sql, "all_tokens_in_given_family_names")

    sql = """
    select name_token, token_count
    from all_tokens_in_human_label_counts

    where name_token not in (select name_token from all_tokens_in_given_family_names)
    order by token_count desc
    """
    pipeline.enqueue_sql(sql, "tokens_in_human_label_not_in_given_family_names")
    df = pipeline.execute_pipeline().df()
    return df
