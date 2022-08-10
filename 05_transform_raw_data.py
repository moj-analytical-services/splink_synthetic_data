import duckdb
from transform_master_data.pipeline import SQLPipeline

path = "/Users/robinlinacre/Documents/data_linking/splink_synthetic_data/out_data/wikidata/processed/one_row_per_person/raw_scraped_one_row_per_person.parquet"
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

sql = """
select original_name as name_token
from  'out_data/wikidata/processed/alt_name_lookups/*.parquet'
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


import pandas as pd

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

df.head(200)


sql = f"""
select *
from  '{path}'
where family_nameLabel[1] = 'Blair'
limit 1
"""

con.execute(sql).df()

df.head(200).sample(10)


sql = """
select original_name as name_token
from  'out_data/wikidata/processed/alt_name_lookups/*.parquet'
where original_name = 'blair'
"""

con.execute(sql).df()
