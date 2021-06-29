# %%
%load_ext autoreload
%autoreload 2


# %%
import pandas as pd
import duckdb

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000
from scrape_wikidata.query_wikidata import (

    dedupe_and_clean_results

)

# %%
df = pd.read_parquet("scrape_wikidata/raw_data/names/")

# %%

print(len(df))
print(len(df.drop_duplicates()))

sql = """
select * from df
where original_name = 'Theodore'
"""
duckdb.query(sql).to_df()

# %%
df = pd.read_parquet("scrape_wikidata/raw_data/persons/page_0_0_to_4999.parquet")



df_clean = dedupe_and_clean_results(df)
print(len(df))
print(len(df_clean))
df_clean.sample(10)
df_clean.sample(100).to_parquet("scrape_wikidata/processed_data/step_1/sample.parquet")
# %%
# pd.options.display.max_columns = 1000
# pd.options.display.max_rows = 1000

# display(df_clean.sample(20))

# # %%
# pd.options.display.max_columns = 1000
# pd.options.display.max_rows = 1000

# df = pd.read_parquet("raw_data/page_0_0_to_2499.parquet")
# df_clean = dedupe_and_clean_results(df)
# df_clean.sample(5)
# # %%


# df_readable = get_readable_columns(df_clean)
# df_readable.sample(20)
# # %%


# df = query_with_offset(QUERY_SHORT_NAME, 0, 2000)
# df


# # %%

# # # # # NEXT STEPS:

# # Get lists of alternative names based on https://www.wikidata.org/wiki/Q4927524).
# # Figure out how to get postcodes in bulk from from https://postcodes.io/.  Can get full address from valuation data https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads


# #

# %%
