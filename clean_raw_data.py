# %%
%load_ext autoreload
%autoreload 2


# %%
import pandas as pd
import duckdb
from math import ceil
import pyarrow.parquet as pq
import numpy as np

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000
from scrape_wikidata.cleaning_fns import (

    dedupe_and_clean_results

)


# %%
df = pd.read_parquet("scrape_wikidata/raw_data/persons/")
df_clean = dedupe_and_clean_results(df)
print(len(df))
print(len(df_clean))
# %%

psize = 2000
n_partition = ceil(len(df_clean)/psize)
df_clean["partition_idx"] = np.random.choice(range(n_partition), size=df_clean.shape[0])

for i in range(n_partition):
    this_df = df_clean[df_clean['partition_idx']==i]
    this_df = this_df.drop('partition_idx', axis=1)
    this_df.to_parquet(f'scrape_wikidata/processed_data/step_1_one_line_per_person/page{i:03}_{i*psize}_to_{(i+1)*psize}.parquet')






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