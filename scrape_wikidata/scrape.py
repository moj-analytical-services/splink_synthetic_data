# %%
%load_ext autoreload
%autoreload 2


# %%
import time


from query_wikidata import (
    query_with_offset,
    dedupe_and_clean_results,
    get_readable_columns,
    QUERY_HUMAN,
    get_standardised_name_table,
)
import pandas as pd

# %%

# ("transliteration", "P2440"),("short_name", "P1813"),
name_tuples = [
    ("said_to_be_the_same_as", "P460"),
    ("nickname", "P1449"),
]

t = name_tuples[0]

for page in range(3, 10, 1):
    pagesize = 5000
    df = get_standardised_name_table(t[0], t[1],page, pagesize)
    print(len(df))
    df = df.drop_duplicates()
    print(len(df))
    df.to_parquet(
        f"raw_data/names/stbtsa_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
    )
    time.sleep(30)

# %%
for page in range(0, 3, 1):
    pagesize = 5000
    df = query_with_offset(QUERY_HUMAN, page, pagesize)
    time.sleep(30)
    df.to_parquet(
        f"raw_data/persons/page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
    )

# %%
df = pd.read_parquet("raw_data/")

df_clean = dedupe_and_clean_results(df)
print(len(df))
print(len(df_clean))
# %%
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

display(df_clean.sample(20))

# %%
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

df = pd.read_parquet("raw_data/page_0_0_to_2499.parquet")
df_clean = dedupe_and_clean_results(df)
df_clean.sample(5)
# %%


df_readable = get_readable_columns(df_clean)
df_readable.sample(20)
# %%


df = query_with_offset(QUERY_SHORT_NAME, 0, 2000)
df


# %%

# # # # NEXT STEPS:

# Get lists of alternative names based on https://www.wikidata.org/wiki/Q4927524).
# Figure out how to get postcodes in bulk from from https://postcodes.io/.  Can get full address from valuation data https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads


#