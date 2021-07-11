import time


from scrape_wikidata.query_wikidata import (
    get_standardised_given_name_table,
    get_standardised_family_name_table,
)
import pandas as pd

# %%

# ("transliteration", "P2440"),("short_name", "P1813"),
name_tuples = [
    ("said_to_be_the_same_as", "P460"),
    ("nickname", "P1449"),
]

# t = name_tuples[0]

# for page in range(0, 100, 1):
#     pagesize = 5000
#     df = get_standardised_given_name_table(t[0], t[1], page, pagesize)
#     print(len(df))
#     df = df.drop_duplicates()
#     print(len(df))
#     df.to_parquet(
#         f"scrape_wikidata/raw_data/names/name_type=given/stbtsa_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
#         index=False,
#     )
#     time.sleep(20)

t = name_tuples[1]

# for page in range(0, 100, 1):
#     pagesize = 5000
#     df = get_standardised_given_name_table(t[0], t[1], page, pagesize)
#     print(len(df))
#     df = df.drop_duplicates()
#     print(len(df))
#     df.to_parquet(
#         f"scrape_wikidata/raw_data/names/name_type=given/nn_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
#         index=False,
#     )
#     time.sleep(20)


t = name_tuples[1]

for page in range(0, 100, 1):
    pagesize = 5000
    df = get_standardised_family_name_table("said_to_be_the_same_as", page, pagesize)
    print(len(df))
    df = df.drop_duplicates()
    print(len(df))
    df.to_parquet(
        f"scrape_wikidata/raw_data/names/name_type=family/fn_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
        index=False,
    )
