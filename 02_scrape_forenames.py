import time


from scrape_wikidata.query_wikidata import (
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

for page in range(26, 30, 1):
    pagesize = 5000
    df = get_standardised_name_table(t[0], t[1], page, pagesize)
    print(len(df))
    df = df.drop_duplicates()
    print(len(df))
    df.to_parquet(
        f"raw_data/names/stbtsa2_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
    )
    time.sleep(20)
