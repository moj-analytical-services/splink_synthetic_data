# %%
import time
import os

from scrape_wikidata.query_wikidata import (
    query_with_offset,
    QUERY_HUMAN,
    QUERY_CHILDREN,
    QUERY_OCCUPATIONS,
)

from scrape_wikidata.cleaning_fns import replace_url

# %%

for doublecheck in range(10):
    for page in range(0,400):
        pagesize = 5000
        filename = f"out_data/wikidata/raw/persons/page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"

        if not os.path.exists(filename):
            print(f"Scraping page {page}")
            try:
                df = query_with_offset(QUERY_HUMAN, page, pagesize)
                df.to_parquet(filename)
            except:
                print(f"Error on page {page}")
            time.sleep(121)



# for page in range(0, 200, 1):
#     pagesize = 20000
#     df = query_with_offset(QUERY_OCCUPATIONS, page, pagesize)
#     df = df.applymap(replace_url)
#     df.to_parquet(
#         f"scrape_wikidata/raw_data/occupations/xpage_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
#     )
#     time.sleep(10)
#     if len(df) < 5:
#         break

# import pandas as pd
# pd.read_parquet("out_data/wikidata/persons/")
# %%
