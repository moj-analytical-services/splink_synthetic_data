# %%
import time

from scrape_wikidata.query_wikidata import (
    query_with_offset,
    QUERY_HUMAN,
    QUERY_CHILDREN,
    QUERY_OCCUPATIONS,
)

from scrape_wikidata.cleaning_fns import replace_url

# %%

# for page in range(0, 200, 1):
#     pagesize = 5000
#     df = query_with_offset(QUERY_HUMAN, page, pagesize)
#     df.to_parquet(
#         f"scrape_wikidata/raw_data/persons/page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
#     )
#     time.sleep(45)
# # %%

# for page in range(46, 100, 1):
#     pagesize = 5000
#     df = query_with_offset(QUERY_CHILDREN, page, pagesize)
#     df = df.applymap(replace_url)
#     df.to_parquet(
#         f"scrape_wikidata/raw_data/family/parent_child/page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
#     )
#     time.sleep(10)


for page in range(0, 200, 1):
    pagesize = 20000
    df = query_with_offset(QUERY_OCCUPATIONS, page, pagesize)
    df = df.applymap(replace_url)
    df.to_parquet(
        f"scrape_wikidata/raw_data/occupations/xpage_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
    )
    time.sleep(10)
    if len(df) < 5:
        break
