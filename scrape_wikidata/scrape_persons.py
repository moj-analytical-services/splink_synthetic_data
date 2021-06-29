import time

from query_wikidata import (
    query_with_offset,
    QUERY_HUMAN,
)


for page in range(111, 200, 1):
    pagesize = 5000
    df = query_with_offset(QUERY_HUMAN, page, pagesize)
    df.to_parquet(
        f"raw_data/persons/page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet"
    )
    time.sleep(45)
