from datetime import datetime
import calendar
import time
import os
import pandas as pd

from scrape_wikidata.query_wikidata import (
    query_with_date,
    QUERY_HUMAN,
    QUERY_CHILDREN,
    QUERY_OCCUPATIONS,
)

from scrape_wikidata.cleaning_fns import replace_url





from pathlib import Path
out_folder = "out_data/wikidata/raw/persons/by_dob"
Path(out_folder).mkdir(parents=True, exist_ok=True)

import datetime
base = datetime.datetime(2020, 1, 1)
num_days = 1000
date_list = [base - datetime.timedelta(days=x) for x in range(num_days)]
date_list = [d.date().isoformat() for d in date_list]


def days_in_month(year, month):
    num_days = calendar.monthrange(year, month)[1]
    date_list = [datetime.date(year, month, day) for day in range(1, num_days+1)]
    date_list = [d.isoformat() for d in date_list]
    return date_list







for year in range(2020,2017,-1):
    for month in range(12,0,-1):
        date_list = days_in_month(year, month)
        filename = f"{out_folder}/dod_{year}_{month:02}.parquet"
        dfs = []
        start_time = time.time()
        if not os.path.exists(filename):
            for this_date in date_list:
                print(f"Scraping date {this_date}")
                df = query_with_date(QUERY_HUMAN, this_date)
                print(f"done {this_date} with record count {len(df)}")
                if len(df) > 0:
                    dfs.append(df)


            df = pd.concat(dfs)
            df.to_parquet(filename)

            end_time = time.time()

            print(f"Time taken: {end_time - start_time}")





# import pandas as pd
# pd.options.display.max_columns = 1000

# pd.read_parquet("/Users/robinlinacre/Documents/data_linking/splink_synthetic_data/out_data/wikidata/raw/persons/by_dob/dod_20200101.parquet")



