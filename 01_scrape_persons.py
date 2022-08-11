import datetime
import calendar
import time
import os
import pandas as pd

from scrape_wikidata.query_wikidata import query_with_date, QUERY_HUMAN

from pathlib import Path

out_folder = "out_data/wikidata/raw/persons/by_dob"
Path(out_folder).mkdir(parents=True, exist_ok=True)


def days_in_month(year, month):
    num_days = calendar.monthrange(year, month)[1]
    date_list = [datetime.date(year, month, day) for day in range(1, num_days + 1)]
    date_list = [d.isoformat() for d in date_list]
    return date_list


for year in range(2000, 1800, -1):
    for month in range(12, 0, -1):
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
