import datetime
import calendar
import time
import os
import pandas as pd

from scrape_wikidata.query_wikidata import query_with_date, QUERY_HUMAN
from path_fns.filepaths import (
    PERSONS_BY_DOD_RAW_OUT_PATH,
    persons_by_dob_raw_filename_year_month,
    persons_by_dob_raw_filename_full_year,
)

from pathlib import Path


Path(PERSONS_BY_DOD_RAW_OUT_PATH).mkdir(parents=True, exist_ok=True)


def days_in_month(year, month):
    num_days = calendar.monthrange(year, month)[1]
    date_list = [datetime.date(year, month, day) for day in range(1, num_days + 1)]
    date_list = [d.isoformat() for d in date_list]
    return date_list


# Lots of records - output in groups of 1 month
for year in range(2000, 1700, -1):
    for month in range(12, 0, -1):
        date_list = days_in_month(year, month)
        filename = persons_by_dob_raw_filename_year_month(year, month)
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

# Few records - output in groups of 1 year
for year in range(1700, -2000, -1):
    print(f"Starting year {year}")

    filename = persons_by_dob_raw_filename_full_year(year)
    if not os.path.exists(filename):
        dfs = []
        start_time = time.time()
        for month in range(12, 0, -1):
            date_list = days_in_month(year, month)

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
