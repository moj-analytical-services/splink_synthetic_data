# %%
import pandas as pd

import os
import glob

from scrape_wikidata.cleaning_fns import postcode_lookup_from_cleaned_person_data

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10

files = sorted(
    glob.glob("scrape_wikidata/processed_data/step_1_one_line_per_person/*.parquet")
)


for i, f in enumerate(files):

    base = os.path.basename(f)
    out_path = f"scrape_wikidata/processed_data/step_2_person_postcode_lookups/{base}"

    if not os.path.exists(out_path):

        df = pd.read_parquet(f)
        pcs = postcode_lookup_from_cleaned_person_data(df)

        pcs.to_parquet(
            out_path,
            index=False,
        )


# %%
## When running docker compose on postcodes io, need to make sure 5432 is open:
#     ports:
#       - 5432:5432
# import psycopg2
# import pandas as pd
# conn = psycopg2.connect("dbname='postcodesiodb' user='postcodesio' host='localhost' password='secret'")
# pd.read_sql("SELECT * FROM pg_catalog.pg_tables;", conn).head(2)
# df = pd.read_sql("select postcode, latitude, longitude from postcodes where latitude is not null", conn)
# df_r = df.sample(n=1000000, replace=True)
# df_r = df_r.reset_index().reset_index().drop("index",axis=1).rename(columns={"level_0":"row_num"})
# df_r.to_parquet(
#     "scrape_wikidata/raw_data/postcodes/random_postcodes.parquet", index=False
# )
